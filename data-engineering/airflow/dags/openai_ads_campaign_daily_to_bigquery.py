"""Read-only ChatGPT Ads reporting; deliberately never calls Conversions API.

Requires an Advertiser API key, not a pixel/Conversions API key.
Contract: https://developers.openai.com/ads/api-reference/insights
"""
from __future__ import annotations

import json
import math
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
try:
    from airflow.operators.python import get_current_context
except ImportError:
    from airflow.decorators import get_current_context
from google.cloud import bigquery

from campaign_daily_common import (
    replace_bigquery_window,
    request_with_retry,
    resolve_daily_window,
    validate_and_deduplicate_rows,
)


API_BASE = "https://api.ads.openai.com/v1"
FIELDS = (
    "metadata.readable_time", "campaign.id", "campaign.name", "campaign.status",
    "campaign.impressions", "campaign.clicks", "campaign.spend",
)


def get_json(path: str, token: str, params=None) -> dict[str, Any]:
    # A fixed host prevents accidentally forwarding credentials to a supplied URL.
    payload = request_with_retry(
        "GET", f"{API_BASE}/{path}",
        headers={"Authorization": f"Bearer {token}"},
        params=params, timeout=120, allow_redirects=False,
    )
    if payload.status_code != 200:
        raise ValueError("OpenAI Ads did not return HTTP 200")
    result = payload.json()
    if not isinstance(result, dict) or "error" in result:
        raise ValueError("Unexpected OpenAI Ads response; BigQuery was not modified")
    return result


def metric(value: Any, integer: bool = False, *, allow_negative: bool = False) -> float | int:
    if value is None or isinstance(value, bool):
        raise ValueError("Missing or invalid OpenAI Ads metric")
    number = float(value)
    if (not math.isfinite(number) or (number < 0 and not allow_negative)
            or (integer and not number.is_integer())):
        raise ValueError("Invalid OpenAI Ads metric")
    return int(number) if integer else number


def map_row(row: dict[str, Any], account_id: str) -> dict[str, Any]:
    for field in ("readable_time", "campaign_id", "campaign_name", "campaign_status"):
        if not isinstance(row.get(field), str) or not row[field]:
            raise ValueError(f"OpenAI Ads report is missing {field}")
    return {
        "data_date": date.fromisoformat(row["readable_time"]).isoformat(),
        "customer_id": account_id,
        "platform": "openai",
        # Unsegmented campaign totals. NULL is not a fabricated device category.
        "device_type": None,
        "campaign_name": row["campaign_name"],
        "campaign_id": row["campaign_id"],
        "campaign_status": row["campaign_status"],
        "impressions": metric(row.get("impressions"), integer=True),
        "clicks": metric(row.get("clicks"), integer=True),
        # Insights spend is in currency units, unlike budget fields in micros.
        # Preserve signed spend reported by the API, including negative totals.
        "cost": metric(row.get("spend"), allow_negative=True),
    }


def fetch_rows(token: str, account: dict[str, Any], start: date, end: date):
    rows = []
    # Per-day requests bound pagination size and use inclusive account-local dates.
    current = start
    while current <= end:
        params = [
            ("aggregation_level", "campaign"), ("time_granularity", "daily"),
            ("limit", "2000"),
            ("time_ranges[]", json.dumps({
                "type": "date_range", "since": str(current), "until": str(current),
                "timezone": account["timezone"],
            })),
            *[("fields[]", field) for field in FIELDS],
        ]
        cursor = None
        seen_cursors = set()
        while True:
            page = get_json(
                "ad_account/insights", token,
                params + ([("after", cursor)] if cursor else []),
            )
            if not isinstance(page.get("data"), list) or not isinstance(page.get("has_more"), bool):
                raise ValueError("Incomplete OpenAI Ads page; refusing partial load")
            mapped = [map_row(row, account["id"]) for row in page["data"]]
            rows.extend(validate_and_deduplicate_rows(
                mapped, current, current, allow_negative_cost=True,
            ))
            if not page["has_more"]:
                break
            cursor = page.get("last_id")
            if not page["data"] or not isinstance(cursor, str) or not cursor or cursor in seen_cursors:
                raise ValueError("Invalid OpenAI Ads pagination; refusing partial load")
            seen_cursors.add(cursor)
        current += timedelta(days=1)
    return validate_and_deduplicate_rows(rows, start, end, allow_negative_cost=True)


def bigquery_window_has_rows(
    client: bigquery.Client, project: str, start: date, end: date,
) -> bool:
    """Check only the requested partitions; query errors must fail the task."""
    query = f"""
        SELECT EXISTS (
            SELECT 1
            FROM `{project}.marketing_marts.openai_campaign_daily_stats`
            WHERE data_date BETWEEN @start_date AND @end_date
        ) AS has_rows
    """
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start),
            bigquery.ScalarQueryParameter("end_date", "DATE", end),
        ],
        use_query_cache=False,
    )
    result = next(iter(client.query(query, job_config=config).result()))
    return bool(result["has_rows"])


@task
def sync_openai_campaign_daily() -> dict[str, Any]:
    # Resolve secrets at execution time, never during scheduler DAG parsing.
    token = Variable.get("OPENAI_ADS_REPORTING_API_KEY")
    account = get_json("ad_account", token)
    for field in ("id", "timezone", "currency_code"):
        if not isinstance(account.get(field), str) or not account[field]:
            raise ValueError(f"OpenAI Ads account is missing {field}")
    expected_account = Variable.get("OPENAI_ADS_ACCOUNT_ID")
    if account["id"] != expected_account:
        raise ValueError("OpenAI Ads account does not match configured account ID")
    expected_currency = Variable.get("OPENAI_ADS_EXPECTED_CURRENCY", default_var="USD")
    if account["currency_code"] != expected_currency:
        raise ValueError("OpenAI Ads currency does not match configured reporting currency")

    context = get_current_context()
    timezone = ZoneInfo(account["timezone"])
    # data_interval_end is the reporting cutoff; Airflow logical_date is the
    # interval START and would make a daily scheduled extraction one day stale.
    cutoff = context.get("data_interval_end") or datetime.now(UTC)
    conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    lookback = int(Variable.get("OPENAI_ADS_LOOKBACK_DAYS", default_var="7"))
    if lookback < 1:
        raise ValueError("OPENAI_ADS_LOOKBACK_DAYS must be positive")
    start, end = resolve_daily_window(cutoff.astimezone(timezone).date().isoformat(), conf, lookback)
    if end >= datetime.now(timezone).date():
        raise ValueError("Only completed account-local reporting days can be loaded")
    rows = fetch_rows(token, account, start, end)
    project = Variable.get("OPENAI_ADS_GCP_PROJECT_ID", default_var="fxr-analytics")
    client = bigquery.Client(project=project)
    if not rows and conf.get("allow_empty") is not True:
        # Empty source and destination is a successful no-op. Existing rows still
        # require explicit approval to erase; lookup errors must never imply empty.
        if bigquery_window_has_rows(client, project, start, end):
            raise ValueError("Empty OpenAI Ads report; existing data preserved. Verify source before allow_empty=true")
        return {
            "rows": 0, "start_date": str(start), "end_date": str(end),
            "currency": account["currency_code"], "timezone": account["timezone"],
            "status": "no_data",
        }
    replace_bigquery_window(
        client, project, "marketing_marts",
        "openai_campaign_daily_stats", rows, start, end,
        allow_negative_cost=True,
    )
    return {"rows": len(rows), "start_date": str(start), "end_date": str(end),
            "currency": account["currency_code"], "timezone": account["timezone"]}


with DAG(
    dag_id="openai_ads_campaign_daily_to_bigquery",
    description="Read-only daily ChatGPT Ads performance export, preserving signed spend.",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule="30 8 * * *", catchup=False, is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 2,
                  "retry_delay": timedelta(minutes=10)},
    tags=["marketing", "openai", "chatgpt", "bigquery"],
) as dag:
    sync_openai_campaign_daily()
