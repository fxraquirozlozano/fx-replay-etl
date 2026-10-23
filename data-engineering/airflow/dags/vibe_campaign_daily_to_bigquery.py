from __future__ import annotations

import json
import os
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
try:
    from airflow.operators.python import get_current_context
except ImportError:
    from airflow.decorators import get_current_context
from google.cloud import bigquery

from campaign_daily_common import (
    context_logical_date,
    replace_bigquery_window,
    request_with_retry,
    resolve_daily_window,
)


VIBE_API_URL = "https://api.vibe.co"


def airflow_var(name: str, default: str = "") -> str:
    return Variable.get(name, default_var=os.getenv(name, default))


PROJECT_ID = airflow_var("VIBE_GCP_PROJECT_ID", "fxr-analytics")
BQ_DATASET = airflow_var("VIBE_BQ_DATASET", "marketing_marts")
BQ_TABLE = airflow_var("VIBE_BQ_TABLE", "vibe_campaign_daily_stats")
SCHEDULE = airflow_var("VIBE_DAG_SCHEDULE", "0 8 * * *")
LOOKBACK_DAYS = int(airflow_var("VIBE_LOOKBACK_DAYS", "7"))
API_REVISION = airflow_var("VIBE_API_REVISION", "2026-06-01")
POLL_SECONDS = int(airflow_var("VIBE_POLL_SECONDS", "10"))
REPORT_TIMEOUT_SECONDS = int(airflow_var("VIBE_REPORT_TIMEOUT_SECONDS", "1800"))


def get_access_token() -> str:
    response = request_with_retry(
        "POST",
        f"{VIBE_API_URL}/oauth2/token",
        auth=(Variable.get("VIBE_CLIENT_ID"), Variable.get("VIBE_CLIENT_SECRET")),
        data={
            "grant_type": "client_credentials",
            "scope": "reporting:read campaigns:read",
        },
        timeout=60,
    )
    return response.json()["access_token"]


def api_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "X-Vibe-Revision": API_REVISION,
        "Content-Type": "application/json",
    }


def campaign_statuses(token: str, advertiser_id: str) -> dict[str, str]:
    statuses: dict[str, str] = {}
    offset = 0
    while True:
        response = request_with_retry(
            "GET",
            f"{VIBE_API_URL}/campaigns",
            headers=api_headers(token),
            params={"advertiser_id": advertiser_id, "limit": 100, "offset": offset},
            timeout=120,
        )
        payload = response.json()
        for campaign in payload["data"]:
            statuses[campaign["id"]] = campaign.get("state") or campaign["status"]
        offset += len(payload["data"])
        if offset >= payload["total"] or not payload["data"]:
            return statuses


def create_report(
    token: str,
    advertiser_id: str,
    start_date: datetime.date,
    end_date: datetime.date,
) -> str:
    response = request_with_retry(
        "POST",
        f"{VIBE_API_URL}/reports",
        headers=api_headers(token),
        json={
            "start_date": start_date.isoformat(),
            # Vibe's end_date is exclusive.
            "end_date": (end_date + timedelta(days=1)).isoformat(),
            "timezone": "UTC",
            "advertiser_ids": [advertiser_id],
            "dimensions": [
                "advertiser_id",
                "campaign_id",
                "campaign_name",
            ],
            "metrics": ["spend", "impressions"],
            "granularity": "DAY",
            "format": "JSON",
        },
        timeout=120,
    )
    return response.json()["id"]


def wait_for_report(token: str, report_id: str) -> str:
    deadline = time.monotonic() + REPORT_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        response = request_with_retry(
            "GET",
            f"{VIBE_API_URL}/reports/{report_id}",
            headers=api_headers(token),
            timeout=120,
        )
        report = response.json()
        if report["status"] == "READY":
            return report["download_url"]
        if report["status"] == "FAILED":
            raise RuntimeError(f"Vibe report failed: {report.get('failure_reason')}")
        time.sleep(POLL_SECONDS)
    raise TimeoutError(f"Vibe report {report_id} did not finish in time")


def report_rows(content: bytes, statuses: dict[str, str]) -> list[dict[str, Any]]:
    payload = json.loads(content)
    source_rows = payload.get("data", payload) if isinstance(payload, dict) else payload
    aggregated: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in source_rows:
        key = (
            row["impression_date"],
            str(row["advertiser_id"]),
            str(row["campaign_id"]),
        )
        output = aggregated.setdefault(
            key,
            {
            "data_date": row["impression_date"],
            "customer_id": str(row["advertiser_id"]),
            "platform": "vibe",
            "device_type": "connected_tv",
            "campaign_name": row["campaign_name"],
            "campaign_id": str(row["campaign_id"]),
            "campaign_status": statuses.get(str(row["campaign_id"])),
            "impressions": 0,
            # Vibe CTV reporting does not expose clicks. Null preserves the shared
            # schema without misrepresenting page views or sessions as ad clicks.
            "clicks": None,
            "cost": 0.0,
            },
        )
        output["impressions"] += int(float(row.get("impressions") or 0))
        output["cost"] += float(row.get("spend") or 0)
    return list(aggregated.values())


def fetch_advertiser_rows(
    token: str,
    advertiser_id: str,
    start_date: datetime.date,
    end_date: datetime.date,
) -> list[dict[str, Any]]:
    statuses = campaign_statuses(token, advertiser_id)
    rows = []
    chunk_start = start_date
    while chunk_start <= end_date:
        # The Vibe reporting API accepts at most 45 calendar days per report.
        chunk_end = min(chunk_start + timedelta(days=44), end_date)
        report_id = create_report(token, advertiser_id, chunk_start, chunk_end)
        download_url = wait_for_report(token, report_id)
        response = request_with_retry("GET", download_url, timeout=120)
        rows.extend(report_rows(response.content, statuses))
        chunk_start = chunk_end + timedelta(days=1)
    return rows


@task
def sync_vibe_campaign_daily() -> dict[str, Any]:
    context = get_current_context()
    start_date, end_date = resolve_daily_window(
        context_logical_date(context),
        (context.get("dag_run") and context["dag_run"].conf) or {},
        LOOKBACK_DAYS,
    )
    advertiser_ids = list(
        dict.fromkeys(
            value.strip()
            for value in Variable.get("VIBE_ADVERTISER_IDS").split(",")
            if value.strip()
        )
    )
    if not advertiser_ids:
        raise ValueError("VIBE_ADVERTISER_IDS must contain at least one advertiser")

    token = get_access_token()
    rows = []
    for advertiser_id in advertiser_ids:
        rows.extend(fetch_advertiser_rows(token, advertiser_id, start_date, end_date))

    replace_bigquery_window(
        bigquery.Client(project=PROJECT_ID),
        PROJECT_ID,
        BQ_DATASET,
        BQ_TABLE,
        rows,
        start_date,
        end_date,
    )
    return {"rows": len(rows), "start_date": str(start_date), "end_date": str(end_date)}


with DAG(
    dag_id="vibe_campaign_daily_to_bigquery",
    description="Loads daily Vibe CTV campaign performance into marketing_marts.",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    schedule=SCHEDULE,
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["marketing", "vibe", "ctv", "bigquery"],
) as dag:
    sync_vibe_campaign_daily()
