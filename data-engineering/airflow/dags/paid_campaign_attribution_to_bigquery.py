from __future__ import annotations

import csv
import io
import json
import os
import time
import zipfile
from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import uuid4

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
try:
    from airflow.operators.python import get_current_context
except ImportError:
    from airflow.decorators import get_current_context
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from microsoft_ads_auth import microsoft_access_token as refresh_microsoft_access_token

from campaign_daily_common import (
    context_logical_date,
    request_with_retry,
    resolve_daily_window,
)


MICROSOFT_REPORTING_URL = (
    "https://reporting.api.bingads.microsoft.com/Reporting/v13/GenerateReport"
)
VIBE_API_URL = "https://api.vibe.co"


def airflow_var(name: str, default: str = "") -> str:
    return Variable.get(name, default_var=os.getenv(name, default))


PROJECT_ID = airflow_var("PAID_ATTRIBUTION_GCP_PROJECT_ID", "fxr-analytics")
BQ_DATASET = airflow_var("PAID_ATTRIBUTION_BQ_DATASET", "marketing_marts")
SCHEDULE = airflow_var("PAID_ATTRIBUTION_DAG_SCHEDULE", "0 9 * * *")
MICROSOFT_LOOKBACK_DAYS = int(
    airflow_var("MICROSOFT_ADS_ATTRIBUTION_LOOKBACK_DAYS", "90")
)
VIBE_LOOKBACK_DAYS = int(airflow_var("VIBE_ATTRIBUTION_LOOKBACK_DAYS", "45"))
POLL_SECONDS = int(airflow_var("PAID_ATTRIBUTION_POLL_SECONDS", "10"))
REPORT_TIMEOUT_SECONDS = int(
    airflow_var("PAID_ATTRIBUTION_REPORT_TIMEOUT_SECONDS", "1800")
)
VIBE_API_REVISION = airflow_var("VIBE_API_REVISION", "2026-06-01")
VIBE_ATTRIBUTION_WINDOW = airflow_var("VIBE_ATTRIBUTION_WINDOW", "30 days")

MICROSOFT_CLICK_TABLE = "microsoft_ads_click_attribution"
VIBE_OUTCOME_TABLE = "vibe_campaign_daily_attribution"
VIBE_PURCHASE_TABLE = "vibe_attributed_purchases"

MICROSOFT_CLICK_SCHEMA = [
    bigquery.SchemaField("click_date", "DATE"),
    bigquery.SchemaField("customer_id", "STRING"),
    bigquery.SchemaField("campaign_id", "STRING"),
    bigquery.SchemaField("device_type", "STRING"),
    bigquery.SchemaField("msclkid", "STRING"),
    bigquery.SchemaField("clicks", "INTEGER"),
    bigquery.SchemaField("conversions", "FLOAT"),
    bigquery.SchemaField("revenue", "FLOAT"),
]

VIBE_OUTCOME_SCHEMA = [
    bigquery.SchemaField("data_date", "DATE"),
    bigquery.SchemaField("customer_id", "STRING"),
    bigquery.SchemaField("campaign_id", "STRING"),
    bigquery.SchemaField("campaign_name", "STRING"),
    bigquery.SchemaField("page_views", "FLOAT"),
    bigquery.SchemaField("sessions", "FLOAT"),
    bigquery.SchemaField("leads", "FLOAT"),
    bigquery.SchemaField("purchases", "FLOAT"),
    bigquery.SchemaField("attributed_revenue", "FLOAT"),
    bigquery.SchemaField("signups", "FLOAT"),
    bigquery.SchemaField("attribution_window", "STRING"),
]

VIBE_PURCHASE_SCHEMA = [
    bigquery.SchemaField("event_date", "DATE"),
    bigquery.SchemaField("purchase_id", "STRING"),
    bigquery.SchemaField("event_type", "STRING"),
    bigquery.SchemaField("event_value", "FLOAT"),
    bigquery.SchemaField("impression_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("event_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("customer_id", "STRING"),
    bigquery.SchemaField("customer_name", "STRING"),
    bigquery.SchemaField("campaign_id", "STRING"),
    bigquery.SchemaField("campaign_name", "STRING"),
    bigquery.SchemaField("strategy_id", "STRING"),
    bigquery.SchemaField("strategy_name", "STRING"),
]


def replace_typed_window(
    client: bigquery.Client,
    table_name: str,
    schema: list[bigquery.SchemaField],
    date_field: str,
    key_fields: tuple[str, ...],
    rows: list[dict[str, Any]],
    start_date: date,
    end_date: date,
) -> None:
    """Validate, deduplicate and atomically replace a date window."""
    unique: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        row_date = date.fromisoformat(str(row[date_field]))
        if not start_date <= row_date <= end_date:
            raise ValueError(f"Row date {row_date} is outside the requested window")
        key = tuple(row.get(field) for field in key_fields)
        if any(value in (None, "") for value in key):
            raise ValueError(f"Attribution row has an incomplete key: {key}")
        existing = unique.get(key)
        if existing is not None and existing != row:
            raise ValueError(f"Conflicting attribution rows returned for key {key}")
        unique[key] = row
    rows = list(unique.values())

    dataset_ref = f"{PROJECT_ID}.{BQ_DATASET}"
    table_ref = f"{dataset_ref}.{table_name}"
    staging_ref = f"{dataset_ref}._staging_{table_name}_{uuid4().hex}"
    client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)
    try:
        client.get_table(table_ref)
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field=date_field
        )
        client.create_table(table)

    client.create_table(bigquery.Table(staging_ref, schema=schema))
    try:
        if rows:
            config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            client.load_table_from_json(rows, staging_ref, job_config=config).result()
        key_sql = ", ".join(key_fields)
        query = f"""
        BEGIN TRANSACTION;
        DELETE FROM `{table_ref}`
        WHERE {date_field} BETWEEN @start_date AND @end_date;
        INSERT INTO `{table_ref}` SELECT * FROM `{staging_ref}`;
        ASSERT NOT EXISTS (
          SELECT 1 FROM `{table_ref}`
          WHERE {date_field} BETWEEN @start_date AND @end_date
          GROUP BY {key_sql} HAVING COUNT(*) > 1
        ) AS 'Duplicate attribution rows detected';
        COMMIT TRANSACTION;
        """
        config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            ]
        )
        client.query(query, job_config=config).result()
    finally:
        client.delete_table(staging_ref, not_found_ok=True)


def microsoft_access_token() -> str:
    return refresh_microsoft_access_token(
        Variable.get("MICROSOFT_ADS_CLIENT_ID"),
        Variable.get("MICROSOFT_ADS_CLIENT_SECRET"),
        airflow_var("MICROSOFT_ADS_SECRET_PROJECT_ID", "fxr-analytics"),
    )


def microsoft_headers(token: str, account_id: str) -> dict[str, str]:
    return {
        "AuthenticationToken": token,
        "CustomerAccountId": account_id,
        "CustomerId": Variable.get("MICROSOFT_ADS_CUSTOMER_ID"),
        "DeveloperToken": Variable.get("MICROSOFT_ADS_DEVELOPER_TOKEN"),
        "Content-Type": "application/json",
    }


def date_payload(value: date) -> dict[str, int]:
    return {"Day": value.day, "Month": value.month, "Year": value.year}


def submit_microsoft_click_report(
    token: str, account_id: str, start_date: date, end_date: date
) -> str:
    response = request_with_retry(
        "POST",
        f"{MICROSOFT_REPORTING_URL}/Submit",
        headers=microsoft_headers(token, account_id),
        json={
            "ReportRequest": {
                "Type": "MSClickIdPerformanceReportRequest",
                "Aggregation": "Daily",
                "ExcludeColumnHeaders": False,
                "ExcludeReportFooter": True,
                "ExcludeReportHeader": True,
                "Format": "Csv",
                "FormatVersion": "2.0",
                "ReportName": "BigQuery Microsoft click attribution",
                "ReturnOnlyCompleteData": True,
                "Columns": [
                    "TimePeriod",
                    "AccountId",
                    "CampaignId",
                    "DeviceType",
                    "MSClickId",
                    "Clicks",
                    "ConversionsQualified",
                    "Revenue",
                ],
                "Scope": {"AccountIds": [int(account_id)], "Campaigns": None},
                "Time": {
                    "CustomDateRangeStart": date_payload(start_date),
                    "CustomDateRangeEnd": date_payload(end_date),
                    "ReportTimeZone": (
                        "GreenwichMeanTimeDublinEdinburghLisbonLondon"
                    ),
                },
            }
        },
        timeout=120,
    )
    report_id = response.json().get("ReportRequestId")
    if not report_id:
        raise RuntimeError("Microsoft Ads did not return a click report ID")
    return report_id


def poll_microsoft_report(token: str, account_id: str, report_id: str) -> str | None:
    deadline = time.monotonic() + REPORT_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        response = request_with_retry(
            "POST",
            f"{MICROSOFT_REPORTING_URL}/Poll",
            headers=microsoft_headers(token, account_id),
            json={"ReportRequestId": report_id},
            timeout=120,
        )
        report = response.json()["ReportRequestStatus"]
        if report["Status"] == "Success":
            return report.get("ReportDownloadUrl")
        if report["Status"] == "Error":
            raise RuntimeError(f"Microsoft Ads click report failed: {report}")
        time.sleep(POLL_SECONDS)
    raise TimeoutError(f"Microsoft Ads click report {report_id} timed out")


def parse_microsoft_click_report(content: bytes) -> list[dict[str, Any]]:
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        csv_name = next(name for name in archive.namelist() if name.endswith(".csv"))
        text = io.TextIOWrapper(archive.open(csv_name), encoding="utf-8-sig")
        source_rows = list(csv.DictReader(text))
    rows = []
    for row in source_rows:
        msclkid = (row.get("MSClickId") or "").strip()
        if not msclkid:
            continue
        rows.append(
            {
                "click_date": row["TimePeriod"],
                "customer_id": str(row["AccountId"]),
                "campaign_id": str(row["CampaignId"]),
                "device_type": row.get("DeviceType") or "Unknown",
                "msclkid": msclkid.lower(),
                "clicks": int(float(row.get("Clicks") or 0)),
                "conversions": float(row.get("ConversionsQualified") or 0),
                "revenue": float(row.get("Revenue") or 0),
            }
        )
    return rows


def vibe_access_token() -> str:
    response = request_with_retry(
        "POST",
        f"{VIBE_API_URL}/oauth2/token",
        auth=(Variable.get("VIBE_CLIENT_ID"), Variable.get("VIBE_CLIENT_SECRET")),
        data={"grant_type": "client_credentials", "scope": "reporting:read"},
        timeout=60,
    )
    return response.json()["access_token"]


def vibe_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "X-Vibe-Revision": VIBE_API_REVISION,
        "Content-Type": "application/json",
    }


def create_vibe_outcome_report(
    token: str, advertiser_id: str, start_date: date, end_date: date
) -> str:
    response = request_with_retry(
        "POST",
        f"{VIBE_API_URL}/reports",
        headers=vibe_headers(token),
        json={
            "start_date": start_date.isoformat(),
            "end_date": (end_date + timedelta(days=1)).isoformat(),
            "timezone": "UTC",
            "advertiser_ids": [advertiser_id],
            "dimensions": ["advertiser_id", "campaign_id", "campaign_name"],
            "metrics": [
                "number_of_page_views",
                "number_of_sessions",
                "number_of_leads",
                "number_of_purchases",
                "amount_of_purchases",
                "number_of_signups",
            ],
            "attribution_window": VIBE_ATTRIBUTION_WINDOW,
            "granularity": "DAY",
            "format": "JSON",
        },
        timeout=120,
    )
    return response.json()["id"]


def poll_vibe_report(token: str, report_id: str) -> str:
    deadline = time.monotonic() + REPORT_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        response = request_with_retry(
            "GET",
            f"{VIBE_API_URL}/reports/{report_id}",
            headers=vibe_headers(token),
            timeout=120,
        )
        report = response.json()
        if report["status"] == "READY":
            return report["download_url"]
        if report["status"] == "FAILED":
            raise RuntimeError(f"Vibe attribution report failed: {report}")
        time.sleep(POLL_SECONDS)
    raise TimeoutError(f"Vibe attribution report {report_id} timed out")


def parse_vibe_outcomes(content: bytes) -> list[dict[str, Any]]:
    payload = json.loads(content.decode("utf-8"))
    source_rows = payload.get("data", payload) if isinstance(payload, dict) else payload
    # Vibe can return more than one row for the same visible dimensions because
    # of internal event-source splits. Collapse those additive rows to the
    # documented daily advertiser/campaign grain before enforcing uniqueness.
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
            "campaign_id": str(row["campaign_id"]),
            "campaign_name": row.get("campaign_name"),
            "page_views": 0.0,
            "sessions": 0.0,
            "leads": 0.0,
            "purchases": 0.0,
            "attributed_revenue": 0.0,
            "signups": 0.0,
            "attribution_window": VIBE_ATTRIBUTION_WINDOW,
            },
        )
        output["page_views"] += float(row.get("number_of_page_views") or 0)
        output["sessions"] += float(row.get("number_of_sessions") or 0)
        output["leads"] += float(row.get("number_of_leads") or 0)
        output["purchases"] += float(row.get("number_of_purchases") or 0)
        output["attributed_revenue"] += float(
            row.get("amount_of_purchases") or 0
        )
        output["signups"] += float(row.get("number_of_signups") or 0)
    return list(aggregated.values())


def fetch_vibe_purchases(
    token: str, advertiser_id: str, start_date: date, end_date: date
) -> list[dict[str, Any]]:
    response = request_with_retry(
        "POST",
        f"{VIBE_API_URL}/reports/purchases",
        headers=vibe_headers(token),
        json={
            "advertiser_id": advertiser_id,
            "start_date": start_date.isoformat(),
            "end_date": (end_date + timedelta(days=1)).isoformat(),
            "timezone": "UTC",
        },
        timeout=120,
    )
    rows = []
    for row in response.json().get("data", []):
        # The purchase endpoint also emits zero-value session_source records
        # for the same purchase/impression key. They are attribution context,
        # not purchase facts, and would duplicate both counts and keys.
        if row.get("event_type") != "purchase":
            continue
        rows.append(
            {
                "event_date": str(row["event_timestamp"])[:10],
                "purchase_id": str(row["purchase_id"]),
                "event_type": row.get("event_type"),
                "event_value": float(row.get("event_value") or 0),
                "impression_timestamp": row.get("impression_timestamp"),
                "event_timestamp": row.get("event_timestamp"),
                "customer_id": str(row["advertiser_id"]),
                "customer_name": row.get("advertiser_name"),
                "campaign_id": str(row["campaign_id"]),
                "campaign_name": row.get("campaign_name"),
                "strategy_id": str(row["strategy_id"]),
                "strategy_name": row.get("strategy_name"),
            }
        )
    return rows


@task
def sync_microsoft_click_attribution() -> dict[str, Any]:
    context = get_current_context()
    start_date, end_date = resolve_daily_window(
        context_logical_date(context),
        (context.get("dag_run") and context["dag_run"].conf) or {},
        MICROSOFT_LOOKBACK_DAYS,
    )
    account_ids = [
        value.strip()
        for value in Variable.get("MICROSOFT_ADS_ACCOUNT_IDS").split(",")
        if value.strip()
    ]
    token = microsoft_access_token()
    rows: list[dict[str, Any]] = []
    for account_id in dict.fromkeys(account_ids):
        report_id = submit_microsoft_click_report(
            token, account_id, start_date, end_date
        )
        download_url = poll_microsoft_report(token, account_id, report_id)
        if download_url:
            response = request_with_retry("GET", download_url, timeout=120)
            rows.extend(parse_microsoft_click_report(response.content))
    replace_typed_window(
        bigquery.Client(project=PROJECT_ID),
        MICROSOFT_CLICK_TABLE,
        MICROSOFT_CLICK_SCHEMA,
        "click_date",
        ("click_date", "customer_id", "campaign_id", "device_type", "msclkid"),
        rows,
        start_date,
        end_date,
    )
    return {"rows": len(rows), "start_date": str(start_date), "end_date": str(end_date)}


@task
def sync_vibe_attribution() -> dict[str, Any]:
    context = get_current_context()
    start_date, end_date = resolve_daily_window(
        context_logical_date(context),
        (context.get("dag_run") and context["dag_run"].conf) or {},
        VIBE_LOOKBACK_DAYS,
    )
    advertiser_ids = [
        value.strip()
        for value in Variable.get("VIBE_ADVERTISER_IDS").split(",")
        if value.strip()
    ]
    token = vibe_access_token()
    outcomes: list[dict[str, Any]] = []
    purchases: list[dict[str, Any]] = []
    for advertiser_id in dict.fromkeys(advertiser_ids):
        chunk_start = start_date
        while chunk_start <= end_date:
            chunk_end = min(chunk_start + timedelta(days=44), end_date)
            report_id = create_vibe_outcome_report(
                token, advertiser_id, chunk_start, chunk_end
            )
            download_url = poll_vibe_report(token, report_id)
            response = request_with_retry("GET", download_url, timeout=120)
            outcomes.extend(parse_vibe_outcomes(response.content))
            purchases.extend(
                fetch_vibe_purchases(token, advertiser_id, chunk_start, chunk_end)
            )
            chunk_start = chunk_end + timedelta(days=1)
    client = bigquery.Client(project=PROJECT_ID)
    replace_typed_window(
        client,
        VIBE_OUTCOME_TABLE,
        VIBE_OUTCOME_SCHEMA,
        "data_date",
        ("data_date", "customer_id", "campaign_id", "attribution_window"),
        outcomes,
        start_date,
        end_date,
    )
    replace_typed_window(
        client,
        VIBE_PURCHASE_TABLE,
        VIBE_PURCHASE_SCHEMA,
        "event_date",
        (
            "event_date",
            "customer_id",
            "purchase_id",
            "campaign_id",
            "strategy_id",
            "impression_timestamp",
        ),
        purchases,
        start_date,
        end_date,
    )
    return {
        "outcome_rows": len(outcomes),
        "purchase_rows": len(purchases),
        "start_date": str(start_date),
        "end_date": str(end_date),
    }


@task
def build_paid_campaign_value_marts() -> None:
    client = bigquery.Client(project=PROJECT_ID)
    dataset = f"{PROJECT_ID}.{BQ_DATASET}"
    query = f"""
    CREATE OR REPLACE VIEW `{dataset}.paid_campaign_daily_stats` AS
    SELECT * FROM `{dataset}.gads_campaign_daily_stats`
    UNION ALL SELECT * FROM `{dataset}.fb_campaign_daily_stats`
    UNION ALL SELECT * FROM `{dataset}.microsoft_ads_campaign_daily_stats`
    UNION ALL SELECT * FROM `{dataset}.vibe_campaign_daily_stats`;

    CREATE OR REPLACE TABLE `{dataset}.microsoft_ads_user_acquisition` AS
    WITH signals AS (
      SELECT
        d.user_id,
        d.signup_date,
        c.event_at AS attribution_event_at,
        LOWER(COALESCE(
          JSON_VALUE(SAFE.PARSE_JSON(c.params_json), '$.query.msclkid'),
          JSON_VALUE(SAFE.PARSE_JSON(c.params_json), '$.msclkid'),
          REGEXP_EXTRACT(c.event_url, r'(?i)[?&]msclkid=([^&#]+)')
        )) AS msclkid
      FROM `{dataset}.dim_marketing_user_acquisition` d
      JOIN `{dataset}.int_marketing_user_acquisition_candidates` c
        ON c.user_id = d.user_id
      WHERE c.event_at <= TIMESTAMP_ADD(d.user_created_at, INTERVAL 1 DAY)
        AND c.event_at >= TIMESTAMP_SUB(d.user_created_at, INTERVAL 90 DAY)
      UNION ALL
      SELECT
        d.user_id,
        d.signup_date,
        t.occurred_at AS attribution_event_at,
        LOWER(t.click_id) AS msclkid
      FROM `{PROJECT_ID}.fxr.tracking_prod_touchpoints` t
      JOIN `{dataset}.dim_marketing_user_acquisition` d USING (user_id)
      WHERE t.click_id IS NOT NULL
        AND t.occurred_at <= TIMESTAMP_ADD(d.user_created_at, INTERVAL 1 DAY)
        AND t.occurred_at >= TIMESTAMP_SUB(d.user_created_at, INTERVAL 90 DAY)
    ), matched AS (
      SELECT
        s.user_id,
        s.signup_date,
        s.attribution_event_at,
        s.msclkid,
        m.customer_id,
        m.campaign_id,
        m.click_date,
        ROW_NUMBER() OVER (
          PARTITION BY s.user_id
          ORDER BY s.attribution_event_at DESC, m.click_date DESC
        ) AS match_rank
      FROM signals s
      JOIN `{dataset}.{MICROSOFT_CLICK_TABLE}` m USING (msclkid)
      WHERE s.msclkid IS NOT NULL
    )
    SELECT
      user_id, signup_date, customer_id, campaign_id, msclkid,
      click_date, attribution_event_at,
      'microsoft_msclkid_last_touch_90d' AS attribution_model
    FROM matched WHERE match_rank = 1;

    CREATE OR REPLACE TABLE `{dataset}.microsoft_ads_campaign_cac_ltv` AS
    WITH attributed AS (
      SELECT * FROM `{dataset}.microsoft_ads_user_acquisition`
    ), invoice_by_user AS (
      SELECT
        a.user_id,
        a.signup_date,
        a.customer_id,
        a.campaign_id,
        COUNT(DISTINCT i.invoice_id) > 0 AS became_paying_customer,
        SUM(IF(i.days_from_signup_to_invoice BETWEEN 0 AND 30, i.amount_usd, 0)) AS revenue_30d,
        SUM(IF(i.days_from_signup_to_invoice BETWEEN 0 AND 60, i.amount_usd, 0)) AS revenue_60d,
        SUM(IF(i.days_from_signup_to_invoice BETWEEN 0 AND 90, i.amount_usd, 0)) AS revenue_90d,
        SUM(IF(i.days_from_signup_to_invoice >= 0, i.amount_usd, 0)) AS revenue_lifetime
      FROM attributed a
      LEFT JOIN `{dataset}.int_subscription_acquisition_channels` s
        ON s.user_id = a.user_id
      LEFT JOIN `{dataset}.int_weekly_cohort_invoices` i
        ON i.subscription_id = s.subscription_id
      GROUP BY 1,2,3,4
    ), cohorts AS (
      SELECT
        signup_date AS cohort_date,
        customer_id,
        campaign_id,
        COUNT(*) AS acquired_signups,
        COUNTIF(became_paying_customer) AS new_paying_customers,
        SUM(revenue_30d) AS revenue_30d,
        SUM(revenue_60d) AS revenue_60d,
        SUM(revenue_90d) AS revenue_90d,
        SUM(revenue_lifetime) AS revenue_lifetime
      FROM invoice_by_user GROUP BY 1,2,3
    ), spend AS (
      SELECT
        data_date AS cohort_date,
        customer_id,
        campaign_id,
        ANY_VALUE(campaign_name) AS campaign_name,
        SUM(cost) AS spend
      FROM `{dataset}.microsoft_ads_campaign_daily_stats`
      GROUP BY 1,2,3
    )
    SELECT
      COALESCE(s.cohort_date, c.cohort_date) AS cohort_date,
      'microsoft' AS platform,
      COALESCE(s.customer_id, c.customer_id) AS customer_id,
      COALESCE(s.campaign_id, c.campaign_id) AS campaign_id,
      s.campaign_name,
      COALESCE(s.spend, 0) AS spend,
      COALESCE(c.acquired_signups, 0) AS acquired_signups,
      COALESCE(c.new_paying_customers, 0) AS new_paying_customers,
      SAFE_DIVIDE(s.spend, c.acquired_signups) AS cost_per_signup,
      SAFE_DIVIDE(s.spend, c.new_paying_customers) AS cac,
      SAFE_DIVIDE(c.revenue_30d, c.acquired_signups) AS ltv_30d,
      SAFE_DIVIDE(c.revenue_60d, c.acquired_signups) AS ltv_60d,
      SAFE_DIVIDE(c.revenue_90d, c.acquired_signups) AS ltv_90d,
      SAFE_DIVIDE(c.revenue_lifetime, c.acquired_signups) AS ltv_lifetime,
      c.revenue_30d,
      c.revenue_60d,
      c.revenue_90d,
      c.revenue_lifetime,
      SAFE_DIVIDE(c.revenue_lifetime, s.spend) AS roas_lifetime,
      'microsoft_msclkid_last_touch_90d' AS attribution_model
    FROM spend s FULL OUTER JOIN cohorts c
      USING (cohort_date, customer_id, campaign_id);

    CREATE OR REPLACE TABLE `{dataset}.vibe_campaign_attribution_performance` AS
    WITH spend AS (
      SELECT data_date, customer_id, campaign_id,
        ANY_VALUE(campaign_name) AS campaign_name, SUM(cost) AS spend
      FROM `{dataset}.vibe_campaign_daily_stats` GROUP BY 1,2,3
    )
    SELECT
      COALESCE(s.data_date, o.data_date) AS data_date,
      'vibe' AS platform,
      COALESCE(s.customer_id, o.customer_id) AS customer_id,
      COALESCE(s.campaign_id, o.campaign_id) AS campaign_id,
      COALESCE(s.campaign_name, o.campaign_name) AS campaign_name,
      COALESCE(s.spend, 0) AS spend,
      COALESCE(o.page_views, 0) AS attributed_page_views,
      COALESCE(o.sessions, 0) AS attributed_sessions,
      COALESCE(o.leads, 0) AS attributed_leads,
      COALESCE(o.purchases, 0) AS attributed_purchases,
      COALESCE(o.attributed_revenue, 0) AS attributed_revenue,
      COALESCE(o.signups, 0) AS attributed_signups,
      SAFE_DIVIDE(s.spend, o.leads) AS cost_per_attributed_lead,
      SAFE_DIVIDE(s.spend, o.purchases) AS cost_per_attributed_purchase,
      SAFE_DIVIDE(o.attributed_revenue, s.spend) AS reported_roas,
      o.attribution_window,
      'vibe_household_view_through' AS attribution_model,
      CAST(NULL AS FLOAT64) AS cac,
      CAST(NULL AS FLOAT64) AS ltv_30d,
      CAST(NULL AS FLOAT64) AS ltv_60d,
      CAST(NULL AS FLOAT64) AS ltv_90d,
      CAST(NULL AS FLOAT64) AS ltv_lifetime
    FROM spend s FULL OUTER JOIN `{dataset}.{VIBE_OUTCOME_TABLE}` o
      USING (data_date, customer_id, campaign_id);
    """
    client.query(query).result()


with DAG(
    dag_id="paid_campaign_attribution_to_bigquery",
    description="Loads Microsoft/Vibe attribution and builds campaign CAC/LTV marts.",
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
    tags=["marketing", "attribution", "cac", "ltv", "bigquery"],
) as dag:
    microsoft = sync_microsoft_click_attribution()
    vibe = sync_vibe_attribution()
    marts = build_paid_campaign_value_marts()
    [microsoft, vibe] >> marts
