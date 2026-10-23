from __future__ import annotations

import csv
import io
import os
import time
import zipfile
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
from microsoft_ads_auth import microsoft_access_token as refresh_microsoft_access_token

from campaign_daily_common import (
    context_logical_date,
    replace_bigquery_window,
    request_with_retry,
    resolve_daily_window,
)


REPORTING_URL = (
    "https://reporting.api.bingads.microsoft.com/Reporting/v13/GenerateReport"
)
MICROSOFT_DEVICE_TYPES = {
    "Computer": "desktop",
    "Smartphone": "smartphone",
    "Tablet": "tablet",
}


def airflow_var(name: str, default: str = "") -> str:
    return Variable.get(name, default_var=os.getenv(name, default))


PROJECT_ID = airflow_var("MICROSOFT_ADS_GCP_PROJECT_ID", "fxr-analytics")
BQ_DATASET = airflow_var("MICROSOFT_ADS_BQ_DATASET", "marketing_marts")
BQ_TABLE = airflow_var(
    "MICROSOFT_ADS_BQ_TABLE", "microsoft_ads_campaign_daily_stats"
)
SCHEDULE = airflow_var("MICROSOFT_ADS_DAG_SCHEDULE", "30 7 * * *")
LOOKBACK_DAYS = int(airflow_var("MICROSOFT_ADS_LOOKBACK_DAYS", "7"))
POLL_SECONDS = int(airflow_var("MICROSOFT_ADS_POLL_SECONDS", "10"))
REPORT_TIMEOUT_SECONDS = int(
    airflow_var("MICROSOFT_ADS_REPORT_TIMEOUT_SECONDS", "1800")
)


def microsoft_access_token() -> str:
    return refresh_microsoft_access_token(
        Variable.get("MICROSOFT_ADS_CLIENT_ID"),
        Variable.get("MICROSOFT_ADS_CLIENT_SECRET"),
        airflow_var("MICROSOFT_ADS_SECRET_PROJECT_ID", "fxr-analytics"),
    )


def report_headers(access_token: str, account_id: str) -> dict[str, str]:
    return {
        "AuthenticationToken": access_token,
        "CustomerAccountId": account_id,
        "CustomerId": Variable.get("MICROSOFT_ADS_CUSTOMER_ID"),
        "DeveloperToken": Variable.get("MICROSOFT_ADS_DEVELOPER_TOKEN"),
        "Content-Type": "application/json",
    }


def date_payload(value: datetime.date) -> dict[str, int]:
    return {"Day": value.day, "Month": value.month, "Year": value.year}


def submit_report(
    access_token: str,
    account_id: str,
    start_date: datetime.date,
    end_date: datetime.date,
) -> str:
    payload = {
        "ReportRequest": {
            "Type": "CampaignPerformanceReportRequest",
            "Aggregation": "Daily",
            "ExcludeColumnHeaders": False,
            "ExcludeReportFooter": True,
            "ExcludeReportHeader": True,
            "Format": "Csv",
            "FormatVersion": "2.0",
            "ReportName": "BigQuery daily campaign performance",
            "ReturnOnlyCompleteData": True,
            "Columns": [
                "TimePeriod",
                "AccountId",
                "CampaignId",
                "CampaignName",
                "CampaignStatus",
                "DeviceType",
                "Impressions",
                "Clicks",
                "Spend",
            ],
            "Scope": {"AccountIds": [int(account_id)], "Campaigns": None},
            "Time": {
                "CustomDateRangeStart": date_payload(start_date),
                "CustomDateRangeEnd": date_payload(end_date),
                "ReportTimeZone": "GreenwichMeanTimeDublinEdinburghLisbonLondon",
            },
        }
    }
    response = request_with_retry(
        "POST",
        f"{REPORTING_URL}/Submit",
        headers=report_headers(access_token, account_id),
        json=payload,
        timeout=120,
    )
    payload = response.json()
    if not payload.get("ReportRequestId"):
        raise RuntimeError("Microsoft Ads did not return a report request ID")
    return payload["ReportRequestId"]


def wait_for_report(
    access_token: str, account_id: str, request_id: str
) -> str | None:
    deadline = time.monotonic() + REPORT_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        response = request_with_retry(
            "POST",
            f"{REPORTING_URL}/Poll",
            headers=report_headers(access_token, account_id),
            json={"ReportRequestId": request_id},
            timeout=120,
        )
        report_status = response.json()["ReportRequestStatus"]
        status = report_status["Status"]
        if status == "Success":
            return report_status["ReportDownloadUrl"]
        if status == "Error":
            raise RuntimeError(f"Microsoft Ads report failed: {report_status}")
        time.sleep(POLL_SECONDS)
    raise TimeoutError(f"Microsoft Ads report {request_id} did not finish in time")


def parse_report(content: bytes) -> list[dict[str, Any]]:
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        csv_name = next(name for name in archive.namelist() if name.endswith(".csv"))
        text = io.TextIOWrapper(archive.open(csv_name), encoding="utf-8-sig")
        source_rows = list(csv.DictReader(text))

    return [
        {
            "data_date": row["TimePeriod"],
            "customer_id": str(row["AccountId"]),
            "platform": "microsoft",
            "device_type": MICROSOFT_DEVICE_TYPES.get(
                row.get("DeviceType", ""), "other"
            ),
            "campaign_name": row["CampaignName"],
            "campaign_id": str(row["CampaignId"]),
            "campaign_status": row.get("CampaignStatus") or None,
            "impressions": int(row.get("Impressions") or 0),
            "clicks": int(row.get("Clicks") or 0),
            "cost": float(row.get("Spend") or 0),
        }
        for row in source_rows
    ]


def fetch_account_rows(
    access_token: str,
    account_id: str,
    start_date: datetime.date,
    end_date: datetime.date,
) -> list[dict[str, Any]]:
    request_id = submit_report(access_token, account_id, start_date, end_date)
    download_url = wait_for_report(access_token, account_id, request_id)
    if not download_url:
        return []
    response = request_with_retry("GET", download_url, timeout=120)
    return parse_report(response.content)


@task
def sync_microsoft_ads_campaign_daily() -> dict[str, Any]:
    context = get_current_context()
    start_date, end_date = resolve_daily_window(
        context_logical_date(context),
        (context.get("dag_run") and context["dag_run"].conf) or {},
        LOOKBACK_DAYS,
    )
    account_ids = list(
        dict.fromkeys(
            value.strip()
            for value in Variable.get("MICROSOFT_ADS_ACCOUNT_IDS").split(",")
            if value.strip()
        )
    )
    if not account_ids:
        raise ValueError("MICROSOFT_ADS_ACCOUNT_IDS must contain at least one account")

    token = microsoft_access_token()
    rows = []
    for account_id in account_ids:
        rows.extend(fetch_account_rows(token, account_id, start_date, end_date))

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
    dag_id="microsoft_ads_campaign_daily_to_bigquery",
    description="Loads daily Microsoft Ads campaign performance into marketing_marts.",
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
    tags=["marketing", "microsoft-ads", "bigquery"],
) as dag:
    sync_microsoft_ads_campaign_daily()
