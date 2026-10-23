from __future__ import annotations

import json
import os
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
try:
    from airflow.operators.python import get_current_context
except ImportError:
    from airflow.decorators import get_current_context
from google.api_core.exceptions import NotFound
from google.cloud import bigquery


META_API_BASE_URL = "https://graph.facebook.com"


def airflow_var(name: str, default: str) -> str:
    return Variable.get(name, default_var=os.getenv(name, default))


PROJECT_ID = airflow_var("FACEBOOK_ADS_GCP_PROJECT_ID", "fxr-analytics")
SCHEDULE = airflow_var("FACEBOOK_ADS_DAG_SCHEDULE", "0 6 * * *")
META_API_VERSION = airflow_var("FACEBOOK_ADS_API_VERSION", "v23.0")
BQ_DATASET = airflow_var("FACEBOOK_ADS_BQ_DATASET", "marketing")
BQ_TABLE_PREFIX = airflow_var("FACEBOOK_ADS_BQ_TABLE_PREFIX", "facebook_ads")
META_ACCOUNT_ID = airflow_var("FACEBOOK_ADS_ACCOUNT_ID", "")
DEFAULT_LOOKBACK_DAYS = int(airflow_var("FACEBOOK_ADS_DEFAULT_LOOKBACK_DAYS", "30"))
INSIGHTS_JOB_TIMEOUT_SECONDS = int(
    airflow_var("FACEBOOK_ADS_INSIGHTS_TIMEOUT_SECONDS", "3600")
)
STATE_VAR_NAME = airflow_var(
    "FACEBOOK_ADS_BOOKMARKS_VAR_NAME",
    "facebook_ads_bookmarks",
)
ACCESS_TOKEN_VAR_NAME = airflow_var(
    "FACEBOOK_ADS_ACCESS_TOKEN_VAR_NAME",
    "FACEBOOK_ADS_ACCESS_TOKEN",
)

ACTION_ATTRIBUTION_WINDOWS = [
    "1d_click",
    "7d_click",
    "28d_click",
    "1d_view",
    "7d_view",
    "28d_view",
]

ACTION_BREAKDOWNS = [
    "action_type",
    "action_target_id",
    "action_destination",
]

INSIGHTS_FIELDS = [
    "ctr",
    "clicks",
    "engagement_rate_ranking",
    "unique_ctr",
    "cost_per_inline_link_click",
    "campaign_id",
    "social_spend",
    "conversion_rate_ranking",
    "cpm",
    "cpp",
    "action_values",
    "cost_per_unique_action_type",
    "date_stop",
    "impressions",
    "campaign_name",
    "ad_name",
    "canvas_avg_view_percent",
    "unique_actions",
    "unique_link_clicks_ctr",
    "video_p25_watched_actions",
    "reach",
    "cost_per_inline_post_engagement",
    "unique_inline_link_click_ctr",
    "date_start",
    "video_30_sec_watched_actions",
    "account_name",
    "cost_per_unique_inline_link_click",
    "conversions",
    "unique_inline_link_clicks",
    "inline_link_clicks",
    "inline_link_click_ctr",
    "video_play_curve_actions",
    "ad_id",
    "adset_name",
    "cost_per_action_type",
    "cost_per_unique_click",
    "frequency",
    "spend",
    "outbound_clicks",
    "adset_id",
    "quality_ranking",
    "website_ctr",
    "account_id",
    "cpc",
    "video_p50_watched_actions",
    "canvas_avg_view_time",
    "inline_post_engagement",
    "unique_clicks",
    "unique_outbound_clicks",
    "video_p100_watched_actions",
    "conversion_values",
    "objective",
    "actions",
    "video_p75_watched_actions",
]

STREAM_CONFIGS: list[dict[str, Any]] = [
    {
        "task_id": "sync_adcreative",
        "stream_name": "adcreative",
        "stream_type": "metadata",
        "table_name": "adcreative",
        "primary_key": "id",
        "fields": [
            "adlabels",
            "product_set_id",
            "effective_object_story_id",
            "applink_treatment",
            "body",
            "object_story_id",
            "image_crops",
            "object_url",
            "link_url",
            "image_hash",
            "id",
            "actor_id",
            "name",
            "url_tags",
            "link_og_id",
            "video_id",
            "call_to_action_type",
            "template_url_spec",
            "status",
            "template_url",
            "title",
            "thumbnail_url",
            "image_url",
            "object_story_spec",
            "account_id",
            "object_type",
            "object_id",
            "instagram_permalink_url",
        ],
    },
    {
        "task_id": "sync_ads",
        "stream_name": "ads",
        "stream_type": "metadata",
        "table_name": "ads",
        "primary_key": "id",
        "fields": [
            "tracking_specs",
            "adlabels",
            "effective_status",
            "bid_info",
            "targeting",
            "recommendations",
            "campaign_id",
            "conversion_specs",
            "source_ad_id",
            "updated_time",
            "bid_amount",
            "id",
            "adset_id",
            "bid_type",
            "name",
            "created_time",
            "status",
            "last_updated_by_app_id",
            "account_id",
            "creative",
        ],
    },
    {
        "task_id": "sync_adsets",
        "stream_name": "adsets",
        "stream_type": "metadata",
        "table_name": "adsets",
        "primary_key": "id",
        "fields": [
            "adlabels",
            "effective_status",
            "bid_info",
            "targeting",
            "campaign_id",
            "daily_budget",
            "promoted_object",
            "updated_time",
            "id",
            "name",
            "lifetime_budget",
            "created_time",
            "budget_remaining",
            "end_time",
            "start_time",
            "account_id",
        ],
    },
    {
        "task_id": "sync_campaigns",
        "stream_name": "campaigns",
        "stream_type": "metadata",
        "table_name": "campaigns",
        "primary_key": "id",
        "fields": [
            "adlabels",
            "effective_status",
            "updated_time",
            "id",
            "name",
            "spend_cap",
            "objective",
            "buying_type",
            "start_time",
            "account_id",
            "ads",
        ],
    },
    {
        "task_id": "sync_ads_insights",
        "stream_name": "ads_insights",
        "stream_type": "insights",
        "table_name": "ads_insights",
        "bookmark_field": "date_start",
        "breakdowns": [],
        "fields": INSIGHTS_FIELDS,
    },
    {
        "task_id": "sync_ads_insights_age_and_gender",
        "stream_name": "ads_insights_age_and_gender",
        "stream_type": "insights",
        "table_name": "ads_insights_age_and_gender",
        "bookmark_field": "date_start",
        "breakdowns": ["age", "gender"],
        "fields": INSIGHTS_FIELDS + ["age", "gender"],
    },
    {
        "task_id": "sync_ads_insights_country",
        "stream_name": "ads_insights_country",
        "stream_type": "insights",
        "table_name": "ads_insights_country",
        "bookmark_field": "date_start",
        "breakdowns": ["country"],
        "fields": INSIGHTS_FIELDS + ["country"],
    },
    {
        "task_id": "sync_ads_insights_platform_and_device",
        "stream_name": "ads_insights_platform_and_device",
        "stream_type": "insights",
        "table_name": "ads_insights_platform_and_device",
        "bookmark_field": "date_start",
        "breakdowns": ["publisher_platform", "platform_position", "impression_device"],
        "fields": INSIGHTS_FIELDS
        + ["publisher_platform", "platform_position", "impression_device", "placement"],
    },
    {
        "task_id": "sync_ads_insights_region",
        "stream_name": "ads_insights_region",
        "stream_type": "insights",
        "table_name": "ads_insights_region",
        "bookmark_field": "date_start",
        "breakdowns": ["region"],
        "fields": INSIGHTS_FIELDS + ["region"],
    },
    {
        "task_id": "sync_ads_insights_dma",
        "stream_name": "ads_insights_dma",
        "stream_type": "insights",
        "table_name": "ads_insights_dma",
        "bookmark_field": "date_start",
        "breakdowns": ["dma"],
        "fields": INSIGHTS_FIELDS + ["dma"],
    },
]


def meta_url(path: str) -> str:
    return f"{META_API_BASE_URL}/{META_API_VERSION}/{path}"


def get_access_token() -> str:
    return Variable.get(ACCESS_TOKEN_VAR_NAME)


def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def get_bookmarks() -> dict[str, dict[str, str]]:
    raw = Variable.get(STATE_VAR_NAME, default_var="{}")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def set_bookmark(stream_name: str, bookmark_field: str, value: str) -> None:
    bookmarks = get_bookmarks()
    bookmarks.setdefault(stream_name, {})
    bookmarks[stream_name][bookmark_field] = value
    Variable.set(STATE_VAR_NAME, json.dumps(bookmarks))


def parse_iso_date(value: str) -> datetime.date:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).date()


def resolve_insights_window(stream_name: str) -> tuple[datetime.date, datetime.date, bool]:
    context = get_current_context()
    ds = datetime.fromisoformat(context["ds"]).date()
    conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    full_refresh = bool(conf.get("full_refresh", False))

    if conf.get("start_date"):
        start_date = datetime.fromisoformat(conf["start_date"]).date()
    else:
        start_date = ds - timedelta(days=int(conf.get("lookback_days", DEFAULT_LOOKBACK_DAYS)))

    end_date = (
        datetime.fromisoformat(conf["end_date"]).date()
        if conf.get("end_date")
        else ds
    )

    if not full_refresh and not conf.get("start_date"):
        bookmark = get_bookmarks().get(stream_name, {}).get("date_start")
        if bookmark:
            start_date = max(start_date, parse_iso_date(bookmark))

    return start_date, end_date, full_refresh


def graph_get(path: str, params: dict[str, Any]) -> dict[str, Any]:
    response = requests.get(
        meta_url(path),
        params={**params, "access_token": get_access_token()},
        timeout=120,
    )
    response.raise_for_status()
    return response.json()


def graph_post(path: str, params: dict[str, Any]) -> dict[str, Any]:
    response = requests.post(
        meta_url(path),
        data={**params, "access_token": get_access_token()},
        timeout=120,
    )
    response.raise_for_status()
    return response.json()


def paginate(url: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    next_url = url
    next_params = params

    while next_url:
        response = requests.get(next_url, params=next_params, timeout=120)
        response.raise_for_status()
        payload = response.json()
        rows.extend(payload.get("data", []))
        next_url = payload.get("paging", {}).get("next")
        next_params = None

    return rows


def fetch_metadata_rows(stream: dict[str, Any]) -> list[dict[str, Any]]:
    account_path = f"act_{META_ACCOUNT_ID}/{stream['stream_name']}"
    params = {
        "fields": ",".join(stream["fields"]),
        "limit": 100,
    }
    rows = paginate(meta_url(account_path), params)
    ingested_at = datetime.now(UTC).isoformat()
    for row in rows:
        row["_ingested_at"] = ingested_at
        row["_stream_name"] = stream["stream_name"]
    return rows


def wait_for_insights_job(report_run_id: str) -> None:
    started_at = time.monotonic()
    sleep_seconds = 10

    while True:
        payload = graph_get(
            report_run_id,
            {"fields": "async_status,async_percent_completion"},
        )
        status = payload.get("async_status", "Unknown")
        percent = int(payload.get("async_percent_completion", 0) or 0)

        if status in {"Job Completed", "COMPLETED"}:
            return

        if status in {"Job Failed", "FAILED"}:
            raise RuntimeError(f"Insights job failed: {report_run_id}")

        elapsed = time.monotonic() - started_at
        if elapsed > INSIGHTS_JOB_TIMEOUT_SECONDS:
            raise TimeoutError(
                f"Insights job {report_run_id} did not complete after "
                f"{INSIGHTS_JOB_TIMEOUT_SECONDS} seconds. Last status: {status} ({percent}%)."
            )

        time.sleep(sleep_seconds)
        sleep_seconds = min(sleep_seconds * 2, 300)


def fetch_async_insights(stream: dict[str, Any], day: datetime.date) -> list[dict[str, Any]]:
    account_path = f"act_{META_ACCOUNT_ID}/insights"
    params = {
        "async": "true",
        "level": "ad",
        "fields": ",".join(stream["fields"]),
        "breakdowns": ",".join(stream.get("breakdowns", [])),
        "action_breakdowns": ",".join(ACTION_BREAKDOWNS),
        "action_attribution_windows": json.dumps(ACTION_ATTRIBUTION_WINDOWS),
        "time_increment": 1,
        "limit": 100,
        "time_range": json.dumps({"since": day.isoformat(), "until": day.isoformat()}),
    }

    params = {key: value for key, value in params.items() if value not in {"", "[]"}}
    report = graph_post(account_path, params)
    report_run_id = report["report_run_id"]
    wait_for_insights_job(report_run_id)

    rows = paginate(meta_url(f"{report_run_id}/insights"))
    ingested_at = datetime.now(UTC).isoformat()
    for row in rows:
        row["_ingested_at"] = ingested_at
        row["_stream_name"] = stream["stream_name"]
        row["_window_date"] = day.isoformat()
    return rows


def delete_table_range(
    client: bigquery.Client,
    table_name: str,
    start_date: datetime.date,
    end_date: datetime.date,
) -> None:
    table_ref = f"{PROJECT_ID}.{BQ_DATASET}.{table_name}"
    query = f"""
    DELETE FROM `{table_ref}`
    WHERE DATE(date_start) BETWEEN @start_date AND @end_date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date.isoformat()),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date.isoformat()),
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
    except NotFound:
        return


def load_rows(
    client: bigquery.Client,
    table_name: str,
    rows: list[dict[str, Any]],
    write_disposition: str,
) -> None:
    table_ref = f"{PROJECT_ID}.{BQ_DATASET}.{table_name}"
    if not rows and write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE:
        try:
            client.delete_table(table_ref, not_found_ok=True)
        except NotFound:
            return
        return

    if not rows:
        return

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition=write_disposition,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    client.load_table_from_json(rows, table_ref, job_config=job_config).result()


def sync_metadata_stream(stream: dict[str, Any]) -> dict[str, Any]:
    client = get_bq_client()
    rows = fetch_metadata_rows(stream)
    load_rows(
        client=client,
        table_name=f"{BQ_TABLE_PREFIX}_{stream['table_name']}",
        rows=rows,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    return {"stream": stream["stream_name"], "rows": len(rows)}


def sync_insights_stream(stream: dict[str, Any]) -> dict[str, Any]:
    client = get_bq_client()
    start_date, end_date, _ = resolve_insights_window(stream["stream_name"])
    if start_date > end_date:
        return {"stream": stream["stream_name"], "rows": 0}

    all_rows: list[dict[str, Any]] = []
    current_day = start_date
    while current_day <= end_date:
        all_rows.extend(fetch_async_insights(stream, current_day))
        current_day += timedelta(days=1)

    table_name = f"{BQ_TABLE_PREFIX}_{stream['table_name']}"
    delete_table_range(client, table_name, start_date, end_date)
    load_rows(
        client=client,
        table_name=table_name,
        rows=all_rows,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    set_bookmark(stream["stream_name"], stream["bookmark_field"], end_date.isoformat())
    return {"stream": stream["stream_name"], "rows": len(all_rows)}


with DAG(
    dag_id="facebook_ads_to_bigquery",
    description="Extrae Facebook Ads desde Meta API y carga a BigQuery desde Airflow.",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["marketing", "facebook-ads", "bigquery", "stitch-replacement"],
) as dag:
    previous_task = None

    for stream in STREAM_CONFIGS:
        if stream["stream_type"] == "metadata":
            current_task = task(task_id=stream["task_id"])(sync_metadata_stream)(stream)
        else:
            current_task = task(task_id=stream["task_id"])(sync_insights_stream)(stream)

        if previous_task is not None:
            previous_task >> current_task

        previous_task = current_task
