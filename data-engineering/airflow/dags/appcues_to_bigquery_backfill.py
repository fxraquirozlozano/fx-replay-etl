from __future__ import annotations

import os
from datetime import UTC, date, datetime, timedelta
from typing import Any

from airflow import DAG
try:
    from airflow.sdk import get_current_context, task
except ImportError:
    from airflow.decorators import task
    try:
        from airflow.operators.python import get_current_context
    except ImportError:
        from airflow.decorators import get_current_context
from google.cloud import bigquery

from appcues_to_bigquery import (
    BQ_TABLE,
    FINAL_BQ_DATASET,
    PROJECT_ID,
    RAW_BQ_DATASET,
    airflow_var,
    download_export,
    extract_rows_from_export,
    get_bq_client,
    load_rows,
    merge_raw_into_history,
    resolve_download_url,
    submit_export_job,
    wait_for_export_job,
)


BACKFILL_BQ_TABLE = airflow_var("APPCUES_BACKFILL_BQ_TABLE", "nps_events_backfill")


def parse_date(value: str) -> date:
    return datetime.fromisoformat(value).date()


def iter_days(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def resolve_backfill_window() -> tuple[date, date]:
    context = get_current_context()
    ds = datetime.fromisoformat(context["ds"]).date()
    conf = (context.get("dag_run") and context["dag_run"].conf) or {}

    start_date = parse_date(conf["start_date"]) if conf.get("start_date") else ds
    end_date = parse_date(conf["end_date"]) if conf.get("end_date") else start_date
    if start_date > end_date:
        raise ValueError("start_date must be before or equal to end_date.")

    return start_date, end_date


def process_backfill_day(
    client: bigquery.Client,
    process_date: date,
    raw_table_ref: str,
    final_table_ref: str,
) -> dict[str, Any]:
    job_url = submit_export_job(process_date, process_date)
    job_payload = wait_for_export_job(job_url)
    download_url = resolve_download_url(job_payload)
    export_file_path = download_export(download_url)

    try:
        rows = extract_rows_from_export(
            export_file_path,
            process_date,
            process_date,
            process_date,
        )
    finally:
        if os.path.exists(export_file_path):
            os.remove(export_file_path)

    load_rows(
        client,
        raw_table_ref,
        rows,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    if rows:
        merge_raw_into_history(client, raw_table_ref, final_table_ref)

    return {"date": process_date.isoformat(), "rows": len(rows)}


def run_appcues_daily_backfill() -> dict[str, Any]:
    client = get_bq_client()
    start_date, end_date = resolve_backfill_window()
    raw_table_ref = f"{PROJECT_ID}.{RAW_BQ_DATASET}.{BACKFILL_BQ_TABLE}"
    final_table_ref = f"{PROJECT_ID}.{FINAL_BQ_DATASET}.{BQ_TABLE}"
    results = [
        process_backfill_day(client, process_date, raw_table_ref, final_table_ref)
        for process_date in iter_days(start_date, end_date)
    ]

    return {
        "raw_table": raw_table_ref,
        "final_table": final_table_ref,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "days": len(results),
        "rows": sum(int(result["rows"]) for result in results),
        "results": results,
    }


with DAG(
    dag_id="appcues_to_bigquery_backfill",
    description="Backfill diario bajo demanda de eventos NPS desde Appcues hacia BigQuery.",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 0,
    },
    tags=["appcues", "nps", "bigquery", "backfill"],
) as dag:
    task(task_id="run_appcues_daily_backfill")(run_appcues_daily_backfill)()
