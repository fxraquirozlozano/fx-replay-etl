from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG

from fxreplay_prod_mysql_to_bigquery import DAG_TIMEZONE, build_table_tasks


with DAG(
    dag_id="fxreplay_prod_mysql_to_bigquery_backfill",
    description=(
        "Backfill manual para tablas de fxreplay_prod hacia BigQuery raw y final. "
        "Soporta dag_run.conf con start_timestamp y end_timestamp."
    ),
    start_date=datetime(2024, 1, 1, 5, 0, tzinfo=ZoneInfo(DAG_TIMEZONE)),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["mysql", "bigquery", "fxreplay-prod", "raw", "backfill"],
) as dag:
    build_table_tasks()
