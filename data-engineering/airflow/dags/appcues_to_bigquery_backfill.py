"""
appcues_to_bigquery_backfill.py

Manual DAG to backfill `appcues.responses` over a historical window.

Reuses the same building blocks as the daily DAG (`appcues_to_bigquery.py`)
through `appcues_lib.py`, so rows written here are byte-identical to what the
daily DAG produces for the same window.

Trigger from the Airflow UI with a config JSON like:
    {"start_date": "2024-01-01", "end_date": "2026-07-31"}

If `end_date` is omitted it defaults to yesterday. `start_date` is required.

Layout
------
Each calendar month is a separate task instance via dynamic task mapping.
One failing month can be retried without redoing the others. Chunks run
sequentially (max_active_tis_per_dag=1) so we do not overload the Appcues
export endpoint. A final cleanup task drops the staging table regardless
of whether any chunk failed.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from airflow import DAG
try:
    from airflow.sdk import task
except ImportError:
    from airflow.decorators import task
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule

from appcues_lib import (
    DEFAULT_STAGING_TABLE,
    drop_table,
    get_bq_client,
    month_chunks,
    run_chunk,
)
from appcues_to_bigquery import (
    BQ_TABLE,
    FINAL_BQ_DATASET,
    PROJECT_ID,
    RAW_BQ_DATASET,
    _build_config,
)


def _parse_date(value: str | None) -> date | None:
    if value in (None, ""):
        return None
    return datetime.fromisoformat(value).date()


@task
def plan_chunks(**context) -> list[dict[str, str]]:
    """
    Validates params and expands the requested window into monthly chunks.
    Returns a list of dicts because dynamic task mapping requires JSON-safe
    values.
    """
    params = context["params"]

    start = _parse_date(params.get("start_date"))
    if start is None:
        raise ValueError("start_date is required (YYYY-MM-DD).")

    end = _parse_date(params.get("end_date")) or (date.today() - timedelta(days=1))
    if start > end:
        raise ValueError(f"start_date ({start}) must be <= end_date ({end}).")

    chunks = [
        {"start": s.isoformat(), "end": e.isoformat()}
        for s, e in month_chunks(start, end)
    ]
    print(f"Planned {len(chunks)} monthly chunks for {start} .. {end}")
    for i, c in enumerate(chunks, 1):
        print(f"  [{i:>3}/{len(chunks)}] {c['start']} .. {c['end']}")
    return chunks


@task(
    max_active_tis_per_dag=1,  # sequential chunks within a DAG run
    retries=2,
    retry_delay=timedelta(minutes=10),
)
def run_chunk_task(chunk: dict[str, str]) -> int:
    """Executes a single monthly chunk: export -> download -> load -> merge."""
    config = _build_config()
    client = get_bq_client(PROJECT_ID)

    staging_table_ref = f"{PROJECT_ID}.{RAW_BQ_DATASET}.{DEFAULT_STAGING_TABLE}"
    final_table_ref = f"{PROJECT_ID}.{FINAL_BQ_DATASET}.{BQ_TABLE}"

    chunk_start = date.fromisoformat(chunk["start"])
    chunk_end = date.fromisoformat(chunk["end"])

    return run_chunk(
        config,
        client,
        chunk_start,
        chunk_end,
        staging_table_ref,
        final_table_ref,
    )


@task(trigger_rule=TriggerRule.ALL_DONE)
def cleanup_staging(**context) -> None:
    """
    Drops the staging table. Runs even if some chunks failed so the raw
    dataset does not accumulate orphan tables. Skipped if the trigger
    config sets keep_staging=true.
    """
    params = context["params"]
    if params.get("keep_staging"):
        print("keep_staging=true, leaving staging table in place.")
        return

    client = get_bq_client(PROJECT_ID)
    staging_table_ref = f"{PROJECT_ID}.{RAW_BQ_DATASET}.{DEFAULT_STAGING_TABLE}"
    print(f"Dropping staging table {staging_table_ref}")
    drop_table(client, staging_table_ref)


with DAG(
    dag_id="appcues_to_bigquery_backfill",
    description=(
        "Manual backfill of appcues.responses. Trigger with "
        '{"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}. '
        "One task instance per calendar month."
    ),
    schedule=None,  # manual trigger only
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    params={
        "start_date": Param(
            None,
            type=["string", "null"],
            format="date",
            title="Start date",
            description="Window start (inclusive), YYYY-MM-DD. Required.",
        ),
        "end_date": Param(
            None,
            type=["string", "null"],
            format="date",
            title="End date",
            description="Window end (inclusive), YYYY-MM-DD. Defaults to yesterday.",
        ),
        "keep_staging": Param(
            False,
            type="boolean",
            title="Keep staging table",
            description="Do not drop the staging table when the run finishes.",
        ),
    },
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
    },
    tags=["appcues", "surveys", "backfill", "bigquery"],
) as dag:
    chunks = plan_chunks()
    chunk_results = run_chunk_task.expand(chunk=chunks)
    chunk_results >> cleanup_staging()
