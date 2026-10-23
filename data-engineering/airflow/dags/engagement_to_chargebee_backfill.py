from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowSkipException

try:
    from airflow.sdk import get_current_context, task
except ImportError:  # pragma: no cover
    from airflow.decorators import task
    try:
        from airflow.operators.python import get_current_context
    except ImportError:
        from airflow.decorators import get_current_context

from engagement_to_chargebee import (
    MAX_RUNTIME_MINUTES,
    ExtractSkipped,
    extract_users_to_gcs,
    persist_failures,
    resolve_target_month,
    sync_to_chargebee,
)


def _run_id_from_context() -> str:
    ctx = get_current_context()
    dag_run = ctx.get("dag_run")
    if dag_run and getattr(dag_run, "run_id", None):
        return dag_run.run_id
    return datetime.now(UTC).isoformat()


def _target_month() -> Any:
    ctx = get_current_context()
    conf = (ctx.get("dag_run") and ctx["dag_run"].conf) or {}
    return resolve_target_month(conf.get("target_month"))


@task(task_id="extract_users_to_gcs")
def extract_task() -> dict[str, Any]:
    try:
        return extract_users_to_gcs(_target_month(), _run_id_from_context())
    except ExtractSkipped as exc:
        raise AirflowSkipException(str(exc)) from exc


@task(task_id="sync_to_chargebee", execution_timeout=timedelta(minutes=MAX_RUNTIME_MINUTES + 30))
def sync_task(extract_result: dict[str, Any]) -> dict[str, Any]:
    return sync_to_chargebee(extract_result, _run_id_from_context())


@task(task_id="persist_failures")
def persist_task(sync_result: dict[str, Any]) -> dict[str, Any]:
    return persist_failures(sync_result)


with DAG(
    dag_id="engagement_to_chargebee_backfill",
    description="On-demand backfill of the BQ -> Chargebee sync for a specific month.",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 0,
    },
    tags=["chargebee", "engagement", "backfill"],
) as dag:
    persist_task(sync_task(extract_task()))
