"""
typeform_to_bigquery_backfill.py

Backfill on-demand de respuestas Typeform -> BigQuery.

Trae TODO el historico de los forms indicados (o de todos los tracked activos).
El daily tambien relee el historico; este DAG permite recuperaciones selectivas.
Pausar el daily y esperar sus runs activos antes de usarlo: comparten staging.

Se dispara manualmente. Config via dag_run.conf:
  {
    "form_ids": ["wAx0kFYd", "VIAp41i1"]   # opcional; default = todos los tracked activos
  }
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from airflow import DAG

try:
    from airflow.sdk import get_current_context, task
except ImportError:  # pragma: no cover
    from airflow.decorators import task
    try:
        from airflow.operators.python import get_current_context
    except ImportError:
        from airflow.decorators import get_current_context

from google.cloud import bigquery

from typeform_to_bigquery import (
    get_bq_client,
    get_forms_to_process,
    get_typeform_token,
    ingest_forms_to_raw,
    merge_responses_raw_into_final,
    validate_ingestion_result,
)


def _resolve_backfill_forms(client: bigquery.Client) -> list[dict[str, Any]]:
    context = get_current_context()
    conf = (context.get("dag_run") and context["dag_run"].conf) or {}

    requested_ids = conf.get("form_ids") or []
    if requested_ids:
        tracked = {f["form_id"]: f for f in get_forms_to_process(client)}
        forms = [
            tracked.get(fid, {"form_id": fid, "form_title": ""})
            for fid in requested_ids
        ]
    else:
        forms = get_forms_to_process(client)
    return forms


@task(task_id="backfill_ingest_forms")
def backfill_ingest_forms() -> dict[str, Any]:
    token = get_typeform_token()
    client = get_bq_client()
    forms = _resolve_backfill_forms(client)
    return ingest_forms_to_raw(client, token, forms, task_prefix="backfill_")


@task(task_id="backfill_merge_to_final")
def backfill_merge_to_final(ingest_result: dict[str, Any]) -> dict[str, Any]:
    if not ingest_result["has_responses"]:
        return {"merged_rows": 0, "skipped": True}
    client = get_bq_client()
    affected = merge_responses_raw_into_final(client)
    return {"merged_rows": affected, "skipped": False}


@task(task_id="backfill_validate_ingestion", retries=0)
def backfill_validate_ingestion(
    ingest_result: dict[str, Any], merge_result: dict[str, Any],
) -> dict[str, Any]:
    validate_ingestion_result(ingest_result)
    return merge_result


with DAG(
    dag_id="typeform_to_bigquery_backfill",
    description="Backfill on-demand del historico completo de respuestas Typeform.",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["typeform", "surveys", "bigquery", "backfill"],
) as dag:
    ingested = backfill_ingest_forms()
    merged = backfill_merge_to_final(ingested)
    backfill_validate_ingestion(ingested, merged)
