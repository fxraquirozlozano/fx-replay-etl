"""
typeform_to_bigquery_daily.py

DAG diario de relectura historica Typeform -> BigQuery.

Flujo:
  1. discover_and_track : GET /forms + filtro de dias -> upsert tracked_forms
  2. ingest_forms       : historico completo por cada form activo,
                          stage NDJSON a GCS, carga raw efimera (WRITE_TRUNCATE)
  3. merge_to_final     : MERGE raw -> responses (dedup por response_id)
  4. validate_ingestion : falla si hubo forms inaccesibles, DESPUES de la carga

Las definiciones de form se appendean en el paso 2 (snapshot por corrida).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from airflow import DAG

try:
    from airflow.sdk import task
except ImportError:  # pragma: no cover
    from airflow.decorators import task

from typeform_to_bigquery import (
    DISCOVERY_SINCE_DAYS,
    SCHEDULE,
    discover_active_forms,
    get_bq_client,
    get_forms_to_process,
    get_typeform_token,
    ingest_forms_to_raw,
    merge_responses_raw_into_final,
    upsert_tracked_forms,
    validate_ingestion_result,
)


@task(task_id="discover_and_track")
def discover_and_track() -> dict[str, Any]:
    token = get_typeform_token()
    client = get_bq_client()
    discovered = discover_active_forms(token, DISCOVERY_SINCE_DAYS)
    changed = upsert_tracked_forms(client, discovered)
    return {"discovered": len(discovered), "tracked_rows_changed": changed}


@task(task_id="ingest_forms")
def ingest_forms(_discovery: dict[str, Any]) -> dict[str, Any]:
    token = get_typeform_token()
    client = get_bq_client()
    forms = get_forms_to_process(client)
    return ingest_forms_to_raw(client, token, forms)


@task(task_id="merge_to_final")
def merge_to_final(ingest_result: dict[str, Any]) -> dict[str, Any]:
    if not ingest_result["has_responses"]:
        return {"merged_rows": 0, "skipped": True}
    client = get_bq_client()
    affected = merge_responses_raw_into_final(client)
    return {"merged_rows": affected, "skipped": False}


@task(task_id="validate_ingestion", retries=0)
def validate_ingestion(
    ingest_result: dict[str, Any], merge_result: dict[str, Any],
) -> dict[str, Any]:
    # merge_result crea la dependencia: nunca fallar por 404 antes del MERGE.
    validate_ingestion_result(ingest_result)
    return merge_result


with DAG(
    dag_id="typeform_to_bigquery_daily",
    description="Relectura diaria historica de respuestas, tags y variables Typeform.",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["typeform", "surveys", "bigquery", "daily"],
) as dag:
    discovery = discover_and_track()
    ingested = ingest_forms(discovery)
    merged = merge_to_final(ingested)
    validate_ingestion(ingested, merged)
