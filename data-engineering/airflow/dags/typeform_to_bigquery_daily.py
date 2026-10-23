"""
typeform_to_bigquery_daily.py

DAG diario incremental Typeform -> BigQuery.

Flujo:
  1. discover_and_track : GET /forms + filtro de dias -> upsert tracked_forms
  2. ingest_forms       : por cada form activo, watermark + since,
                          stage NDJSON a GCS, carga raw efimera (WRITE_TRUNCATE)
  3. merge_to_final     : MERGE raw -> responses (dedup por response_id)

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

from google.cloud import bigquery

from typeform_to_bigquery import (
    DATASET,
    DEFINITIONS_SCHEMA,
    DEFINITIONS_TABLE,
    DISCOVERY_SINCE_DAYS,
    PROJECT_ID,
    RESPONSES_RAW_TABLE,
    RESPONSES_SCHEMA,
    SCHEDULE,
    build_form_definition_rows,
    build_response_row,
    discover_active_forms,
    fetch_form_definition,
    fetch_form_responses,
    get_bq_client,
    get_form_watermark,
    get_forms_to_process,
    get_typeform_token,
    load_ndjson_uri,
    merge_responses_raw_into_final,
    stage_rows_to_gcs,
    upsert_tracked_forms,
)


@task(task_id="discover_and_track")
def discover_and_track() -> dict[str, Any]:
    token = get_typeform_token()
    client = get_bq_client()
    discovered = discover_active_forms(token, DISCOVERY_SINCE_DAYS)
    inserted = upsert_tracked_forms(client, discovered)
    return {"discovered": len(discovered), "newly_tracked": inserted}


@task(task_id="ingest_forms")
def ingest_forms(_discovery: dict[str, Any]) -> dict[str, Any]:
    token = get_typeform_token()
    client = get_bq_client()
    loaded_at = datetime.now(UTC).isoformat()
    snapshot_at = loaded_at

    raw_ref = f"{PROJECT_ID}.{DATASET}.{RESPONSES_RAW_TABLE}"
    defs_ref = f"{PROJECT_ID}.{DATASET}.{DEFINITIONS_TABLE}"

    forms = get_forms_to_process(client)
    all_resp_rows: list[dict[str, Any]] = []
    all_def_rows: list[dict[str, Any]] = []
    per_form: list[dict[str, Any]] = []

    for form in forms:
        form_id = form["form_id"]

        # Snapshot de la definicion del form
        definition = fetch_form_definition(form_id, token)
        all_def_rows.extend(build_form_definition_rows(definition, snapshot_at))

        # Completas: incremental con watermark. Parciales: full cada corrida.
        watermark = get_form_watermark(client, form_id)
        items = fetch_form_responses(form_id, token, completed_since=watermark)
        resp_rows = [build_response_row(it, form_id, loaded_at) for it in items]
        all_resp_rows.extend(resp_rows)

        per_form.append({
            "form_id": form_id,
            "since": watermark.isoformat() if watermark else None,
            "fetched": len(resp_rows),
        })

    # Definiciones: append (snapshot historico)
    defs_uri = stage_rows_to_gcs(all_def_rows, "form_definitions")
    if defs_uri:
        load_ndjson_uri(
            client, defs_ref, DEFINITIONS_SCHEMA, defs_uri,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

    # Respuestas: a la raw efimera (TRUNCATE: solo lo de esta corrida)
    resp_uri = stage_rows_to_gcs(all_resp_rows, "responses_raw")
    if resp_uri:
        load_ndjson_uri(
            client, raw_ref, RESPONSES_SCHEMA, resp_uri,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
    else:
        # Sin respuestas nuevas: vaciar la raw para que el MERGE no re-aplique lo viejo
        client.query(f"TRUNCATE TABLE `{raw_ref}`").result()

    return {
        "forms_processed": len(forms),
        "definitions_rows": len(all_def_rows),
        "response_rows": len(all_resp_rows),
        "has_new_responses": bool(resp_uri),
        "per_form": per_form,
    }


@task(task_id="merge_to_final")
def merge_to_final(ingest_result: dict[str, Any]) -> dict[str, Any]:
    if not ingest_result.get("has_new_responses"):
        return {"merged_rows": 0, "skipped": True}
    client = get_bq_client()
    affected = merge_responses_raw_into_final(client)
    return {"merged_rows": affected, "skipped": False}


with DAG(
    dag_id="typeform_to_bigquery_daily",
    description="Ingesta diaria incremental de respuestas Typeform hacia BigQuery.",
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
    merge_to_final(ingested)
