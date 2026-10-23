"""
typeform_to_bigquery_backfill.py

Backfill on-demand de respuestas Typeform -> BigQuery.

A diferencia del daily, ignora el watermark y trae TODO el historico
de los forms indicados (o de todos los tracked si no se especifica).

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
    DATASET,
    DEFINITIONS_SCHEMA,
    DEFINITIONS_TABLE,
    PROJECT_ID,
    RESPONSES_RAW_TABLE,
    RESPONSES_SCHEMA,
    build_form_definition_rows,
    build_response_row,
    fetch_form_definition,
    fetch_form_responses,
    get_bq_client,
    get_forms_to_process,
    get_typeform_token,
    load_ndjson_uri,
    merge_responses_raw_into_final,
    stage_rows_to_gcs,
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
    loaded_at = datetime.now(UTC).isoformat()
    snapshot_at = loaded_at

    raw_ref = f"{PROJECT_ID}.{DATASET}.{RESPONSES_RAW_TABLE}"
    defs_ref = f"{PROJECT_ID}.{DATASET}.{DEFINITIONS_TABLE}"

    forms = _resolve_backfill_forms(client)
    all_resp_rows: list[dict[str, Any]] = []
    all_def_rows: list[dict[str, Any]] = []
    per_form: list[dict[str, Any]] = []

    for form in forms:
        form_id = form["form_id"]

        definition = fetch_form_definition(form_id, token)
        all_def_rows.extend(build_form_definition_rows(definition, snapshot_at))

        # completed_since=None -> trae TODO el historico (completas + parciales)
        items = fetch_form_responses(form_id, token, completed_since=None)
        resp_rows = [build_response_row(it, form_id, loaded_at) for it in items]
        all_resp_rows.extend(resp_rows)
        per_form.append({"form_id": form_id, "fetched": len(resp_rows)})

    defs_uri = stage_rows_to_gcs(all_def_rows, "backfill_form_definitions")
    if defs_uri:
        load_ndjson_uri(
            client, defs_ref, DEFINITIONS_SCHEMA, defs_uri,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

    resp_uri = stage_rows_to_gcs(all_resp_rows, "backfill_responses_raw")
    if resp_uri:
        load_ndjson_uri(
            client, raw_ref, RESPONSES_SCHEMA, resp_uri,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
    else:
        client.query(f"TRUNCATE TABLE `{raw_ref}`").result()

    return {
        "forms_processed": len(forms),
        "definitions_rows": len(all_def_rows),
        "response_rows": len(all_resp_rows),
        "has_new_responses": bool(resp_uri),
        "per_form": per_form,
    }


@task(task_id="backfill_merge_to_final")
def backfill_merge_to_final(ingest_result: dict[str, Any]) -> dict[str, Any]:
    if not ingest_result.get("has_new_responses"):
        return {"merged_rows": 0, "skipped": True}
    client = get_bq_client()
    affected = merge_responses_raw_into_final(client)
    return {"merged_rows": affected, "skipped": False}


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
    backfill_merge_to_final(backfill_ingest_forms())
