"""
notion_enps_to_bigquery_weekly.py

DAG semanal incremental Notion (eNPS) -> BigQuery.

Flujo:
  1. ingest_enps    : lee el watermark (MAX last_edited_time en la final),
                      query filtrada a Notion (respuestas nuevas o editadas
                      despues del watermark), transforma cada pagina a fila BQ
                      (con report_name/report_quarter derivados de Created
                      time), stage NDJSON a GCS, carga a la raw efimera
                      (WRITE_TRUNCATE). Si viene 0 filas, es semana sin
                      actividad y salta al skip.
  2. merge_to_final : MERGE raw -> enps_responses (upsert-only, sin DELETE).

Se dispara los viernes 08:00 UTC por default. La encuesta se envia los lunes,
asi que viernes da 4 dias de margen para que las respuestas se carguen en
Notion. La cadencia se puede ajustar por Variable sin tocar codigo.

Trigger manual: se puede correr on-demand desde la UI de Airflow para
forzar una re-sincronizacion; con el watermark en la final, la corrida
manual solo trae el delta desde la ultima corrida exitosa.
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

from notion_enps_to_bigquery import (
    DATASET,
    ENPS_DATABASE_ID,
    PROJECT_ID,
    RESPONSES_RAW_TABLE,
    RESPONSES_SCHEMA,
    SCHEDULE,
    build_response_row,
    fetch_database_pages,
    get_bq_client,
    get_enps_watermark,
    get_notion_token,
    load_ndjson_uri,
    merge_responses_raw_into_final,
    stage_rows_to_gcs,
)


@task(task_id="ingest_enps")
def ingest_enps() -> dict[str, Any]:
    token = get_notion_token()
    client = get_bq_client()
    loaded_at = datetime.now(UTC).isoformat()

    raw_ref = f"{PROJECT_ID}.{DATASET}.{RESPONSES_RAW_TABLE}"

    # Watermark: MAX(last_edited_time) de la tabla final. None = primera
    # corrida (tabla vacia o inexistente) -> backfill total sin filtro.
    watermark = get_enps_watermark(client)

    pages = fetch_database_pages(
        ENPS_DATABASE_ID, token, since_last_edited=watermark,
    )
    rows = [build_response_row(p, loaded_at) for p in pages]

    # Sin filas nuevas: vaciamos la raw y salimos limpios. Es un estado
    # normal (semanas sin actividad no traen nada).
    if not rows:
        client.query(f"TRUNCATE TABLE `{raw_ref}`").result()
        return {
            "database_id": ENPS_DATABASE_ID,
            "watermark": watermark.isoformat() if watermark else None,
            "response_rows": 0,
            "has_new_responses": False,
        }

    resp_uri = stage_rows_to_gcs(rows, "enps_responses_raw")
    load_ndjson_uri(
        client, raw_ref, RESPONSES_SCHEMA, resp_uri,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    return {
        "database_id": ENPS_DATABASE_ID,
        "watermark": watermark.isoformat() if watermark else None,
        "response_rows": len(rows),
        "has_new_responses": True,
    }


@task(task_id="merge_to_final")
def merge_to_final(ingest_result: dict[str, Any]) -> dict[str, Any]:
    if not ingest_result.get("has_new_responses"):
        return {"merged_rows": 0, "skipped": True}
    client = get_bq_client()
    affected = merge_responses_raw_into_final(client)
    return {"merged_rows": affected, "skipped": False}


with DAG(
    dag_id="notion_enps_to_bigquery_weekly",
    description="Ingesta semanal incremental (por last_edited_time) de respuestas eNPS desde Notion hacia BigQuery.",
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
    tags=["notion", "enps", "surveys", "bigquery", "weekly"],
) as dag:
    merge_to_final(ingest_enps())
