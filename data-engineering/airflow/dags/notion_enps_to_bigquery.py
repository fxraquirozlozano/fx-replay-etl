"""
notion_enps_to_bigquery.py

Modulo base de la integracion Notion (eNPS) -> BigQuery.
Sigue el patron del repo: la logica vive aca y el DAG la importa.

Diseno:
  - Fuente: Notion database "eNPS" (todas las respuestas de encuestas eNPS).
  - Incremental por watermark en `last_edited_time` (mismo patron que el DAG
    de Typeform). El filtro es server-side en la Notion API. La primera
    corrida detecta que la tabla esta vacia y hace un backfill total.
  - Notion incluye en el fetch tanto respuestas nuevas como respuestas
    editadas despues del watermark, asi que edits en Feedback/Category/etc
    se reflejan en la proxima corrida via el UPDATE del MERGE.
  - Los borrados en Notion NO se propagan a BQ (a proposito). BQ actua
    como archivo, inmune a borrados accidentales del origen.
  - Derivacion: `Created time` -> `report_name` ("Q2 2026 NPS Report") y
    `report_quarter` ("2026Q2"). Reemplaza el JOIN con la tabla Reports de
    Notion, que no tiene relacion formal con eNPS.
  - Carga: NDJSON -> GCS -> raw efimera (WRITE_TRUNCATE con solo el delta)
    -> MERGE a la final. Si la API falla, la final queda intacta.

Tablas (en fxr-analytics.notion_raw):
  - enps_responses      (final, upsert-only via MERGE sin DELETE)
  - enps_responses_raw  (buffer efimero por corrida, solo lo del delta)
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from datetime import UTC, datetime
from typing import Any

import requests
from google.cloud import bigquery, secretmanager, storage

try:
    from airflow.sdk import Variable
except ImportError:  # pragma: no cover
    from airflow.models import Variable


logger = logging.getLogger(__name__)


# ============================================================
# Config (airflow_var con defaults, igual que los otros DAGs)
# ============================================================

def airflow_var(name: str, default: str) -> str:
    try:
        return Variable.get(name)
    except Exception:
        return default


PROJECT_ID = airflow_var("NOTION_ENPS_GCP_PROJECT_ID", "fxr-analytics")
SCHEDULE = airflow_var("NOTION_ENPS_DAG_SCHEDULE", "0 8 * * 5")  # Viernes 08:00 UTC

DATASET = airflow_var("NOTION_ENPS_BQ_DATASET", "notion_raw")
RESPONSES_TABLE = airflow_var("NOTION_ENPS_RESPONSES_TABLE", "enps_responses")
RESPONSES_RAW_TABLE = airflow_var("NOTION_ENPS_RESPONSES_RAW_TABLE", "enps_responses_raw")

# ID de la Notion database "eNPS". El default es el que ya venis usando en el
# script de test; se sobreescribe con la Variable si hace falta.
ENPS_DATABASE_ID = airflow_var(
    "NOTION_ENPS_DATABASE_ID",
    "2e09db20-2074-8046-8a3d-c88ff4e0dcfc",
)

# Token de Notion en Secret Manager
TOKEN_SECRET_ID = airflow_var("NOTION_ENPS_TOKEN_SECRET_ID", "FXR_NOTION_ENPS_TOKEN")

# GCS staging (mismo bucket que el resto del repo)
GCS_STAGING_BUCKET = airflow_var("NOTION_ENPS_GCS_STAGING_BUCKET", "fx-replay-etl")
GCS_STAGING_PREFIX = airflow_var("NOTION_ENPS_GCS_STAGING_PREFIX", "tmp/notion_enps_to_bigquery")

# API
BASE_URL = airflow_var("NOTION_API_BASE_URL", "https://api.notion.com/v1")
NOTION_VERSION = airflow_var("NOTION_API_VERSION", "2022-06-28")
PAGE_SIZE = int(airflow_var("NOTION_ENPS_PAGE_SIZE", "100"))  # 100 es el max de Notion
REQUEST_TIMEOUT_SECONDS = int(airflow_var("NOTION_REQUEST_TIMEOUT_SECONDS", "60"))
RATE_LIMIT_SLEEP = float(airflow_var("NOTION_RATE_LIMIT_SLEEP", "0.35"))  # ~3 req/s
MAX_RETRIES = int(airflow_var("NOTION_MAX_RETRIES", "5"))


# ============================================================
# Clients
# ============================================================

def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def get_gcs_client() -> storage.Client:
    return storage.Client(project=PROJECT_ID)


def get_secret_value(secret_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
    try:
        response = client.access_secret_version(request={"name": secret_path})
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            f"No se pudo leer el secret '{secret_id}'. Otorga "
            f"'roles/secretmanager.secretAccessor' a la service account del worker."
        ) from exc
    return response.payload.data.decode("utf-8").strip()


def get_notion_token() -> str:
    return get_secret_value(TOKEN_SECRET_ID)


# ============================================================
# Notion API
# ============================================================

def _auth_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def post_with_retry(url: str, token: str, body: dict[str, Any]) -> dict:
    headers = _auth_headers(token)
    last_error: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(
                url, headers=headers, json=body,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            last_error = exc
            time.sleep((attempt + 1) * 2)
            continue
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in (429, 500, 502, 503, 504):
            logger.warning("Notion %s en %s, reintentando", resp.status_code, url)
            time.sleep((attempt + 1) * 2)
            continue
        raise RuntimeError(f"HTTP {resp.status_code} en {url}: {resp.text}")
    raise RuntimeError(f"Reintentos agotados para {url}: {last_error}")


def fetch_database_pages(
    database_id: str,
    token: str,
    since_last_edited: datetime | None = None,
) -> list[dict[str, Any]]:
    """Query completa (paginado) de una Notion database.

    Si se pasa `since_last_edited`, aplica un filtro server-side por
    `last_edited_time >= since_last_edited`. Notion incluye en el resultado
    tanto respuestas nuevas como respuestas editadas despues del watermark.
    """
    url = f"{BASE_URL}/databases/{database_id}/query"
    all_pages: list[dict[str, Any]] = []
    cursor: str | None = None
    while True:
        body: dict[str, Any] = {"page_size": PAGE_SIZE}
        if since_last_edited:
            body["filter"] = {
                "timestamp": "last_edited_time",
                "last_edited_time": {"on_or_after": since_last_edited.isoformat()},
            }
        if cursor:
            body["start_cursor"] = cursor
        data = post_with_retry(url, token, body)
        all_pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
        time.sleep(RATE_LIMIT_SLEEP)
    return all_pages


# ============================================================
# Transformaciones a filas BQ
# ============================================================

def flatten_property(prop: dict[str, Any]) -> Any:
    """Aplana una property de Notion a un valor escalar (o None).
    Version portada del `plano()` del script de test."""
    t = prop["type"]
    v = prop.get(t)
    if t in ("title", "rich_text"):
        return "".join(x.get("plain_text", "") for x in (v or []))
    if t == "number":
        return v
    if t in ("select", "status"):
        return v["name"] if v else None
    if t == "multi_select":
        return ", ".join(x["name"] for x in (v or []))
    if t in ("created_time", "last_edited_time"):
        return v
    if t == "date":
        return v["start"] if v else None
    if t == "formula":
        return v.get(v["type"]) if v else None
    if t == "rollup":
        return v.get(v["type"]) if v else None
    if t == "people":
        return ", ".join(x.get("name", "") for x in (v or []))
    if t == "relation":
        return ", ".join(x["id"] for x in (v or []))
    if t == "checkbox":
        return v
    if t in ("url", "email", "phone_number"):
        return v
    return v


def derive_report_labels(iso_str: str | None) -> tuple[str | None, str | None]:
    """Deriva (report_name, report_quarter) desde un ISO timestamp.

    Ej: '2026-05-11T15:11:00.000Z' -> ('Q2 2026 NPS Report', '2026Q2').
    El report_name matchea exactamente los nombres de la Notion DB 'eNPS Reports',
    por si en el futuro hace falta joinear.
    """
    if not iso_str:
        return None, None
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    quarter = (dt.month - 1) // 3 + 1
    return f"Q{quarter} {dt.year} NPS Report", f"{dt.year}Q{quarter}"


def build_response_row(page: dict[str, Any], loaded_at: str) -> dict[str, Any]:
    props = page.get("properties", {})
    flat = {k: flatten_property(p) for k, p in props.items()}
    created_time = flat.get("Created time")
    report_name, report_quarter = derive_report_labels(created_time)
    return {
        "notion_page_id": page["id"],
        "team": flat.get("Team"),
        "created_time": created_time,
        "category": flat.get("Category"),
        "score": flat.get("Score"),
        "employee_type": flat.get("Employee Type"),
        "feedback": flat.get("Feedback"),
        "report_name": report_name,
        "report_quarter": report_quarter,
        "last_edited_time": page.get("last_edited_time"),
        "_loaded_at": loaded_at,
    }


# ============================================================
# GCS staging + carga BQ
# ============================================================

def _blob_name(task_tag: str) -> str:
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f")
    return f"{GCS_STAGING_PREFIX}/{task_tag}/{ts}.jsonl"


def _stage_ndjson_to_gcs(ndjson_text: str, blob_name: str) -> str:
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8",
                                     suffix=".jsonl", delete=False) as tmp:
        file_path = tmp.name
        tmp.write(ndjson_text)
    try:
        client = get_gcs_client()
        bucket = client.bucket(GCS_STAGING_BUCKET)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(file_path, content_type="application/x-ndjson")
        return f"gs://{GCS_STAGING_BUCKET}/{blob_name}"
    finally:
        if os.path.exists(file_path):
            os.unlink(file_path)


def stage_rows_to_gcs(rows: list[dict[str, Any]], task_tag: str) -> str | None:
    if not rows:
        return None
    ndjson = "\n".join(json.dumps(r, separators=(",", ":"), default=str) for r in rows)
    return _stage_ndjson_to_gcs(ndjson, _blob_name(task_tag))


def load_ndjson_uri(
    client: bigquery.Client,
    table_ref: str,
    schema: list[bigquery.SchemaField],
    source_uri: str,
    write_disposition: str = bigquery.WriteDisposition.WRITE_TRUNCATE,
) -> None:
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=write_disposition,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    client.load_table_from_uri(source_uri, table_ref, job_config=job_config).result()


# Schema explicito (debe matchear la tabla en BQ)
RESPONSES_SCHEMA = [
    bigquery.SchemaField("notion_page_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("team", "STRING"),
    bigquery.SchemaField("created_time", "TIMESTAMP"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("score", "INTEGER"),
    bigquery.SchemaField("employee_type", "STRING"),
    bigquery.SchemaField("feedback", "STRING"),
    bigquery.SchemaField("report_name", "STRING"),
    bigquery.SchemaField("report_quarter", "STRING"),
    bigquery.SchemaField("last_edited_time", "TIMESTAMP"),
    bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="REQUIRED"),
]


def get_enps_watermark(client: bigquery.Client) -> datetime | None:
    """Devuelve MAX(last_edited_time) de la tabla final, o None si no existe
    o esta vacia. Sirve para arrancar la primera corrida como backfill total
    (sin filtro) y desde ahi ir incremental."""
    from google.api_core.exceptions import NotFound
    final_ref = f"{PROJECT_ID}.{DATASET}.{RESPONSES_TABLE}"
    query = f"SELECT MAX(last_edited_time) AS w FROM `{final_ref}`"
    try:
        result = list(client.query(query).result())
    except NotFound:
        return None
    if not result or result[0]["w"] is None:
        return None
    return result[0]["w"]


def merge_responses_raw_into_final(client: bigquery.Client) -> int:
    """MERGE incremental de la raw efimera a la final.

    - MATCHED: update (respuestas editadas en Notion desde el ultimo run).
    - NOT MATCHED (nuevas en Notion): insert.

    Sin `WHEN NOT MATCHED BY SOURCE THEN DELETE`: los borrados en Notion NO
    se propagan (a proposito). BQ se comporta como archivo append-mostly.

    El ROW_NUMBER es defensivo (no deberian haber duplicados de notion_page_id
    en la raw, pero por las dudas gana el _loaded_at mas nuevo).
    """
    raw_ref = f"{PROJECT_ID}.{DATASET}.{RESPONSES_RAW_TABLE}"
    final_ref = f"{PROJECT_ID}.{DATASET}.{RESPONSES_TABLE}"
    merge_sql = f"""
    MERGE `{final_ref}` T
    USING (
      SELECT * EXCEPT(rn) FROM (
        SELECT *, ROW_NUMBER() OVER (
          PARTITION BY notion_page_id ORDER BY _loaded_at DESC
        ) AS rn
        FROM `{raw_ref}`
      ) WHERE rn = 1
    ) S
    ON T.notion_page_id = S.notion_page_id
    WHEN MATCHED THEN UPDATE SET
      team = S.team,
      created_time = S.created_time,
      category = S.category,
      score = S.score,
      employee_type = S.employee_type,
      feedback = S.feedback,
      report_name = S.report_name,
      report_quarter = S.report_quarter,
      last_edited_time = S.last_edited_time,
      _loaded_at = S._loaded_at
    WHEN NOT MATCHED THEN
      INSERT ROW
    """
    job = client.query(merge_sql)
    job.result()
    return job.num_dml_affected_rows or 0
