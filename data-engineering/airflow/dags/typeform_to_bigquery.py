"""
typeform_to_bigquery.py

Modulo base de la integracion Typeform -> BigQuery.
Sigue el patron del repo: la logica vive aca y los DAGs (daily / backfill)
importan estas funciones.

Diseno:
  - Discovery: GET /forms + filtro por fecha -> upsert en tracked_forms
               (agrega forms nuevos, nunca los saca).
  - Lista a procesar: SELECT de typeform_raw.tracked_forms (is_active = TRUE).
  - Relectura diaria: historico completo de respuestas completas y parciales,
                     incluyendo tags/variables modificados despues del envio.
  - Carga: NDJSON -> GCS -> raw efimera (WRITE_TRUNCATE) -> MERGE a la final
           dedup por response_id (mismo patron que los demas DAGs).

Tablas (en fxr-analytics.typeform_raw):
  - responses          (final, append/merge)
  - responses_raw      (buffer efimero por corrida)
  - form_definitions   (snapshot de fields, append)
  - form_metadata_snapshots (variables y logica, snapshot cuando cambian)
  - tracked_forms      (estado: que forms procesar)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
import time
from datetime import UTC, datetime, timedelta
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


PROJECT_ID = airflow_var("TYPEFORM_GCP_PROJECT_ID", "fxr-analytics")
SCHEDULE = airflow_var("TYPEFORM_DAG_SCHEDULE", "0 7 * * *")  # 07:00 UTC diario

DATASET = airflow_var("TYPEFORM_BQ_DATASET", "typeform_raw")
RESPONSES_TABLE = airflow_var("TYPEFORM_RESPONSES_TABLE", "responses")
RESPONSES_RAW_TABLE = airflow_var("TYPEFORM_RESPONSES_RAW_TABLE", "responses_raw")
DEFINITIONS_TABLE = airflow_var("TYPEFORM_DEFINITIONS_TABLE", "form_definitions")
METADATA_TABLE = airflow_var("TYPEFORM_METADATA_TABLE", "form_metadata_snapshots")
TRACKED_FORMS_TABLE = airflow_var("TYPEFORM_TRACKED_FORMS_TABLE", "tracked_forms")

# Token de Typeform en Secret Manager
TOKEN_SECRET_ID = airflow_var("TYPEFORM_TOKEN_SECRET_ID", "TYPEFORM_TOKEN")

# Discovery
DISCOVERY_SINCE_DAYS = int(airflow_var("TYPEFORM_DISCOVERY_SINCE_DAYS", "365"))
# Forms a forzar inclusion / exclusion (CSV de ids en una Variable, opcional)
FORCE_INCLUDE_IDS = {
    s.strip() for s in airflow_var("TYPEFORM_FORCE_INCLUDE_IDS", "").split(",") if s.strip()
}
EXCLUDE_IDS = {
    s.strip() for s in airflow_var("TYPEFORM_EXCLUDE_IDS", "").split(",") if s.strip()
}

# GCS staging (mismo bucket que el resto del repo)
GCS_STAGING_BUCKET = airflow_var("TYPEFORM_GCS_STAGING_BUCKET", "fx-replay-etl")
GCS_STAGING_PREFIX = airflow_var("TYPEFORM_GCS_STAGING_PREFIX", "tmp/typeform_to_bigquery")

# API
BASE_URL = airflow_var("TYPEFORM_API_BASE_URL", "https://api.typeform.com")
PAGE_SIZE = int(airflow_var("TYPEFORM_PAGE_SIZE", "1000"))
FORMS_PAGE_SIZE = int(airflow_var("TYPEFORM_FORMS_PAGE_SIZE", "200"))
REQUEST_TIMEOUT_SECONDS = int(airflow_var("TYPEFORM_REQUEST_TIMEOUT_SECONDS", "60"))
RATE_LIMIT_SLEEP = float(airflow_var("TYPEFORM_RATE_LIMIT_SLEEP", "0.6"))
MAX_RETRIES = int(airflow_var("TYPEFORM_MAX_RETRIES", "5"))

KNOWN_HIDDEN = ("user_id", "email", "plan_tier")


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


def get_typeform_token() -> str:
    return get_secret_value(TOKEN_SECRET_ID)


# ============================================================
# Typeform API
# ============================================================

def _auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


class TypeformHTTPError(RuntimeError):
    """Error HTTP identificable sin incluir payloads de respuestas en los logs."""

    def __init__(self, status_code: int, url: str):
        self.status_code = status_code
        super().__init__(f"HTTP {status_code} en {url}")


def get_with_retry(url: str, token: str, params: dict | None = None) -> dict:
    headers = _auth_headers(token)
    last_error: Exception | None = None
    for attempt in range(MAX_RETRIES):
        # Aplicar el limite tambien entre forms y endpoints, no solo entre paginas.
        time.sleep(RATE_LIMIT_SLEEP)
        try:
            resp = requests.get(
                url, headers=headers, params=params or {},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            last_error = exc
            time.sleep((attempt + 1) * 2)
            continue
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in (429, 500, 502, 503):
            logger.warning("Typeform %s en %s, reintentando", resp.status_code, url)
            time.sleep((attempt + 1) * 2)
            continue
        raise TypeformHTTPError(resp.status_code, url)
    raise RuntimeError(f"Reintentos agotados para {url}: {last_error}")


def fetch_all_forms(token: str) -> list[dict[str, Any]]:
    """Lista todos los forms de la cuenta (paginado)."""
    all_forms: list[dict[str, Any]] = []
    page = 1
    while True:
        data = get_with_retry(
            f"{BASE_URL}/forms", token,
            {"page": page, "page_size": FORMS_PAGE_SIZE},
        )
        items = data.get("items", [])
        if not items:
            break
        all_forms.extend(items)
        if len(items) < FORMS_PAGE_SIZE:
            break
        page += 1
    return all_forms


def fetch_form_definition(form_id: str, token: str) -> dict[str, Any]:
    return get_with_retry(f"{BASE_URL}/forms/{form_id}", token)


def fetch_responses_since(
    form_id: str,
    token: str,
    response_type: str,
    since: datetime | None,
) -> list[dict[str, Any]]:
    """Trae respuestas de un form para un response_type, opcionalmente desde `since`."""
    url = f"{BASE_URL}/forms/{form_id}/responses"
    all_items: list[dict[str, Any]] = []
    before: str | None = None

    while True:
        params: dict[str, Any] = {
            "page_size": PAGE_SIZE,
            "response_type": response_type,
        }
        if since is not None:
            # Typeform espera ISO 8601 UTC.
            params["since"] = since.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        if before:
            params["before"] = before

        data = get_with_retry(url, token, params)
        items = data.get("items", [])
        if not items:
            break
        all_items.extend(items)
        before = items[-1].get("token")
        if not before or len(items) < PAGE_SIZE:
            break

    return all_items


def fetch_form_responses(
    form_id: str,
    token: str,
    completed_since: datetime | None,
) -> list[dict[str, Any]]:
    """Trae respuestas de un form combinando dos pulls:

      - COMPLETED: historico completo cuando completed_since=None (daily/backfill).
        El filtro opcional se conserva para consumidores que lo necesiten.
      - PARTIAL: full refresh (since=None) en cada corrida. Las parciales son pocas
        en terminos absolutos y MUTAN (una parcial puede completarse despues, pasando
        a 'completed' con el mismo response_id). Trayendolas enteras siempre, el MERGE
        por response_id captura esa transicion y reemplaza la fila vieja.

    Ambos DAGs usan completed_since=None para recuperar cambios historicos.
    """
    # Leer completas despues de parciales para capturar transiciones durante el pull.
    partial = fetch_responses_since(form_id, token, "partial", None)
    completed = fetch_responses_since(form_id, token, "completed", completed_since)
    return completed + partial


# ============================================================
# Discovery + tracked_forms
# ============================================================

def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def discover_active_forms(token: str, since_days: int) -> list[dict[str, Any]]:
    """Forms candidatos: updated en los ultimos `since_days`, salvo EXCLUDE_IDS.
    FORCE_INCLUDE_IDS entran siempre."""
    cutoff = datetime.now(UTC) - timedelta(days=since_days)
    discovered: list[dict[str, Any]] = []
    for f in fetch_all_forms(token):
        form_id = f.get("id")
        if not form_id or form_id in EXCLUDE_IDS:
            continue
        updated_at = _parse_iso(f.get("last_updated_at"))
        recent = updated_at is not None and updated_at >= cutoff
        if recent or form_id in FORCE_INCLUDE_IDS:
            discovered.append({
                "form_id": form_id,
                "form_title": f.get("title") or "",
            })
    return discovered


def upsert_tracked_forms(client: bigquery.Client, discovered: list[dict[str, Any]]) -> int:
    """Inserta forms y actualiza titulos. Preserva first_seen e is_active.

    Devuelve el total de filas insertadas o actualizadas.
    """
    if not discovered:
        return 0
    tracked_ref = f"{PROJECT_ID}.{DATASET}.{TRACKED_FORMS_TABLE}"

    # Los cambios de nombre no deben reactivar forms deshabilitados manualmente.
    rows_json = "\n".join(
        json.dumps({"form_id": d["form_id"], "form_title": d["form_title"]})
        for d in discovered
    )
    tmp_uri = _stage_ndjson_to_gcs(rows_json, _blob_name("tracked_forms_discovery"))

    staging_ref = f"{PROJECT_ID}.{DATASET}._tracked_forms_staging"
    load_ndjson_uri(
        client,
        staging_ref,
        [
            bigquery.SchemaField("form_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("form_title", "STRING"),
        ],
        tmp_uri,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    merge_sql = f"""
    MERGE `{tracked_ref}` T
    USING `{staging_ref}` S
    ON T.form_id = S.form_id
    WHEN MATCHED AND T.form_title IS DISTINCT FROM S.form_title THEN
      UPDATE SET form_title = S.form_title
    WHEN NOT MATCHED THEN
      INSERT (form_id, form_title, first_seen, is_active)
      VALUES (S.form_id, S.form_title, CURRENT_TIMESTAMP(), TRUE)
    """
    job = client.query(merge_sql)
    job.result()
    client.query(f"DROP TABLE IF EXISTS `{staging_ref}`").result()
    return job.num_dml_affected_rows or 0


def get_forms_to_process(client: bigquery.Client) -> list[dict[str, Any]]:
    """Lee tracked_forms activos. Esta es la fuente de verdad de que se procesa."""
    tracked_ref = f"{PROJECT_ID}.{DATASET}.{TRACKED_FORMS_TABLE}"
    query = f"""
      SELECT form_id, form_title
      FROM `{tracked_ref}`
      WHERE is_active = TRUE
      ORDER BY first_seen
    """
    return [
        {"form_id": row.form_id, "form_title": row.form_title}
        for row in client.query(query).result()
    ]


def get_form_watermark(client: bigquery.Client, form_id: str) -> datetime | None:
    """MAX(submitted_at) de un form en la tabla final. None si no hay nada."""
    responses_ref = f"{PROJECT_ID}.{DATASET}.{RESPONSES_TABLE}"
    query = f"""
      SELECT MAX(submitted_at) AS wm
      FROM `{responses_ref}`
      WHERE form_id = @form_id
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("form_id", "STRING", form_id)]
        ),
    )
    row = next(iter(job.result()), None)
    return row.wm if row and row.wm else None


# ============================================================
# Transformaciones a filas BQ
# ============================================================

def _walk_fields(fields: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    order = [0]

    def _recurse(items: list[dict[str, Any]]) -> None:
        for f in items or []:
            out.append({
                "id": f.get("id"),
                "ref": f.get("ref"),
                "type": f.get("type"),
                "title": f.get("title", ""),
                "order": order[0],
            })
            order[0] += 1
            props = f.get("properties") or {}
            if props.get("fields"):
                _recurse(props["fields"])

    _recurse(fields)
    return out


def build_form_definition_rows(form: dict[str, Any], snapshot_at: str) -> list[dict[str, Any]]:
    return [{
        "form_id": form.get("id"),
        "form_title": form.get("title"),
        "field_id": f["id"],
        "field_ref": f["ref"],
        "field_type": f["type"],
        "field_title": f["title"],
        "field_order": f["order"],
        "snapshot_at": snapshot_at,
    } for f in _walk_fields(form.get("fields", []))]


def build_response_row(item: dict[str, Any], form_id: str, loaded_at: str) -> dict[str, Any]:
    hidden = item.get("hidden") or {}
    known = {f"hidden_{k}": hidden.get(k) for k in KNOWN_HIDDEN}
    extra = {k: v for k, v in hidden.items() if k not in KNOWN_HIDDEN}
    metadata = item.get("metadata") or {}
    calculated = item.get("calculated") or {}

    if item.get("submitted_at"):
        rtype = "completed"
    elif item.get("landed_at"):
        rtype = "partial"
    else:
        rtype = "started"

    return {
        "response_id": item.get("response_id") or item.get("landing_id"),
        "form_id": form_id,
        "response_type": rtype,
        "landed_at": item.get("landed_at"),
        "submitted_at": item.get("submitted_at"),
        "platform": metadata.get("platform"),
        "referer": metadata.get("referer"),
        "calculated_score": calculated.get("score"),
        **known,
        # columnas JSON: van como objeto/array nativo; NDJSON las serializa,
        # y BQ las parsea al cargar (columna tipo JSON).
        "hidden_extra": extra or None,
        "answers": item.get("answers") or [],
        # El endpoint sin proyecciones omite tags cuando no hay etiquetas.
        # [] reemplaza tags anteriores; NULL en filas viejas significa no revalidado.
        "tags": item.get("tags") or [],
        "variables": item.get("variables") or [],
        "_loaded_at": loaded_at,
    }


def build_form_metadata_row(form: dict[str, Any], snapshot_at: str) -> dict[str, Any]:
    """Configuracion del form, separada de los valores de cada respuesta."""
    metadata = {
        "form_title": form.get("title"),
        "variables": form.get("variables") or {},
        "logic": form.get("logic") or [],
    }
    canonical = json.dumps(metadata, sort_keys=True, separators=(",", ":"))
    return {
        "form_id": form["id"],
        **metadata,
        "definition_hash": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        "snapshot_at": snapshot_at,
    }


def get_form_metadata_hashes(client: bigquery.Client) -> dict[str, str]:
    query = f"""
      SELECT form_id, definition_hash
      FROM `{PROJECT_ID}.{DATASET}.{METADATA_TABLE}`
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY form_id ORDER BY snapshot_at DESC, definition_hash DESC
      ) = 1
    """
    return {row.form_id: row.definition_hash for row in client.query(query).result()}


def ingest_forms_to_raw(
    client: bigquery.Client,
    token: str,
    forms: list[dict[str, Any]],
    task_prefix: str = "",
) -> dict[str, Any]:
    """Carga los forms accesibles. Los 404 se reportan para validar DESPUES del MERGE.

    No publicar respuestas incompletas de un form cuyo pull falla a mitad.
    Daily y backfill comparten staging: deben ejecutarse sin superposicion.
    """
    loaded_at = datetime.now(UTC).isoformat()
    previous_hashes = get_form_metadata_hashes(client)
    response_rows: list[dict[str, Any]] = []
    definition_rows: list[dict[str, Any]] = []
    metadata_rows: list[dict[str, Any]] = []
    per_form: list[dict[str, Any]] = []
    failed_forms: list[dict[str, Any]] = []

    for form in forms:
        form_id = form["form_id"]
        try:
            definition = fetch_form_definition(form_id, token)
            items = fetch_form_responses(form_id, token, completed_since=None)
        except TypeformHTTPError as exc:
            if exc.status_code != 404:
                raise
            logger.warning("Form %s inaccesible (404); continuando con los demas", form_id)
            failed_forms.append({"form_id": form_id, "status_code": 404})
            continue

        rows = [build_response_row(item, form_id, loaded_at) for item in items]
        response_rows.extend(rows)
        definition_rows.extend(build_form_definition_rows(definition, loaded_at))
        metadata = build_form_metadata_row(definition, loaded_at)
        if previous_hashes.get(form_id) != metadata["definition_hash"]:
            metadata_rows.append(metadata)
            previous_hashes[form_id] = metadata["definition_hash"]
        per_form.append({
            "form_id": form_id,
            "fetched": len(rows),
            "tagged": sum(bool(row["tags"]) for row in rows),
            "with_variables": sum(bool(row["variables"]) for row in rows),
        })

    for rows, table, schema, tag in (
        (definition_rows, DEFINITIONS_TABLE, DEFINITIONS_SCHEMA, "form_definitions"),
        (metadata_rows, METADATA_TABLE, METADATA_SCHEMA, "form_metadata_snapshots"),
    ):
        uri = stage_rows_to_gcs(rows, f"{task_prefix}{tag}")
        if uri:
            load_ndjson_uri(
                client, f"{PROJECT_ID}.{DATASET}.{table}", schema, uri,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )

    raw_ref = f"{PROJECT_ID}.{DATASET}.{RESPONSES_RAW_TABLE}"
    responses_uri = stage_rows_to_gcs(response_rows, f"{task_prefix}responses_raw")
    if responses_uri:
        load_ndjson_uri(client, raw_ref, RESPONSES_SCHEMA, responses_uri)
    else:
        # Incluye el caso de todos los forms en 404: no reusar staging viejo.
        client.query(f"TRUNCATE TABLE `{raw_ref}`").result()

    return {
        "forms_attempted": len(forms),
        "forms_processed": len(per_form),
        "definitions_rows": len(definition_rows),
        "metadata_rows": len(metadata_rows),
        "response_rows": len(response_rows),
        "has_responses": bool(responses_uri),
        "failed_forms": failed_forms,
        "per_form": per_form,
    }


def validate_ingestion_result(ingest_result: dict[str, Any]) -> None:
    """Fallar al final sin revertir la carga de los forms disponibles."""
    failed_forms = ingest_result["failed_forms"]
    if failed_forms:
        details = ", ".join(f"{f['form_id']} ({f['status_code']})" for f in failed_forms)
        raise RuntimeError(
            f"Carga incompleta: {ingest_result['forms_processed']} formularios procesados; "
            f"{len(failed_forms)} inaccesibles: {details}. "
            "Las respuestas de los formularios disponibles ya fueron cargadas."
        )


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


# Schemas explicitos (deben matchear las tablas en BQ)
RESPONSES_SCHEMA = [
    bigquery.SchemaField("response_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("form_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("response_type", "STRING"),
    bigquery.SchemaField("landed_at", "TIMESTAMP"),
    bigquery.SchemaField("submitted_at", "TIMESTAMP"),
    bigquery.SchemaField("platform", "STRING"),
    bigquery.SchemaField("referer", "STRING"),
    bigquery.SchemaField("calculated_score", "INTEGER"),
    bigquery.SchemaField("hidden_user_id", "STRING"),
    bigquery.SchemaField("hidden_email", "STRING"),
    bigquery.SchemaField("hidden_plan_tier", "STRING"),
    bigquery.SchemaField("hidden_extra", "JSON"),
    bigquery.SchemaField("answers", "JSON"),
    bigquery.SchemaField("_loaded_at", "TIMESTAMP"),
    bigquery.SchemaField("tags", "JSON"),
    bigquery.SchemaField("variables", "JSON"),
]

DEFINITIONS_SCHEMA = [
    bigquery.SchemaField("form_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("form_title", "STRING"),
    bigquery.SchemaField("field_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("field_ref", "STRING"),
    bigquery.SchemaField("field_type", "STRING"),
    bigquery.SchemaField("field_title", "STRING"),
    bigquery.SchemaField("field_order", "INTEGER"),
    bigquery.SchemaField("snapshot_at", "TIMESTAMP"),
]


METADATA_SCHEMA = [
    bigquery.SchemaField("form_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("form_title", "STRING"),
    bigquery.SchemaField("variables", "JSON"),
    bigquery.SchemaField("logic", "JSON"),
    bigquery.SchemaField("definition_hash", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("snapshot_at", "TIMESTAMP", mode="REQUIRED"),
]


def merge_responses_raw_into_final(client: bigquery.Client) -> int:
    """MERGE de la raw efimera a la final, dedup por response_id (gana el _loaded_at mas nuevo)."""
    raw_ref = f"{PROJECT_ID}.{DATASET}.{RESPONSES_RAW_TABLE}"
    final_ref = f"{PROJECT_ID}.{DATASET}.{RESPONSES_TABLE}"
    columns = ", ".join(f"`{field.name}`" for field in RESPONSES_SCHEMA)
    values = ", ".join(f"S.`{field.name}`" for field in RESPONSES_SCHEMA)
    merge_sql = f"""
    MERGE `{final_ref}` T
    USING (
      SELECT * EXCEPT(rn) FROM (
        SELECT *, ROW_NUMBER() OVER (
          PARTITION BY response_id ORDER BY _loaded_at DESC,
            IF(response_type = 'completed', 1, 0) DESC, submitted_at DESC
        ) AS rn
        FROM `{raw_ref}`
      ) WHERE rn = 1
    ) S
    ON T.response_id = S.response_id
    WHEN MATCHED THEN UPDATE SET
      form_id = S.form_id,
      response_type = S.response_type,
      landed_at = S.landed_at,
      submitted_at = S.submitted_at,
      platform = S.platform,
      referer = S.referer,
      calculated_score = S.calculated_score,
      hidden_user_id = S.hidden_user_id,
      hidden_email = S.hidden_email,
      hidden_plan_tier = S.hidden_plan_tier,
      hidden_extra = S.hidden_extra,
      answers = S.answers,
      tags = S.tags,
      variables = S.variables,
      _loaded_at = S._loaded_at
    WHEN NOT MATCHED THEN
      INSERT ({columns}) VALUES ({values})
    """
    job = client.query(merge_sql)
    job.result()
    return job.num_dml_affected_rows or 0
