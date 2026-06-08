from __future__ import annotations

import base64
import json
import logging
import os
import re
import tempfile
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from airflow import DAG

try:
    from airflow.sdk import Variable, get_current_context, task
except ImportError:
    from airflow.decorators import task
    from airflow.models import Variable
    try:
        from airflow.operators.python import get_current_context
    except ImportError:
        from airflow.decorators import get_current_context

from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery
from google.cloud import secretmanager
from google.cloud import storage

try:
    import psycopg2
    from psycopg2 import extensions as psycopg2_extensions
except ImportError:
    psycopg2 = None
    psycopg2_extensions = None


logger = logging.getLogger(__name__)
DAG_REVISION = "2026-05-30T07:02:00-fxr-postgres-gcs-raw-load-job"
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
POSTGRES_TEXT_CASTERS = (
    ((1082,), "FXR_DATE_STR", None),
    ((1083,), "FXR_TIME_STR", None),
    ((1114,), "FXR_TIMESTAMP_STR", None),
    ((1184,), "FXR_TIMESTAMPTZ_STR", None),
    ((1266,), "FXR_TIMETZ_STR", None),
)
POSTGRES_TEXT_ARRAY_CASTERS = (
    ((1182,), "FXR_DATE_STR_ARRAY", "FXR_DATE_STR"),
    ((1183,), "FXR_TIME_STR_ARRAY", "FXR_TIME_STR"),
    ((1115,), "FXR_TIMESTAMP_STR_ARRAY", "FXR_TIMESTAMP_STR"),
    ((1185,), "FXR_TIMESTAMPTZ_STR_ARRAY", "FXR_TIMESTAMPTZ_STR"),
    ((1270,), "FXR_TIMETZ_STR_ARRAY", "FXR_TIMETZ_STR"),
)
TEMPORAL_BQ_TYPES = {"DATE", "TIME", "TIMESTAMP"}


def airflow_var(name: str, default: str) -> str:
    env_value = os.getenv(name)
    if env_value not in (None, ""):
        return env_value

    try:
        return Variable.get(name)
    except Exception:
        return default


PROJECT_ID = airflow_var("FXR_POSTGRES_GCP_PROJECT_ID", "fxr-analytics")
DAG_TIMEZONE = airflow_var("FXR_POSTGRES_DAG_TIMEZONE", "America/Lima")
SCHEDULE = airflow_var("FXR_POSTGRES_JOURNALED_TRADE_HOURLY_DAG_SCHEDULE", "0 * * * *")
POSTGRES_SECRET_NAME = airflow_var(
    "FXR_POSTGRES_SECRET_NAME",
    "fxr-postgres-connection",
)
POSTGRES_FETCH_SIZE = int(airflow_var("FXR_POSTGRES_FETCH_SIZE", "10000"))
BQ_LOAD_BATCH_SIZE = int(airflow_var("FXR_POSTGRES_BQ_LOAD_BATCH_SIZE", "5000"))
WATERMARK_LOOKBACK_SECONDS = int(
    airflow_var("FXR_POSTGRES_WATERMARK_LOOKBACK_SECONDS", "0")
)
GCS_STAGING_BUCKET = airflow_var("FXR_POSTGRES_GCS_STAGING_BUCKET", "fx-replay-etl")
GCS_STAGING_PREFIX = airflow_var(
    "FXR_POSTGRES_GCS_STAGING_PREFIX",
    "staging/fxr_postgres_raw",
)
RAW_BQ_DATASET = airflow_var("FXR_POSTGRES_RAW_BQ_DATASET", "fxr_ugd_raw")
FINAL_BQ_DATASET = airflow_var("FXR_POSTGRES_FINAL_BQ_DATASET", "fxr_ugd")


TABLE_SPECS: tuple[tuple[str, str, tuple[str, ...], str], ...] = (
    ("journal", "journaled_trade", ("id",), "updated_at"),
)


def validate_identifier(value: str, label: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


def to_snake_case(value: str) -> str:
    value = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", value)
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    return value.lower()


def postgres_column_key(column_name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", column_name.lower())


def normalize_table_config(table_config: dict[str, Any]) -> dict[str, Any]:
    config = dict(table_config)
    config["task_name"] = str(config["task_name"])
    config["postgres_schema"] = validate_identifier(
        str(config["postgres_schema"]),
        f"PostgreSQL schema for {config['task_name']}",
    )
    config["postgres_table"] = validate_identifier(
        str(config["postgres_table"]),
        f"PostgreSQL table for {config['task_name']}",
    )
    config["raw_bq_dataset"] = validate_identifier(
        str(config["raw_bq_dataset"]),
        f"raw BigQuery dataset for {config['task_name']}",
    )
    config["raw_bq_table"] = validate_identifier(
        str(config["raw_bq_table"]),
        f"raw BigQuery table for {config['task_name']}",
    )
    config["final_bq_dataset"] = validate_identifier(
        str(config["final_bq_dataset"]),
        f"final BigQuery dataset for {config['task_name']}",
    )
    config["final_bq_table"] = validate_identifier(
        str(config["final_bq_table"]),
        f"final BigQuery table for {config['task_name']}",
    )
    config["required_source_columns"] = tuple(
        str(value) for value in config.get("required_source_columns", ())
    )
    merge_config = dict(config.get("merge_config", {}))
    config["merge_config"] = {
        "join_keys": tuple(str(value) for value in merge_config.get("join_keys", ())),
        "partition_by": tuple(
            str(value) for value in merge_config.get("partition_by", ())
        ),
        "order_by": tuple(
            {
                "column": str(item["column"]),
                "direction": str(item.get("direction", "DESC")).upper(),
            }
            for item in merge_config.get("order_by", ())
        ),
        "source_incremental_column": (
            str(merge_config["source_incremental_column"])
            if merge_config.get("source_incremental_column") not in (None, "")
            else None
        ),
        "target_incremental_column": (
            str(merge_config["target_incremental_column"])
            if merge_config.get("target_incremental_column") not in (None, "")
            else None
        ),
    }
    if not config["merge_config"]["join_keys"]:
        raise ValueError(f"Table {config['task_name']!r} must define merge join_keys.")
    if not config["merge_config"]["partition_by"]:
        raise ValueError(
            f"Table {config['task_name']!r} must define merge partition_by."
        )
    if not config["merge_config"]["order_by"]:
        raise ValueError(f"Table {config['task_name']!r} must define merge order_by.")
    source_incremental_column = config["merge_config"]["source_incremental_column"]
    target_incremental_column = config["merge_config"]["target_incremental_column"]
    if (source_incremental_column is None) != (target_incremental_column is None):
        raise ValueError(
            f"Table {config['task_name']!r} must define both source and target "
            "incremental columns, or neither."
        )
    return config


def build_table_configs() -> tuple[dict[str, Any], ...]:
    configs: list[dict[str, Any]] = []
    for schema_name, table_name, source_keys, source_incremental in TABLE_SPECS:
        target_keys = tuple(to_snake_case(value) for value in source_keys)
        task_name = f"{schema_name}_{table_name}"
        bq_table_name = table_name
        target_incremental = to_snake_case(source_incremental)
        configs.append(
            normalize_table_config(
                {
                    "task_name": task_name,
                    "postgres_schema": schema_name,
                    "postgres_table": table_name,
                    "raw_bq_dataset": RAW_BQ_DATASET,
                    "raw_bq_table": bq_table_name,
                    "final_bq_dataset": FINAL_BQ_DATASET,
                    "final_bq_table": bq_table_name,
                    "required_source_columns": source_keys + (source_incremental,),
                    "merge_config": {
                        "join_keys": target_keys,
                        "partition_by": target_keys,
                        "order_by": (
                            {"column": target_incremental, "direction": "DESC"},
                            {"column": "_loaded_at", "direction": "DESC"},
                        ),
                        "source_incremental_column": source_incremental,
                        "target_incremental_column": target_incremental,
                    },
                }
            )
        )
    return tuple(configs)


TABLE_CONFIGS = build_table_configs()


def get_secret(secret_name: str) -> str:
    env_value = os.getenv(secret_name)
    if env_value not in (None, ""):
        return env_value

    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"
    try:
        response = client.access_secret_version(request={"name": secret_path})
    except Exception as exc:
        raise ValueError(
            f"GCP Secret Manager secret '{secret_name}' is required."
        ) from exc

    value = response.payload.data.decode("utf-8")
    if not value:
        raise ValueError(f"GCP Secret Manager secret '{secret_name}' is empty.")
    return value


def get_postgres_config() -> dict[str, Any]:
    secret_payload = get_secret(POSTGRES_SECRET_NAME)
    creds = json.loads(secret_payload)
    host = creds.get("HOST") or creds.get("host")
    username = (
        creds.get("USER")
        or creds.get("USERNAME")
        or creds.get("user")
        or creds.get("username")
    )
    password = (
        creds.get("PASS")
        or creds.get("PASSWORD")
        or creds.get("pass")
        or creds.get("password")
    )
    database = (
        creds.get("DB")
        or creds.get("DATABASE")
        or creds.get("db")
        or creds.get("database")
    )
    port = creds.get("PORT") or creds.get("port") or 5432
    sslmode = creds.get("SSLMODE") or creds.get("sslmode") or "prefer"

    if not host or not username or not password or not database:
        raise ValueError(
            f"Secret {POSTGRES_SECRET_NAME!r} must contain HOST, USER, PASS, and DB."
        )

    return {
        "host": str(host),
        "username": str(username),
        "password": str(password),
        "database": str(database),
        "port": int(port),
        "sslmode": str(sslmode),
    }


def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def get_gcs_client() -> storage.Client:
    return storage.Client(project=PROJECT_ID)


def register_safe_postgres_typecasters(connection) -> None:
    if psycopg2_extensions is None:
        return

    scalar_casters: dict[str, Any] = {}
    for oids, caster_name, _ in POSTGRES_TEXT_CASTERS:
        caster = psycopg2_extensions.new_type(
            oids,
            caster_name,
            lambda value, cursor: value,
        )
        psycopg2_extensions.register_type(caster, connection)
        scalar_casters[caster_name] = caster

    for oids, array_caster_name, scalar_caster_name in POSTGRES_TEXT_ARRAY_CASTERS:
        scalar_caster = scalar_casters.get(str(scalar_caster_name))
        if scalar_caster is None:
            continue
        array_caster = psycopg2_extensions.new_array_type(
            oids,
            array_caster_name,
            scalar_caster,
        )
        psycopg2_extensions.register_type(array_caster, connection)


def get_postgres_connection(postgres_config: dict[str, Any]):
    if psycopg2 is None:
        raise ImportError(
            "This DAG requires `psycopg2` or `psycopg2-binary` to read from PostgreSQL."
        )

    connection = psycopg2.connect(
        host=postgres_config["host"],
        port=int(postgres_config["port"]),
        user=postgres_config["username"],
        password=postgres_config["password"],
        dbname=postgres_config["database"],
        sslmode=postgres_config["sslmode"],
        connect_timeout=15,
    )
    register_safe_postgres_typecasters(connection)
    return connection


def map_postgres_type_to_bigquery(
    data_type: str,
    numeric_scale: int | None,
    udt_name: str | None = None,
) -> str:
    normalized = data_type.lower()
    normalized_udt_name = (udt_name or "").lower()

    if normalized in {"smallint", "integer", "bigint"}:
        return "INT64"
    if normalized in {"numeric", "decimal"}:
        return "NUMERIC" if (numeric_scale or 0) <= 9 else "BIGNUMERIC"
    if normalized in {"real", "double precision"}:
        return "FLOAT64"
    if normalized == "boolean":
        return "BOOL"
    if normalized == "date":
        return "DATE"
    if normalized in {"timestamp without time zone", "timestamp with time zone"}:
        return "TIMESTAMP"
    if normalized == "time without time zone":
        return "TIME"
    if normalized in {"json", "jsonb"}:
        return "JSON"
    if normalized == "bytea":
        return "BYTES"
    if normalized == "uuid":
        return "STRING"
    if normalized == "array" or normalized_udt_name.startswith("_"):
        return "JSON"
    return "STRING"


def resolve_final_column_name(source_name: str) -> str:
    return to_snake_case(source_name)


def fetch_postgres_columns(
    connection,
    postgres_config: dict[str, Any],
    table_config: dict[str, Any],
) -> list[dict[str, Any]]:
    del postgres_config
    query = """
    SELECT
      column_name,
      data_type,
      udt_name,
      is_nullable,
      numeric_scale
    FROM information_schema.columns
    WHERE table_catalog = %s
      AND table_schema = %s
      AND table_name = %s
    ORDER BY ordinal_position
    """

    cursor = connection.cursor()
    try:
        cursor.execute(
            query,
            (
                connection.info.dbname,
                table_config["postgres_schema"],
                table_config["postgres_table"],
            ),
        )
        columns: list[dict[str, Any]] = []
        for column_name, data_type, udt_name, is_nullable, numeric_scale in cursor.fetchall():
            columns.append(
                {
                    "source_name": str(column_name),
                    "data_type": str(data_type),
                    "udt_name": str(udt_name),
                    "numeric_scale": numeric_scale,
                    "is_nullable": str(is_nullable).upper() == "YES",
                }
            )
    finally:
        cursor.close()

    source_keys = {postgres_column_key(column["source_name"]) for column in columns}
    required_keys = {
        postgres_column_key(column_name)
        for column_name in table_config["required_source_columns"]
    }
    missing_required = required_keys - source_keys
    if missing_required:
        raise ValueError(
            f"PostgreSQL source schema for "
            f"{table_config['postgres_schema']}.{table_config['postgres_table']!r} "
            f"is missing required columns: {', '.join(sorted(missing_required))}"
        )

    return columns


def build_raw_schema(
    postgres_columns: list[dict[str, Any]],
) -> list[bigquery.SchemaField]:
    schema = [
        bigquery.SchemaField(
            column["source_name"],
            map_postgres_type_to_bigquery(
                column["data_type"],
                column["numeric_scale"],
                column.get("udt_name"),
            ),
            mode="NULLABLE",
        )
        for column in postgres_columns
    ]
    schema.append(bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="NULLABLE"))
    return schema


def build_final_column_specs(
    postgres_columns: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    seen_target_names: set[str] = set()

    for column in postgres_columns:
        source_name = str(column["source_name"])
        target_name = resolve_final_column_name(source_name)
        if target_name in seen_target_names:
            raise ValueError(
                "PostgreSQL schema produces duplicate final column name "
                f"{target_name!r}."
            )

        target_type = map_postgres_type_to_bigquery(
            column["data_type"],
            column["numeric_scale"],
            column.get("udt_name"),
        )
        select_expression = f"`{source_name}`"
        if target_type == "BOOL":
            select_expression = f"CAST(`{source_name}` AS BOOL)"

        specs.append(
            {
                "source_name": source_name,
                "target_name": target_name,
                "field": bigquery.SchemaField(target_name, target_type, mode="NULLABLE"),
                "select_expression": select_expression,
            }
        )
        seen_target_names.add(target_name)

    specs.append(
        {
            "source_name": "_loaded_at",
            "target_name": "_loaded_at",
            "field": bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="NULLABLE"),
            "select_expression": "_loaded_at",
        }
    )
    return specs


def build_final_schema(
    postgres_columns: list[dict[str, Any]],
) -> list[bigquery.SchemaField]:
    return [spec["field"] for spec in build_final_column_specs(postgres_columns)]


def ensure_dataset(client: bigquery.Client, dataset_name: str) -> None:
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{dataset_name}")
    client.create_dataset(dataset_ref, exists_ok=True)


def ensure_table_exists(
    client: bigquery.Client,
    table_ref: str,
    desired_schema: list[bigquery.SchemaField],
) -> bigquery.Table:
    try:
        current_table = client.get_table(table_ref)
    except NotFound:
        logger.info("Creating missing BigQuery table %s", table_ref)
        table = bigquery.Table(table_ref, schema=desired_schema)
        return client.create_table(table, exists_ok=True)

    if str(getattr(current_table, "table_type", "")).upper() != "TABLE":
        client.delete_table(table_ref, not_found_ok=True)
        table = bigquery.Table(table_ref, schema=desired_schema)
        return client.create_table(table, exists_ok=True)

    return current_table


def reconcile_table_schema(
    client: bigquery.Client,
    table_ref: str,
    desired_schema: list[bigquery.SchemaField],
) -> bigquery.Table:
    current_table = ensure_table_exists(client, table_ref, desired_schema)
    current_map = {field.name: field for field in current_table.schema}
    desired_map = {field.name: field for field in desired_schema}

    for field_name, desired_field in desired_map.items():
        if field_name not in current_map:
            query = (
                f"ALTER TABLE `{table_ref}` "
                f"ADD COLUMN `{field_name}` {desired_field.field_type}"
            )
            client.query(query).result()

    for field_name, current_field in current_map.items():
        if field_name not in desired_map:
            query = f"ALTER TABLE `{table_ref}` DROP COLUMN `{field_name}`"
            client.query(query).result()
            continue

        desired_field = desired_map[field_name]
        if current_field.field_type.upper() != desired_field.field_type.upper():
            query = (
                f"ALTER TABLE `{table_ref}` "
                f"ALTER COLUMN `{field_name}` SET DATA TYPE {desired_field.field_type}"
            )
            try:
                client.query(query).result()
            except BadRequest as exc:
                logger.warning(
                    "Skipping incompatible type change for %s.%s from %s to %s: %s",
                    table_ref,
                    field_name,
                    current_field.field_type,
                    desired_field.field_type,
                    exc,
                )

    return client.get_table(table_ref)


def get_max_table_timestamp(
    client: bigquery.Client,
    table_ref: str,
    timestamp_column: str,
) -> datetime | None:
    query = f"""
    SELECT MAX(`{timestamp_column}`) AS max_timestamp
    FROM `{table_ref}`
    """

    try:
        rows = list(client.query(query).result())
    except NotFound:
        return None

    if not rows:
        return None
    return rows[0]["max_timestamp"]


def parse_runtime_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def ensure_utc_datetime(value: datetime | None) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def resolve_runtime_window(last_timestamp: datetime | None) -> tuple[datetime | None, datetime | None]:
    context = get_current_context()
    conf = (context.get("dag_run") and context["dag_run"].conf) or {}

    # By default the hourly watermark starts from the current MAX(updated_at)
    # already present in the BigQuery final table. This can be overridden with
    # dag_run.conf.start_timestamp / end_timestamp for manual bounded reruns.
    start_timestamp = last_timestamp
    if conf.get("start_timestamp"):
        start_timestamp = parse_runtime_timestamp(str(conf["start_timestamp"]))

    if (
        start_timestamp is not None
        and WATERMARK_LOOKBACK_SECONDS > 0
        and not conf.get("start_timestamp")
    ):
        start_timestamp = start_timestamp - timedelta(
            seconds=WATERMARK_LOOKBACK_SECONDS
        )

    end_timestamp = None
    if conf.get("end_timestamp"):
        end_timestamp = parse_runtime_timestamp(str(conf["end_timestamp"]))

    return start_timestamp, end_timestamp


def build_postgres_query(
    start_timestamp: datetime | None,
    end_timestamp: datetime | None,
    table_config: dict[str, Any],
) -> tuple[str, tuple[Any, ...]]:
    schema_name = validate_identifier(
        table_config["postgres_schema"],
        "PostgreSQL schema name",
    )
    table_name = validate_identifier(
        table_config["postgres_table"],
        "PostgreSQL table name",
    )
    source_incremental_column = table_config["merge_config"]["source_incremental_column"]
    qualified_table_name = f'"{schema_name}"."{table_name}"'

    if source_incremental_column is None:
        return f"SELECT * FROM {qualified_table_name}", ()

    updated_column = validate_identifier(
        source_incremental_column,
        "incremental source column",
    )
    if start_timestamp is None and end_timestamp is None:
        return f'SELECT * FROM {qualified_table_name} ORDER BY "{updated_column}" ASC', ()

    filters: list[str] = []
    params: list[Any] = []

    if start_timestamp is not None:
        filters.append(f'"{updated_column}" > %s')
        params.append(start_timestamp)
    if end_timestamp is not None:
        filters.append(f'"{updated_column}" <= %s')
        params.append(end_timestamp)

    where_clause = " AND ".join(filters)
    query = (
        f"SELECT * FROM {qualified_table_name} "
        f"WHERE {where_clause} ORDER BY \"{updated_column}\" ASC"
    )
    return query, tuple(params)


def normalize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.isoformat(sep=" ")
        return value.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, (list, dict)):
        return value
    return str(value)


def chunked(rows: list[dict[str, Any]], size: int):
    for index in range(0, len(rows), size):
        yield rows[index:index + size]


def sanitize_gcs_path_component(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_") or "unknown"


def sanitize_temporal_string(value: str, field: bigquery.SchemaField) -> str | None:
    field_type = str(field.field_type).upper()
    if field_type not in TEMPORAL_BQ_TYPES:
        return value

    normalized = value.strip()
    if not normalized:
        return None
    if " BC" in normalized.upper():
        return None
    if normalized.startswith("-"):
        return None

    return value


def coerce_row_to_schema(
    row: dict[str, Any],
    schema: list[bigquery.SchemaField],
) -> dict[str, Any]:
    schema_map = {field.name: field for field in schema}
    coerced: dict[str, Any] = {}

    for key, value in row.items():
        field = schema_map.get(key)
        if value is None or field is None:
            coerced[key] = value
            continue

        if isinstance(value, str):
            coerced[key] = sanitize_temporal_string(value, field)
            continue

        if isinstance(value, (list, dict)):
            if field.mode.upper() == "REPEATED":
                coerced[key] = value
            elif field.field_type.upper() == "JSON":
                coerced[key] = value
            else:
                coerced[key] = json.dumps(value, ensure_ascii=False)
            continue

        coerced[key] = value

    return coerced


def upload_file_to_gcs(file_path: str, blob_name: str) -> str:
    gcs_client = get_gcs_client()
    bucket = gcs_client.bucket(GCS_STAGING_BUCKET)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path, content_type="application/x-ndjson")
    return f"gs://{GCS_STAGING_BUCKET}/{blob_name}"


def load_rows_from_jsonl_uri(
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


def load_rows(
    client: bigquery.Client,
    table_ref: str,
    schema: list[bigquery.SchemaField],
    rows: list[dict[str, Any]],
    write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
) -> None:
    if not rows:
        return

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=write_disposition,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    prepared_rows = [coerce_row_to_schema(row, schema) for row in rows]
    client.load_table_from_json(
        prepared_rows,
        table_ref,
        job_config=job_config,
    ).result()


def truncate_table(client: bigquery.Client, table_ref: str) -> None:
    client.query(f"TRUNCATE TABLE `{table_ref}`").result()


def sync_table_schemas(table_config: dict[str, Any]) -> dict[str, Any]:
    table_config = normalize_table_config(table_config)
    logger.info("Running DAG revision %s", DAG_REVISION)

    client = get_bq_client()
    postgres_config = get_postgres_config()
    connection = get_postgres_connection(postgres_config)

    try:
        postgres_columns = fetch_postgres_columns(connection, postgres_config, table_config)
        raw_table_ref = (
            f"{PROJECT_ID}.{table_config['raw_bq_dataset']}.{table_config['raw_bq_table']}"
        )
        final_table_ref = (
            f"{PROJECT_ID}.{table_config['final_bq_dataset']}.{table_config['final_bq_table']}"
        )

        ensure_dataset(client, table_config["raw_bq_dataset"])
        ensure_dataset(client, table_config["final_bq_dataset"])
        raw_table = reconcile_table_schema(
            client,
            raw_table_ref,
            build_raw_schema(postgres_columns),
        )
        final_table = reconcile_table_schema(
            client,
            final_table_ref,
            build_final_schema(postgres_columns),
        )

        return {
            "task_name": table_config["task_name"],
            "raw_table": raw_table_ref,
            "final_table": final_table_ref,
            "postgres_columns": len(postgres_columns),
            "raw_columns": len(raw_table.schema),
            "final_columns": len(final_table.schema),
        }
    finally:
        connection.close()


def stage_table_raw_to_gcs(
    schema_result: dict[str, Any],
    table_config: dict[str, Any],
) -> dict[str, Any]:
    table_config = normalize_table_config(table_config)
    logger.info("Running DAG revision %s", DAG_REVISION)
    context = get_current_context()
    client = get_bq_client()
    raw_table_ref = str(schema_result["raw_table"])
    final_table_ref = str(schema_result["final_table"])
    loaded_at = datetime.now(UTC).isoformat()
    postgres_config = get_postgres_config()
    connection = get_postgres_connection(postgres_config)

    try:
        target_incremental_column = table_config["merge_config"]["target_incremental_column"]
        last_timestamp = (
            get_max_table_timestamp(
                client,
                final_table_ref,
                target_incremental_column,
            )
            if target_incremental_column
            else None
        )
        start_timestamp, end_timestamp = resolve_runtime_window(last_timestamp)
        query, params = build_postgres_query(start_timestamp, end_timestamp, table_config)

        raw_table = client.get_table(raw_table_ref)
        total_rows = 0
        run_id = sanitize_gcs_path_component(str(context.get("run_id", "manual")))
        logical_date = context.get("logical_date")
        logical_date_part = (
            sanitize_gcs_path_component(str(logical_date.isoformat()))
            if isinstance(logical_date, datetime)
            else "no-logical-date"
        )
        gcs_blob_name = (
            f"{GCS_STAGING_PREFIX.rstrip('/')}/"
            f"{table_config['task_name']}/"
            f"{logical_date_part}/"
            f"{run_id}.jsonl"
        )
        logger.info(
            "Raw staging target for %s: gs://%s/%s",
            raw_table_ref,
            GCS_STAGING_BUCKET,
            gcs_blob_name,
        )
        snapshot_file = tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=f"-{table_config['task_name']}.jsonl",
            delete=False,
        )
        snapshot_file_path = snapshot_file.name
        cursor = connection.cursor()
        cursor.arraysize = POSTGRES_FETCH_SIZE
        try:
            cursor.execute(query, params)
            if cursor.description is None:
                raise ValueError("PostgreSQL cursor metadata is unavailable after execute.")
            column_names = [column.name for column in cursor.description]

            while True:
                batch = cursor.fetchmany(POSTGRES_FETCH_SIZE)
                if not batch:
                    break

                json_rows = []
                for record in batch:
                    row = {
                        column_name: normalize_value(value)
                        for column_name, value in zip(column_names, record)
                    }
                    row["_loaded_at"] = loaded_at
                    json_rows.append(coerce_row_to_schema(row, raw_table.schema))

                for row in json_rows:
                    snapshot_file.write(json.dumps(row, separators=(",", ":")))
                    snapshot_file.write("\n")

                total_rows += len(json_rows)
                logger.info("Staged %s rows for %s", total_rows, raw_table_ref)
        finally:
            cursor.close()
            snapshot_file.close()

        try:
            staged_gcs_uri = upload_file_to_gcs(snapshot_file_path, gcs_blob_name)
            logger.info("Uploaded staged rows for %s to %s", raw_table_ref, staged_gcs_uri)
        finally:
            if os.path.exists(snapshot_file_path):
                os.unlink(snapshot_file_path)

        return {
            "task_name": table_config["task_name"],
            "target_table": raw_table_ref,
            "final_table": final_table_ref,
            "rows_loaded": total_rows,
            "staged_gcs_uri": staged_gcs_uri,
            "last_timestamp": (
                last_timestamp.isoformat()
                if isinstance(last_timestamp, datetime)
                else None
            ),
            "effective_start_timestamp": (
                start_timestamp.isoformat()
                if isinstance(start_timestamp, datetime)
                else None
            ),
            "effective_end_timestamp": (
                end_timestamp.isoformat()
                if isinstance(end_timestamp, datetime)
                else None
            ),
            "loaded_at": loaded_at,
        }
    finally:
        connection.close()


def load_table_raw_from_gcs(
    staged_result: dict[str, Any],
    table_config: dict[str, Any],
) -> dict[str, Any]:
    table_config = normalize_table_config(table_config)
    logger.info("Running DAG revision %s", DAG_REVISION)
    client = get_bq_client()
    raw_table_ref = str(staged_result["target_table"])
    staged_gcs_uri = str(staged_result["staged_gcs_uri"])
    rows_loaded = int(staged_result.get("rows_loaded", 0))
    raw_table = client.get_table(raw_table_ref)

    if rows_loaded > 0:
        load_rows_from_jsonl_uri(
            client,
            raw_table_ref,
            raw_table.schema,
            staged_gcs_uri,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
    else:
        truncate_table(client, raw_table_ref)

    return {
        "task_name": staged_result["task_name"],
        "target_table": raw_table_ref,
        "final_table": staged_result["final_table"],
        "rows_loaded": rows_loaded,
        "staged_gcs_uri": staged_gcs_uri,
        "last_timestamp": staged_result.get("last_timestamp"),
        "effective_start_timestamp": staged_result.get("effective_start_timestamp"),
        "effective_end_timestamp": staged_result.get("effective_end_timestamp"),
        "loaded_at": staged_result.get("loaded_at"),
    }


def build_final_insert_query(
    postgres_columns: list[dict[str, Any]],
    raw_table_ref: str,
    final_table_ref: str,
    table_config: dict[str, Any],
    raw_field_types: dict[str, str] | None = None,
    raw_field_modes: dict[str, str] | None = None,
    final_field_types: dict[str, str] | None = None,
) -> str:
    specs = build_final_column_specs(postgres_columns)
    del table_config

    def normalize_bq_type_name(type_name: str) -> str:
        normalized = str(type_name).upper()
        if normalized == "INTEGER":
            return "INT64"
        return normalized

    def merge_select_expression(spec: dict[str, Any]) -> str:
        expression = str(spec["select_expression"])
        source_name = str(spec["source_name"])
        target_name = str(spec["target_name"])
        desired_target_type = normalize_bq_type_name(str(spec["field"].field_type))
        source_type = normalize_bq_type_name(
            (raw_field_types or {}).get(source_name, desired_target_type)
        )
        source_mode = str((raw_field_modes or {}).get(source_name, "NULLABLE")).upper()
        target_type = normalize_bq_type_name(
            (final_field_types or {}).get(target_name, desired_target_type)
        )

        if source_type == target_type:
            return expression
        if source_mode == "REPEATED" and target_type == "JSON":
            return f"PARSE_JSON(TO_JSON_STRING({expression}))"
        if source_mode == "REPEATED" and target_type == "STRING":
            return f"TO_JSON_STRING({expression})"
        if target_type == "STRING" and source_type == "JSON":
            return f"TO_JSON_STRING({expression})"
        if target_type == "INT64" and source_type == "BOOL":
            return f"IF({expression}, 1, 0)"
        if target_type == "JSON" and source_type == "STRING":
            return f"PARSE_JSON({expression})"
        if target_type in {"NUMERIC", "BIGNUMERIC"}:
            if source_type == "FLOAT64":
                return (
                    f"CASE WHEN {expression} IS NULL OR IS_NAN({expression}) "
                    f"OR IS_INF({expression}) "
                    f"THEN NULL ELSE SAFE_CAST({expression} AS {target_type}) END"
                )
            if source_type == "STRING":
                normalized_expression = f"LOWER(TRIM({expression}))"
                return (
                    f"CASE WHEN {expression} IS NULL "
                    f"OR {normalized_expression} IN "
                    f"('nan', '+nan', '-nan', 'inf', '+inf', '-inf', "
                    f"'infinity', '+infinity', '-infinity') "
                    f"THEN NULL ELSE SAFE_CAST({expression} AS {target_type}) END"
                )
            return f"SAFE_CAST({expression} AS {target_type})"
        if target_type in {
            "STRING",
            "INT64",
            "FLOAT64",
            "BOOL",
            "DATE",
            "TIME",
            "TIMESTAMP",
            "BYTES",
        }:
            return f"CAST({expression} AS {target_type})"
        if target_type == "STRING":
            return f"CAST({expression} AS STRING)"
        return expression

    select_clause = ",\n          ".join(
        f"{merge_select_expression(spec)} AS `{spec['target_name']}`"
        for spec in specs
    )
    insert_columns = ",\n      ".join(f"`{spec['target_name']}`" for spec in specs)
    return f"""
    INSERT INTO `{final_table_ref}` (
      {insert_columns}
    )
    SELECT
      {select_clause}
    FROM `{raw_table_ref}`
    """


def delete_final_window(
    client: bigquery.Client,
    final_table_ref: str,
    table_config: dict[str, Any],
    start_timestamp: datetime | None,
    end_timestamp: datetime | None,
) -> int | None:
    target_incremental_column = table_config["merge_config"]["target_incremental_column"]
    if target_incremental_column is None:
        raise ValueError(
            f"Hourly delete requires target_incremental_column for {final_table_ref}."
        )

    normalized_start = ensure_utc_datetime(start_timestamp)
    normalized_end = ensure_utc_datetime(end_timestamp)
    filters: list[str] = []
    if isinstance(normalized_start, datetime):
        filters.append(
            f"`{target_incremental_column}` > TIMESTAMP('{normalized_start.isoformat().replace('+00:00', 'Z')}')"
        )
    if isinstance(normalized_end, datetime):
        filters.append(
            f"`{target_incremental_column}` <= TIMESTAMP('{normalized_end.isoformat().replace('+00:00', 'Z')}')"
        )
    if not filters:
        raise ValueError(f"Hourly delete window is required for {final_table_ref}.")

    query = f"""
    DELETE FROM `{final_table_ref}`
    WHERE {" AND ".join(filters)}
    """
    job = client.query(query)
    job.result()
    return job.num_dml_affected_rows


def merge_table_to_final(
    raw_result: dict[str, Any],
    table_config: dict[str, Any],
) -> dict[str, Any]:
    table_config = normalize_table_config(table_config)
    logger.info("Running DAG revision %s", DAG_REVISION)
    client = get_bq_client()
    raw_table_ref = str(raw_result["target_table"])
    final_table_ref = str(raw_result["final_table"])
    postgres_config = get_postgres_config()
    connection = get_postgres_connection(postgres_config)
    try:
        postgres_columns = fetch_postgres_columns(connection, postgres_config, table_config)
    finally:
        connection.close()

    final_table = reconcile_table_schema(
        client,
        final_table_ref,
        build_final_schema(postgres_columns),
    )
    raw_table = client.get_table(raw_table_ref)
    raw_field_types = {
        field.name: str(field.field_type).upper()
        for field in raw_table.schema
    }
    raw_field_modes = {
        field.name: str(field.mode).upper()
        for field in raw_table.schema
    }
    final_field_types = {
        field.name: str(field.field_type).upper()
        for field in final_table.schema
    }
    delete_count = delete_final_window(
        client,
        final_table_ref,
        table_config,
        parse_runtime_timestamp(str(raw_result["effective_start_timestamp"]))
        if raw_result.get("effective_start_timestamp")
        else None,
        parse_runtime_timestamp(str(raw_result["effective_end_timestamp"]))
        if raw_result.get("effective_end_timestamp")
        else None,
    )
    query = build_final_insert_query(
        postgres_columns,
        raw_table_ref,
        final_table_ref,
        table_config,
        raw_field_types,
        raw_field_modes,
        final_field_types,
    )
    job = client.query(query)
    job.result()

    return {
        "task_name": table_config["task_name"],
        "raw_table": raw_table_ref,
        "final_table": final_table_ref,
        "raw_rows_loaded": int(raw_result.get("rows_loaded", 0)),
        "deleted_rows": delete_count,
        "inserted_rows": job.num_dml_affected_rows,
        "loaded_at": raw_result.get("loaded_at"),
        "effective_start_timestamp": raw_result.get("effective_start_timestamp"),
        "effective_end_timestamp": raw_result.get("effective_end_timestamp"),
    }


def build_table_tasks(table_configs: tuple[dict[str, Any], ...]) -> None:
    for table_config in table_configs:
        normalized_config = normalize_table_config(table_config)
        task_suffix = normalized_config["task_name"]

        sync_schema_task = task(task_id=f"sync_{task_suffix}_schemas")(
            sync_table_schemas
        )(normalized_config)
        stage_gcs_task = task(task_id=f"stage_{task_suffix}_raw_to_gcs")(
            stage_table_raw_to_gcs
        )(
            sync_schema_task,
            normalized_config,
        )
        sync_raw_task = task(task_id=f"load_{task_suffix}_raw_from_gcs")(
            load_table_raw_from_gcs
        )(
            stage_gcs_task,
            normalized_config,
        )
        merge_final_task = task(task_id=f"merge_{task_suffix}_to_final")(
            merge_table_to_final
        )(
            sync_raw_task,
            normalized_config,
        )

        sync_schema_task >> stage_gcs_task >> sync_raw_task >> merge_final_task


with DAG(
    dag_id="fxr_postgres_to_bigquery_journaled_trade_hourly",
    description=(
        "Carga incremental por hora desde PostgreSQL FXR hacia BigQuery raw y "
        "final para journal.journaled_trade usando watermark "
        "(start_timestamp = MAX(updated_at) actual en BigQuery final por defecto)."
    ),
    start_date=datetime(2024, 1, 1, 5, 0, tzinfo=ZoneInfo(DAG_TIMEZONE)),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["postgres", "bigquery", "fxr", "incremental", "raw"],
) as dag:
    build_table_tasks(TABLE_CONFIGS)
