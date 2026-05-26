from __future__ import annotations

import base64
import json
import logging
import os
import re
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
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import secretmanager

try:
    import pymysql
except ImportError:
    pymysql = None

try:
    import mysql.connector as mysql_connector
except ImportError:
    mysql_connector = None


logger = logging.getLogger(__name__)


def airflow_var(name: str, default: str) -> str:
    env_value = os.getenv(name)
    if env_value not in (None, ""):
        return env_value

    try:
        return Variable.get(name)
    except Exception:
        return default


PROJECT_ID = airflow_var("FXREPLAY_PROD_GCP_PROJECT_ID", "fxr-analytics")
DAG_TIMEZONE = airflow_var("FXREPLAY_PROD_DAG_TIMEZONE", "America/Lima")
SCHEDULE = airflow_var("FXREPLAY_PROD_DAG_SCHEDULE", "0 5 * * *")
MYSQL_SECRET_NAME = airflow_var(
    "FXREPLAY_PROD_MYSQL_SECRET_NAME",
    "fxreplay_prod-mysql-connection",
)
MYSQL_FETCH_SIZE = int(airflow_var("FXREPLAY_PROD_MYSQL_FETCH_SIZE", "10000"))
BQ_LOAD_BATCH_SIZE = int(airflow_var("FXREPLAY_PROD_BQ_LOAD_BATCH_SIZE", "5000"))
WATERMARK_LOOKBACK_SECONDS = int(
    airflow_var("FXREPLAY_PROD_WATERMARK_LOOKBACK_SECONDS", "0")
)

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

TABLE_CONFIGS: tuple[dict[str, Any], ...] = (
    {
        "task_name": "timeanalytic",
        "mysql_table": "timeanalytic",
        "raw_bq_dataset": "fxr_ugd_raw",
        "raw_bq_table": "timeanalytic",
        "final_bq_dataset": "fxr_ugd",
        "final_bq_table": "time_analytics",
        "required_source_columns": ("userId", "date", "updatedAt"),
        "datetime_from_unix_seconds_columns": ("date",),
        "merge_config": {
            "join_keys": ("user_id", "date"),
            "partition_by": ("user_id", "date"),
            "order_by": (
                {"column": "updated_at", "direction": "DESC"},
            ),
            "source_incremental_column": "updatedAt",
            "target_incremental_column": "updated_at",
        },
    },
    {
        "task_name": "positions",
        "mysql_table": "positions",
        "raw_bq_dataset": "fxr_ugd_raw",
        "raw_bq_table": "positions",
        "final_bq_dataset": "fxr_ugd",
        "final_bq_table": "positions",
        "required_source_columns": ("id", "updated_at"),
        "datetime_from_unix_seconds_columns": (),
        "merge_config": {
            "join_keys": ("id",),
            "partition_by": ("id",),
            "order_by": (
                {"column": "updated_at", "direction": "DESC"},
                {"column": "_loaded_at", "direction": "DESC"},
            ),
            "source_incremental_column": "updated_at",
            "target_incremental_column": "updated_at",
        },
    },
    {
        "task_name": "emaillead",
        "mysql_table": "emaillead",
        "raw_bq_dataset": "fxr_ugd_raw",
        "raw_bq_table": "emaillead",
        "final_bq_dataset": "fxr_ugd",
        "final_bq_table": "email_leads",
        "required_source_columns": ("id", "createdAt"),
        "datetime_from_unix_seconds_columns": (),
        "merge_config": {
            "join_keys": ("id",),
            "partition_by": ("id",),
            "order_by": (
                {"column": "created_at", "direction": "DESC"},
                {"column": "_loaded_at", "direction": "DESC"},
            ),
            "source_incremental_column": "createdAt",
            "target_incremental_column": "created_at",
        },
    },
    {
        "task_name": "symbol_library",
        "mysql_table": "symbol_library",
        "raw_bq_dataset": "fxr_ugd_raw",
        "raw_bq_table": "symbol_library",
        "final_bq_dataset": "fxr_ugd",
        "final_bq_table": "symbol_library",
        "required_source_columns": ("id",),
        "datetime_from_unix_seconds_columns": (),
        "merge_config": {
            "join_keys": ("id",),
            "partition_by": ("id",),
            "order_by": (
                {"column": "_loaded_at", "direction": "DESC"},
            ),
            "source_incremental_column": None,
            "target_incremental_column": None,
            "delete_not_matched_by_source": True,
        },
    },
    {
        "task_name": "backtesting_sessions",
        "mysql_table": "backtesting_sessions",
        "raw_bq_dataset": "fxr_ugd_raw",
        "raw_bq_table": "backtesting_sessions",
        "final_bq_dataset": "fxr_ugd",
        "final_bq_table": "backtesting_sessions",
        "required_source_columns": ("id", "updated_at"),
        "datetime_from_unix_seconds_columns": (),
        "merge_config": {
            "join_keys": ("id",),
            "partition_by": ("id",),
            "order_by": (
                {"column": "updated_at", "direction": "DESC"},
                {"column": "_loaded_at", "direction": "DESC"},
            ),
            "source_incremental_column": "updated_at",
            "target_incremental_column": "updated_at",
            "delete_not_matched_by_source": False,
        },
    },
    {
        "task_name": "users",
        "mysql_table": "users",
        "raw_bq_dataset": "fxr_ugd_raw",
        "raw_bq_table": "users",
        "final_bq_dataset": "fxr_ugd",
        "final_bq_table": "users",
        "required_source_columns": ("userId", "email"),
        "datetime_from_unix_seconds_columns": (),
        "merge_config": {
            "join_keys": ("user_id",),
            "partition_by": ("user_id",),
            "order_by": (
                {"column": "_loaded_at", "direction": "DESC"},
            ),
            "source_incremental_column": None,
            "target_incremental_column": None,
            "delete_not_matched_by_source": True,
        },
    },
)


def validate_identifier(value: str, label: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


def normalize_table_config(table_config: dict[str, Any]) -> dict[str, Any]:
    config = dict(table_config)
    config["task_name"] = str(config["task_name"])
    config["mysql_table"] = validate_identifier(
        str(config["mysql_table"]),
        f"MySQL table name for {config['task_name']}",
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
    config["datetime_from_unix_seconds_columns"] = tuple(
        str(value) for value in config.get("datetime_from_unix_seconds_columns", ())
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
        "delete_not_matched_by_source": bool(
            merge_config.get("delete_not_matched_by_source", False)
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


def get_mysql_config() -> dict[str, Any]:
    secret_payload = get_secret(MYSQL_SECRET_NAME)
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
    port = creds.get("PORT") or creds.get("port") or 3306

    if not host or not username or not password or not database:
        raise ValueError(
            f"Secret {MYSQL_SECRET_NAME!r} must contain HOST, USER, PASS, and DB."
        )

    return {
        "host": str(host),
        "username": str(username),
        "password": str(password),
        "database": validate_identifier(str(database), "MySQL database name"),
        "port": int(port),
    }


def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def get_mysql_connection(mysql_config: dict[str, Any]):
    if pymysql is not None:
        return pymysql.connect(
            host=mysql_config["host"],
            port=int(mysql_config["port"]),
            user=mysql_config["username"],
            password=mysql_config["password"],
            database=mysql_config["database"],
            charset="utf8mb4",
            autocommit=True,
            cursorclass=pymysql.cursors.SSCursor,
        )

    if mysql_connector is not None:
        return mysql_connector.connect(
            host=mysql_config["host"],
            port=int(mysql_config["port"]),
            user=mysql_config["username"],
            password=mysql_config["password"],
            database=mysql_config["database"],
            autocommit=True,
            use_pure=True,
        )

    raise ImportError(
        "This DAG requires either `pymysql` or `mysql-connector-python` "
        "to read from MySQL."
    )


def map_mysql_type_to_bigquery(data_type: str, numeric_scale: int | None) -> str:
    normalized = data_type.lower()

    if normalized in {"tinyint", "smallint", "mediumint", "int", "integer", "bigint"}:
        return "INT64"
    if normalized in {"decimal", "numeric"}:
        return "NUMERIC" if (numeric_scale or 0) <= 9 else "BIGNUMERIC"
    if normalized in {"float", "double", "double precision", "real"}:
        return "FLOAT64"
    if normalized in {"bool", "boolean", "bit"}:
        return "BOOL"
    if normalized in {"date"}:
        return "DATE"
    if normalized in {"datetime", "timestamp"}:
        return "TIMESTAMP"
    if normalized in {"time"}:
        return "TIME"
    if normalized in {"json"}:
        return "JSON"
    if normalized in {"binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob"}:
        return "BYTES"
    return "STRING"


def to_snake_case(value: str) -> str:
    value = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", value)
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    return value.lower()


def mysql_column_key(column_name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", column_name.lower())


def resolve_final_column_name(source_name: str, table_config: dict[str, Any]) -> str:
    return to_snake_case(source_name)


def fetch_mysql_columns(
    connection,
    mysql_config: dict[str, Any],
    table_config: dict[str, Any],
) -> list[dict[str, Any]]:
    query = """
    SELECT
      COLUMN_NAME,
      DATA_TYPE,
      IS_NULLABLE,
      NUMERIC_SCALE
    FROM information_schema.columns
    WHERE table_schema = %s
      AND table_name = %s
    ORDER BY ORDINAL_POSITION
    """

    cursor = connection.cursor()
    try:
        cursor.execute(
            query,
            (mysql_config["database"], table_config["mysql_table"]),
        )
        columns: list[dict[str, Any]] = []
        for column_name, data_type, is_nullable, numeric_scale in cursor.fetchall():
            columns.append(
                {
                    "source_name": str(column_name),
                    "data_type": str(data_type),
                    "numeric_scale": numeric_scale,
                    "is_nullable": str(is_nullable).upper() == "YES",
                }
            )
    finally:
        cursor.close()

    source_keys = {mysql_column_key(column["source_name"]) for column in columns}
    required_keys = {
        mysql_column_key(column_name)
        for column_name in table_config["required_source_columns"]
    }
    missing_required = required_keys - source_keys
    if missing_required:
        raise ValueError(
            f"MySQL source schema for {table_config['mysql_table']!r} is missing "
            f"required columns: {', '.join(sorted(missing_required))}"
        )

    return columns


def build_raw_schema(mysql_columns: list[dict[str, Any]]) -> list[bigquery.SchemaField]:
    schema = [
        bigquery.SchemaField(
            column["source_name"],
            map_mysql_type_to_bigquery(column["data_type"], column["numeric_scale"]),
            mode="NULLABLE",
        )
        for column in mysql_columns
    ]
    schema.append(bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="NULLABLE"))
    return schema


def build_final_column_specs(
    mysql_columns: list[dict[str, Any]],
    table_config: dict[str, Any],
) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    seen_target_names: set[str] = set()
    datetime_columns = {
        mysql_column_key(value)
        for value in table_config["datetime_from_unix_seconds_columns"]
    }

    for column in mysql_columns:
        source_name = str(column["source_name"])
        lookup_key = mysql_column_key(source_name)
        target_name = resolve_final_column_name(source_name, table_config)
        if target_name in seen_target_names:
            raise ValueError(
                f"MySQL schema for {table_config['mysql_table']!r} produces duplicate "
                f"final column name {target_name!r}."
            )

        target_type = (
            "DATETIME"
            if lookup_key in datetime_columns
            else map_mysql_type_to_bigquery(column["data_type"], column["numeric_scale"])
        )
        select_expression = (
            f"DATETIME(TIMESTAMP_SECONDS(CAST(`{source_name}` AS INT64)))"
            if lookup_key in datetime_columns
            else f"`{source_name}`"
        )
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
        logger.info(
            "Replacing non-table BigQuery object %s of type %s with TABLE",
            table_ref,
            getattr(current_table, "table_type", "UNKNOWN"),
        )
        client.delete_table(table_ref, not_found_ok=True)
        table = bigquery.Table(table_ref, schema=desired_schema)
        return client.create_table(table, exists_ok=True)

    return current_table


def get_max_table_timestamp(
    client: bigquery.Client,
    table_ref: str,
    timestamp_column: str,
) -> datetime | None:
    query = f"""
    SELECT MAX(`{timestamp_column}`) AS max_updatedat
    FROM `{table_ref}`
    """

    try:
        rows = list(client.query(query).result())
    except NotFound:
        return None

    if not rows:
        return None
    return rows[0]["max_updatedat"]


def reconcile_table_schema(
    client: bigquery.Client,
    table_ref: str,
    desired_schema: list[bigquery.SchemaField],
) -> bigquery.Table:
    desired_map = {field.name: field for field in desired_schema}
    current_table = ensure_table_exists(client, table_ref, desired_schema)

    current_map = {field.name: field for field in current_table.schema}

    for field in desired_schema:
        if field.name not in current_map:
            query = (
                f"ALTER TABLE `{table_ref}` "
                f"ADD COLUMN `{field.name}` {field.field_type}"
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
            client.query(query).result()

    return client.get_table(table_ref)


def parse_runtime_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def resolve_runtime_window(
    last_timestamp: datetime | None,
) -> tuple[datetime | None, datetime | None]:
    context = get_current_context()
    conf = (context.get("dag_run") and context["dag_run"].conf) or {}

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


def build_mysql_query(
    start_timestamp: datetime | None,
    end_timestamp: datetime | None,
    mysql_config: dict[str, Any],
    table_config: dict[str, Any],
) -> tuple[str, tuple[Any, ...]]:
    database_name = validate_identifier(mysql_config["database"], "MySQL database name")
    table_name = validate_identifier(table_config["mysql_table"], "MySQL table name")
    merge_config = table_config["merge_config"]
    qualified_table_name = f"`{database_name}`.`{table_name}`"
    source_incremental_column = merge_config["source_incremental_column"]

    if source_incremental_column is None:
        logger.info("Performing full snapshot load for %s.%s", database_name, table_name)
        return f"SELECT * FROM {qualified_table_name}", ()

    updated_column = validate_identifier(
        source_incremental_column,
        "incremental source column",
    )
    if start_timestamp is None and end_timestamp is None:
        logger.info("Performing initial load for %s.%s", database_name, table_name)
        return f"SELECT * FROM {qualified_table_name} ORDER BY `{updated_column}` ASC", ()

    filters: list[str] = []
    params: list[Any] = []

    if start_timestamp is not None:
        filters.append(f"`{updated_column}` > %s")
        params.append(start_timestamp)
    if end_timestamp is not None:
        filters.append(f"`{updated_column}` <= %s")
        params.append(end_timestamp)

    logger.info(
        "Loading MySQL rows for %s with %s window start=%s end=%s",
        table_name,
        updated_column,
        start_timestamp,
        end_timestamp,
    )
    where_clause = " AND ".join(filters)
    query = (
        f"SELECT * FROM {qualified_table_name} "
        f"WHERE {where_clause} ORDER BY `{updated_column}` ASC"
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
    if isinstance(value, timedelta):
        return value.total_seconds()
    return str(value)


def chunked(rows: list[dict[str, Any]], size: int):
    for index in range(0, len(rows), size):
        yield rows[index:index + size]


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
    client.load_table_from_json(rows, table_ref, job_config=job_config).result()


def truncate_table(client: bigquery.Client, table_ref: str) -> None:
    client.query(f"TRUNCATE TABLE `{table_ref}`").result()


def build_final_schema(
    mysql_columns: list[dict[str, Any]],
    table_config: dict[str, Any],
) -> list[bigquery.SchemaField]:
    return [spec["field"] for spec in build_final_column_specs(mysql_columns, table_config)]


def sync_table_schemas(table_config: dict[str, Any]) -> dict[str, Any]:
    table_config = normalize_table_config(table_config)

    client = get_bq_client()
    mysql_config = get_mysql_config()
    connection = get_mysql_connection(mysql_config)

    try:
        mysql_columns = fetch_mysql_columns(connection, mysql_config, table_config)
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
            build_raw_schema(mysql_columns),
        )
        final_table = reconcile_table_schema(
            client,
            final_table_ref,
            build_final_schema(mysql_columns, table_config),
        )

        return {
            "task_name": table_config["task_name"],
            "raw_table": raw_table_ref,
            "final_table": final_table_ref,
            "mysql_columns": len(mysql_columns),
            "raw_columns": len(raw_table.schema),
            "final_columns": len(final_table.schema),
        }
    finally:
        connection.close()


def sync_table_raw(
    schema_result: dict[str, Any],
    table_config: dict[str, Any],
) -> dict[str, Any]:
    table_config = normalize_table_config(table_config)
    client = get_bq_client()
    raw_table_ref = str(schema_result["raw_table"])
    final_table_ref = str(schema_result["final_table"])
    loaded_at = datetime.now(UTC).isoformat()
    mysql_config = get_mysql_config()
    connection = get_mysql_connection(mysql_config)

    try:
        raw_table = client.get_table(raw_table_ref)
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
        query, params = build_mysql_query(
            start_timestamp,
            end_timestamp,
            mysql_config,
            table_config,
        )

        truncate_table(client, raw_table_ref)
        total_rows = 0
        cursor = connection.cursor()
        try:
            cursor.execute(query, params)
            column_names = [column[0] for column in cursor.description]

            while True:
                batch = cursor.fetchmany(MYSQL_FETCH_SIZE)
                if not batch:
                    break

                json_rows = []
                for record in batch:
                    row = {
                        column_name: normalize_value(value)
                        for column_name, value in zip(column_names, record)
                    }
                    row["_loaded_at"] = loaded_at
                    json_rows.append(row)

                for load_batch in chunked(json_rows, BQ_LOAD_BATCH_SIZE):
                    load_rows(
                        client,
                        raw_table_ref,
                        raw_table.schema,
                        load_batch,
                        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    )

                total_rows += len(json_rows)
                logger.info("Loaded %s rows into %s", total_rows, raw_table_ref)
        finally:
            cursor.close()

        return {
            "task_name": table_config["task_name"],
            "target_table": raw_table_ref,
            "final_table": final_table_ref,
            "rows_loaded": total_rows,
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


def build_final_merge_query(
    mysql_columns: list[dict[str, Any]],
    raw_table_ref: str,
    final_table_ref: str,
    table_config: dict[str, Any],
) -> str:
    specs = build_final_column_specs(mysql_columns, table_config)
    merge_config = table_config["merge_config"]
    select_clause = ",\n          ".join(
        f"{spec['select_expression']} AS `{spec['target_name']}`"
        for spec in specs
    )
    insert_columns = ",\n      ".join(f"`{spec['target_name']}`" for spec in specs)
    insert_values = ",\n      ".join(
        f"source.`{spec['target_name']}`" for spec in specs
    )
    merge_keys = set(merge_config["join_keys"])
    update_specs = [spec for spec in specs if spec["target_name"] not in merge_keys]
    update_clause = ",\n      ".join(
        f"`{spec['target_name']}` = source.`{spec['target_name']}`"
        for spec in update_specs
    )
    partition_clause = ", ".join(
        f"`{value}`" for value in merge_config["partition_by"]
    )
    order_clause = ", ".join(
        f"`{item['column']}` {item['direction']}"
        for item in merge_config["order_by"]
    )
    on_clause = " AND ".join(
        f"target.`{value}` = source.`{value}`"
        for value in merge_config["join_keys"]
    )
    source_incremental_column = merge_config["source_incremental_column"]
    target_incremental_column = merge_config["target_incremental_column"]
    staged_where_clause = ""
    if source_incremental_column and target_incremental_column:
        staged_where_clause = f"""
        WHERE `{source_incremental_column}` > COALESCE(
          (SELECT MAX(`{target_incremental_column}`) FROM `{final_table_ref}`),
          TIMESTAMP("1970-01-01 00:00:00+00")
        )"""
    delete_not_matched_clause = (
        "\n    WHEN NOT MATCHED BY SOURCE THEN DELETE"
        if merge_config["delete_not_matched_by_source"]
        else ""
    )

    return f"""
    MERGE `{final_table_ref}` AS target
    USING (
      WITH staged AS (
        SELECT
          {select_clause}
        FROM `{raw_table_ref}`
{staged_where_clause}
      )
      SELECT *
      FROM staged
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY {partition_clause}
        ORDER BY {order_clause}
      ) = 1
    ) AS source
    ON {on_clause}
    WHEN MATCHED THEN UPDATE SET
      {update_clause}
    WHEN NOT MATCHED THEN INSERT (
      {insert_columns}
    )
    VALUES (
      {insert_values}
    ){delete_not_matched_clause}
    """


def merge_table_to_final(
    raw_result: dict[str, Any],
    table_config: dict[str, Any],
) -> dict[str, Any]:
    table_config = normalize_table_config(table_config)
    client = get_bq_client()
    raw_table_ref = str(raw_result["target_table"])
    final_table_ref = str(raw_result["final_table"])
    mysql_config = get_mysql_config()
    connection = get_mysql_connection(mysql_config)
    try:
        mysql_columns = fetch_mysql_columns(connection, mysql_config, table_config)
    finally:
        connection.close()

    query = build_final_merge_query(
        mysql_columns,
        raw_table_ref,
        final_table_ref,
        table_config,
    )
    job = client.query(query)
    job.result()

    return {
        "task_name": table_config["task_name"],
        "raw_table": raw_table_ref,
        "final_table": final_table_ref,
        "raw_rows_loaded": int(raw_result.get("rows_loaded", 0)),
        "merged_rows": job.num_dml_affected_rows,
        "loaded_at": raw_result.get("loaded_at"),
        "effective_start_timestamp": raw_result.get("effective_start_timestamp"),
        "effective_end_timestamp": raw_result.get("effective_end_timestamp"),
    }


def build_table_tasks() -> None:
    for table_config in TABLE_CONFIGS:
        normalized_config = normalize_table_config(table_config)
        task_suffix = normalized_config["task_name"]

        sync_schema_task = task(task_id=f"sync_{task_suffix}_schemas")(
            sync_table_schemas
        )(normalized_config)
        sync_raw_task = task(task_id=f"sync_{task_suffix}_raw")(sync_table_raw)(
            sync_schema_task,
            normalized_config,
        )
        merge_final_task = task(task_id=f"merge_{task_suffix}_to_final")(
            merge_table_to_final
        )(
            sync_raw_task,
            normalized_config,
        )

        sync_schema_task >> sync_raw_task >> merge_final_task


with DAG(
    dag_id="fxreplay_prod_mysql_to_bigquery",
    description=(
        "Carga incremental diaria de multiples tablas de fxreplay_prod hacia "
        "BigQuery raw y final."
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
    tags=["mysql", "bigquery", "fxreplay-prod", "raw", "daily"],
) as dag:
    build_table_tasks()
