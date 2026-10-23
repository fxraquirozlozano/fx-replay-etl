from __future__ import annotations

import json
import os
import tempfile
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from airflow import DAG

try:
    from airflow.sdk import get_current_context, task
except ImportError:
    from airflow.decorators import task
    try:
        from airflow.operators.python import get_current_context
    except ImportError:
        from airflow.decorators import get_current_context

from google.cloud import bigquery

from fxreplay_prod_mysql_to_bigquery_30min import (
    load_rows_from_jsonl_uri,
    sanitize_gcs_path_component,
    upload_file_to_gcs,
)
from fxreplay_prod_mysql_to_bigquery_daily import (
    BQ_LOAD_BATCH_SIZE,
    DAG_TIMEZONE,
    MYSQL_FETCH_SIZE,
    TABLE_CONFIGS,
    build_final_column_specs,
    build_final_schema,
    build_mysql_query,
    chunked,
    fetch_mysql_columns,
    get_bq_client,
    get_mysql_config,
    get_mysql_connection,
    normalize_table_config,
    normalize_value,
    reconcile_table_schema,
    sync_table_schemas,
    truncate_table,
)


BACKFILL_TABLE_CONFIGS = tuple(
    normalize_table_config(
        {
            **table_config,
            "previous_day_window": False,
            "fixed_start_timestamp": datetime(
                2026,
                3,
                5,
                20,
                53,
                5,
                tzinfo=ZoneInfo("America/Lima"),
            ).astimezone(UTC),
        }
    )
    for table_config in TABLE_CONFIGS
    if str(table_config.get("task_name")) == "backtesting_sessions"
)


def stage_json_rows_to_gcs(
    rows: list[dict[str, Any]],
    blob_name: str,
) -> str:
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".jsonl",
        delete=False,
    ) as temp_file:
        file_path = temp_file.name
        for row in rows:
            temp_file.write(json.dumps(row, separators=(",", ":")))
            temp_file.write("\n")

    try:
        return upload_file_to_gcs(file_path, blob_name)
    finally:
        if os.path.exists(file_path):
            os.unlink(file_path)


def export_backtesting_sessions_to_gcs(
    schema_result: dict[str, Any],
    table_config: dict[str, Any],
) -> dict[str, Any]:
    table_config = normalize_table_config(table_config)
    context = get_current_context()
    final_table_ref = str(schema_result["final_table"])
    loaded_at = datetime.now(UTC).isoformat()
    mysql_config = get_mysql_config()
    connection = get_mysql_connection(mysql_config)

    try:
        mysql_columns = fetch_mysql_columns(connection, mysql_config, table_config)
        raw_bool_columns = {
            str(column["source_name"])
            for column in mysql_columns
            if str(column["column_type"]).lower() == "tinyint(1)"
        }
        start_timestamp = table_config.get("fixed_start_timestamp")
        query, params = build_mysql_query(
            start_timestamp if isinstance(start_timestamp, datetime) else None,
            None,
            mysql_config,
            table_config,
        )
        run_id = sanitize_gcs_path_component(str(context.get("run_id", "manual")))
        logical_date = context.get("logical_date")
        logical_date_part = (
            sanitize_gcs_path_component(str(logical_date.isoformat()))
            if isinstance(logical_date, datetime)
            else "no-logical-date"
        )

        total_rows = 0
        staged_chunk_uris: list[str] = []
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
                        column_name: (
                            bool(value)
                            if column_name in raw_bool_columns and value is not None
                            else normalize_value(value)
                        )
                        for column_name, value in zip(column_names, record)
                    }
                    row["_loaded_at"] = loaded_at
                    json_rows.append(row)

                for load_batch in chunked(json_rows, BQ_LOAD_BATCH_SIZE):
                    chunk_index = len(staged_chunk_uris) + 1
                    gcs_blob_name = (
                        "tmp/fxreplay_prod_mysql_to_bigquery_backfill/"
                        f"{table_config['task_name']}/"
                        f"{logical_date_part}/"
                        f"{run_id}/"
                        f"chunk-{chunk_index:05d}.jsonl"
                    )
                    source_uri = stage_json_rows_to_gcs(
                        load_batch,
                        gcs_blob_name,
                    )
                    staged_chunk_uris.append(source_uri)

                total_rows += len(json_rows)
        finally:
            cursor.close()

        return {
            "task_name": table_config["task_name"],
            "target_table": str(schema_result["raw_table"]),
            "final_table": final_table_ref,
            "rows_loaded": total_rows,
            "staged_chunk_count": len(staged_chunk_uris),
            "chunk_uris": staged_chunk_uris,
            "first_staged_chunk_uri": (
                staged_chunk_uris[0] if staged_chunk_uris else None
            ),
            "last_staged_chunk_uri": (
                staged_chunk_uris[-1] if staged_chunk_uris else None
            ),
            "effective_start_timestamp": (
                start_timestamp.isoformat()
                if isinstance(start_timestamp, datetime)
                else None
            ),
            "loaded_at": loaded_at,
        }
    finally:
        connection.close()


def load_backtesting_sessions_raw(
    export_result: dict[str, Any],
    table_config: dict[str, Any],
) -> dict[str, Any]:
    table_config = normalize_table_config(table_config)
    client = get_bq_client()
    raw_table_ref = str(export_result["target_table"])
    raw_table = client.get_table(raw_table_ref)
    chunk_uris = [
        str(value)
        for value in (export_result.get("chunk_uris") or [])
        if value
    ]

    if not chunk_uris:
        truncate_table(client, raw_table_ref)
    else:
        for index, source_uri in enumerate(chunk_uris):
            load_rows_from_jsonl_uri(
                client,
                raw_table_ref,
                raw_table.schema,
                source_uri,
                write_disposition=(
                    bigquery.WriteDisposition.WRITE_TRUNCATE
                    if index == 0
                    else bigquery.WriteDisposition.WRITE_APPEND
                ),
            )

    return {
        "task_name": table_config["task_name"],
        "target_table": raw_table_ref,
        "final_table": str(export_result["final_table"]),
        "rows_loaded": int(export_result.get("rows_loaded", 0)),
        "staged_chunk_count": int(export_result.get("staged_chunk_count", 0)),
        "chunk_uris": chunk_uris,
        "first_staged_chunk_uri": export_result.get("first_staged_chunk_uri"),
        "last_staged_chunk_uri": export_result.get("last_staged_chunk_uri"),
        "effective_start_timestamp": export_result.get("effective_start_timestamp"),
        "loaded_at": export_result.get("loaded_at"),
    }


def build_full_refresh_insert_query(
    mysql_columns: list[dict[str, Any]],
    raw_table_ref: str,
    final_table_ref: str,
    table_config: dict[str, Any],
    final_field_types: dict[str, str] | None = None,
) -> str:
    specs = build_final_column_specs(mysql_columns, table_config)
    merge_config = table_config["merge_config"]

    def normalize_bq_type_name(type_name: str) -> str:
        normalized = str(type_name).upper()
        if normalized == "INTEGER":
            return "INT64"
        return normalized

    def insert_select_expression(spec: dict[str, Any]) -> str:
        expression = str(spec["select_expression"])
        desired_type = normalize_bq_type_name(str(spec["field"].field_type))
        actual_type = normalize_bq_type_name(
            (final_field_types or {}).get(str(spec["target_name"]), desired_type)
        )

        if actual_type == desired_type:
            return expression
        if actual_type == "STRING" and desired_type == "JSON":
            return f"TO_JSON_STRING({expression})"
        if actual_type == "INT64" and desired_type == "BOOL":
            return f"IF({expression}, 1, 0)"
        if actual_type == "INT64" and desired_type == "DATETIME":
            return f"UNIX_SECONDS(TIMESTAMP({expression}))"
        if actual_type == "STRING":
            return f"CAST({expression} AS STRING)"
        return expression

    select_clause = ",\n          ".join(
        f"{insert_select_expression(spec)} AS `{spec['target_name']}`"
        for spec in specs
    )
    insert_columns = ",\n      ".join(f"`{spec['target_name']}`" for spec in specs)
    partition_clause = ", ".join(
        f"`{value}`" for value in merge_config["partition_by"]
    )
    order_clause = ", ".join(
        f"`{item['column']}` {item['direction']}"
        for item in merge_config["order_by"]
    )

    return f"""
    INSERT INTO `{final_table_ref}` (
      {insert_columns}
    )
    WITH staged AS (
      SELECT
          {select_clause}
      FROM `{raw_table_ref}`
    )
    SELECT *
    FROM staged
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY {partition_clause}
      ORDER BY {order_clause}
    ) = 1
    """


def delete_insert_backtesting_sessions_final(
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

    final_table = reconcile_table_schema(
        client,
        final_table_ref,
        build_final_schema(mysql_columns, table_config),
    )
    final_field_types = {
        field.name: str(field.field_type).upper()
        for field in final_table.schema
    }

    target_incremental_column = str(
        table_config["merge_config"]["target_incremental_column"]
    )
    effective_start_timestamp = raw_result.get("effective_start_timestamp")
    if effective_start_timestamp:
        delete_query = f"""
        DELETE FROM `{final_table_ref}`
        WHERE `{target_incremental_column}` >= TIMESTAMP('{str(effective_start_timestamp).replace("+00:00", "Z")}')
        """
    else:
        delete_query = f"DELETE FROM `{final_table_ref}` WHERE TRUE"

    delete_job = client.query(delete_query)
    delete_job.result()

    insert_query = build_full_refresh_insert_query(
        mysql_columns,
        raw_table_ref,
        final_table_ref,
        table_config,
        final_field_types,
    )
    insert_job = client.query(insert_query)
    insert_job.result()

    return {
        "task_name": table_config["task_name"],
        "raw_table": raw_table_ref,
        "final_table": final_table_ref,
        "raw_rows_loaded": int(raw_result.get("rows_loaded", 0)),
        "deleted_rows": delete_job.num_dml_affected_rows,
        "inserted_rows": insert_job.num_dml_affected_rows,
        "loaded_at": raw_result.get("loaded_at"),
        "first_staged_chunk_uri": raw_result.get("first_staged_chunk_uri"),
        "last_staged_chunk_uri": raw_result.get("last_staged_chunk_uri"),
    }


def build_backfill_tasks(table_configs: tuple[dict[str, Any], ...]) -> None:
    for table_config in table_configs:
        normalized_config = normalize_table_config(table_config)
        task_suffix = normalized_config["task_name"]

        sync_schema_task = task(task_id=f"sync_{task_suffix}_schemas")(
            sync_table_schemas
        )(normalized_config)
        export_gcs_task = task(task_id=f"export_{task_suffix}_to_gcs")(
            export_backtesting_sessions_to_gcs
        )(
            sync_schema_task,
            normalized_config,
        )
        load_raw_task = task(task_id=f"load_{task_suffix}_raw")(
            load_backtesting_sessions_raw
        )(
            export_gcs_task,
            normalized_config,
        )
        delete_insert_final_task = task(task_id=f"delete_insert_{task_suffix}_to_final")(
            delete_insert_backtesting_sessions_final
        )(
            load_raw_task,
            normalized_config,
        )

        sync_schema_task >> export_gcs_task >> load_raw_task >> delete_insert_final_task


with DAG(
    dag_id="fxreplay_prod_mysql_to_bigquery_backfill",
    description=(
        "Backfill completo de backtesting_sessions desde fxreplay_prod hacia "
        "BigQuery usando MySQL -> GCS -> raw y refresh final con DELETE + INSERT."
    ),
    start_date=datetime(2024, 1, 1, 5, 0, tzinfo=ZoneInfo(DAG_TIMEZONE)),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["mysql", "bigquery", "fxreplay-prod", "raw", "backfill"],
) as dag:
    build_backfill_tasks(BACKFILL_TABLE_CONFIGS)
