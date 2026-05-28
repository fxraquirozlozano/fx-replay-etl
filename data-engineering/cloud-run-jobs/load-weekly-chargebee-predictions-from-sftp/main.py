import csv
import json
import os
import stat
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from fnmatch import fnmatch
from io import StringIO

from google.cloud import bigquery
from google.cloud import storage
import paramiko


PROJECT_ID = os.getenv("GCP_PROJECT", "fxr-analytics")
DATASET = os.getenv("BQ_DATASET", "chargebee")
TABLE = os.getenv("BQ_TABLE", "chargebee_predictions")
RAW_BQ_DATASET = os.getenv("RAW_BQ_DATASET", "chargebee_raw")
RAW_BQ_TABLE = os.getenv("RAW_BQ_TABLE", "chargebee_predictions")
RAW_BUCKET = os.getenv("RAW_BUCKET", "fxr-chargebee-exports")
RAW_PREFIX = os.getenv("RAW_PREFIX", "chargebee_predictions_raw").strip("/")
SFTP_HOST = os.getenv("SFTP_HOST", "").strip()
SFTP_PORT = int(os.getenv("SFTP_PORT", "22"))
SFTP_USER = os.getenv("SFTP_USER", "").strip()
SFTP_PRIVATE_KEY = os.getenv("SFTP_PRIVATE_KEY", "").strip()
SFTP_ROOT_PATH = os.getenv("SFTP_ROOT_PATH", "/predictions").strip() or "/predictions"
SFTP_BATCH_PATH = os.getenv("SFTP_BATCH_PATH", "").strip()
SFTP_FILE_GLOB = os.getenv("SFTP_FILE_GLOB", "predictions_part_*.csv")
WRITE_DISPOSITION = os.getenv("WRITE_DISPOSITION", "WRITE_APPEND").upper()
REQUIRED_CONTROL_FILES = ("metadata.json", "schema.csv")
SCHEMA_TYPE_MAP = {
    "STRING": "STRING",
    "TEXT": "STRING",
    "VARCHAR": "STRING",
    "CHAR": "STRING",
    "ENUM": "STRING",
    "CONSTANT": "STRING",
    "INTEGER": "INT64",
    "INT": "INT64",
    "INT64": "INT64",
    "FLOAT": "FLOAT64",
    "FLOAT64": "FLOAT64",
    "DOUBLE": "FLOAT64",
    "NUMERIC": "NUMERIC",
    "BIGNUMERIC": "BIGNUMERIC",
    "BOOLEAN": "BOOL",
    "BOOL": "BOOL",
    "DATE": "DATE",
    "DATETIME": "DATETIME",
    "TIMESTAMP": "TIMESTAMP",
    "TIME": "TIME",
    "JSON": "JSON",
    "BYTES": "BYTES",
}


def sftp_client() -> tuple[paramiko.SFTPClient, paramiko.Transport]:
    missing = [name for name, value in {
        "SFTP_HOST": SFTP_HOST,
        "SFTP_USER": SFTP_USER,
        "SFTP_PRIVATE_KEY": SFTP_PRIVATE_KEY,
    }.items() if not value]
    if missing:
        missing_names = ", ".join(missing)
        raise RuntimeError(f"Missing required SFTP configuration: {missing_names}")

    private_key = paramiko.Ed25519Key.from_private_key(StringIO(SFTP_PRIVATE_KEY))
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, pkey=private_key)
    return paramiko.SFTPClient.from_transport(transport), transport


def normalize_remote_dir(path: str) -> str:
    if not path.startswith("/"):
        return f"/{path}"
    return path.rstrip("/") or "/"


def latest_batch_path(sftp: paramiko.SFTPClient) -> str:
    root_path = normalize_remote_dir(SFTP_ROOT_PATH)
    entries = sftp.listdir_attr(root_path)
    dirs = [entry for entry in entries if stat.S_ISDIR(entry.st_mode)]
    if not dirs:
        raise FileNotFoundError(f"No batch directories found in {root_path}")

    latest_dir = max(dirs, key=lambda entry: (entry.st_mtime, entry.filename))
    return f"{root_path}/{latest_dir.filename}"


def list_batch_files(sftp: paramiko.SFTPClient, batch_path: str) -> list[str]:
    normalized = normalize_remote_dir(batch_path)
    entries = sftp.listdir_attr(normalized)
    files = [
        entry.filename
        for entry in entries
        if not stat.S_ISDIR(entry.st_mode)
    ]
    csv_files = sorted(name for name in files if fnmatch(name, SFTP_FILE_GLOB))
    missing_controls = [name for name in REQUIRED_CONTROL_FILES if name not in files]
    if not csv_files:
        raise FileNotFoundError(
            f"No files matching {SFTP_FILE_GLOB} were found in {normalized}"
        )
    if missing_controls:
        raise FileNotFoundError(
            f"Missing required files in {normalized}: {', '.join(missing_controls)}"
        )
    return csv_files + list(REQUIRED_CONTROL_FILES)


def download_files(
    sftp: paramiko.SFTPClient,
    batch_path: str,
    filenames: list[str],
    local_dir: Path,
) -> dict[str, Path]:
    downloaded: dict[str, Path] = {}
    normalized = normalize_remote_dir(batch_path)
    for filename in filenames:
        remote_path = f"{normalized}/{filename}"
        local_path = local_dir / filename
        sftp.get(remote_path, str(local_path))
        downloaded[filename] = local_path
    return downloaded


def upload_raw_files(batch_id: str, local_files: dict[str, Path]) -> dict[str, str]:
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(RAW_BUCKET)
    uris: dict[str, str] = {}
    for filename, local_path in local_files.items():
        blob_name = f"{RAW_PREFIX}/{batch_id}/{filename}" if RAW_PREFIX else f"{batch_id}/{filename}"
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(str(local_path))
        uris[filename] = f"gs://{RAW_BUCKET}/{blob_name}"
    return uris


def parse_schema(schema_path: Path) -> list[bigquery.SchemaField]:
    with schema_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("schema.csv is empty")

        normalized_headers = {header.strip().lower(): header for header in reader.fieldnames}
        name_key = next(
            (
                normalized_headers[key]
                for key in ("name", "field", "field_name", "column", "column_name")
                if key in normalized_headers
            ),
            None,
        )
        type_key = next(
            (
                normalized_headers[key]
                for key in ("type", "data_type", "field_type")
                if key in normalized_headers
            ),
            None,
        )
        mode_key = next(
            (
                normalized_headers[key]
                for key in ("mode", "nullable", "field_mode")
                if key in normalized_headers
            ),
            None,
        )
        description_key = normalized_headers.get("description")

        if not name_key or not type_key:
            raise ValueError(
                "schema.csv must include a column name and type header "
                "(for example: name,type)"
            )

        schema: list[bigquery.SchemaField] = []
        for row in reader:
            raw_name = (row.get(name_key) or "").strip()
            if not raw_name:
                continue
            raw_type = (row.get(type_key) or "STRING").strip().upper()
            bq_type = SCHEMA_TYPE_MAP.get(raw_type, "STRING")

            raw_mode = (row.get(mode_key) or "NULLABLE").strip().upper() if mode_key else "NULLABLE"
            if raw_mode in {"TRUE", "YES"}:
                raw_mode = "NULLABLE"
            elif raw_mode in {"FALSE", "NO"}:
                raw_mode = "REQUIRED"
            elif raw_mode not in {"NULLABLE", "REQUIRED", "REPEATED"}:
                raw_mode = "NULLABLE"

            description = (row.get(description_key) or "").strip() if description_key else None
            schema.append(
                bigquery.SchemaField(
                    raw_name,
                    bq_type,
                    mode=raw_mode,
                    description=description or None,
                )
            )

    if not schema:
        raise ValueError("schema.csv did not contain any fields")
    return schema


def raw_table_schema(schema: list[bigquery.SchemaField]) -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_file_name", "STRING"),
        bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
        *schema,
    ]


def final_table_schema(schema: list[bigquery.SchemaField]) -> list[bigquery.SchemaField]:
    return [
        *schema,
        bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
    ]


def normalize_value(raw_value: str, field: bigquery.SchemaField) -> str:
    if raw_value == "":
        return raw_value

    if field.field_type.upper() == "TIMESTAMP":
        for time_format in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(raw_value, time_format).strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
        raise ValueError(f"Unsupported timestamp value: {raw_value}")

    return raw_value


def normalize_csv_for_bigquery(
    source_path: Path,
    destination_path: Path,
    schema: list[bigquery.SchemaField],
    batch_id: str,
) -> None:
    schema_by_name = {field.name: field for field in schema}
    output_fieldnames = ["batch_id", "source_file_name", "loaded_at", *[field.name for field in schema]]
    loaded_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    with source_path.open("r", encoding="utf-8-sig", newline="") as source_handle:
        reader = csv.DictReader(source_handle)
        if not reader.fieldnames:
            raise ValueError(f"CSV file is empty: {source_path}")

        with destination_path.open("w", encoding="utf-8", newline="") as destination_handle:
            writer = csv.DictWriter(destination_handle, fieldnames=output_fieldnames)
            writer.writeheader()

            for row in reader:
                normalized_row = {
                    "batch_id": batch_id,
                    "source_file_name": source_path.name,
                    "loaded_at": loaded_at,
                }
                for column_name in [field.name for field in schema]:
                    field = schema_by_name.get(column_name)
                    value = row.get(column_name, "")
                    normalized_row[column_name] = normalize_value(value, field) if field else value
                writer.writerow(normalized_row)


def sanitize_identifier(value: str) -> str:
    return "".join(char if char.isalnum() else "_" for char in value)


def ensure_dataset(client: bigquery.Client, dataset_id: str) -> None:
    dataset_ref = f"{PROJECT_ID}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)


def ensure_table(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    schema: list[bigquery.SchemaField],
) -> None:
    table_ref = f"{PROJECT_ID}.{dataset_id}.{table_id}"
    ensure_dataset(client, dataset_id)
    try:
        table = client.get_table(table_ref)
        existing_names = {field.name for field in table.schema}
        missing_fields = [field for field in schema if field.name not in existing_names]
        if missing_fields:
            table.schema = [*table.schema, *missing_fields]
            client.update_table(table, ["schema"])
    except Exception:
        client.create_table(bigquery.Table(table_ref, schema=schema))


def load_into_table(
    client: bigquery.Client,
    csv_paths: list[Path],
    table_ref: str,
    schema: list[bigquery.SchemaField],
    write_disposition: str,
) -> None:
    for index, csv_path in enumerate(csv_paths):
        effective_write_disposition = write_disposition if index == 0 else "WRITE_APPEND"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=effective_write_disposition,
            schema=schema,
        )
        with csv_path.open("rb") as handle:
            client.load_table_from_file(handle, table_ref, job_config=job_config).result()


def merge_raw_table(
    client: bigquery.Client,
    temp_table_ref: str,
    raw_table_ref: str,
    schema: list[bigquery.SchemaField],
) -> None:
    column_names = ["batch_id", "source_file_name", "loaded_at", *[field.name for field in schema]]
    insert_columns = ", ".join(column_names)
    insert_values = ", ".join(f"src.{column}" for column in column_names)
    sql = f"""
    MERGE `{raw_table_ref}` AS target
    USING `{temp_table_ref}` AS src
    ON target.batch_id = src.batch_id
       AND target.unique_id = src.unique_id
    WHEN NOT MATCHED THEN
      INSERT ({insert_columns})
      VALUES ({insert_values})
    """
    client.query(sql).result()


def merge_final_table(
    client: bigquery.Client,
    temp_table_ref: str,
    final_table_ref: str,
    schema: list[bigquery.SchemaField],
) -> None:
    column_names = [field.name for field in schema]
    insert_columns = ", ".join(column_names)
    insert_values = ", ".join(f"src.{column}" for column in column_names)
    sql = f"""
    MERGE `{final_table_ref}` AS target
    USING `{temp_table_ref}` AS src
    ON target.unique_id = src.unique_id
    WHEN NOT MATCHED THEN
      INSERT ({insert_columns}, _ingested_at)
      VALUES ({insert_values}, src.loaded_at)
    """
    client.query(sql).result()


def load_predictions(batch_id: str, csv_paths: list[Path], schema: list[bigquery.SchemaField]) -> None:
    client = bigquery.Client(project=PROJECT_ID)
    final_table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    raw_table_ref = f"{PROJECT_ID}.{RAW_BQ_DATASET}.{RAW_BQ_TABLE}"
    temp_table_ref = (
        f"{PROJECT_ID}.{RAW_BQ_DATASET}."
        f"_tmp_{sanitize_identifier(RAW_BQ_TABLE)}_{sanitize_identifier(batch_id)}"
    )
    raw_schema = raw_table_schema(schema)
    final_schema = final_table_schema(schema)

    ensure_table(client, RAW_BQ_DATASET, RAW_BQ_TABLE, raw_schema)
    ensure_table(client, DATASET, TABLE, final_schema)

    try:
        load_into_table(client, csv_paths, temp_table_ref, raw_schema, "WRITE_TRUNCATE")
        merge_raw_table(client, temp_table_ref, raw_table_ref, schema)
        merge_final_table(client, temp_table_ref, final_table_ref, schema)
    finally:
        client.delete_table(temp_table_ref, not_found_ok=True)


def read_metadata(metadata_path: Path) -> dict:
    with metadata_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def main() -> None:
    sftp, transport = sftp_client()
    try:
        batch_path = normalize_remote_dir(SFTP_BATCH_PATH) if SFTP_BATCH_PATH else latest_batch_path(sftp)
        batch_id = batch_path.rsplit("/", 1)[-1]
        filenames = list_batch_files(sftp, batch_path)

        with tempfile.TemporaryDirectory() as tmp_dir:
            local_files = download_files(sftp, batch_path, filenames, Path(tmp_dir))
            upload_raw_files(batch_id, local_files)

            schema = parse_schema(local_files["schema.csv"])
            csv_filenames = sorted(name for name in filenames if fnmatch(name, SFTP_FILE_GLOB))
            metadata = read_metadata(local_files["metadata.json"])
            normalized_csv_paths: list[Path] = []
            for csv_filename in csv_filenames:
                normalized_path = Path(tmp_dir) / f"normalized-{csv_filename}"
                normalize_csv_for_bigquery(
                    local_files[csv_filename],
                    normalized_path,
                    schema,
                    batch_id,
                )
                normalized_csv_paths.append(normalized_path)

            load_predictions(batch_id, normalized_csv_paths, schema)

        print(
            "Predictions batch loaded",
            {
                "project": PROJECT_ID,
                "table": f"{PROJECT_ID}.{DATASET}.{TABLE}",
                "raw_table": f"{PROJECT_ID}.{RAW_BQ_DATASET}.{RAW_BQ_TABLE}",
                "batch_path": batch_path,
                "batch_id": batch_id,
                "raw_bucket": RAW_BUCKET,
                "raw_prefix": RAW_PREFIX,
                "csv_files": csv_filenames,
                "metadata_keys": sorted(metadata.keys()),
                "write_disposition": WRITE_DISPOSITION,
            },
        )
    finally:
        sftp.close()
        transport.close()


if __name__ == "__main__":
    main()
