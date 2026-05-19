import csv
import json
import os
import stat
import tempfile
from datetime import datetime
from pathlib import Path
from fnmatch import fnmatch
from io import StringIO

from google.cloud import bigquery
from google.cloud import storage
import paramiko


PROJECT_ID = os.getenv("GCP_PROJECT", "fxr-analytics")
DATASET = os.getenv("BQ_DATASET", "chargebee")
TABLE = os.getenv("BQ_TABLE", "chargebee_predictions")
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
) -> None:
    schema_by_name = {field.name: field for field in schema}

    with source_path.open("r", encoding="utf-8-sig", newline="") as source_handle:
        reader = csv.DictReader(source_handle)
        if not reader.fieldnames:
            raise ValueError(f"CSV file is empty: {source_path}")

        with destination_path.open("w", encoding="utf-8", newline="") as destination_handle:
            writer = csv.DictWriter(destination_handle, fieldnames=reader.fieldnames)
            writer.writeheader()

            for row in reader:
                normalized_row = {}
                for column_name in reader.fieldnames:
                    field = schema_by_name.get(column_name)
                    value = row.get(column_name, "")
                    normalized_row[column_name] = normalize_value(value, field) if field else value
                writer.writerow(normalized_row)


def load_predictions(csv_paths: list[Path], schema: list[bigquery.SchemaField]) -> None:
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    for index, csv_path in enumerate(csv_paths):
        write_disposition = WRITE_DISPOSITION if index == 0 else "WRITE_APPEND"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=write_disposition,
            schema=schema,
        )
        with csv_path.open("rb") as handle:
            client.load_table_from_file(handle, table_ref, job_config=job_config).result()


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
                normalize_csv_for_bigquery(local_files[csv_filename], normalized_path, schema)
                normalized_csv_paths.append(normalized_path)

            load_predictions(normalized_csv_paths, schema)

        print(
            "Predictions batch loaded",
            {
                "project": PROJECT_ID,
                "table": f"{PROJECT_ID}.{DATASET}.{TABLE}",
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
