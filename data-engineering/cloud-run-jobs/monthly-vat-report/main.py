import csv
import hashlib
import json
import os
import re
import traceback
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote
from zoneinfo import ZoneInfo

import msal
import requests
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
ROAM_ALERT_URL = "https://api.ro.am/v1/chat.sendMessage"
DEFAULT_OUTPUT_DIR = "/tmp"
MAX_SIMPLE_UPLOAD_BYTES = 250 * 1024 * 1024
MONTH_ABBREVIATIONS = (
    "JAN",
    "FEB",
    "MAR",
    "APR",
    "MAY",
    "JUN",
    "JUL",
    "AUG",
    "SEP",
    "OCT",
    "NOV",
    "DEC",
)

REPORT_COLUMNS = (
    "month",
    "invoice_id",
    "customer_id",
    "subscription_id",
    "country_name",
    "country_code",
    "state",
    "state_code",
    "plan",
    "invoice_date",
    "paid_at_utc",
    "status",
    "currency_code",
    "base_currency_code",
    "exchange_rate",
    "sub_total",
    "total_billing",
    "total_discounts",
    "total_adjustments",
    "adjustment_credit_note_ids",
    "total_issued_credit_notes",
    "issued_credit_note_ids",
    "total_applied_credits",
    "applied_credit_note_ids",
    "net_billing",
    "snapshot_at",
    "source_as_of",
)


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False
    raise ValueError(f"{name} must be true or false")


def validate_identifier(value: str, name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ValueError(f"{name} contains unsupported characters: {value!r}")
    return value


@dataclass(frozen=True)
class Config:
    project_id: str
    bq_dataset: str
    source_table: str
    snapshot_table: str
    business_timezone: str
    report_month_override: str
    source_as_of: str
    refresh_snapshot: bool
    output_dir: Path
    sharepoint_hostname: str
    sharepoint_site_path: str
    sharepoint_drive_id: str
    sharepoint_folder_path: str
    tenant_id: str
    client_id: str
    client_secret: str
    roam_api_key: str
    roam_group_id: str
    overwrite_existing: bool
    dry_run: bool
    request_timeout_seconds: int

    @classmethod
    def from_env(cls) -> "Config":
        dry_run = env_bool("DRY_RUN", False)
        config = cls(
            project_id=validate_identifier(
                os.getenv("GCP_PROJECT", "fxr-analytics"), "GCP_PROJECT"
            ),
            bq_dataset=validate_identifier(
                os.getenv("BQ_DATASET", "reporting"), "BQ_DATASET"
            ),
            source_table=validate_identifier(
                os.getenv("SOURCE_TABLE", "fct_invoices"), "SOURCE_TABLE"
            ),
            snapshot_table=validate_identifier(
                os.getenv("SNAPSHOT_TABLE", "vat_report_monthly_snapshots"),
                "SNAPSHOT_TABLE",
            ),
            business_timezone=os.getenv(
                "BUSINESS_TIMEZONE", "America/Chicago"
            ).strip(),
            report_month_override=os.getenv("REPORT_MONTH", "").strip(),
            source_as_of=os.getenv("SOURCE_AS_OF", "").strip(),
            refresh_snapshot=env_bool("REFRESH_SNAPSHOT", False),
            output_dir=Path(os.getenv("OUTPUT_DIR", DEFAULT_OUTPUT_DIR)),
            sharepoint_hostname=os.getenv(
                "SHAREPOINT_HOSTNAME", "fxreplay.sharepoint.com"
            ).strip(),
            sharepoint_site_path=os.getenv(
                "SHAREPOINT_SITE_PATH", "sites/DataTeam"
            ).strip("/ "),
            sharepoint_drive_id=os.getenv(
                "SHAREPOINT_DRIVE_ID",
                "b!Mi_zgpjDy0apgS3ExKpIDJsXOuxMNwRKj7LzgT7nHqaDTMP01zt1R6i62KC3eata",
            ).strip(),
            sharepoint_folder_path=os.getenv(
                "SHAREPOINT_FOLDER_PATH", "Finance/VAT Report"
            ).strip("/ "),
            tenant_id=os.getenv("TENANT_ID", "").strip(),
            client_id=os.getenv("CLIENT_ID", "").strip(),
            client_secret=os.getenv("CLIENT_SECRET", "").strip(),
            roam_api_key=os.getenv("ROAM_API_KEY", "").strip(),
            roam_group_id=os.getenv("ROAM_GROUP_ID", "").strip(),
            overwrite_existing=env_bool("OVERWRITE_EXISTING", True),
            dry_run=dry_run,
            request_timeout_seconds=int(
                os.getenv("REQUEST_TIMEOUT_SECONDS", "120")
            ),
        )

        ZoneInfo(config.business_timezone)
        if config.source_as_of:
            try:
                datetime.fromisoformat(config.source_as_of.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ValueError("SOURCE_AS_OF must be an ISO-8601 timestamp") from exc
        if config.request_timeout_seconds < 1:
            raise ValueError("REQUEST_TIMEOUT_SECONDS must be at least 1")

        if not dry_run:
            for name, value in (
                ("TENANT_ID", config.tenant_id),
                ("CLIENT_ID", config.client_id),
                ("CLIENT_SECRET", config.client_secret),
            ):
                if not value:
                    raise ValueError(f"Missing required environment variable: {name}")

        return config


@dataclass(frozen=True)
class ValidationSummary:
    report_month: str
    row_count: int
    net_billing: Decimal
    rows_by_status: dict[str, int]
    net_billing_by_status: dict[str, Decimal]


def previous_calendar_month(current_date: date) -> date:
    first_of_month = current_date.replace(day=1)
    if first_of_month.month == 1:
        return date(first_of_month.year - 1, 12, 1)
    return date(first_of_month.year, first_of_month.month - 1, 1)


def resolve_report_month(
    override: str, business_timezone: str, now: datetime | None = None
) -> date:
    if override:
        try:
            parsed = datetime.strptime(override, "%Y-%m").date()
        except ValueError as exc:
            raise ValueError("REPORT_MONTH must use YYYY-MM format") from exc
        return parsed.replace(day=1)

    current = now or datetime.now(ZoneInfo(business_timezone))
    if current.tzinfo is None:
        current = current.replace(tzinfo=ZoneInfo(business_timezone))
    else:
        current = current.astimezone(ZoneInfo(business_timezone))
    return previous_calendar_month(current.date())


def report_filename(report_month: date) -> str:
    abbreviation = MONTH_ABBREVIATIONS[report_month.month - 1]
    return f"{abbreviation} {report_month.year}.csv"


def report_query(project_id: str, dataset: str, snapshot_table: str) -> str:
    source = f"`{project_id}.{dataset}.{snapshot_table}`"
    selected_columns = ",\n      ".join(REPORT_COLUMNS)
    return f"""
    SELECT
      {selected_columns}
    FROM {source}
    WHERE report_month = @report_month
      AND NOT (
        COALESCE(total_billing, 0) = 0
        AND net_billing = 0
      )
    ORDER BY invoice_date, invoice_id
    """


def snapshot_select_sql(config: Config, include_filter: bool = True) -> str:
    source = f"`{config.project_id}.{config.bq_dataset}.{config.source_table}`"
    if config.source_as_of:
        parsed = datetime.fromisoformat(config.source_as_of.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=ZoneInfo(config.business_timezone))
        source_as_of = parsed.astimezone(ZoneInfo("UTC"))
    else:
        source_as_of = datetime.now(ZoneInfo("UTC"))
    as_of_literal = source_as_of.isoformat()
    system_time = (
        f" FOR SYSTEM_TIME AS OF TIMESTAMP('{as_of_literal}')"
        if config.source_as_of
        else ""
    )
    where_clause = """
    WHERE i.date >= DATETIME(@report_month)
      AND i.date < DATETIME(DATE_ADD(@report_month, INTERVAL 1 MONTH))
      AND i.status != 'voided'
    """ if include_filter else "WHERE FALSE"
    return f"""
    SELECT
      @report_month AS report_month,
      CURRENT_TIMESTAMP() AS snapshot_at,
      TIMESTAMP('{as_of_literal}') AS source_as_of,
      FORMAT_DATE('%Y-%m', @report_month) AS month,
      i.id AS invoice_id,
      i.customer_id,
      i.subscription_id,
      i.country_name,
      i.country_code,
      i.state,
      i.state_code,
      i.plan,
      i.date AS invoice_date,
      i.paid_at_utc,
      i.status,
      i.currency_code,
      i.base_currency_code,
      i.exchange_rate,
      i.sub_total,
      i.total_billing,
      i.total_discounts,
      i.total_adjustments,
      i.adjustment_credit_note_ids,
      i.total_issued_credit_notes,
      i.issued_credit_note_ids,
      i.total_applied_credits,
      i.applied_credit_note_ids,
      i.net_billing
    FROM {source} AS i{system_time}
    {where_clause}
    """


def ensure_monthly_snapshot(
    client: bigquery.Client, config: Config, report_month: date
) -> str:
    target = f"`{config.project_id}.{config.bq_dataset}.{config.snapshot_table}`"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("report_month", "DATE", report_month)
        ]
    )
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {target}
    PARTITION BY report_month
    CLUSTER BY invoice_id AS
    {snapshot_select_sql(config, include_filter=False)}
    """
    client.query(create_sql, job_config=job_config).result()

    count_sql = f"SELECT COUNT(*) AS row_count FROM {target} WHERE report_month = @report_month"
    existing_count = next(
        iter(client.query(count_sql, job_config=job_config).result())
    ).row_count

    if existing_count and not config.refresh_snapshot:
        return "reused"
    if existing_count:
        client.query(
            f"DELETE FROM {target} WHERE report_month = @report_month",
            job_config=job_config,
        ).result()

    insert_sql = f"INSERT INTO {target} {snapshot_select_sql(config)}"
    client.query(insert_sql, job_config=job_config).result()
    return "replaced" if existing_count else "created"


def fetch_report_rows(
    client: bigquery.Client, config: Config, report_month: date
) -> Iterable[dict[str, Any]]:
    query = report_query(
        config.project_id, config.bq_dataset, config.snapshot_table
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "report_month", "DATE", report_month
            )
        ]
    )
    result = client.query(query, job_config=job_config).result()
    return (dict(row.items()) for row in result)


def is_missing(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def decimal_value(value: Any, field_name: str, movement_id: str) -> Decimal:
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(
            f"{field_name} is not numeric for movement_id={movement_id}"
        ) from exc
    if not amount.is_finite():
        raise ValueError(
            f"{field_name} is not finite for movement_id={movement_id}"
        )
    return amount


def process_report_rows(
    rows: Iterable[dict[str, Any]],
    report_month: date,
    writer: csv.DictWriter | None = None,
) -> ValidationSummary:
    expected_month = report_month.strftime("%Y-%m")
    invoice_ids: set[str] = set()
    rows_by_status: dict[str, int] = {}
    totals_by_status: dict[str, Decimal] = {}
    row_count = 0

    for row_number, row in enumerate(rows, start=2):
        if row_count == 0:
            missing_columns = [
                column for column in REPORT_COLUMNS if column not in row
            ]
            if missing_columns:
                raise ValueError(f"Missing expected columns: {missing_columns}")

        row_month = str(row.get("month", ""))
        if row_month != expected_month:
            raise ValueError(
                f"Row {row_number} belongs to {row_month!r}; expected {expected_month}"
            )

        invoice_id = str(row.get("invoice_id", "")).strip()
        if not invoice_id:
            raise ValueError(f"Row {row_number} has an empty invoice_id")
        if invoice_id in invoice_ids:
            raise ValueError(f"Duplicate invoice_id: {invoice_id}")
        invoice_ids.add(invoice_id)

        invoice_date = row.get("invoice_date")
        if invoice_date is None or invoice_date.strftime("%Y-%m") != expected_month:
            raise ValueError(
                f"Invoice {invoice_id} has invoice_date={invoice_date!r}; "
                f"expected month {expected_month}"
            )
        status = str(row.get("status", "")).strip()
        if not status:
            raise ValueError(f"Invoice {invoice_id} has an empty status")
        if status == "voided":
            raise ValueError(f"Voided invoice {invoice_id} is not reportable")

        amount = decimal_value(row.get("net_billing"), "net_billing", invoice_id)
        rows_by_status[status] = rows_by_status.get(status, 0) + 1
        totals_by_status[status] = totals_by_status.get(status, Decimal("0")) + amount
        row_count += 1
        if writer is not None:
            writer.writerow(
                {column: csv_value(row.get(column)) for column in REPORT_COLUMNS}
            )

    if row_count == 0:
        raise ValueError("VAT report query returned 0 rows")

    return ValidationSummary(
        report_month=expected_month,
        row_count=row_count,
        net_billing=sum(totals_by_status.values(), Decimal("0")),
        rows_by_status=rows_by_status,
        net_billing_by_status=totals_by_status,
    )


def validate_report(
    rows: Iterable[dict[str, Any]], report_month: date
) -> ValidationSummary:
    return process_report_rows(rows, report_month)


def csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return json.dumps(list(value), separators=(",", ":"))
    return value


def write_report_csv(
    rows: list[dict[str, Any]], output_dir: Path, filename: str
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename
    with output_path.open("w", newline="", encoding="utf-8-sig") as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=REPORT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {column: csv_value(row.get(column)) for column in REPORT_COLUMNS}
            )
    return output_path


def validate_and_write_report(
    rows: Iterable[dict[str, Any]],
    report_month: date,
    output_dir: Path,
    filename: str,
) -> tuple[Path, ValidationSummary]:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename
    temporary_path = output_dir / f".{filename}.tmp"
    try:
        with temporary_path.open(
            "w", newline="", encoding="utf-8-sig"
        ) as file_handle:
            writer = csv.DictWriter(
                file_handle, fieldnames=REPORT_COLUMNS, extrasaction="ignore"
            )
            writer.writeheader()
            summary = process_report_rows(rows, report_month, writer)
        temporary_path.replace(output_path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise
    return output_path, summary


def create_http_session() -> requests.Session:
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "PUT", "POST"}),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    return session


def acquire_graph_token(config: Config) -> str:
    app = msal.ConfidentialClientApplication(
        client_id=config.client_id,
        authority=f"https://login.microsoftonline.com/{config.tenant_id}",
        client_credential=config.client_secret,
    )
    result = app.acquire_token_for_client(
        scopes=["https://graph.microsoft.com/.default"]
    )
    if "access_token" not in result:
        error = result.get("error_description") or result.get("error") or "unknown error"
        raise RuntimeError(f"Microsoft Graph authentication failed: {error}")
    return result["access_token"]


def resolve_site_id(
    session: requests.Session, config: Config, headers: dict[str, str]
) -> str:
    site_path = quote(config.sharepoint_site_path, safe="/")
    url = (
        f"{GRAPH_BASE_URL}/sites/{config.sharepoint_hostname}:/{site_path}"
    )
    response = session.get(
        url, headers=headers, timeout=config.request_timeout_seconds
    )
    if not response.ok:
        raise RuntimeError(
            f"SharePoint site resolution failed: HTTP {response.status_code} "
            f"{response.text[:500]}"
        )
    site_id = response.json().get("id")
    if not site_id:
        raise RuntimeError("Microsoft Graph response did not include a site id")
    return site_id


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def upload_report(
    session: requests.Session,
    config: Config,
    site_id: str,
    access_token: str,
    output_path: Path,
) -> str:
    content = output_path.read_bytes()
    if len(content) > MAX_SIMPLE_UPLOAD_BYTES:
        raise ValueError(
            f"Report is {len(content)} bytes; Microsoft Graph simple upload "
            f"supports at most {MAX_SIMPLE_UPLOAD_BYTES} bytes"
        )

    destination = quote(
        f"{config.sharepoint_folder_path}/{output_path.name}", safe="/"
    )
    item_url = (
        f"{GRAPH_BASE_URL}/sites/{site_id}/drives/{config.sharepoint_drive_id}"
        f"/root:/{destination}"
    )
    headers = {"Authorization": f"Bearer {access_token}"}
    check = session.get(
        item_url, headers=headers, timeout=config.request_timeout_seconds
    )

    if check.status_code == 200:
        existing = session.get(
            f"{item_url}:/content",
            headers=headers,
            timeout=config.request_timeout_seconds,
        )
        if not existing.ok:
            raise RuntimeError(
                f"Existing SharePoint file download failed: HTTP "
                f"{existing.status_code} {existing.text[:500]}"
            )
        if sha256_bytes(existing.content) == sha256_bytes(content):
            return "unchanged"
        if not config.overwrite_existing:
            raise FileExistsError(
                f"{output_path.name} already exists with different content and "
                "OVERWRITE_EXISTING=false"
            )
        action = "replaced"
    elif check.status_code == 404:
        action = "created"
    else:
        raise RuntimeError(
            f"SharePoint file check failed: HTTP {check.status_code} "
            f"{check.text[:500]}"
        )

    upload = session.put(
        f"{item_url}:/content",
        headers={**headers, "Content-Type": "text/csv"},
        data=content,
        timeout=config.request_timeout_seconds,
    )
    if upload.status_code not in (200, 201):
        raise RuntimeError(
            f"SharePoint upload failed: HTTP {upload.status_code} "
            f"{upload.text[:500]}"
        )
    return action


def send_roam_alert(
    session: requests.Session,
    config: Config,
    text: str,
) -> None:
    if not config.roam_api_key or not config.roam_group_id:
        log_event("roam_alert_skipped", reason="credentials_not_configured")
        return
    response = session.post(
        ROAM_ALERT_URL,
        headers={
            "Authorization": f"Bearer {config.roam_api_key}",
            "Content-Type": "application/json",
        },
        json={
            "text": text,
            "sender": {"id": "vat_automation", "name": "VAT Report Automation"},
            "recipients": [config.roam_group_id],
        },
        timeout=config.request_timeout_seconds,
    )
    if not response.ok:
        raise RuntimeError(
            f"Roam alert failed: HTTP {response.status_code} {response.text[:500]}"
        )


def decimal_json(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.01")), "f")


def log_event(event: str, **details: Any) -> None:
    print(json.dumps({"event": event, **details}, default=str, sort_keys=True))


def run(config: Config) -> dict[str, Any]:
    report_month = resolve_report_month(
        config.report_month_override, config.business_timezone
    )
    client = bigquery.Client(project=config.project_id)
    snapshot_action = ensure_monthly_snapshot(client, config, report_month)
    rows = fetch_report_rows(client, config, report_month)
    filename = report_filename(report_month)
    output_path, summary = validate_and_write_report(
        rows, report_month, config.output_dir, filename
    )

    log_event(
        "vat_report_validated",
        report_month=summary.report_month,
        row_count=summary.row_count,
        net_billing=decimal_json(summary.net_billing),
        rows_by_status=summary.rows_by_status,
        snapshot_action=snapshot_action,
        output_path=str(output_path),
    )

    if config.dry_run:
        upload_action = "dry_run"
    else:
        session = create_http_session()
        access_token = acquire_graph_token(config)
        headers = {"Authorization": f"Bearer {access_token}"}
        site_id = resolve_site_id(session, config, headers)
        upload_action = upload_report(
            session, config, site_id, access_token, output_path
        )
        send_roam_alert(
            session,
            config,
            "\n".join(
                [
                    "✅ Monthly VAT report completed successfully.",
                    f"Month: {summary.report_month}",
                    f"Rows: {summary.row_count}",
                    f"Net Billing: USD {decimal_json(summary.net_billing)}",
                    f"SharePoint action: {upload_action}",
                    f"File: {filename}",
                ]
            ),
        )

    result = {
        "report_month": summary.report_month,
        "row_count": summary.row_count,
        "net_billing": decimal_json(summary.net_billing),
        "rows_by_status": summary.rows_by_status,
        "net_billing_by_status": {
            key: decimal_json(value)
            for key, value in summary.net_billing_by_status.items()
        },
        "snapshot_action": snapshot_action,
        "filename": filename,
        "upload_action": upload_action,
    }
    log_event("vat_report_completed", **result)
    return result


def main() -> None:
    config: Config | None = None
    try:
        config = Config.from_env()
        run(config)
    except Exception as exc:
        log_event(
            "vat_report_failed",
            error_type=type(exc).__name__,
            error=str(exc),
            traceback=traceback.format_exc(),
        )
        if config is not None and not config.dry_run:
            try:
                send_roam_alert(
                    create_http_session(),
                    config,
                    "\n".join(
                        [
                            "❌ Monthly VAT report automation FAILED.",
                            f"Error: {type(exc).__name__}: {exc}",
                            "Please check Cloud Run Job logs.",
                        ]
                    ),
                )
            except Exception as alert_exc:
                log_event(
                    "roam_failure_alert_failed",
                    error_type=type(alert_exc).__name__,
                    error=str(alert_exc),
                )
        raise


if __name__ == "__main__":
    main()
