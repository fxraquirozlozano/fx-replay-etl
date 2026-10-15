import sys
from pathlib import Path

HELPERS_DIR = Path("data-engineering/cloud-run-jobs").resolve()
if str(HELPERS_DIR) not in sys.path:
    sys.path.insert(0, str(HELPERS_DIR))

from helpers import CloudRunApp, PROJECT_ID, print_commands, run_commands


app = CloudRunApp()


@app.job(
    "sync-hourly-ploy-to-bigquery",
    schedule="0 * * * *",
    timezone="UTC",
    source_dir="data-engineering/cloud-run-jobs/sync-hourly-ploy-to-bigquery",
    memory="1Gi",
    task_timeout="25m",
    task_max_retries=1,
    require_all_secrets=True,
    env_vars={
        "GCP_PROJECT": PROJECT_ID,
        "BQ_DATASET": "ploy",
        "BQ_TABLE": "submissions",
        "BQ_LOCATION": "US",
        "PLOY_DATABASE_ID": "pdb_269c8fcd20e44d9d8f96",
        "PLOY_TABLE_ID": "tbl_53358e8d464d4fbe9712",
        "LOOKBACK_HOURS": "168",
        "SAFETY_LAG_SECONDS": "60",
        "PAGE_SIZE": "500",
        "REQUEST_TIMEOUT_SECONDS": "60",
    },
    secret_env_vars={"PLOY_ACCESS_KEY": "ploy-access-key-runtime"},
)
def sync_hourly_ploy_to_bigquery(event=None):
    """Incrementally merge Ploy Submissions into BigQuery every hour."""
    return event


@app.job(
    "monthly-vat-report",
    schedule="0 15 1 * *",
    timezone="UTC",
    source_dir="data-engineering/cloud-run-jobs/monthly-vat-report",
    memory="1Gi",
    task_timeout="30m",
    task_max_retries=1,
    require_all_secrets=True,
    env_vars={
        "GCP_PROJECT": PROJECT_ID,
        "BQ_DATASET": "reporting",
        "SOURCE_TABLE": "fct_invoices",
        "SNAPSHOT_TABLE": "vat_report_monthly_snapshots",
        "BUSINESS_TIMEZONE": "America/Chicago",
        "REPORT_MONTH": "",
        "SOURCE_AS_OF": "",
        "REFRESH_SNAPSHOT": "false",
        "SHAREPOINT_HOSTNAME": "fxreplay.sharepoint.com",
        "SHAREPOINT_SITE_PATH": "sites/DataTeam",
        "SHAREPOINT_DRIVE_ID": "b!Mi_zgpjDy0apgS3ExKpIDJsXOuxMNwRKj7LzgT7nHqaDTMP01zt1R6i62KC3eata",
        "SHAREPOINT_FOLDER_PATH": "Finance/VAT Report",
        "OVERWRITE_EXISTING": "true",
        "REQUEST_TIMEOUT_SECONDS": "120",
        "DRY_RUN": "false",
    },
    secret_env_vars={
        "TENANT_ID": "sharepoint-tenant-id-runtime",
        "CLIENT_ID": "sharepoint-client-id-runtime",
        "CLIENT_SECRET": "sharepoint-client-secret-runtime",
    },
)
def monthly_vat_report(event=None):
    """Snapshot invoice-date Net Billing and upload the VAT report."""
    return event


@app.job(
    "refresh-daily-tracking-events-chargebee",
    schedule="0 14 * * *",
    timezone="UTC",
    source_dir="data-engineering/cloud-run-jobs/refresh-daily-tracking-events-chargebee",
    env_vars={
        "GCP_PROJECT": PROJECT_ID,
        "SOURCE_DATASET": "reporting",
        "SOURCE_TABLE": "tracking_events",
        "USERS_DATASET": "dbt_cloud",
        "USERS_TABLE": "dim_user",
        "TARGET_DATASET": "sandbox",
        "TARGET_TABLE": "tracking_events_chargebee",
        "PROCESS_TIME_ZONE": "UTC",
        "PROCESS_LOOKBACK_DAYS": "3",
        "MIN_ROWS_PER_DAY": "100000",
    },
)
def refresh_daily_tracking_events_chargebee(event=None):
    """Reconcile recent Chargebee tracking events after the dbt source refresh."""
    return event


@app.job(
    "sync-user-plan-fields-to-chargebee",
    schedule="0 15 * * *",
    timezone="UTC",
    source_dir="data-engineering/cloud-run-jobs/sync-user-plan-fields-to-chargebee",
    memory="2Gi",
    task_timeout="300m",
    task_max_retries=1,
    require_all_secrets=True,
    env_vars={
        "GCP_PROJECT": PROJECT_ID,
        "SOURCE_DATASET": "analytics_marts",
        "SOURCE_TABLE": "users_view",
        "CHARGEBEE_DATASET": "chargebee",
        "CHARGEBEE_CUSTOMERS_TABLE": "customers",
        "CHARGEBEE_SITE": "fxreplay",
        "STATE_DATASET": "sandbox",
        "SOURCE_SNAPSHOT_TABLE": "chargebee_customer_plan_fields_source_snapshot",
        "STATE_TABLE": "chargebee_customer_plan_fields_sync_state",
        "FAILURE_TABLE": "chargebee_customer_plan_fields_sync_failures",
        "CHARGEBEE_BEGINNER_CATEGORY_FIELD": "cf_Beginner_Category",
        "CHARGEBEE_PLAN_TYPE_FIELD": "cf_Plan_Type",
        "NULL_PLAN_TYPE_VALUE": "No Plan",
        "SYNC_VERSION": "v1",
        "MAX_CUSTOMERS_PER_RUN": "200000",
        "CHARGEBEE_REQUESTS_PER_MINUTE": "800",
        "HTTP_CONCURRENCY": "12",
        "REQUEST_TIMEOUT_SECONDS": "30",
        "MAX_RETRIES": "3",
        "BASE_RETRY_DELAY_SECONDS": "1",
        "DRY_RUN": "true",
        "FAIL_ON_PARTIAL_ERRORS": "true",
    },
    secret_env_vars={
        "CHARGEBEE_API_KEY": "CHARGEBEE_API_KEY",
    },
)
def sync_user_plan_fields_to_chargebee(event=None):
    """Send Beginner category and plan type to Chargebee Customers."""
    return event


@app.job(
    "upload-weekly-usage-events-to-chargebee",
    schedule="0 16 * * 2",
    timezone="UTC",
    source_dir="data-engineering/cloud-run-jobs/upload-weekly-usage-events-to-chargebee",
    memory="4Gi",
    task_timeout="60m",
    env_vars={
        "GCP_PROJECT": PROJECT_ID,
        "BQ_DATASET": "sandbox",
        "BQ_TABLE": "tracking_events_chargebee",
        "EXPORT_BUCKET": "fxr-chargebee-exports",
        "EXPORT_PREFIX": "tracking_events_chargebee",
        "EXPORT_FORMAT": "CSV",
        "EXPORT_COMPRESSION": "GZIP",
        "EXPORT_TIME_ZONE": "UTC",
        "SFTP_BATCH_TIME_ZONE": "UTC",
        "SFTP_BATCH_ID_OVERRIDE": "",
        "SFTP_PORT": "22",
        "SFTP_REMOTE_PATH": "usage_data",
        "ROWS_PER_OUTPUT_FILE": "250000",
        "MIN_ROWS_PER_DAY": "100000",
    },
    secret_env_vars={
        "SFTP_HOST": "chargebee-sftp-host-runtime",
        "SFTP_USER": "chargebee-sftp-user-runtime",
        "SFTP_PRIVATE_KEY": "chargebee-sftp-private-key",
    },
)
def upload_weekly_usage_events_to_chargebee(event=None):
    """Export weekly Chargebee usage data to GCS and then upload it to SFTP."""
    return event


@app.job(
    "send-weekly-activity-to-appcues",
    schedule="0 0 * * 1",
    timezone="America/Chicago",
    source_dir="data-engineering/cloud-run-jobs/send-weekly-activity-to-appcues",
    memory="2Gi",
    task_timeout="60m",
    task_max_retries=0,
    env_vars={
        "GCP_PROJECT": PROJECT_ID,
        "BQ_DATASET": "product_analytics",
        "BQ_TABLE": "weekly_user_activity_with_commitment",
        "COUNTRY_BQ_DATASET": "dbt_cloud",
        "COUNTRY_BQ_TABLE": "dim_user",
        "BQ_WEEK_START_FIELD": "week_start",
        "BQ_WEEK_END_FIELD": "week_end",
        "USER_ID_FIELD": "user_id",
        "EVENT_NAME_FIELD": "event_name",
        "EVENT_TIMESTAMP_FIELD": "event_timestamp",
        "APPCUES_BASE_URL": "https://api.appcues.com",
        "APPCUES_EVENT_NAME": "weekly_product_activity",
        "WINDOW_TIME_ZONE": "America/Chicago",
        "BACKFILL_START_WEEK": "",
        "BACKFILL_END_WEEK": "",
        "REQUEST_TIMEOUT_SECONDS": "30",
        "MAX_RETRIES": "5",
        "BASE_RETRY_DELAY_SECONDS": "2",
        "APPCUES_JOB_POLL_SECONDS": "5",
        "APPCUES_JOB_TIMEOUT_SECONDS": "1800",
        "APPCUES_IMPORT_CHUNK_BYTES": "18000000",
        "SKIP_EXISTING_EVENTS": "true",
        "SYNC_COUNTRY_PROFILES": "true",
        "COUNTRY_PROFILE_BACKFILL_ONLY": "false",
        "DRY_RUN": "false",
        "FAIL_ON_PARTIAL_ERRORS": "true",
    },
    secret_env_vars={
        "APPCUES_ACCOUNT_ID": "appcues-account-id-runtime",
        "APPCUES_API_KEY": "appcues-api-key-runtime",
        "APPCUES_API_SECRET": "appcues-api-secret-runtime",
    },
)
def send_weekly_activity_to_appcues(event=None):
    """Send weekly BigQuery user activity metrics to Appcues."""
    return event


@app.job(
    "load-weekly-chargebee-predictions-from-sftp",
    schedule="0 17 * * 3",
    timezone="UTC",
    source_dir="data-engineering/cloud-run-jobs/load-weekly-chargebee-predictions-from-sftp",
    env_vars={
        "GCP_PROJECT": PROJECT_ID,
        "BQ_DATASET": "chargebee",
        "BQ_TABLE": "chargebee_predictions",
        "RAW_BQ_DATASET": "chargebee_raw",
        "RAW_BQ_TABLE": "chargebee_predictions",
        "RAW_BUCKET": "fxr-chargebee-exports",
        "RAW_PREFIX": "chargebee_predictions_raw",
        "SFTP_PORT": "22",
        "SFTP_ROOT_PATH": "/predictions",
        "SFTP_FILE_GLOB": "predictions_part_*.csv",
        "MAX_BATCH_AGE_DAYS": "14",
        "WRITE_DISPOSITION": "WRITE_APPEND",
    },
    secret_env_vars={
        "SFTP_HOST": "chargebee-sftp-host-runtime",
        "SFTP_USER": "chargebee-sftp-user-runtime",
        "SFTP_PRIVATE_KEY": "chargebee-sftp-private-key",
    },
)
def load_weekly_chargebee_predictions_from_sftp(event=None):
    """Fetch the latest Chargebee predictions batch from SFTP and load it into BigQuery."""
    return event


@app.job(
    "upload-weekly-events-to-chargebee-backfill",
    source_dir="data-engineering/cloud-run-jobs/upload-weekly-events-to-chargebee-backfill",
    env_vars={
        "GCP_PROJECT": PROJECT_ID,
        "BQ_DATASET": "sandbox",
        "BQ_TABLE": "tracking_events_chargebee",
        "EXPORT_BUCKET": "fxr-chargebee-exports",
        "EXPORT_PREFIX": "tracking_events_chargebee_backfill",
        "EXPORT_FORMAT": "CSV",
        "EXPORT_COMPRESSION": "GZIP",
        "EXPORT_START_TS": "",
        "EXPORT_END_TS": "",
        "SFTP_BATCH_TIME_ZONE": "UTC",
        "SFTP_BATCH_ID_OVERRIDE": "",
        "SFTP_OVERWRITE_BATCH": "false",
        "SFTP_PORT": "22",
        "SFTP_REMOTE_PATH": "usage_data",
        "ROWS_PER_OUTPUT_FILE": "250000",
    },
    secret_env_vars={
        "SFTP_HOST": "chargebee-sftp-host-runtime",
        "SFTP_USER": "chargebee-sftp-user-runtime",
        "SFTP_PRIVATE_KEY": "chargebee-sftp-private-key",
    },
)
def upload_weekly_events_to_chargebee_backfill(event=None):
    """Manual backfill job for Chargebee usage events with explicit UTC timestamps."""
    return event


if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else "print"
    target_job = sys.argv[2] if len(sys.argv) > 2 else None

    if action == "deploy":
        run_commands(app, target_job)
    else:
        print_commands(app, target_job)
