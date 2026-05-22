Daily incremental refresh for `fxr-analytics.sandbox.tracking_events_chargebee`.

Schedule:
- Every day at `08:00` in `America/Chicago`

Behavior:
- computes the previous local day in `America/Chicago`
- deletes that day from the target table
- rebuilds and inserts only that day's rows from `reporting.tracking_events`
- joins with `dbt_cloud.dim_user`

Main env vars:
- `GCP_PROJECT`
- `SOURCE_DATASET`
- `SOURCE_TABLE`
- `USERS_DATASET`
- `USERS_TABLE`
- `TARGET_DATASET`
- `TARGET_TABLE`
- `PROCESS_TIME_ZONE`
