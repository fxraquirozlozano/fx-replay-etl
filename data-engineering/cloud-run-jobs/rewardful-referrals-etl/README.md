Incremental load of Rewardful referrals into `fxr-analytics.rewardful.referrals`.

The same image runs two Cloud Run Jobs in `us-central1`, both with the service account `rewardful-etl@fxr-analytics.iam.gserviceaccount.com`:
- `rewardful-referrals-etl`: the regular load.
- `rewardful-referrals-backfill`: walks a fixed historical range (backfill mode, see below).

Schedule:
- `rewardful-referrals-etl-schedule` (Cloud Scheduler): `20 1-22/3 * * *` UTC, every 3 hours starting at `01:20`
- it runs at `:20` because the Rewardful API returns 503 mostly in the first 10 minutes of each hour
- `rewardful-referrals-backfill-schedule` is paused; the historical backfill finished on 2026-11-03

Behavior:
- reads a window of `updated_at` from the Rewardful API, starting at `MAX(updated_at)` in the target table and lasting up to `WINDOW_HOURS`, never past now
- before downloading, reads the window's page count; if it is over `MAX_PAGES_PER_RUN`, halves the window until it fits
- pages are written to a staging table per execution, `rewardful_raw.referrals__<execution>`, dropped at the end
- compares the fetched rows with the API `total_count` and fetches the window again if any are missing
- `MERGE` into the target by `id`, keeping the most recent `updated_at`; the next run continues from there
- aborts without `MERGE` after `MAX_RUNTIME_MINUTES`, so a failed run loses only its own window
- retries 429 (waits for `RateLimit-Reset`) and 5xx with backoff; after 5 failed attempts the run fails
- only one watermark run at a time: if another execution of the same job is running, the newer one exits without doing anything (needs `roles/run.viewer` on the job)

Backfill mode (`rewardful-referrals-backfill`):
- enabled by `BACKFILL_RANGE_START` and `BACKFILL_RANGE_END` on the job
- each execution loads the next window after the progress saved in `rewardful_raw.referrals_backfill_progress`, then records it
- once the progress reaches `BACKFILL_RANGE_END`, executions do nothing (`Backfill complete`)

One-off range on the regular job:
- pass `BACKFILL_START` / `BACKFILL_END` per execution; it does not touch the watermark guard or the progress table

```bash
gcloud run jobs execute rewardful-referrals-etl --region=us-central1 --project=fxr-analytics --update-env-vars=BACKFILL_START=2026-10-07T00:00:00Z,BACKFILL_END=2026-10-08T00:00:00Z --wait
```

Main env vars:
- `REWARDFUL_DATASET` (`rewardful`)
- `REFERRALS_TABLE` (`referrals`)
- `REWARDFUL_RAW_DATASET` (`rewardful_raw`)
- `REWARDFUL_API_KEY_SECRET` (`rewardful-api-key`, in Secret Manager)
- `WINDOW_HOURS` (`12` on the regular job, `336` on the backfill job)
- `MAX_RUNTIME_MINUTES` (`165` on the regular job, default `45`)
- `MAX_PAGES_PER_RUN` (default `300`)
- `MIN_REQUEST_INTERVAL_SECONDS` (default `1`)
- `BACKFILL_RANGE_START` / `BACKFILL_RANGE_END` (backfill job only)
- `BACKFILL_START` / `BACKFILL_END` (per execution only)

Deploy:

```bash
gcloud run jobs deploy rewardful-referrals-etl --source . --region=us-central1 --project=fxr-analytics --service-account=rewardful-etl@fxr-analytics.iam.gserviceaccount.com --memory=2Gi --cpu=1 --task-timeout=10500s --max-retries=0 --set-env-vars=REWARDFUL_DATASET=rewardful,REFERRALS_TABLE=referrals,REWARDFUL_RAW_DATASET=rewardful_raw,REWARDFUL_API_KEY_SECRET=rewardful-api-key,WINDOW_HOURS=12,MAX_RUNTIME_MINUTES=165
```
