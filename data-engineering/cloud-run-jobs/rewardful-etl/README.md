Full extracts of Rewardful campaigns, affiliates, payouts and commissions into `fxr-analytics.rewardful`.

Replaces the Cloud Run function `rewardful-commissions-etl-v4`. Referrals are loaded by the separate job `rewardful-referrals-etl` (incremental, every 3 hours).

Cloud Run Job `rewardful-etl` in `us-central1`, service account `rewardful-etl@fxr-analytics.iam.gserviceaccount.com`.

Schedule:
- `rewardful-etl-schedule` (Cloud Scheduler): `50 0,12 * * *` UTC
- off the hour because the Rewardful API returns 503 mostly in the first minutes of each hour, and ends before the referrals job at `:20`

Behavior:
- the API ignores `updated_since` on these endpoints, so each run downloads every endpoint completely
- endpoints run one after the other, so the job never competes with itself for the API rate limit (~45 requests per 30 s)
- if the unique ids fetched are fewer than the API `total_count`, the endpoint is fetched a second time
- each endpoint fails on its own; the run exits with an error at the end if any failed
- only one execution at a time: if another one is running, the newer one exits without doing anything (needs `roles/run.viewer` on the job)

Tables:

| table | endpoint | load | grain |
|---|---|---|---|
| `rewardful.campaigns` | `/v1/campaigns` | replaced each run | one row per campaign |
| `rewardful.affiliates` | `/v1/affiliates?expand[]=links&expand[]=campaign` | replaced each run | one row per affiliate; `links` repeated, `campaign` record |
| `rewardful.payouts` | `/v1/payouts?expand[]=commissions&expand[]=affiliate` | replaced each run | one row per payout; `commissions` repeated, `affiliate_id` |
| `rewardful.commissions` | `/v1/commissions?expand[]=sale&expand[]=campaign` | `MERGE` by `id` | one row per commission; `sale` and `campaign` records |

- replaced tables are written with a single `WRITE_TRUNCATE` load, so readers never see them half-loaded; an incomplete download keeps the previous snapshot
- commissions are loaded to `rewardful_raw.commissions__<execution>` and merged; the merge never deletes rows Rewardful removed
- `affiliate_links` and `sales` are not loaded: links come nested in affiliates, and sales nested in commissions

Main env vars:
- `REWARDFUL_DATASET` (`rewardful`)
- `REWARDFUL_RAW_DATASET` (`rewardful_raw`)
- `REWARDFUL_API_KEY_SECRET` (`rewardful-api-key`, in Secret Manager)
- `COMMISSIONS_TABLE` (default `rewardful.commissions`; `dataset.table` to write commissions elsewhere)
- `ENDPOINTS` (comma-separated subset, e.g. `campaigns,payouts`; all by default)
- `MIN_REQUEST_INTERVAL_SECONDS` (default `1`)

Deploy:

```bash
gcloud run jobs deploy rewardful-etl --source . --region=us-central1 --project=fxr-analytics --service-account=rewardful-etl@fxr-analytics.iam.gserviceaccount.com --memory=2Gi --cpu=1 --task-timeout=10800s --max-retries=0 --set-env-vars=REWARDFUL_DATASET=rewardful,REWARDFUL_RAW_DATASET=rewardful_raw,REWARDFUL_API_KEY_SECRET=rewardful-api-key
```

Run only some endpoints:

```bash
gcloud run jobs execute rewardful-etl --region=us-central1 --project=fxr-analytics --update-env-vars=ENDPOINTS=campaigns,payouts --wait
```
