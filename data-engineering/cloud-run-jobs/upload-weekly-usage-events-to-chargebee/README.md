# Tracking Events Chargebee Export

Cloud Run Job que exporta la consulta:

```sql
SELECT * EXCEPT(customer_id, user_id)
FROM `fxr-analytics.sandbox.tracking_events_chargebee`
```

hacia:

`gs://fxr-chargebee-exports/tracking_events_chargebee/`

## Variables

- `GCP_PROJECT`: proyecto de BigQuery. Default: `fxr-analytics`
- `BQ_DATASET`: dataset fuente. Default: `sandbox`
- `BQ_TABLE`: tabla fuente. Default: `tracking_events_chargebee`
- `EXPORT_BUCKET`: bucket destino. Default: `fxr-chargebee-exports`
- `EXPORT_PREFIX`: prefijo dentro del bucket. Default: `tracking_events_chargebee`
- `EXPORT_FORMAT`: `CSV` o `PARQUET`. Default: `CSV`
- `EXPORT_COMPRESSION`: compresion para CSV. Default: `GZIP`

## Salida

Por cada corrida se genera una carpeta diaria:

`gs://fxr-chargebee-exports/tracking_events_chargebee/run_date=YYYY-MM-DD/`
