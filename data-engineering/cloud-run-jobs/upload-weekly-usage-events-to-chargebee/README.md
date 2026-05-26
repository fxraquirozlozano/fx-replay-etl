# Upload Weekly Usage Events To Chargebee

Cloud Run Job que:
- exporta datos semanales desde `fxr-analytics.sandbox.tracking_events_chargebee` a GCS
- recompone los shards exportados en archivos `usage_events_part_XXX.csv`
- sube un batch al SFTP de Chargebee bajo `usage_data/<batch_id>/`
- genera `metadata.json` con nombres esperados, `row_counts` y `min/max` timestamp

## Variables

- `GCP_PROJECT`: proyecto de BigQuery. Default: `fxr-analytics`
- `BQ_DATASET`: dataset fuente. Default: `sandbox`
- `BQ_TABLE`: tabla fuente. Default: `tracking_events_chargebee`
- `EXPORT_BUCKET`: bucket destino. Default: `fxr-chargebee-exports`
- `EXPORT_PREFIX`: prefijo dentro del bucket. Default: `tracking_events_chargebee`
- `EXPORT_FORMAT`: `CSV` o `PARQUET`. Default: `CSV`
- `EXPORT_COMPRESSION`: compresion para CSV. Default: `GZIP`
- `ROWS_PER_OUTPUT_FILE`: filas por archivo CSV final. Default: `250000`
- `SFTP_REMOTE_PATH`: carpeta remota. Default: `usage_data`

## Salida intermedia en GCS

Por cada corrida se genera una carpeta semanal:

`gs://fxr-chargebee-exports/tracking_events_chargebee/week_start=YYYY-MM-DD/week_end=YYYY-MM-DD/`

## Salida final en SFTP

Cada batch se sube como:

`usage_data/YYYY-MM-DD-HH-MM-SS/usage_events_part_001.csv`

`usage_data/YYYY-MM-DD-HH-MM-SS/metadata.json`

Ejemplo de `metadata.json`:

```json
{
  "batch_id": "2026-05-22-07-57-00",
  "expected_file_count": 3,
  "expected_file_names": [
    "usage_events_part_001.csv",
    "usage_events_part_002.csv",
    "usage_events_part_003.csv"
  ],
  "row_counts": {
    "usage_events_part_001.csv": 250000,
    "usage_events_part_002.csv": 250000,
    "usage_events_part_003.csv": 12845
  },
  "min_event_timestamp": 1746921600000,
  "max_event_timestamp": 1747526399000
}
```
