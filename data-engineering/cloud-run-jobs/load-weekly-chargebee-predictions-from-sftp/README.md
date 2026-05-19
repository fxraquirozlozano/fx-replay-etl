# Chargebee Predictions SFTP Load

Cloud Run Job que:

1. Lee el batch mas reciente en `SFTP_ROOT_PATH` o el batch indicado en `SFTP_BATCH_PATH`.
2. Valida el contrato de archivos:
   - `predictions_part_*.csv`
   - `metadata.json`
   - `schema.csv`
3. Copia los archivos crudos a GCS.
4. Carga los `predictions_part_*.csv` a BigQuery usando el esquema derivado de `schema.csv`.

## Variables

- `GCP_PROJECT`: proyecto de BigQuery. Default: `fxr-analytics`
- `BQ_DATASET`: dataset destino. Default: `chargebee`
- `BQ_TABLE`: tabla destino. Default: `chargebee_predictions`
- `RAW_BUCKET`: bucket donde se guardan los archivos crudos. Default: `fxr-chargebee-exports`
- `RAW_PREFIX`: prefijo dentro del bucket. Default: `chargebee_predictions_raw`
- `SFTP_ROOT_PATH`: carpeta raiz con batches. Default: `/predictions`
- `SFTP_BATCH_PATH`: batch especifico a procesar. Opcional.
- `SFTP_FILE_GLOB`: patron de archivos CSV. Default: `predictions_part_*.csv`
- `WRITE_DISPOSITION`: politica de carga BigQuery. Default: `WRITE_APPEND`

## Scheduler

Registrado en `cloud-run.py` para ejecutarse todos los miercoles a las `17:00 UTC`, antes de la ventana pedida de `18:00:00 UTC`.
