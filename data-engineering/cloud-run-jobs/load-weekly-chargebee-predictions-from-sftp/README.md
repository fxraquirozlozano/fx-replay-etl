# Chargebee Predictions SFTP Load

Cloud Run Job que:

1. Lee el batch mas reciente en `SFTP_ROOT_PATH` o el batch indicado en `SFTP_BATCH_PATH`.
2. Valida el contrato de archivos:
   - `predictions_part_*.csv`
   - `metadata.json`
   - `schema.csv`
3. Copia los archivos crudos a GCS.
4. Carga primero a un dataset raw/staging.
5. Hace `MERGE` hacia la tabla final usando `unique_id`, para evitar duplicados si el job corre dos veces.

## Variables

- `GCP_PROJECT`: proyecto de BigQuery. Default: `fxr-analytics`
- `BQ_DATASET`: dataset destino. Default: `chargebee`
- `BQ_TABLE`: tabla destino. Default: `chargebee_predictions`
- `RAW_BQ_DATASET`: dataset raw/staging. Default: `chargebee_raw`
- `RAW_BQ_TABLE`: tabla raw/staging. Default: `chargebee_predictions`
- `RAW_BUCKET`: bucket donde se guardan los archivos crudos. Default: `fxr-chargebee-exports`
- `RAW_PREFIX`: prefijo dentro del bucket. Default: `chargebee_predictions_raw`
- `SFTP_ROOT_PATH`: carpeta raiz con batches. Default: `/predictions`
- `SFTP_BATCH_PATH`: batch especifico a procesar. Opcional.
- `SFTP_FILE_GLOB`: patron de archivos CSV. Default: `predictions_part_*.csv`
- `WRITE_DISPOSITION`: politica base de carga. Default: `WRITE_APPEND`

## Idempotencia

- La tabla raw guarda columnas de auditoria como `batch_id`, `source_file_name` y `loaded_at`.
- La tabla raw evita duplicados por `batch_id + unique_id`.
- La tabla final evita duplicados por `unique_id`.
- Si el mismo batch se procesa dos veces, no vuelve a insertar registros ya existentes en la tabla final.

## Scheduler

Registrado en `cloud-run.py` para ejecutarse todos los miercoles a las `17:00 UTC`, antes de la ventana pedida de `18:00:00 UTC`.
