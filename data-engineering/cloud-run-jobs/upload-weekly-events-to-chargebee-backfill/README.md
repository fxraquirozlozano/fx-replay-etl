# Upload Usage Events To Chargebee Backfill

Cloud Run Job para backfills manuales de usage events hacia Chargebee.

No tiene scheduler. Se despliega como job manual y usa rangos explícitos.

Variables principales:
- `EXPORT_START_TS`: inicio inclusivo en UTC. Ejemplo: `2026-06-02T00:00:00Z`
- `EXPORT_END_TS`: fin exclusivo en UTC. Ejemplo: `2026-06-09T00:00:00Z`
- `SFTP_BATCH_ID_OVERRIDE`: carpeta remota a usar en `usage_data/`
- `SFTP_OVERWRITE_BATCH`: si es `true`, limpia primero el contenido de la carpeta remota antes de subir el nuevo batch

Ejemplo de carpeta remota:
- `usage_data/2026-06-09-08-00-00/`
