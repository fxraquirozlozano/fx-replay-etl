# Monthly VAT Report

Cloud Run Job que genera el reporte mensual de VAT desde BigQuery y lo publica en
SharePoint (`Finance / VAT Report`). Reemplaza el notebook programado en Hex.

## Fuente y reglas

- Fuente default: `fxr-analytics.reporting.fct_invoices`.
- Replica la métrica del Finance Report: fecha de emisión de la factura,
  `SUM(net_billing)` y exclusión únicamente de facturas `voided`.
- Incluye facturas pagadas y pendientes, tal como el dashboard.
- El primer run de cada mes guarda una copia inmutable por factura en
  `reporting.vat_report_monthly_snapshots`; los reruns reutilizan esa copia para
  evitar que cambios posteriores de estado alteren meses cerrados.
- El CSV tiene grano de una fila por `invoice_id` y valida mes, IDs únicos,
  estado y monto.
- Excluye facturas con `total_billing = 0` y `net_billing = 0`; el snapshot
  conserva esas filas para auditoría. Las facturas compensadas con créditos se
  mantienen aunque su Net Billing sea cero.
- El resultado se escribe en `/tmp` como `AUG 2026.csv`, con UTF-8 BOM.

`SOURCE_AS_OF` y `REFRESH_SNAPSHOT=true` están reservados para backfills
controlados. Permiten reconstruir un cierre histórico mediante BigQuery time
travel y reemplazar solamente el mes indicado.

## Idempotencia

Antes de subir, el job descarga el archivo existente y compara SHA-256:

- contenido idéntico: no vuelve a subir (`unchanged`);
- contenido distinto: reemplaza el archivo (`replaced`);
- archivo inexistente: lo crea (`created`).

El snapshot mensual también es idempotente: se crea una sola vez y los reruns lo
reutilizan. `REFRESH_SNAPSHOT=true` debe usarse solamente para una corrección
intencional del mes indicado.

## Variables

- `GCP_PROJECT`: default `fxr-analytics`.
- `BQ_DATASET`: default `reporting`.
- `SOURCE_TABLE`: default `fct_invoices`.
- `SNAPSHOT_TABLE`: default `vat_report_monthly_snapshots`.
- `BUSINESS_TIMEZONE`: default `America/Chicago`.
- `REPORT_MONTH`: opcional, formato `YYYY-MM`; sirve para backfills.
- `SOURCE_AS_OF`: timestamp ISO-8601 opcional usado para un backfill histórico.
- `REFRESH_SNAPSHOT`: default `false`; reemplaza el snapshot del mes cuando es
  `true`.
- `OUTPUT_DIR`: default `/tmp`.
- `SHAREPOINT_HOSTNAME`: default `fxreplay.sharepoint.com`.
- `SHAREPOINT_SITE_PATH`: default `sites/DataTeam`.
- `SHAREPOINT_DRIVE_ID`: drive de Shared Documents configurado actualmente.
- `SHAREPOINT_FOLDER_PATH`: default `Finance/VAT Report`.
- `OVERWRITE_EXISTING`: default `true`.
- `REQUEST_TIMEOUT_SECONDS`: default `120`.
- `DRY_RUN`: crea o reutiliza el snapshot y valida el CSV, pero no llama Graph
  ni Roam.

Secretos requeridos para publicar en SharePoint:

- `TENANT_ID` -> `sharepoint-tenant-id-runtime`
- `CLIENT_ID` -> `sharepoint-client-id-runtime`
- `CLIENT_SECRET` -> `sharepoint-client-secret-runtime`

Alertas opcionales de Roam:

- `ROAM_API_KEY` -> `FXR_ROAM_API_KEY`
- `ROAM_GROUP_ID` -> `roam-vat-group-id-runtime`

La identidad de Microsoft debe tener acceso de escritura al sitio DataTeam.

## Ejecución local segura

Desde esta carpeta:

```powershell
$env:DRY_RUN = "true"
$env:REPORT_MONTH = "2026-08"
python main.py
```

Pruebas unitarias:

```powershell
python -m unittest -v test_main.py
```

## Programación

El job está registrado en `cloud-run.py` para el primer día de cada mes a las
`15:00 UTC`, después del refresh horario de `fct_invoices`. Un rerun reutiliza el
snapshot mensual y es seguro. Antes de apagar Hex se debe completar una ejecución
paralela, comparar conteos/total/CSV y confirmar el archivo en SharePoint.
