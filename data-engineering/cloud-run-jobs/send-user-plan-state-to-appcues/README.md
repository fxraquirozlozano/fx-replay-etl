# send-user-plan-state-to-appcues

Sincroniza el estado de suscripcion de BigQuery con perfiles de Appcues.

## Archivos

- `main.py`: codigo y todas las consultas SQL integradas.
- `Dockerfile`, `requirements.txt`, `.dockerignore`, `.gcloudignore`: construccion de la imagen.
- `deployed.json`: configuracion consultada del job desplegado, con referencias a secretos.
- `test_main.py`: pruebas locales sin envios reales.

No requiere archivos ni carpetas SQL externos. Los borradores SQL y resultados temporales de la muestra fueron eliminados.

## Datos enviados

Una fila por `user_id` con `bq_plan_state`, `bq_plan_state_changed_date` y `bq_plan_state_as_of_date`.
Estados: paid, free, trial, paused, future, cancelled y unknown. Se agregan las suscripciones de los customers vinculados por dim_user antes de determinar el estado.
La fecha de cambio representa el dia de negocio de America/Chicago, convertido a medianoche UTC; no es la hora exacta del evento. Si no hay un cambio comprobable, se envia null.

## Procesamiento

`RUN_MODE=auto` calcula los cierres nuevos diariamente y recalcula el historial desde 2024-12-01 en la primera ejecucion de cada mes. El historico inicial ya se calculo durante la prueba: la carga general puede reutilizarlo y enviar los pendientes sin repetir ese calculo.

Solo mantiene `appcues_sync.user_plan_state` (estado calculado) y `appcues_sync.user_plan_last_sent` (ultimo envio confirmado). Envia perfiles nuevos o con estado/fecha de cambio diferente; cambiar solo la fecha de corte no dispara un envio. Puede crear perfiles que no existian en Appcues.

La carga general elimina el limite de 100 y el filtro de usuarios de prueba. Usa lotes secuenciales de hasta 20.000 perfiles y 18 MB, una tarea, sin reintentos automaticos de Cloud Run. Cada lote se confirma en BigQuery despues de completar su importacion en Appcues. Si se interrumpe, una nueva ejecucion auto retoma los pendientes. Un lote importado pero no confirmado podria reenviarse con los mismos valores.

## Monitoreo

Los logs JSON incluyen el nombre de ejecucion y:
- BigQuery query started/completed: job_id, bytes procesados y duracion.
- Profile delivery starting: total de perfiles pendientes.
- Appcues batch starting/confirmed: lote, perfiles y job de importacion.
- Profile delivery progress: acumulado, porcentaje, tiempo y estimacion restante.
- Appcues import status: estado de la importacion mientras se espera.
- Appcues request retry / batch failed: errores y reintentos sin credenciales ni payloads.
- Plan sync completed: resumen final.

Appcues puede devolver COMPLETED sin contadores por perfil; en ese caso se registra explicitamente esa limitacion. La prueba de 100 perfiles se verifico mediante lectura de Appcues y el usuario valido los casos con Chargebee.

```powershell
gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="send-user-plan-state-to-appcues"' --project=fxr-analytics --freshness=1h --limit=30 --format=json
```

## Operacion

Timeout de Cloud Run: 6 horas. Timeout de cada importacion: 30 minutos. No se configura Scheduler en este cambio. El modo auto habilita la logica diaria/mensual cuando se ejecuta el job; no lo programa por si mismo.

```powershell
python -m unittest test_main.py
gcloud run jobs execute send-user-plan-state-to-appcues --project=fxr-analytics --region=us-central1 --async
```

Para forzar un recalculo historico, ejecutar con override `--update-env-vars=RUN_MODE=historical`. Para reanudar una carga interrumpida usar auto y evitar recalcular el historial innecesariamente.
