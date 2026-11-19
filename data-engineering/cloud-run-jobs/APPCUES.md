# Procesos de envío a Appcues

Verificación del despliegue: 2026-11-17, proyecto `fxr-analytics`, región
`us-central1`. Esta carpeta registra código y configuración; copiar estos archivos
no despliega ni ejecuta los jobs.

## Procedencia del código

Se consultaron los jobs y Cloud Scheduler con `gcloud`. Se recuperaron
`/app/main.py` y `/app/requirements.txt` directamente de las capas de las imágenes
en Artifact Registry, autenticando con el token de gcloud sin guardarlo en el repo.
Los blobs descargados se verificaron contra sus digests SHA-256.

| Job | Digest de imagen recuperada | SHA-256 de main.py |
| --- | --- | --- |
| `send-weekly-activity-to-appcues` | `sha256:ec28367773ec501f01b7b5a8fb4c83a085ff36363a4dcce4f9cb01345aecf089` | `a826e681463848decf0bb58d98c73e7556928d2a8bf5462b39d65b716b3cd8b6` |
| `send-onboarding-data-to-appcues` | `sha256:f47e29c3aa9164f9b65834a129b56ebfcf79688eec20fa0544e386730611812e` | `884d2050a53572f03f90b644118a2c01a875db7ddfdccf84e6e399fb00f6b7ad` |

Ambos `requirements.txt` tienen SHA-256
`12e111905f255a5b44731261cb89ca843d48d901e06f773009f1e56867efcc6e`.
Git puede convertir LF a CRLF al hacer checkout; los hashes anteriores corresponden
a los bytes extraídos de las imágenes.

El job semanal usa `:latest`; se comprobó que el digest actual del tag coincide
con la ejecución `send-weekly-activity-to-appcues-q9k5g` del 16 de noviembre y con
el build `af548e60-63fb-42c9-9be9-89ebe0f44d25` del 14 de septiembre. El código
recuperado coincide con el código ya versionado, salvo la representación de saltos
de línea. El job de onboarding está configurado con el digest explícito indicado.

Cada carpeta incluye `deployed.json`: configuración y última ejecución observadas,
horario y referencias a Secret Manager, sin valores de secretos. El Dockerfile de
onboarding fue reconstruido a partir del historial de imagen; no es un Dockerfile
original recuperado. Recompilar con un tag base mutable y dependencias transitivas
sin fijar no garantiza reproducir el mismo digest. La declaración existente en
`cloud-run.py` cubre el job semanal; onboarding se registra aquí mediante su código
y snapshot, sin añadir un nuevo mecanismo de despliegue.

## 1. Actividad semanal y país

Código: [send-weekly-activity-to-appcues/main.py](send-weekly-activity-to-appcues/main.py).

- Scheduler habilitado: lunes 00:00, `America/Chicago` (`0 0 * * 1`).
- Lee `fxr-analytics.product_analytics.weekly_user_activity_with_commitment`.
  Normalmente selecciona la semana anterior, de lunes a lunes, con fin exclusivo.
- Crea eventos `weekly_product_activity`, identificados por `user_id`, con
  `week_start`, `week_end`, `trades_count`, `time_spent_seconds`,
  `historical_seconds`, `wpa_tier_week`, `hit_wpa_week` y `wpa_tier_week_0`.
  Los valores nulos no se incluyen en el payload.
- El tier semanal toma la mayor categoría verdadera entre WPA90, WPA50, WPA25 y
  WPA10. `hit_wpa_week` usa `is_wpa_10`; el tier de semana cero toma la categoría
  más alta entre filas con `weeks_since_first_commit = 0`. El job consume esos
  indicadores, no calcula sus reglas de negocio desde operaciones individuales.
- La consulta fecha el evento al cierre de la semana a las 00:00 UTC. La ventana
  se calcula en Chicago: no debe interpretarse como el instante de una acción
  individual ni de un cambio de plan.
- Deduplica por usuario + nombre de evento + timestamp. Con la configuración
  actual exporta primero los eventos existentes de Appcues para omitir esas claves.
  Una nueva ejecución no corrige atributos de eventos que ya tengan la misma clave.
- Además lee `fxr-analytics.dbt_cloud.dim_user` y manda `country`, `country_code`
  y `country_name` como propiedades de perfil. En modo normal selecciona usuarios
  creados dentro de esa semana; no revisa cambios de país de todos los usuarios.
- Soporta rangos históricos mediante `BACKFILL_START_WEEK`/`BACKFILL_END_WEEK`
  y un modo exclusivo de backfill de país. Ninguno está activado actualmente.

## 2. Respuestas de onboarding

Código: [send-onboarding-data-to-appcues/main.py](send-onboarding-data-to-appcues/main.py).

- Scheduler habilitado: lunes 00:15, `America/Chicago` (`15 0 * * 1`).
- Lee `fxr-analytics.engagement_prod.onboarding_questionnaire_response` completo:
  no tiene filtro temporal ni estado incremental. Reenvía el conjunto de perfiles
  obtenido cada semana.
- Conserva la última versión por `_id` usando `_sdc_sequence` y
  `_sdc_extracted_at`; luego elige una respuesta por `userid`, ordenando por
  extracción, secuencia e ID. Es recencia de ingesta, no necesariamente la última
  respuesta según su fecha de negocio.
- Desanida las respuestas y genera propiedades de perfil `onboarding_*`:
  experiencia, mercados, estilo, encaje con la vida cotidiana, objetivo, canal
  de descubrimiento, textos libres de mercado/canal, versión y fecha de respuesta.
- Las selecciones múltiples se convierten en texto separado por `|`. Genera
  indicadores de mercado para crypto, forex, futures, indices y stocks.
- `onboarding_completed` es verdadero si existen las cinco claves experience,
  markets, style, life_fit y goal sin marcar como omitidas. No exige discovery_channel
  ni comprueba que cada respuesta tenga contenido no vacío.
- Envía perfiles a `/import/profiles`; no emite eventos de onboarding. Omite
  propiedades nulas: no envía una instrucción explícita para borrar valores previos.

## Transporte y observación de ejecución

Ambos usan la API v2 de Appcues, Basic Auth con secretos inyectados en runtime,
NDJSON en lotes de hasta 18.000.000 bytes, polling de trabajos asíncronos cada
5 segundos y hasta 30 minutos de espera por trabajo. Reintentan errores de red,
HTTP 429 y 5xx, hasta cinco intentos. Fallan ante errores parciales reportados.
Cloud Run tiene timeout de una hora y cero reintentos por tarea.
`DRY_RUN=false` en ambos despliegues, aunque onboarding tiene default local `true`.

Logs del 2026-11-16:

| Proceso | Resultado reportado |
| --- | --- |
| Actividad semanal, ventana 2026-11-09 a 2026-11-16 exclusiva | 0 filas fuente; 0 eventos; 15.847 perfiles de país; 0 errores |
| Onboarding | 222.875 perfiles enviados en 7 lotes; 0 errores |

Los contadores son los reportados por los jobs, no una auditoría independiente
de perfiles en destino. Ambos finalizaron correctamente. Las cero filas semanales
requieren revisar disponibilidad y actualización de la tabla fuente y su coordinación
con el horario del job; esta inspección no determinó la causa ni ejecutó consultas
de datos de negocio. No se ejecutó ni modificó ningún job remoto.

## Requisito de NPS: upgrade a paid y downgrade a free

Ninguno de estos dos procesos consulta planes, compara estados de suscripción
ni envía transiciones paid/free. Esto no prueba que el frontend u otra integración
externa al alcance no envíe ya alguna propiedad de plan.

Propuesta para implementar después de validar la fuente y definición de paid/free:

1. Mantener propiedades de perfil, por ejemplo `plan_status` (`paid`/`free`),
   `plan_changed_at`, `last_upgraded_to_paid_at` y `last_downgraded_to_free_at`.
   Permiten combinar estado actual y antigüedad del cambio en el targeting.
2. Emitir eventos `upgraded_to_paid` y `downgraded_to_free` por transición efectiva,
   con fecha real, estado anterior/nuevo e identificador estable del cambio.
   Estos nombres son propuestos, no propiedades/eventos existentes verificados.
3. Usar el mismo ID de usuario que identifica al usuario en Appcues. Validar el
   mapeo desde la fuente; no sustituirlo sin más por customer_id de Chargebee.
4. Distinguir cancelación programada de pérdida efectiva de acceso pagado,
   trial de paid y cambios entre planes pagados de cambios paid/free. Un pago o
   una cancelación aislados no bastan para establecer la transición de acceso.
5. Preferir un proceso separado con frecuencia acorde al NPS, o eventos de backend
   para baja latencia. Un snapshot periódico permite comparar estados observados,
   pero puede perder transiciones intermedias y no da por sí solo su fecha exacta.
6. Registrar entregas y reintentos de forma durable. La carga inicial del estado
   actual no debe fabricar eventos históricos de upgrade/downgrade. Para backfill
   de transiciones se necesita historial fiable.

Hay un patrón local de sincronización con estado en
`../cloud-run-jobs/sync-user-plan-fields-to-chargebee/main.py`, pero su campo
`last_subscription_plan_type` no está validado aquí como estado efectivo paid/free.

Referencias oficiales consultadas el 2026-11-17:
[propiedades y targeting](https://docs.appcues.com/users-groups-events/user-properties-overview),
[targeting por propiedades](https://docs.appcues.com/en_US/user-experiences-targeting/targeting-by-user-property),
[API pública y bulk imports](https://api.appcues.com/v2/docs).
