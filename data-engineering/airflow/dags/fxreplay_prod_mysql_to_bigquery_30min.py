from fxreplay_prod_mysql_to_bigquery_daily import (
    TABLE_CONFIGS,
    FAST_SCHEDULE,
    FAST_TASK_NAMES,
    create_pipeline_dag,
    normalize_table_config,
)

FAST_TABLE_CONFIGS = tuple(
    normalize_table_config(
        {
            **table_config,
            "fixed_window_minutes": 30,
        }
    )
    for table_config in TABLE_CONFIGS
    if str(table_config["task_name"]) in FAST_TASK_NAMES
)


dag = create_pipeline_dag(
    dag_id="fxreplay_prod_mysql_to_bigquery_30min",
    description=(
        "Carga incremental cada 30 minutos de emaillead y users desde "
        "fxreplay_prod hacia BigQuery raw y final."
    ),
    schedule=FAST_SCHEDULE,
    tags=["mysql", "bigquery", "fxreplay-prod", "raw", "30min"],
    table_configs=FAST_TABLE_CONFIGS,
)
