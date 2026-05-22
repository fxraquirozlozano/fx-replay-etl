import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from google.cloud import bigquery


PROJECT_ID = os.getenv("GCP_PROJECT", "fxr-analytics")
SOURCE_DATASET = os.getenv("SOURCE_DATASET", "reporting")
SOURCE_TABLE = os.getenv("SOURCE_TABLE", "tracking_events")
USERS_DATASET = os.getenv("USERS_DATASET", "dbt_cloud")
USERS_TABLE = os.getenv("USERS_TABLE", "dim_user")
TARGET_DATASET = os.getenv("TARGET_DATASET", "sandbox")
TARGET_TABLE = os.getenv("TARGET_TABLE", "tracking_events_chargebee")
PROCESS_TIME_ZONE = os.getenv("PROCESS_TIME_ZONE", "America/Chicago")

TARGET_SCHEMA = """
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.{table}` (
  subscription_id STRING,
  subscription_plan STRING,
  timestamp INT64,
  event_type STRING,
  customer_id STRING,
  user_id STRING,
  is_session_created INT64,
  is_quick_buy_sell INT64,
  is_partials_taken INT64,
  is_position_managed INT64,
  is_replay_activated INT64,
  is_news_visibility_clicked INT64,
  is_order_placed INT64,
  created_session_type STRING,
  quick_buy_sell_type STRING,
  partials_taken_percentage STRING,
  partials_taken_position_amount_type STRING,
  position_managed_type STRING,
  order_has_auto_be STRING,
  order_has_object_selected STRING,
  order_has_stop_loss STRING,
  order_tag_count STRING,
  order_has_tag_selected STRING,
  order_has_take_profit STRING,
  order_take_profit_type STRING,
  order_save_type STRING
)
"""


def date_window() -> tuple[str, str]:
    local_now = datetime.now(ZoneInfo(PROCESS_TIME_ZONE))
    end_date = local_now.date()
    start_date = end_date - timedelta(days=1)
    return start_date.isoformat(), end_date.isoformat()


def query_params(start_date: str, end_date: str) -> list[bigquery.ScalarQueryParameter]:
    return [
        bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
        bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
    ]


def run_query(
    client: bigquery.Client,
    query: str,
    params: list[bigquery.ScalarQueryParameter] | None = None,
):
    config = bigquery.QueryJobConfig(query_parameters=params or [])
    job = client.query(query, job_config=config)
    return job.result()


def create_target_table(client: bigquery.Client) -> None:
    query = TARGET_SCHEMA.format(
        project=PROJECT_ID,
        dataset=TARGET_DATASET,
        table=TARGET_TABLE,
    )
    run_query(client, query)


def delete_target_day(client: bigquery.Client, start_date: str, end_date: str) -> None:
    target = f"`{PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}`"
    query = f"""
    DELETE FROM {target}
    WHERE DATE(TIMESTAMP_MILLIS(timestamp), '{PROCESS_TIME_ZONE}') >= @start_date
      AND DATE(TIMESTAMP_MILLIS(timestamp), '{PROCESS_TIME_ZONE}') < @end_date
    """
    run_query(client, query, query_params(start_date, end_date))


def insert_target_day(client: bigquery.Client, start_date: str, end_date: str):
    source = f"`{PROJECT_ID}.{SOURCE_DATASET}.{SOURCE_TABLE}`"
    users = f"`{PROJECT_ID}.{USERS_DATASET}.{USERS_TABLE}`"
    target = f"`{PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}`"

    query = f"""
    INSERT INTO {target} (
      subscription_id,
      subscription_plan,
      timestamp,
      event_type,
      customer_id,
      user_id,
      is_session_created,
      is_quick_buy_sell,
      is_partials_taken,
      is_position_managed,
      is_replay_activated,
      is_news_visibility_clicked,
      is_order_placed,
      created_session_type,
      quick_buy_sell_type,
      partials_taken_percentage,
      partials_taken_position_amount_type,
      position_managed_type,
      order_has_auto_be,
      order_has_object_selected,
      order_has_stop_loss,
      order_tag_count,
      order_has_tag_selected,
      order_has_take_profit,
      order_take_profit_type,
      order_save_type
    )
    WITH events AS (
      SELECT
        user_id,
        customer_id,
        event_timestamp,
        name AS event_type,
        is_session_created,
        is_quick_buy_sell,
        is_partials_taken,
        is_position_managed,
        is_replay_activated,
        is_news_visibility_open,
        is_order_placed,
        created_session_type,
        quick_buy_sell_type,
        partials_taken_percentage,
        partials_taken_position_amount_type,
        position_managed_type,
        order_has_auto_be,
        order_has_object_selected,
        order_has_stop_loss,
        order_tag_count,
        order_has_tag_selected,
        order_has_take_profit,
        order_take_profit_type,
        order_save_type
      FROM {source}
      WHERE DATE(event_timestamp, '{PROCESS_TIME_ZONE}') >= @start_date
        AND DATE(event_timestamp, '{PROCESS_TIME_ZONE}') < @end_date
        AND name IN (
          'session_created',
          'quick_buy_sell',
          'partials_taken',
          'position_managed',
          'replay_activated',
          'news_visibility_clicked',
          'order_placed'
        )
        AND (
          COALESCE(is_session_created, 0) +
          COALESCE(is_quick_buy_sell, 0) +
          COALESCE(is_partials_taken, 0) +
          COALESCE(is_position_managed, 0) +
          COALESCE(is_replay_activated, 0) +
          COALESCE(is_news_visibility_open, 0) +
          COALESCE(is_order_placed, 0)
        ) = 1
        AND (
          (name = 'session_created' AND is_session_created = 1 AND created_session_type IS NOT NULL) OR
          (name = 'quick_buy_sell' AND is_quick_buy_sell = 1 AND quick_buy_sell_type IS NOT NULL) OR
          (name = 'partials_taken' AND is_partials_taken = 1 AND partials_taken_percentage IS NOT NULL AND partials_taken_position_amount_type IS NOT NULL) OR
          (name = 'position_managed' AND is_position_managed = 1 AND position_managed_type IS NOT NULL) OR
          (name = 'replay_activated' AND is_replay_activated = 1) OR
          (name = 'news_visibility_clicked' AND is_news_visibility_open = 1) OR
          (name = 'order_placed' AND is_order_placed = 1)
        )
        AND (name = 'session_created' OR created_session_type IS NULL)
        AND (name = 'quick_buy_sell' OR quick_buy_sell_type IS NULL)
        AND (name = 'partials_taken' OR (partials_taken_percentage IS NULL AND partials_taken_position_amount_type IS NULL))
        AND (name = 'position_managed' OR position_managed_type IS NULL)
        AND (name = 'order_placed' OR (
          order_has_auto_be IS NULL AND
          order_has_object_selected IS NULL AND
          order_has_stop_loss IS NULL AND
          order_tag_count IS NULL AND
          order_has_tag_selected IS NULL AND
          order_has_take_profit IS NULL AND
          order_take_profit_type IS NULL AND
          order_save_type IS NULL
        ))
    ),
    users AS (
      SELECT
        user_id,
        last_subscription_id AS subscription_id,
        last_subscription_plan AS subscription_plan
      FROM {users}
      WHERE last_subscription_id IS NOT NULL
        AND last_subscription_id != ''
        AND last_subscription_plan NOT LIKE '%BEGINNER%'
    )
    SELECT
      u.subscription_id,
      u.subscription_plan,
      UNIX_MILLIS(e.event_timestamp) AS timestamp,
      e.event_type,
      e.customer_id,
      e.user_id,
      e.is_session_created,
      e.is_quick_buy_sell,
      e.is_partials_taken,
      e.is_position_managed,
      e.is_replay_activated,
      e.is_news_visibility_open AS is_news_visibility_clicked,
      e.is_order_placed,
      e.created_session_type,
      e.quick_buy_sell_type,
      e.partials_taken_percentage,
      e.partials_taken_position_amount_type,
      e.position_managed_type,
      e.order_has_auto_be,
      e.order_has_object_selected,
      e.order_has_stop_loss,
      e.order_tag_count,
      e.order_has_tag_selected,
      e.order_has_take_profit,
      e.order_take_profit_type,
      e.order_save_type
    FROM events e
    INNER JOIN users u
      ON e.user_id = u.user_id
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=query_params(start_date, end_date)
        ),
    )
    job.result()
    return job.num_dml_affected_rows


def count_target_day(client: bigquery.Client, start_date: str, end_date: str) -> int:
    target = f"`{PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}`"
    query = f"""
    SELECT COUNT(*) AS row_count
    FROM {target}
    WHERE DATE(TIMESTAMP_MILLIS(timestamp), '{PROCESS_TIME_ZONE}') >= @start_date
      AND DATE(TIMESTAMP_MILLIS(timestamp), '{PROCESS_TIME_ZONE}') < @end_date
    """
    rows = list(run_query(client, query, query_params(start_date, end_date)))
    return rows[0]["row_count"] if rows else 0


def main() -> None:
    start_date, end_date = date_window()
    client = bigquery.Client(project=PROJECT_ID)

    create_target_table(client)
    delete_target_day(client, start_date, end_date)
    inserted_rows = insert_target_day(client, start_date, end_date)
    final_rows = count_target_day(client, start_date, end_date)

    print(
        "Daily refresh completed",
        {
            "project": PROJECT_ID,
            "source_table": f"{PROJECT_ID}.{SOURCE_DATASET}.{SOURCE_TABLE}",
            "target_table": f"{PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}",
            "time_zone": PROCESS_TIME_ZONE,
            "start_date": start_date,
            "end_date": end_date,
            "inserted_rows": inserted_rows,
            "final_rows_for_day": final_rows,
        },
    )


if __name__ == "__main__":
    main()
