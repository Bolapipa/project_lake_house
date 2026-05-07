import os
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


LOCAL_TIMEZONE = "America/Sao_Paulo"
DEFAULT_SCHEDULE = "0 1 * * *"
DEFAULT_CONN_ID = "databricks_default"
DEFAULT_POLLING_SECONDS = 30
DEFAULT_DATABRICKS_RETRY_LIMIT = 5
DEFAULT_DATABRICKS_RETRY_DELAY_SECONDS = 30
DEFAULT_TASK_RETRIES = 2
DEFAULT_TASK_RETRY_MINUTES = 15
DEFAULT_EXECUTION_TIMEOUT_HOURS = 6

DAG_ID = "open_meteo_databricks_dev"
TASK_ID = "executar_job_open_meteo_dev"
DEFAULT_JOB_ID = 104751199505011


def _read_int_env(key: str, default: int) -> int:
    raw_value = os.getenv(key)
    if raw_value is None or raw_value.strip() == "":
        return default
    return int(raw_value)


DATABRICKS_JOB_ID = _read_int_env("OPEN_METEO_DEV_JOB_ID", DEFAULT_JOB_ID)
DATABRICKS_CONN_ID = os.getenv("OPEN_METEO_DEV_CONN_ID", DEFAULT_CONN_ID)
DAG_SCHEDULE = os.getenv("OPEN_METEO_DEV_SCHEDULE", DEFAULT_SCHEDULE) or DEFAULT_SCHEDULE
POLLING_SECONDS = _read_int_env("OPEN_METEO_DATABRICKS_POLLING_SECONDS", DEFAULT_POLLING_SECONDS)
DATABRICKS_RETRY_LIMIT = _read_int_env(
    "OPEN_METEO_DATABRICKS_RETRY_LIMIT",
    DEFAULT_DATABRICKS_RETRY_LIMIT,
)
DATABRICKS_RETRY_DELAY_SECONDS = _read_int_env(
    "OPEN_METEO_DATABRICKS_RETRY_DELAY_SECONDS",
    DEFAULT_DATABRICKS_RETRY_DELAY_SECONDS,
)

default_args = {
    "owner": "data_engineering",
    "retries": DEFAULT_TASK_RETRIES,
    "retry_delay": timedelta(minutes=DEFAULT_TASK_RETRY_MINUTES),
}

with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TIMEZONE),
    schedule=DAG_SCHEDULE,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=DEFAULT_EXECUTION_TIMEOUT_HOURS),
    default_args=default_args,
    tags=["open_meteo", "databricks", "dev"],
) as dag:
    # Dispara o job do Databricks em dev com politica de retry no Airflow e no hook Databricks.
    executar_job_open_meteo_dev = DatabricksRunNowOperator(
        task_id=TASK_ID,
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=DATABRICKS_JOB_ID,
        polling_period_seconds=POLLING_SECONDS,
        databricks_retry_limit=DATABRICKS_RETRY_LIMIT,
        databricks_retry_delay=DATABRICKS_RETRY_DELAY_SECONDS,
        execution_timeout=timedelta(hours=DEFAULT_EXECUTION_TIMEOUT_HOURS),
        wait_for_termination=True,
    )
