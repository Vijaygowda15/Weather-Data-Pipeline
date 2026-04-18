import sys
import os
import json
import logging

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

sys.path.insert(0, "/opt/airflow")

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner":            "vijay",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
}

def task_extract(**context):
    """
    Call OpenWeatherMap API for all configured cities.
    Saves raw JSON to disk and pushes the filepath to XCom.
    """
    from extract import extract_all_cities, save_raw_json

    log.info("Starting extraction from OpenWeatherMap API...")
    raw_data = extract_all_cities()

    if not raw_data:
        raise ValueError(
            "Extraction returned 0 cities. "
            "Check WEATHER_API_KEY and network connectivity."
        )

    filepath = save_raw_json(raw_data)

    context["ti"].xcom_push(key="raw_filepath",   value=filepath)
    context["ti"].xcom_push(key="cities_count",   value=len(raw_data))
    log.info(f"Extracted {len(raw_data)} cities → {filepath}")

def task_store(**context):
    """
    Save raw JSON to S3 (if configured) and convert to Parquet.
    Pushes the Parquet filepath to XCom for the transform task.
    """
    from store import store_raw, flatten_and_save_parquet

    raw_filepath = context["ti"].xcom_pull(
        key="raw_filepath", task_ids="extract"
    )

    with open(raw_filepath, "r") as f:
        raw_data = json.load(f)

    store_raw(raw_data, raw_filepath)
    parquet_path = flatten_and_save_parquet(raw_data)

    context["ti"].xcom_push(key="parquet_path", value=parquet_path)
    log.info(f"Parquet saved → {parquet_path}")

def task_transform(**context):
    """
    Read Parquet with PySpark, clean and enrich data.
    Saves two output Parquet files (clean rows + daily summary).
    Pushes both paths to XCom for the load task.
    """
    from transform import transform

    parquet_path = context["ti"].xcom_pull(
        key="parquet_path", task_ids="store"
    )

    log.info(f"Transforming: {parquet_path}")
    df_clean, df_summary = transform(parquet_path)

    clean_path   = parquet_path.replace(".parquet", "_clean")
    summary_path = parquet_path.replace(".parquet", "_summary")

    df_clean.write.mode("overwrite").parquet(clean_path)
    df_summary.write.mode("overwrite").parquet(summary_path)

    context["ti"].xcom_push(key="clean_path",   value=clean_path)
    context["ti"].xcom_push(key="summary_path", value=summary_path)
    log.info("Transform complete.")


def task_load(**context):
    """
    Load both Spark output Parquets into PostgreSQL.
    Uses the fixed load_to_postgres function (idempotent for summary table).
    Stops the Spark session after loading.
    """
    from load import create_tables, load_to_postgres
    from pyspark.sql import SparkSession

    clean_path   = context["ti"].xcom_pull(key="clean_path",   task_ids="transform")
    summary_path = context["ti"].xcom_pull(key="summary_path", task_ids="transform")

    spark = SparkSession.builder \
        .appName("WeatherLoad") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    df_clean   = spark.read.parquet(clean_path)
    df_summary = spark.read.parquet(summary_path)

    create_tables()
    load_to_postgres(df_clean,   "weather_readings")
    load_to_postgres(df_summary, "weather_summary")

    spark.stop()
    log.info("Load complete. Spark session stopped.")

with DAG(
    dag_id="weather_pipeline",
    default_args=DEFAULT_ARGS,
    description="Hourly weather ETL: OpenWeatherMap API → Parquet → PySpark → PostgreSQL",
    schedule_interval="0 * * * *",     # every hour at :00
    start_date=datetime(2025, 1, 1),
    catchup=False,                      # don't backfill missed runs
    max_active_runs=1,                  # only one run at a time
    tags=["weather", "etl", "pyspark", "portfolio"],
) as dag:

    start = EmptyOperator(task_id="start")

    extract = PythonOperator(
        task_id="extract",
        python_callable=task_extract,
    )

    store = PythonOperator(
        task_id="store",
        python_callable=task_store,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=task_transform,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=task_load,
    )

    end = EmptyOperator(task_id="end")

    start >> extract >> store >> transform_task >> load >> end
