"""§8: Airflow-scheduled periodic pull from OpenWeatherMap (current +
forecast) for the 5 fixed locations in data_generators/config.yaml - NOT a
Kafka/Spark job, since this domain has no streaming source at all, just a
REST API to poll. Writes into iot.weather_observations (a hypertable,
genuinely time-series, just Airflow-populated instead of Spark).

Config is read inside the task callable (not at DAG-parse/module level) so
the dag-processor (which only parses this file, never executes task code)
doesn't need the config.yaml mount - only airflow-scheduler does.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

import psycopg2
import yaml
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from psycopg2.extras import execute_values

from common.weather_client import fetch_observations

CONFIG_PATH = os.environ.get("CONFIG_PATH", "/opt/config/config.yaml")


def _pg_conn():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["PIPELINE_DB_USER"],
        password=os.environ["PIPELINE_DB_PASSWORD"],
    )


def pull_weather(**_context) -> None:
    api_key = os.environ.get("OPENWEATHERMAP_API_KEY", "")
    if not api_key:
        raise RuntimeError(
            "OPENWEATHERMAP_API_KEY is not set in .env - sign up at "
            "https://openweathermap.org/api and fill it in."
        )

    with open(CONFIG_PATH) as f:
        locations = yaml.safe_load(f)["weather"]["locations"]

    all_rows: list[dict] = []
    for location in locations:
        all_rows.extend(fetch_observations(location, api_key))

    if not all_rows:
        return

    values = [
        (
            r["location_name"], r["lat"], r["lon"], r["observed_at"],
            r["temperature"], r["humidity"], r["pressure"],
            r["weather_condition"], r["forecast_horizon_hours"],
        )
        for r in all_rows
    ]

    with _pg_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO iot.weather_observations
                    (location_name, lat, lon, observed_at, temperature, humidity,
                     pressure, weather_condition, forecast_horizon_hours, ingested_at)
                VALUES %s
                """,
                values,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, now())",
            )
        conn.commit()


with DAG(
    dag_id="weather_enrichment_dag",
    description="Pulls current + forecast weather from OpenWeatherMap for 5 fixed locations (§8)",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["weather", "external-api"],
) as dag:
    PythonOperator(
        task_id="pull_weather_observations",
        python_callable=pull_weather,
    )
