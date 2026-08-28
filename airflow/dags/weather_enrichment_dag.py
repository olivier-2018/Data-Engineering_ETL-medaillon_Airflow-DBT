"""§8: Airflow-scheduled periodic pull from OpenWeatherMap (current +
forecast) for the fixed locations in reference.weather_stations (moved out
of data_generators/config.yaml per the redesign plan §2a/decision #12 -
station locations are structural reference data, not a generator
hyperparameter) - NOT a Kafka/Spark job, since this domain has no streaming
source at all, just a REST API to poll. Writes into iot.weather_observations
(a hypertable, genuinely time-series, just Airflow-populated instead of
Spark).

Locations are read inside the task callable (not at DAG-parse/module level)
so the dag-processor (which only parses this file, never executes task
code) never needs a database connection - only airflow-scheduler does."""
from __future__ import annotations

import os
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from psycopg2.extras import execute_values

from common.weather_client import fetch_observations


def _pg_conn():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["PIPELINE_DB_USER"],
        password=os.environ["PIPELINE_DB_PASSWORD"],
    )


def _fetch_stations() -> list[dict]:
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name, lat, lon FROM reference.weather_stations")
            return [{"name": name, "lat": lat, "lon": lon} for name, lat, lon in cur.fetchall()]


def pull_weather(**_context) -> None:
    api_key = os.environ.get("OPENWEATHERMAP_API_KEY", "")
    if not api_key:
        raise RuntimeError(
            "OPENWEATHERMAP_API_KEY is not set in .env - sign up at "
            "https://openweathermap.org/api and fill it in."
        )

    locations = _fetch_stations()

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
