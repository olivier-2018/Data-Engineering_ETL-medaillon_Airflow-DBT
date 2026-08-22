"""Monitors the ONE persistent Structured Streaming job (truck-position
ingestion, §1a) - checks Spark Master's REST API every ~5 min and FAILS
the task (visible as an alert in the Airflow UI) if it isn't currently
running.

ARCHITECTURAL NOTE (revised after testing): this DAG no longer resubmits
the job itself. Spark Standalone does not support --deploy-mode cluster
for Python applications at all (confirmed by testing:
"Cluster deploy mode is currently not supported for python applications on
standalone clusters") - client mode's driver would instead block an
Airflow task for the query's entire (indefinite) lifetime, which is wrong
for LocalExecutor. The job now runs as its own docker-compose service
(`spark-streaming-truck-position`, restart: unless-stopped), so Docker
itself handles resupervision/restart - this DAG is monitoring/alerting
only, not an active resubmitter."""
from __future__ import annotations

from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator

APP_NAME = "bronze-truck-position-ingest"
SPARK_MASTER_UI = "http://spark-master:8080"


def check_streaming_app_running(**_context) -> None:
    try:
        resp = requests.get(f"{SPARK_MASTER_UI}/json/", timeout=10)
        resp.raise_for_status()
        active_apps = resp.json().get("activeapps", [])
    except requests.RequestException as exc:
        raise AirflowException(f"Could not reach Spark Master to check app status: {exc}") from exc

    running = any(app.get("name") == APP_NAME for app in active_apps)
    if not running:
        raise AirflowException(
            f"'{APP_NAME}' is not listed as a running Spark application. "
            "The spark-streaming-truck-position container should restart it "
            "automatically (restart: unless-stopped) - check its logs "
            "(`docker compose logs spark-streaming-truck-position`) if this "
            "persists across multiple checks."
        )


with DAG(
    dag_id="bronze_streaming_supervisor_dag",
    description="Alerts if the truck-position streaming job isn't running (§1a) - monitoring only, Docker handles restart",
    schedule=timedelta(minutes=5),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["bronze", "streaming", "monitoring"],
) as dag:
    PythonOperator(
        task_id="check_streaming_app_running",
        python_callable=check_streaming_app_running,
    )
