"""Shared psycopg2 connection helper for Airflow task callables (PythonOperator/
ShortCircuitOperator functions) - the DAG-authoring-time equivalent of the
pg_conn() helpers duplicated in spark-streaming-jobs/ and spark-batch-jobs/."""
from __future__ import annotations

import os

import psycopg2


def pg_conn():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["PIPELINE_DB_USER"],
        password=os.environ["PIPELINE_DB_PASSWORD"],
    )
