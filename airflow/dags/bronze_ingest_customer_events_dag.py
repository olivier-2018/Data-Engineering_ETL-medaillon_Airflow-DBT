"""Periodic-batch Kafka -> bronze ingestion for iot.customer_events (§7:
bronze_ingestion_dag.py split into one DAG per domain)."""
from __future__ import annotations

from airflow import DAG
from common.assets import BRONZE_CUSTOMER_EVENTS
from common.spark_ingestion_dag_factory import make_bronze_ingestion_dag

dag: DAG = make_bronze_ingestion_dag(
    dag_id="bronze_ingest_customer_events_dag",
    script="ingest_customer_events.py",
    description="Kafka iot.customer_events -> bronze (§7)",
    outlet=BRONZE_CUSTOMER_EVENTS,
    fileloc=__file__,
)
