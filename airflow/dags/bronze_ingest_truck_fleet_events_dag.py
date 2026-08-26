"""Periodic-batch Kafka -> bronze ingestion for iot.truck_fleet_events (§7:
bronze_ingestion_dag.py split into one DAG per domain). Low-volume domain
(seeded once + rare updates) but still gets its own DAG for consistency
with the other 5-6 domains, rather than a special case."""
from __future__ import annotations

from airflow import DAG
from common.assets import BRONZE_TRUCK_FLEET_EVENTS
from common.spark_ingestion_dag_factory import make_bronze_ingestion_dag

dag: DAG = make_bronze_ingestion_dag(
    dag_id="bronze_ingest_truck_fleet_events_dag",
    script="ingest_truck_fleet_events.py",
    description="Kafka iot.truck_fleet_events -> bronze (§7)",
    outlet=BRONZE_TRUCK_FLEET_EVENTS,
    fileloc=__file__,
)
