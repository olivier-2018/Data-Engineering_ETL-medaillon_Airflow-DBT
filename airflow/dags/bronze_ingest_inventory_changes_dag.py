"""Periodic-batch Kafka -> bronze ingestion for iot.inventory_changes (§7:
bronze_ingestion_dag.py split into one DAG per domain).

NOTE: this domain has TWO distinct write paths into iot.inventory_changes -
this DAG only covers one of them. The generator emits genuine Kafka events
here for every sale (Product.decrement_for_sale(), change_reason='sale'),
so it needs the same Kafka -> bronze ingestion treatment as every other
domain. Separately, spark-batch-jobs/silver_processing/restock_check.py
writes change_reason='restock' rows directly via psycopg2 (bypassing Kafka
entirely, since it already runs as a plain Postgres-connected script inside
silver_inventory_dag.py, right after this data has been read back out of
silver) - that write path needs no ingestion DAG of its own."""
from __future__ import annotations

from airflow import DAG
from common.assets import BRONZE_INVENTORY_CHANGES
from common.spark_ingestion_dag_factory import make_bronze_ingestion_dag

dag: DAG = make_bronze_ingestion_dag(
    dag_id="bronze_ingest_inventory_changes_dag",
    script="ingest_inventory_changes.py",
    description="Kafka iot.inventory_changes -> bronze, sale events only (§7)",
    outlet=BRONZE_INVENTORY_CHANGES,
    fileloc=__file__,
)
