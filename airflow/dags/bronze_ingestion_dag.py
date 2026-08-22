"""§6a: periodic-batch Kafka -> bronze ingestion for the 5 non-streaming
domains (customer/product/order/payment/inventory events). Each task is an
independent client-mode SparkSubmitOperator - client mode is fine here
since these are short-lived batch jobs, not forever-running queries, so
nothing blocks the Airflow task for long."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

JOBS = {
    "ingest_customer_events": "ingest_customer_events.py",
    "ingest_product_events": "ingest_product_events.py",
    "ingest_order_events": "ingest_order_events.py",
    "ingest_payment_events": "ingest_payment_events.py",
    "ingest_inventory_changes": "ingest_inventory_changes.py",
}

with DAG(
    dag_id="bronze_ingestion_dag",
    description="Periodic-batch Kafka -> bronze ingestion for the 5 non-streaming domains (§6a)",
    schedule=timedelta(minutes=10),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["bronze", "batch"],
) as dag:
    for task_id, script in JOBS.items():
        SparkSubmitOperator(
            task_id=task_id,
            application=f"/opt/spark-batch-jobs/bronze_ingestion/{script}",
            name=task_id,
            conn_id="spark_default",
            deploy_mode="client",
            conf={
                "spark.driver.memory": "512m",
                # Spark defaults executor memory to 1g if unset - with 2 worker
                # containers whose total SPARK_WORKER_MEMORY is only 1600m/1200m,
                # even one 1g executor left almost no room for anything else to
                # run concurrently, so every other job just sat WAITING forever
                # (cores were free, memory wasn't) - confirmed by testing.
                "spark.executor.memory": "512m",
                "spark.cores.max": "2",
                # Starting port for this job's SparkUI - 4040 is reserved for
                # the streaming job's own container; batch jobs here start
                # one above it and auto-increment for concurrent jobs.
                "spark.ui.port": "4041",
                # Pins the driver's SparkUI link (Master's "Application Detail
                # UI") to a stable, published address instead of the random
                # container ID - see docs/OBSERVABILITY.md.
                "spark.driver.host": "airflow-scheduler",
            },
        )
        # Independent domains, no interdependencies - can run in parallel
        # (bounded by AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=2).
