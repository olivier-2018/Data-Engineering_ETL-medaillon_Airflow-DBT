"""Factory for the per-domain bronze-ingestion DAGs (§7 of the redesign
plan): bronze_ingestion_dag.py's single 5-task DAG was split into one DAG
per Kafka topic/domain, so a slow/failing domain no longer shares a single
DAG run's scheduling slot (`max_active_runs=1`) with the other domains, and
each shows up as its own row in the Airflow UI. The 6-7 resulting DAG files
are otherwise identical, hence this one shared builder rather than
hand-duplicating the SparkSubmitOperator boilerplate across all of them.

IMPORTANT - two things every bronze_ingest_*_dag.py caller must do, both
confirmed against a live Airflow 3 instance:

1. Pass `fileloc=__file__`. `DAG.__init__` infers `fileloc` from the
   caller's stack frame - since the literal `DAG(...)` call lives in THIS
   file, a DAG built here would otherwise get `fileloc` pointing at
   spark_ingestion_dag_factory.py, not at the actual per-domain file that
   called this function, and get silently attributed to the wrong file.
   Handled below by requiring callers to pass their own `__file__` and
   setting `dag.fileloc` explicitly.
2. Each caller file must itself contain the literal substring "airflow"
   somewhere in its source (case-insensitive) - not something this factory
   can do on a caller's behalf. Airflow's DAG-discovery heuristic
   (airflow.utils.file.might_contain_dag_via_default_heuristic) only
   attempts to parse a file as a DAG if its raw text contains "airflow"
   and either "dag" or "asset". This check is hardcoded to always run in
   the dag-processor's actual parsing subprocess (airflow/dag_processing/
   processor.py's `_parse_file()` passes `safe_mode=True` as a literal,
   ignoring the `core.dag_discovery_safe_mode` config value entirely) - a
   file that only imports from `common.*` (never `airflow` directly) fails
   this check and is skipped with no import error, no exception, nothing
   in `airflow dags list-import-errors`. Every bronze_ingest_*_dag.py file
   satisfies this by importing `DAG` from `airflow` for a real type
   annotation on its own `dag: DAG = make_bronze_ingestion_dag(...)` line -
   not a cosmetic string, genuinely meaningful code that happens to also
   satisfy the heuristic."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import Asset

from common.pipeline_config import BRONZE_INGESTION_SCHEDULE


def make_bronze_ingestion_dag(
    dag_id: str,
    script: str,
    description: str,
    outlet: Asset,
    fileloc: str,
) -> DAG:
    with DAG(
        dag_id=dag_id,
        description=description,
        schedule=BRONZE_INGESTION_SCHEDULE,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        max_active_runs=1,
        default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
        tags=["bronze", "batch"],
    ) as dag:
        dag.fileloc = fileloc
        SparkSubmitOperator(
            task_id=dag_id.removesuffix("_dag"),
            application=f"/opt/spark-batch-jobs/bronze_ingestion/{script}",
            name=dag_id,
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
                # Only ingest_purchase_order_events.py's distributed=True
                # foreachPartition path actually needs executors to `import
                # shared_ingestion_utils` (the other 6 domains stay on the
                # driver-side-only default path) - set for all 7 anyway since
                # they share this one factory/conf and it's harmless for the
                # ones that don't use it. Same rationale as
                # silver_purchase_orders_dag.py's own PYTHONPATH addition.
                "spark.executorEnv.PYTHONPATH": "/opt/spark-batch-jobs/bronze_ingestion",
                # Same reasoning as silver_purchase_orders_dag.py's own
                # addition (see docs/SPARK_PROJECT.md §7 Recipe B): forces
                # the driver to wait for 100% of requested executors to
                # register before scheduling any task, removing the
                # registration-timing race that can otherwise put both of
                # ingest_purchase_order_events.py's repartition(2) write
                # tasks on the same executor. Set for all 7 domains anyway
                # since they share this one conf - harmless small (<=3s)
                # startup wait for the 6 that don't use the distributed path.
                "spark.scheduler.minRegisteredResourcesRatio": "1.0",
                "spark.scheduler.maxRegisteredResourcesWaitingTime": "3s",
            },
            outlets=[outlet],
        )
    return dag
