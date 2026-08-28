"""Bronze -> silver for invoices (§7) - replaces the old payments domain
entirely, per redesign plan decision #6 (payments folded into the invoice
lifecycle: created -> pending -> settled/cancelled, with reminders)."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLThresholdCheckOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator

from common.assets import SILVER_INVOICES
from common.dq_checks import has_new_bronze_data
from common.pipeline_config import SILVER_PROCESSING_SCHEDULE

with DAG(
    dag_id="silver_invoices_dag",
    description="Bronze -> silver for invoices, incl. status + reminder-count validation (§7)",
    schedule=SILVER_PROCESSING_SCHEDULE,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["silver", "batch"],
) as dag:
    check = ShortCircuitOperator(
        task_id="check_new_invoice_events",
        python_callable=lambda: has_new_bronze_data("iot.invoice_events", "invoice_events"),
    )
    run_silver = SparkSubmitOperator(
        task_id="invoices_to_silver",
        application="/opt/spark-batch-jobs/silver_processing/invoices_to_silver.py",
        name="invoices-to-silver",
        conn_id="spark_default",
        deploy_mode="client",
        conf={
            "spark.driver.memory": "512m",
            "spark.executor.memory": "512m",
            "spark.cores.max": "2",
            "spark.ui.port": "4041",
            "spark.driver.host": "airflow-scheduler",
            # Now writes via repartition+foreachPartition - see
            # silver_purchase_orders_dag.py's identical addition and
            # docs/SPARK_PROJECT.md for the full rationale.
            "spark.executorEnv.PYTHONPATH": "/opt/spark-batch-jobs/silver_processing",
            "spark.scheduler.minRegisteredResourcesRatio": "1.0",
            "spark.scheduler.maxRegisteredResourcesWaitingTime": "3s",
        },
        outlets=[SILVER_INVOICES],
    )
    check_errors = SQLThresholdCheckOperator(
        task_id="check_invoice_error_rate",
        conn_id="postgres_iot",
        sql="SELECT COUNT(*) FROM silver.error_invoices WHERE rejected_at > now() - interval '15 minutes'",
        min_threshold=0,
        max_threshold=1000,
    )

    check >> run_silver >> check_errors
