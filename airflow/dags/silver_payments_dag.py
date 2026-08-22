from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLThresholdCheckOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator

from common.assets import SILVER_PAYMENTS
from common.dq_checks import has_new_bronze_data

with DAG(
    dag_id="silver_payments_dag",
    description="Bronze -> silver for payments, incl. payment-status transition validation (§7)",
    schedule=timedelta(minutes=10),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["silver", "batch"],
) as dag:
    check = ShortCircuitOperator(
        task_id="check_new_payment_events",
        python_callable=lambda: has_new_bronze_data("iot.payment_events", "payment_events"),
    )
    run_silver = SparkSubmitOperator(
        task_id="payments_to_silver",
        application="/opt/spark-batch-jobs/payments_to_silver.py",
        name="payments-to-silver",
        conn_id="spark_default",
        deploy_mode="client",
        conf={"spark.driver.memory": "512m", "spark.executor.memory": "512m", "spark.cores.max": "2", "spark.ui.port": "4041", "spark.driver.host": "airflow-scheduler"},
        outlets=[SILVER_PAYMENTS],
    )
    check_errors = SQLThresholdCheckOperator(
        task_id="check_payment_error_rate",
        conn_id="postgres_iot",
        sql="SELECT COUNT(*) FROM silver.error_payments WHERE rejected_at > now() - interval '15 minutes'",
        min_threshold=0,
        max_threshold=1000,
    )

    check >> run_silver >> check_errors
