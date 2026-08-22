"""Bronze -> silver for the two low-frequency reference-data domains
(customers, products) - slower cadence than the transactional domains since
these barely change. Each domain: ShortCircuit (skip if nothing new) ->
SparkSubmitOperator (outlets the corresponding Asset) -> threshold check on
its error table."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLThresholdCheckOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator

from common.assets import SILVER_CUSTOMERS, SILVER_PRODUCTS
from common.dq_checks import has_new_bronze_data

with DAG(
    dag_id="silver_customers_products_dag",
    description="Bronze -> silver for customers and products (§7)",
    schedule=timedelta(minutes=15),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["silver", "batch"],
) as dag:
    check_customers = ShortCircuitOperator(
        task_id="check_new_customer_events",
        python_callable=lambda: has_new_bronze_data("iot.customer_events", "customer_events"),
    )
    customers_to_silver = SparkSubmitOperator(
        task_id="customers_to_silver",
        application="/opt/spark-batch-jobs/customers_to_silver.py",
        name="customers-to-silver",
        conn_id="spark_default",
        deploy_mode="client",
        conf={"spark.driver.memory": "512m", "spark.executor.memory": "512m", "spark.cores.max": "2", "spark.ui.port": "4041", "spark.driver.host": "airflow-scheduler"},
        outlets=[SILVER_CUSTOMERS],
    )
    check_customer_errors = SQLThresholdCheckOperator(
        task_id="check_customer_error_rate",
        conn_id="postgres_iot",
        sql="SELECT COUNT(*) FROM silver.error_customers WHERE rejected_at > now() - interval '15 minutes'",
        min_threshold=0,
        max_threshold=1000,
    )

    check_products = ShortCircuitOperator(
        task_id="check_new_product_events",
        python_callable=lambda: has_new_bronze_data("iot.product_events", "product_events"),
    )
    products_to_silver = SparkSubmitOperator(
        task_id="products_to_silver",
        application="/opt/spark-batch-jobs/products_to_silver.py",
        name="products-to-silver",
        conn_id="spark_default",
        deploy_mode="client",
        conf={"spark.driver.memory": "512m", "spark.executor.memory": "512m", "spark.cores.max": "2", "spark.ui.port": "4041", "spark.driver.host": "airflow-scheduler"},
        outlets=[SILVER_PRODUCTS],
    )
    check_product_errors = SQLThresholdCheckOperator(
        task_id="check_product_error_rate",
        conn_id="postgres_iot",
        sql="SELECT COUNT(*) FROM silver.error_products WHERE rejected_at > now() - interval '15 minutes'",
        min_threshold=0,
        max_threshold=1000,
    )

    check_customers >> customers_to_silver >> check_customer_errors
    check_products >> products_to_silver >> check_product_errors
