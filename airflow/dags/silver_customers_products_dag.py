"""Bronze -> silver for the three low-frequency reference-data domains
(customers, products, truck fleet) - slower-changing than the
transactional domains, so they share one DAG rather than each getting its
own. Truck fleet folds in here (§7) rather than getting a dedicated DAG -
it's a near-static dimension (seeded once + rare updates), the same
rationale as customers/products. Each domain: ShortCircuit (skip if
nothing new) -> SparkSubmitOperator (outlets the corresponding Asset) ->
threshold check on its error table - the three domains are otherwise
fully independent, so they run in parallel."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLThresholdCheckOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator

from common.assets import SILVER_CUSTOMERS, SILVER_PRODUCTS, SILVER_TRUCK_FLEET
from common.dq_checks import has_new_bronze_data
from common.pipeline_config import SILVER_PROCESSING_SCHEDULE

SPARK_CONF = {
    "spark.driver.memory": "512m",
    "spark.executor.memory": "512m",
    "spark.cores.max": "2",
    "spark.ui.port": "4041",
    "spark.driver.host": "airflow-scheduler",
    # All three jobs now write via repartition+foreachPartition, needing
    # executors to `import shared_utils` - see
    # silver_purchase_orders_dag.py's own identical addition and
    # docs/SPARK_PROJECT.md for the full rationale.
    "spark.executorEnv.PYTHONPATH": "/opt/spark-batch-jobs/silver_processing",
    "spark.scheduler.minRegisteredResourcesRatio": "1.0",
    "spark.scheduler.maxRegisteredResourcesWaitingTime": "3s",
}

with DAG(
    dag_id="silver_customers_products_dag",
    description="Bronze -> silver for customers, products, and truck fleet (§7)",
    schedule=SILVER_PROCESSING_SCHEDULE,
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
        application="/opt/spark-batch-jobs/silver_processing/customers_to_silver.py",
        name="customers-to-silver",
        conn_id="spark_default",
        deploy_mode="client",
        conf=SPARK_CONF,
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
        application="/opt/spark-batch-jobs/silver_processing/products_to_silver.py",
        name="products-to-silver",
        conn_id="spark_default",
        deploy_mode="client",
        conf=SPARK_CONF,
        outlets=[SILVER_PRODUCTS],
    )
    check_product_errors = SQLThresholdCheckOperator(
        task_id="check_product_error_rate",
        conn_id="postgres_iot",
        sql="SELECT COUNT(*) FROM silver.error_products WHERE rejected_at > now() - interval '15 minutes'",
        min_threshold=0,
        max_threshold=1000,
    )

    check_truck_fleet = ShortCircuitOperator(
        task_id="check_new_truck_fleet_events",
        python_callable=lambda: has_new_bronze_data("iot.truck_fleet_events", "truck_fleet_events"),
    )
    truck_fleet_to_silver = SparkSubmitOperator(
        task_id="truck_fleet_to_silver",
        application="/opt/spark-batch-jobs/silver_processing/truck_fleet_to_silver.py",
        name="truck-fleet-to-silver",
        conn_id="spark_default",
        deploy_mode="client",
        conf=SPARK_CONF,
        outlets=[SILVER_TRUCK_FLEET],
    )
    check_truck_fleet_errors = SQLThresholdCheckOperator(
        task_id="check_truck_fleet_error_rate",
        conn_id="postgres_iot",
        sql="SELECT COUNT(*) FROM silver.error_truck_fleet WHERE rejected_at > now() - interval '15 minutes'",
        min_threshold=0,
        max_threshold=1000,
    )

    check_customers >> customers_to_silver >> check_customer_errors
    check_products >> products_to_silver >> check_product_errors
    check_truck_fleet >> truck_fleet_to_silver >> check_truck_fleet_errors
