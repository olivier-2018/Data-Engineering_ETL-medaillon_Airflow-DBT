"""Bronze -> silver for purchase orders + their line items (§7). Runs both
purchase_orders_to_silver.py and product_on_orders_to_silver.py in one DAG,
sequenced (not two separate DAGs with a cross-DAG Asset dependency),
because product_on_orders_to_silver.py's orphan check reads
silver.purchase_orders_current - it must run after the header table is
up to date, not concurrently with it."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLThresholdCheckOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator

from common.assets import SILVER_PRODUCT_ON_ORDERS, SILVER_PURCHASE_ORDERS
from common.dq_checks import has_new_bronze_data
from common.pipeline_config import SILVER_PROCESSING_SCHEDULE

SPARK_CONF = {
    "spark.driver.memory": "512m",
    "spark.executor.memory": "512m",
    "spark.cores.max": "2",
    "spark.ui.port": "4041",
    "spark.driver.host": "airflow-scheduler",
    # purchase_orders_to_silver.py's foreachPartition write path needs
    # executors to `import shared_utils` - no executor has ever needed to
    # import this module before (every prior shared_utils/
    # shared_ingestion_utils call was driver-side only), so this wasn't
    # needed until now. The file already exists at this identical path on
    # every executor container (spark-worker-1/2 bind-mount
    # ./spark-batch-jobs:/opt/spark-batch-jobs:ro, same as the driver) -
    # this just makes it importable there too.
    "spark.executorEnv.PYTHONPATH": "/opt/spark-batch-jobs/silver_processing",
    # Forces the driver to wait until 100% of the requested executor
    # resources (both 1-core executors, per spark.cores.max=2 + the
    # cluster's spreadOut=true) have registered before scheduling ANY
    # task - without this, a registration-timing race can let both of
    # repartition(2)'s write tasks land on the same executor (confirmed
    # happening in practice), running sequentially on its 1 core instead
    # of genuinely in parallel across both. See docs/SPARK_PROJECT.md §7
    # Recipe B.
    "spark.scheduler.minRegisteredResourcesRatio": "1.0",
    "spark.scheduler.maxRegisteredResourcesWaitingTime": "3s",
}

with DAG(
    dag_id="silver_purchase_orders_dag",
    description="Bronze -> silver for purchase orders + line items, incl. 8-state lifecycle validation (§7)",
    schedule=SILVER_PROCESSING_SCHEDULE,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["silver", "batch"],
) as dag:
    check_orders = ShortCircuitOperator(
        task_id="check_new_purchase_order_events",
        python_callable=lambda: has_new_bronze_data("iot.purchase_order_events", "purchase_order_events"),
    )
    purchase_orders_to_silver = SparkSubmitOperator(
        task_id="purchase_orders_to_silver",
        application="/opt/spark-batch-jobs/silver_processing/purchase_orders_to_silver.py",
        name="purchase-orders-to-silver",
        conn_id="spark_default",
        deploy_mode="client",
        conf=SPARK_CONF,
        outlets=[SILVER_PURCHASE_ORDERS],
    )
    check_order_errors = SQLThresholdCheckOperator(
        task_id="check_purchase_order_error_rate",
        conn_id="postgres_iot",
        sql="SELECT COUNT(*) FROM silver.error_purchase_orders WHERE rejected_at > now() - interval '15 minutes'",
        min_threshold=0,
        max_threshold=1000,
    )

    check_line_items = ShortCircuitOperator(
        task_id="check_new_product_on_order_events",
        python_callable=lambda: has_new_bronze_data("iot.product_on_order_events", "product_on_order_events"),
    )
    product_on_orders_to_silver = SparkSubmitOperator(
        task_id="product_on_orders_to_silver",
        application="/opt/spark-batch-jobs/silver_processing/product_on_orders_to_silver.py",
        name="product-on-orders-to-silver",
        conn_id="spark_default",
        deploy_mode="client",
        conf=SPARK_CONF,
        outlets=[SILVER_PRODUCT_ON_ORDERS],
        # Must run after purchase_orders_to_silver (its orphan check reads
        # silver.purchase_orders_current), but purchase_orders_to_silver is
        # routinely SKIPPED (via check_orders' ShortCircuitOperator) on any
        # cycle with no new order events - that's not a failure, and must
        # not block line items from processing when there ARE new line
        # items that cycle. trigger_rule=none_failed treats "skipped" and
        # "succeeded" upstream the same (only an actual failure blocks),
        # so this task runs whenever check_line_items itself passes,
        # independent of whether purchase_orders_to_silver had anything to
        # do this cycle.
        trigger_rule="none_failed",
    )
    check_line_item_errors = SQLThresholdCheckOperator(
        task_id="check_product_on_order_error_rate",
        conn_id="postgres_iot",
        sql="SELECT COUNT(*) FROM silver.error_product_on_orders WHERE rejected_at > now() - interval '15 minutes'",
        min_threshold=0,
        max_threshold=1000,
    )

    check_orders >> purchase_orders_to_silver >> check_order_errors
    purchase_orders_to_silver >> product_on_orders_to_silver
    check_line_items >> product_on_orders_to_silver >> check_line_item_errors
