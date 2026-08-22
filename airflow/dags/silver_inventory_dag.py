"""Bronze -> silver for inventory, PLUS the automatic restock-check task
(§1c) immediately after - it needs fresh silver.inventory_current, so it
belongs here rather than in bronze_ingestion_dag.py (documented placement
decision from the plan). restock_check.py is pure psycopg2 (no Spark
workload to justify a JVM boot), so it runs via BashOperator, not
SparkSubmitOperator - one more example of the "several operator types"
learning goal."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLThresholdCheckOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator

from common.assets import SILVER_INVENTORY
from common.dq_checks import has_new_bronze_data

with DAG(
    dag_id="silver_inventory_dag",
    description="Bronze -> silver for inventory + automatic restock check (§7, §1c)",
    schedule=timedelta(minutes=10),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["silver", "batch"],
) as dag:
    check = ShortCircuitOperator(
        task_id="check_new_inventory_changes",
        python_callable=lambda: has_new_bronze_data("iot.inventory_changes", "inventory_changes"),
    )
    run_silver = SparkSubmitOperator(
        task_id="inventory_to_silver",
        application="/opt/spark-batch-jobs/inventory_to_silver.py",
        name="inventory-to-silver",
        conn_id="spark_default",
        deploy_mode="client",
        conf={"spark.driver.memory": "512m", "spark.executor.memory": "512m", "spark.cores.max": "2", "spark.ui.port": "4041", "spark.driver.host": "airflow-scheduler"},
        outlets=[SILVER_INVENTORY],
    )
    check_errors = SQLThresholdCheckOperator(
        task_id="check_inventory_error_rate",
        conn_id="postgres_iot",
        sql="SELECT COUNT(*) FROM silver.error_inventory_changes WHERE rejected_at > now() - interval '15 minutes'",
        min_threshold=0,
        max_threshold=1000,
    )
    restock_check = BashOperator(
        task_id="restock_check",
        bash_command="cd /opt/spark-batch-jobs/bronze_ingestion && python3 restock_check.py",
    )

    check >> run_silver >> check_errors >> restock_check
