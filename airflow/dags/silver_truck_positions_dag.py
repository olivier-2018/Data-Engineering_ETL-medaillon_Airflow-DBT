"""Bronze -> silver for truck positions - the validated/enriched historical
view + error-routing (NOT the live map's primary path, which reads
silver.truck_positions_current as upserted directly by the streaming job).
Runs more frequently than the other silver DAGs since bronze itself is
populated continuously here (§1a)."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLThresholdCheckOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator

from common.assets import SILVER_TRUCK_POSITIONS
from common.dq_checks import has_new_bronze_data

with DAG(
    dag_id="silver_truck_positions_dag",
    description="Bronze -> silver for truck positions, bounding-box + orphan-order validation (§7)",
    schedule=timedelta(minutes=2),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["silver", "batch"],
) as dag:
    check = ShortCircuitOperator(
        task_id="check_new_truck_position_events",
        python_callable=lambda: has_new_bronze_data("iot.truck_position_events", "truck_position_events"),
    )
    run_silver = SparkSubmitOperator(
        task_id="truck_positions_to_silver",
        application="/opt/spark-batch-jobs/truck_positions_to_silver.py",
        name="truck-positions-to-silver",
        conn_id="spark_default",
        deploy_mode="client",
        conf={"spark.driver.memory": "512m", "spark.executor.memory": "512m", "spark.cores.max": "2", "spark.ui.port": "4041", "spark.driver.host": "airflow-scheduler"},
        outlets=[SILVER_TRUCK_POSITIONS],
    )
    check_errors = SQLThresholdCheckOperator(
        task_id="check_truck_position_error_rate",
        conn_id="postgres_iot",
        sql="SELECT COUNT(*) FROM silver.error_truck_positions WHERE rejected_at > now() - interval '15 minutes'",
        min_threshold=0,
        max_threshold=2000,
    )

    check >> run_silver >> check_errors
