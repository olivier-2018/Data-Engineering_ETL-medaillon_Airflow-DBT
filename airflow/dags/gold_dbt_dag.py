"""§10: silver -> gold via dbt. schedule=[all 6 silver Assets] uses Airflow
3's AND-semantics for a list of assets - this DAG only fires once every
silver domain has produced at least one update since its last run, avoiding
a gold rebuild from a partial/inconsistent set of silver domains. Net
effect: gold refreshes roughly on the cadence of the slowest silver domain
(~10-15 min), matching the 5-15 min gold-lag target in
docs/TODO_transformations.md §2.3.

dbt invoked via plain BashOperator (not astronomer-cosmos) per the explicit
decision to keep dbt's own mechanics visible while learning it. snapshot/run/
test are kept as 3 separate tasks (not one `dbt build`) for clearer failure
isolation."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from airflow.providers.standard.operators.bash import BashOperator

from common.assets import ALL_SILVER_ASSETS

DBT_PROJECT_DIR = "/opt/dbt"
DBT_CMD = f"cd {DBT_PROJECT_DIR} && dbt"

with DAG(
    dag_id="gold_dbt_dag",
    description="silver -> gold star schema via dbt, triggered once all silver domains have updated (§9, §10)",
    schedule=ALL_SILVER_ASSETS,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=3)},
    tags=["gold", "dbt"],
) as dag:
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT_CMD} snapshot --profiles-dir {DBT_PROJECT_DIR}",
    )
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT_CMD} run --profiles-dir {DBT_PROJECT_DIR}",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_CMD} test --profiles-dir {DBT_PROJECT_DIR}",
    )
    check_gold_row_counts = SQLCheckOperator(
        task_id="check_gold_row_counts",
        conn_id="postgres_iot",
        sql="SELECT (SELECT COUNT(*) FROM gold.fact_sales_orders) > 0",
    )

    dbt_snapshot >> dbt_run >> dbt_test >> check_gold_row_counts
