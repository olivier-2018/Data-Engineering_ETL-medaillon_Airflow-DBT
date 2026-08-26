"""§10: silver -> gold via dbt. schedule=GOLD_TRIGGER_ASSETS uses Airflow 3's
AND-semantics for a list of assets - this DAG only fires once every listed
silver domain has produced at least one update since its last run, avoiding
a gold rebuild from a partial/inconsistent set of silver domains. Net
effect: gold refreshes roughly on the cadence of the slowest triggering
silver domain, bounded above by the 3-minute silver_processing_schedule_minutes
cadence (see common/pipeline_config.py).

GOLD_TRIGGER_ASSETS is 7 of the 8 silver domains, not all 8 - it excludes
silver://truck_fleet_current (common/assets.py has the full reasoning):
that asset only ever fires once, at the one-time fleet seed, then never
again in steady state, since the fleet is near-static. Including it in an
AND-condition would mean this DAG fires exactly once, ever.

dbt invoked via plain BashOperator (not astronomer-cosmos) per the explicit
decision to keep dbt's own mechanics visible while learning it. snapshot/run/
test are kept as separate tasks (not one `dbt build`) for clearer failure
isolation.

dbt_deps runs first: `dbt/dbt_packages` (dbt_utils) is not checked into git and is wiped by
`scripts/reset.sh` along with every other regenerable artifact, so a fresh stack has no
installed packages until something runs `dbt deps`. `dbt deps` is itself idempotent/fast
once packages are already present, so running it every cycle is a non-issue.

dbt_run_staging runs next, before dbt_snapshot: customer_snapshot/product_snapshot/etc.
ref() the staging views, which don't exist yet on a schema where `dbt run` has never been
called - dbt_snapshot fails outright against those. Building staging first makes every
cycle - including the very first one ever, or one following a full reset - bootstrap-safe
without a special first-run case. The extra `dbt run --select staging` is cheap (pure
`CREATE VIEW`, not a table rebuild)."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from airflow.providers.standard.operators.bash import BashOperator

from common.assets import GOLD_TRIGGER_ASSETS

DBT_PROJECT_DIR = "/opt/dbt"
DBT_CMD = f"cd {DBT_PROJECT_DIR} && dbt"

with DAG(
    dag_id="gold_dbt_dag",
    description="silver -> gold star schema via dbt, triggered once the trigger silver domains have updated (§9, §10)",
    schedule=GOLD_TRIGGER_ASSETS,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=3)},
    tags=["gold", "dbt"],
) as dag:
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT_CMD} deps --profiles-dir {DBT_PROJECT_DIR}",
    )
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_CMD} run --select staging --profiles-dir {DBT_PROJECT_DIR}",
    )
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
        sql="SELECT (SELECT COUNT(*) FROM gold.fact_purchase_orders) > 0",
    )

    dbt_deps >> dbt_run_staging >> dbt_snapshot >> dbt_run >> dbt_test >> check_gold_row_counts
