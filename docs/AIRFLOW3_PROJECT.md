# Airflow 3 — This Project's Setup

How Airflow 3 is wired up and used in *this* pipeline. For general Airflow 3 concepts (TaskFlow, executors,
trigger rules, Assets) not specific to this repo, see
[`docs/AIRFLOW3_BACKGROUND_OVERVIEW.md`](AIRFLOW3_BACKGROUND_OVERVIEW.md) first. For the bronze/silver/gold
table definitions referenced throughout, see [`docs/DATA_SCHEMA.md`](DATA_SCHEMA.md). For the gold dbt layer
that `gold_dbt_dag.py` invokes, see [`docs/DBT_MODEL.md`](DBT_MODEL.md). For overall system design, see
[`docs/ARCHITECTURE.md`](ARCHITECTURE.md).

## 1. Where Airflow sits in the pipeline

```
Kafka → Airflow-scheduled Spark batch (bronze ingestion) → iot.* (bronze, immutable)
                                                                │
                                    Airflow-scheduled Spark batch (bronze→silver)
                                                                ▼
                                                      silver.* (validated, current-state)
                                                                │
                                              Airflow-scheduled dbt (silver→gold)
                                                                ▼
                                                      gold.* (star schema)
```

Airflow owns orchestration, not computation: it decides when something runs and whether it succeeded, then
hands the work to Spark (`SparkSubmitOperator`), dbt (`BashOperator` calling the `dbt` CLI), or a plain
Python/SQL check. The one exception is truck-position ingestion — a persistent Spark Structured Streaming job
running as its own docker-compose service, supervised by Docker (`restart: unless-stopped`); Airflow only
monitors it (`bronze_streaming_supervisor_dag.py`), it doesn't submit it. See `docs/ARCHITECTURE.md` for why.

## 2. The 5 Airflow containers

| Container | Role |
|---|---|
| `airflow-postgres` | Airflow's own metadata DB (DAG state, task history, connections) — separate from the pipeline's own TimescaleDB/PostGIS `postgres`. |
| `airflow-init` | One-shot: runs migrations, creates the admin user, generates the Simple Auth Manager password file. |
| `airflow-api-server` | Serves the UI and the Execution API (`/execution/`) that task processes talk to via the Task SDK. |
| `airflow-scheduler` | Decides what's ready to run, and — since this project uses `LocalExecutor` — also runs every task as a subprocess inside itself. Every client-mode `SparkSubmitOperator`'s driver JVM boots here. |
| `airflow-dag-processor` | Its own mandatory component in Airflow 3. Parses every `.py` file under `airflow/dags/`, builds the DAG objects, and writes their serialized form to the metadata DB. |

`AIRFLOW__CORE__EXECUTOR: LocalExecutor` — no Celery/Redis, no Kubernetes; tasks run as local subprocesses.
`AIRFLOW__CORE__PARALLELISM: 3` caps how many task instances can run concurrently across the whole instance
(every DAG combined) — see §9.

## 3. `airflow/dags/` structure

```
airflow/dags/
├── bronze_streaming_supervisor_dag.py  # monitoring-only, check on Spark streaming job (truck-position) running outside airflow
├── bronze_ingest_customer_events_dag.py        \
├── bronze_ingest_product_events_dag.py          |  7 thin per-domain files, one per
├── bronze_ingest_purchase_order_events_dag.py   |  Kafka topic - each just calls the
├── bronze_ingest_product_on_order_events_dag.py |  shared factory below with its own
├── bronze_ingest_invoice_events_dag.py          |  (dag_id, script, description, outlet)
├── bronze_ingest_truck_fleet_events_dag.py       |
├── bronze_ingest_inventory_changes_dag.py       /
├── silver_customers_products_dag.py    # §5
├── silver_purchase_orders_dag.py       # §5
├── silver_invoices_dag.py              # §5
├── silver_inventory_dag.py             # §5
├── silver_truck_positions_dag.py       # §5
├── gold_dbt_dag.py                     # dbt deps -> run(staging) -> snapshot -> run -> test -> row-count check
├── weather_enrichment_dag.py           # PythonOperator, no Spark - just a REST API poll
└── common/
    ├── spark_ingestion_dag_factory.py  # shared builder for the 7 bronze_ingest_*.py files (§4)
    ├── pipeline_config.py              # schedule constants, read from config/airflow/airflow_config.yml (§6)
    ├── assets.py                       # every Asset object, one per bronze/silver domain (§7)
    ├── dq_checks.py                    # has_new_bronze_data() - the ShortCircuitOperator gate every silver DAG uses
    ├── pg.py                           # tiny psycopg2 connection helper shared by dq_checks.py
    └── weather_client.py               # OpenWeatherMap REST client used by weather_enrichment_dag.py
```

## 4. The bronze-ingestion DAG factory

Each bronze domain gets its own DAG (rather than one DAG with 7 parallel tasks) so a slow/failing domain
doesn't share a DAG run's scheduling slot (`max_active_runs=1`) with the others, and each shows up as its own
row in the UI, mapping 1:1 onto its Kafka topic. Since the 7 files are otherwise identical,
`common/spark_ingestion_dag_factory.py` holds the shared construction:

```python
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
            application=f"/opt/spark-batch-jobs/bronze_ingestion/{script}",     <== per-Domain script
            name=dag_id,
            conn_id="spark_default",
            deploy_mode="client",
            conf={
                ...
            },
            outlets=[outlet],
        )
    return dag
```

Each per-domain file is then just a call site:

```python
from airflow import DAG
from common.assets import BRONZE_CUSTOMER_EVENTS
from common.spark_ingestion_dag_factory import make_bronze_ingestion_dag

dag: DAG = make_bronze_ingestion_dag(
    dag_id="bronze_ingest_customer_events_dag",
    script="ingest_customer_events.py",
    description="Kafka iot.customer_events -> bronze",
    outlet=BRONZE_CUSTOMER_EVENTS,
    fileloc=__file__,
)
```

Two details every caller needs, that a hand-written `with DAG(...)` block wouldn't:

- **`fileloc=__file__`.** `DAG.__init__` infers `fileloc` from the caller's stack frame, which would otherwise
  point at `spark_ingestion_dag_factory.py` (where the literal `DAG(...)` call lives) rather than the actual
  per-domain file — the standard fix for any DAG-factory pattern.
- **`from airflow import DAG`, used as the type annotation on `dag: DAG = ...`.** Airflow's DAG-discovery
  heuristic (`airflow.utils.file.might_contain_dag_via_default_heuristic`) only attempts to parse a `.py` file
  as a DAG if its raw text contains the literal substring `"airflow"` and either `"dag"` or `"asset"`. This
  check runs unconditionally in the dag-processor's actual parsing subprocess — `AIRFLOW__CORE__DAG_DISCOVERY_SAFE_MODE`
  does not disable it there, since `airflow/dag_processing/processor.py`'s `_parse_file()` hardcodes
  `safe_mode=True` regardless of that config value. A file that only imports from `common.*` never contains
  the word "airflow" and would be silently skipped, with no import error anywhere. The type annotation makes
  the import genuinely meaningful rather than a bare unused import.

The 7 bronze domains, their Kafka topics, and bronze table shapes are listed in
[`docs/DATA_SCHEMA.md`](DATA_SCHEMA.md#bronze-iot-schema).

## 5. The silver DAGs

Each silver DAG reads its domain's bronze table, validates/deduplicates it via a Spark job in
`spark-batch-jobs/silver_processing/`, and upserts into the corresponding `silver.*_current` table (see
[`docs/DATA_SCHEMA.md`](DATA_SCHEMA.md#silver-silver-schema) for exact columns). All five share the same
`ShortCircuitOperator → SparkSubmitOperator → SQLThresholdCheckOperator` shape from `common/dq_checks.py`,
gated by `control.silver_watermarks` so a quiet domain doesn't spin up a Spark JVM for nothing.

| DAG | Spark job(s) | Silver table(s) | Notes |
|---|---|---|---|
| `silver_customers_products_dag` | `customers_to_silver.py`, `products_to_silver.py`, `truck_fleet_to_silver.py` | `silver.customers_current`, `silver.products_current`, `silver.truck_fleet_current` | 3 independent parallel tasks — grouped into one DAG since all three are low-frequency/near-static reference domains, not because they're related to each other. |
| `silver_purchase_orders_dag` | `purchase_orders_to_silver.py`, `product_on_orders_to_silver.py` | `silver.purchase_orders_current`, `silver.product_on_orders_current` | The two jobs are sequenced, not parallel: line items' orphan check reads the purchase-orders table the first job writes. |
| `silver_invoices_dag` | `invoices_to_silver.py` | `silver.invoices_current` | Validates the invoice status/reminder lifecycle. |
| `silver_inventory_dag` | `inventory_to_silver.py`, then `restock_check.py` | `silver.inventory_current`, `silver.inventory_history` | `restock_check.py` runs as a `BashOperator` (plain psycopg2, no Spark) immediately after the silver upsert — it reads `silver.products_current.restock_required` and emits a `refill` bronze event when needed, closing the loop back into bronze. |
| `silver_truck_positions_dag` | `truck_positions_to_silver.py` | `silver.truck_current_position` | Runs faster (2 min) than the other silver DAGs, since bronze here is populated continuously by the streaming job, not on a batch cadence. **Not** the live map's data path — the streaming job upserts `silver.truck_current_position` directly; this DAG only adds validation/error-routing on top. |

`silver_purchase_orders_dag`'s two-job sequencing needs one non-default `trigger_rule` to behave correctly —
see §8.

## 6. Schedule cadences, parametrized in one place

Bronze ingestion runs every 5 minutes; silver processing every 3 minutes (deliberately faster than bronze, so
a bronze batch is usually picked up on silver's very next tick); truck positions every 2 minutes. These three
numbers live in one file, `config/airflow/airflow_config.yml`:

```yaml
bronze_ingestion_schedule_minutes: 5
silver_processing_schedule_minutes: 3
truck_positions_schedule_minutes: 2
```

`common/pipeline_config.py` reads this file at import time and exposes each as a `timedelta`
(`BRONZE_INGESTION_SCHEDULE`, `SILVER_PROCESSING_SCHEDULE`, `TRUCK_POSITIONS_SCHEDULE`), which every DAG file
imports rather than hardcoding its own `timedelta(minutes=N)`.

A DAG's `schedule=` is evaluated at parse time, not task-execution time — so both `airflow-scheduler` and
`airflow-dag-processor` mount `config/airflow/airflow_config.yml` (`docker-compose.yml`). This is different
from `weather_enrichment_dag.py`'s own config/DB reads, which are deferred into its task callable specifically
so only `airflow-scheduler` (which executes tasks) needs that dependency — a schedule interval has no task
callable to defer into.

## 7. Assets: cross-domain lineage

`common/assets.py` defines one `Asset` per domain — 8 silver-level (`SILVER_CUSTOMERS`,
`SILVER_PURCHASE_ORDERS`, `SILVER_PRODUCT_ON_ORDERS`, `SILVER_INVOICES`, `SILVER_INVENTORY`,
`SILVER_TRUCK_FLEET`, `SILVER_TRUCK_POSITIONS`, `SILVER_PRODUCTS`) and 7 bronze-level (one per Kafka topic).
Every bronze/silver `SparkSubmitOperator` task declares `outlets=[...]` for the Asset it produces.

`gold_dbt_dag.py` is the one DAG scheduled off Assets rather than a timedelta:

```python
schedule=GOLD_TRIGGER_ASSETS   # 7 of the 8 silver assets - AND semantics, not "any"
```

`GOLD_TRIGGER_ASSETS` deliberately excludes `SILVER_TRUCK_FLEET`: that asset only ever fires once, at the
one-time fleet seed, then never again in steady state (the fleet is near-static, so its own silver job's
`ShortCircuitOperator` legitimately skips every later cycle). Including it in the AND condition would mean
`gold_dbt_dag` fires exactly once, ever — `dim_truck` still gets rebuilt on every gold run regardless, since
`dbt run` rebuilds every model, not just the domain that triggered the run; it just isn't part of what
triggers it.

This fires gold's dbt run only once every trigger domain has updated since gold's last run, not on any
individual domain's own cadence — avoiding a gold rebuild from a partial, inconsistent slice of silver. Gold
therefore refreshes on the cadence of whichever trigger domain is slowest to update, bounded above by the
3-minute silver cadence. See [`docs/DBT_MODEL.md`](DBT_MODEL.md) for what `gold_dbt_dag.py` actually builds.

The 7 bronze-level Assets aren't consumed by any DAG's `schedule=` yet — every silver DAG still gates on a
plain `timedelta` plus the `ShortCircuitOperator` from §5/§8. Declaring the bronze outlets now means
Asset-driven silver triggering, if wanted later, is a schedule-line change rather than a bronze-DAG rewrite.

## 8. Operator mix, and the two recurring gates

This project mixes operator types deliberately: `SparkSubmitOperator` (every bronze/silver Spark job),
`BashOperator` (dbt CLI calls, `restock_check.py`), `PythonOperator` (`weather_enrichment_dag.py`, the
streaming supervisor), `ShortCircuitOperator` (every silver DAG's "is there new data" gate),
`SQLThresholdCheckOperator`/`SQLCheckOperator` (error-rate and row-count checks).

Two gates recur across the silver DAGs, both in `common/dq_checks.py`:

- **`has_new_bronze_data(source_table, watermark_key)`** — a `ShortCircuitOperator` callable comparing
  `control.silver_watermarks` against the bronze table's `ingested_at`. Skips the Spark submission entirely
  when there's nothing new.
- **`error_count_since(error_table, minutes)`** — feeds the `SQLThresholdCheckOperator` tasks that alert when
  a domain's `silver.error_*` table is filling up faster than expected.

`silver_purchase_orders_dag.py`'s two sequenced jobs (§5) need an asymmetric dependency: its second task,
`product_on_orders_to_silver`, has `trigger_rule="none_failed"` and depends on both its own `check_line_items`
gate (normal short-circuit behavior — skips it when there are no new line items) and `purchase_orders_to_silver`
directly (whose own skip, on a cycle with no new orders, must *not* block line-item processing).

## 9. Concurrency model

`AIRFLOW__CORE__PARALLELISM: 3` is matched to Spark's own capacity: `SPARK_WORKER_CORES=4/4` (8 cores total)
minus the streaming job's permanent 2 leaves 6 for batch, and every batch job requests `spark.cores.max=2` —
so at most 3 can hold executors at once, regardless of how many DAGs are scheduled.

With 7 bronze DAGs sharing a 5-minute schedule and ~9 silver Spark tasks across 5 DAGs sharing a 3-minute
schedule, all synchronized on the same `start_date`, every 15 minutes (when a 5-min and 3-min tick coincide)
up to ~16 tasks can become ready at once, competing for those 3 global slots. Whether this drains within the
3-minute silver budget under real concurrent load is a live capacity question for this deployment, not
something the schedule numbers alone guarantee. See [`docs/DEPLOYMENT.md`](DEPLOYMENT.md) for container
resource budgets.

## 10. Connections and credentials

- **`spark_default`** (`AIRFLOW_CONN_SPARK_DEFAULT`): every `SparkSubmitOperator` submits through this, pointed
  at `spark://spark-master:7077`.
- **`postgres_iot`** (`AIRFLOW_CONN_POSTGRES_IOT`): used by every `SQLThresholdCheckOperator`/`SQLCheckOperator`
  task, connects as `pipeline_rw` — the same least-privilege role every Spark job and dbt use (see
  [`docs/DATA_SCHEMA.md`](DATA_SCHEMA.md) for role grants).
- Plain env vars (`POSTGRES_HOST`/`PORT`/`DB`, `PIPELINE_DB_USER`/`PASSWORD`) are also injected into
  `airflow-scheduler` directly, for the psycopg2-based Python code that runs there (client-mode Spark driver
  JVMs, `common/dq_checks.py`, `weather_enrichment_dag.py`, `restock_check.py`).

## 11. Data interface summary

**Inputs** (read-only): `iot.*` (bronze, watermark-based reads in every silver DAG's `ShortCircuitOperator`),
`silver.*` (watermarks + error-rate checks), `control.silver_watermarks`, `control.kafka_offsets`,
`reference.weather_stations` (only by `weather_enrichment_dag.py`).

**Outputs**: Airflow orchestrates writers (Spark, dbt, `restock_check.py`) rather than writing pipeline data
itself, with one exception — `weather_enrichment_dag.py`'s `PythonOperator` writes directly to
`iot.weather_observations` via psycopg2, since that domain has no Spark job.

**Credentials**: `pipeline_rw`, same least-privilege role every other tool in this pipeline connects as.

**Metadata**: Airflow's own DAG/task-instance/log state lives entirely in `airflow-postgres`, a separate
database from the pipeline's own data.

## 12. Docker compose parameters

### airflow-init service

- AIRFLOW__CORE__DAG_DISCOVERY_SAFE_MODE
      # Airflow's DAG-discovery "safe mode" pre-filters dags/ files: a file is
      # only parsed as a possible DAG if its raw text contains the literal
      # substring "airflow" and either "dag" or "asset" (case-insensitive) -
      # a fast-path heuristic for large codebases with many non-DAG .py files
      # mixed in (see airflow.utils.file.might_contain_dag_via_default_heuristic).
      # Set here for completeness/other DagBag call sites, but note this does
      # NOT reach the dag-processor's actual per-file parsing subprocess in
      # Airflow 3.0.3 - airflow/dag_processing/processor.py's _parse_file()
      # hardcodes safe_mode=True rather than reading this config value, so
      # every DAG file's own source must independently satisfy the heuristic
      # regardless of this setting (see airflow/dags/common/spark_ingestion_dag_factory.py
      # for how the per-domain bronze-ingestion files do this).

- AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "false"
      # Airflow's own default is to create every newly-discovered DAG paused,
      # requiring a manual unpause per DAG. This project wants every DAG
      # running the moment Airflow starts, so every new DAG is created
      # already unpaused instead. Only affects DAGs at the moment they're
      # first registered in the metadata DB - an already-registered DAG's
      # paused/unpaused state is a separate, persisted toggle this setting
      # doesn't touch retroactively.

- AIRFLOW__CORE__EXECUTION_API_SERVER_URL: "http://airflow-api-server:8080/execution/"      
  # Every task runs in a subprocess that talks to this URL via the Task SDK
      # (Airflow 3's "Execution API"), served by airflow-api-server at /execution/.
      # Left unset, it defaults to localhost - unreachable from airflow-scheduler's
      # own container, so every single task instance fails immediately with
      # httpx.ConnectError before ever really starting (confirmed by testing).
      
-       AIRFLOW__API__SECRET_KEY: ${AIRFLOW_API_SECRET_KEY:?AIRFLOW_API_SECRET_KEY must be set in .env}
      # Must match across every component - the scheduler signs each task's
      # internal Execution API JWT with these, and airflow-api-server verifies
      # it. Left unset, each container generates its own random value and every
      # task fails with 403 Forbidden (confirmed by testing).

- AIRFLOW_CONN_SPARK_DEFAULT: "spark://spark%3A%2F%2Fspark-master:7077"
      # SparkSubmitHook builds --master as "{conn.host}:{conn.port}" - it does NOT
      # prepend the connection's conn_type/scheme itself. So the scheme has to be
      # double-encoded into the host segment (spark%3A%2F%2Fspark-master decodes
      # to host="spark://spark-master"), or every spark-submit gets invoked with
      # "--master spark-master:7077" (missing scheme) and fails to parse the
      # master URL entirely - confirmed by testing.

- POSTGRES_HOST: postgres
      # For client-mode SparkSubmitOperator jobs, whose driver runs inside
      # airflow-scheduler itself - the psycopg2-based batch scripts read
      # these plain env vars directly, same convention as the Spark nodes.

### airflow-api-server service

- command: airflow api-server --workers 1
    # --workers 1: confirmed by testing that both the default (4) and even 2
    # workers crash-loop under this container's memory budget ("Child
    # process died" repeatedly); --workers 1 ran clean and stayed up. Fine
    # for this demo's UI concurrency (single user). mem_limit bumped
    # 512m->768m for headroom since even 1 worker sat close to 512m.

    