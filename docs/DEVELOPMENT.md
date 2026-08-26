# Development Guide

## Local dev environment (uv)

The root `pyproject.toml` provides a **local-dev convenience** environment (`uv sync`) — not something the
pipeline itself depends on. It exists so you can iterate quickly without rebuilding containers:

```bash
uv sync
source .venv/bin/activate
```

Includes `dbt-core`/`dbt-postgres`/`psycopg2-binary` for fast local `dbt debug`/`dbt run` iteration against
Postgres's exposed `localhost:5432`, plus `pytest`. **The actual pipeline never runs anything from here** —
`gold_dbt_dag.py`'s `BashOperator` tasks run `dbt` inside `airflow-scheduler` (which has its own `dbt-core`
install via `config/airflow/Dockerfile`), all Spark jobs run via `spark-submit` inside the Spark containers,
and the generator runs in its own container with its own `pyproject.toml`.

## Running dbt locally against the dockerized Postgres

```bash
cd dbt
export POSTGRES_HOST=localhost POSTGRES_PORT=5432 POSTGRES_DB=iot_database
export PIPELINE_DB_USER=pipeline_rw PIPELINE_DB_PASSWORD=<from .env>
dbt debug
dbt snapshot && dbt run && dbt test
```

Order matters: snapshots must run before `dbt run` (the SCD2 dimensions read from them), and both before
`dbt test`.

## Running a Spark job locally (against the docker cluster)

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.cores.max=2 \
  --conf spark.driver.memory=512m \
  --conf spark.executor.memory=512m \
  /opt/spark-batch-jobs/customers_to_silver.py
```

Every job in this project sets **all three** of these confs explicitly, and all three are load-bearing, not
optional extras:
- `spark.cores.max=2` — caps cores so no single submission can claim the whole cluster (confirmed this happens
  by default otherwise).
- `spark.driver.memory` / `spark.executor.memory` — Spark defaults both to 1g if unset. With only
  1200-1600m total `SPARK_WORKER_MEMORY` per worker, even one or two default-sized jobs running concurrently can
  exhaust a worker's memory entirely, leaving every other job registered with Master but stuck at
  `WAITING`/0 cores forever (`"Initial job has not accepted any resources"` — cores were free, memory wasn't;
  this broke every batch DAG in this project until diagnosed). Also note Spark's hard floor: below ~450MB for
  either setting, the job fails outright with `INVALID_DRIVER_MEMORY`/`INVALID_EXECUTOR_MEMORY` rather than a
  silent bad guess — 512m is a safe, verified value.

For the persistent streaming job specifically, don't submit it manually like this for testing — it already runs
as its own supervised docker-compose service (`spark-streaming-truck-position`); manually launching a second
instance would double-write to the same Kafka consumer group / checkpoint directory.

## `data_generators/config.yaml` — full field reference

The single source of truth for every generator-tunable parameter (secrets stay in `.env`, never here).

Structural/reference data (delivery zones/geography, product categories, weather station locations) lives in
Postgres (`reference.*`/`silver.*`) instead, read once at generator startup - see
[`docs/DATA_SCHEMA.md`](DATA_SCHEMA.md).

| Field | Value | Affects |
|---|---|---|
| `customers.creation_rate_per_minute` | 2 | How fast new customers register. |
| `customers.max_customers` | 500 | Ceiling on total customer-base size, reached gradually via the creation rate above. |
| `customers.verification_delay_seconds` | 3-8 | Delay between account creation and `account_verified`. |
| `customers.attribute_update_rate_per_hour` | 5 | Address/segment-change rate — feeds the customer SCD2 snapshot. |
| `customers.disable_rate_per_hour` | 0.1 | Chance per customer of an `account_disabled` event. |
| `customers.re_enable_rate_per_hour` | 5 | Chance per disabled customer of an `account_enabled` event. |
| `orders.max_concurrent_orders_per_customer` | 3 | Per-customer cap on simultaneously open (non-terminal) purchase orders. |
| `orders.order_arrival_rate_per_minute` | 6 | How fast new purchase orders appear system-wide. |
| `orders.cancellation_probability` | 0.05 | Chance a purchase order gets cancelled by the customer. |
| `orders.max_products_per_po` | 5 | Max distinct products (line items) per purchase order. |
| `orders.max_products_qty_per_po` | 10 | Max quantity ordered per individual line item. |
| `orders.home_delivery_probability` | 0.95 | Chance the delivery zone is the customer's own home city rather than a different zone. |
| `orders.delivery_buffer_days` | 1 | Added on top of travel time when computing `target_delivery_date`. |
| `invoices.due_minutes` | 1 (test value) | Time to settle before goods can be loaded onto a truck. |
| `invoices.non_payment_probability` | 0.10 | Chance an invoice isn't paid by its due time. |
| `invoices.max_reminders` | 3 | Reminders before the linked purchase order is cancelled. |
| `dispatch.min_batch_size` | 5 | Min paid orders queued in a zone before dispatching a truck. |
| `dispatch.max_wait_minutes` | 3 (test value) | Max time an order waits on-hold before dispatch anyway. |
| `trucks.fleet_size` | 20 | Number of trucks in the fleet. |
| `trucks.capacity_products` | 500 | Max product units (summed across line items) per truck run. |
| `trucks.avg_speed_kmh` | 80 | Drives simulated in-transit leg timing. |
| `trucks.position_ping_interval_seconds` | 15 | Controls live-map update frequency — the one setting that directly affects the streaming job's load. |
| `trucks.load_seconds_per_order` | 10 | Simulated time to load one order onto a truck. |
| `products.num_products` | 100 | Fixed warehouse catalog size, seeded once at generator startup. |
| `products.nominal_capacity_min`/`nominal_capacity_max` | 10 / 50 (test values) | Range each product's own `nominal_capacity` is generated within at seed time. |
| `products.restock_threshold_pct` | 20 | Refill trigger: `restock_required` when `qty` < this % of `nominal_capacity` (computed in `products_to_silver.py`). |
| `products.restock_target_pct` | 100 | A refill (`restock_check.py`) brings stock back up to this % of `nominal_capacity`. |
| `products.refill_discount_pct` | 40 | Refill unit price is this % below the listed sale price. |
| `products.attribute_update_rate_per_hour` | 2 | Rate of price/category-change events — feeds the product SCD2 snapshot. |
| `weather.poll_interval_hours` | 1 | How often `weather_enrichment_dag` pulls from OpenWeatherMap (station locations come from `reference.weather_stations`, not this file). |
| `kafka.topics.*` | see file | Topic name mapping — change only if you also update every job that reads/writes that topic. |
| `kafka.partitions_per_topic`/`replication_factor` | 2 / 1 | Kafka topic creation parameters. |

## Adding a new domain

Following the established pattern (using an existing domain as a template, e.g. `invoices`):

1. **Schema**: add the bronze table (`config/postgres/initdb/01-init-schema.sql`) — decide hypertable vs. plain
   table based on whether it's genuinely time-series. Add a `silver.<domain>_current` table + a matching
   `silver.error_<domain>` table. Grant `pipeline_rw` `INSERT` (not `UPDATE`) on the new bronze table in
   `03-create-role-and-grants.sh`.
2. **Generator**: add `data_generators/generators/<domain>.py`, wire it into `main.py`'s tick loop.
3. **Ingestion**: decide streaming vs. periodic batch (see [`docs/ARCHITECTURE.md`](ARCHITECTURE.md) for the
   reasoning that governs this choice — only genuinely sub-minute-freshness domains justify a persistent
   streaming job). For periodic batch, add `spark-batch-jobs/bronze_ingestion/ingest_<domain>_events.py`
   calling the shared `run_ingestion()` helper, and a `bronze_ingest_<domain>_events_dag.py` in `airflow/dags/`
   built from the shared `common/spark_ingestion_dag_factory.py` (see step 5 for the two things every caller
   of that factory must do).
4. **Silver**: add `spark-batch-jobs/silver_processing/<domain>_to_silver.py`, following the watermark-read →
   validate → upsert → watermark-write pattern in `shared_utils.py`. Notes that apply to every new job here:
   - Build the DataFrame with an explicit `StructType` (`spark.createDataFrame([Row(...)], schema=SCHEMA)`)
     rather than relying on inference - a batch where every row happens to have `NULL` in some nullable column
     fails inference outright with `[CANNOT_DETERMINE_TYPE]`.
   - Cast any Postgres `DECIMAL`/`NUMERIC` column to `float8` in the SQL query itself if the schema declares it
     `DoubleType()` - psycopg2 returns `DECIMAL` as `decimal.Decimal`, which PySpark's `DoubleType` verifier
     rejects outright.
   - If a `*_current` table has a `created_at NOT NULL` column derived via `coalesce_cols` from a one-time
     `created`/`status='created'` bronze event, add a fallback (earliest `event_at` for that key in the batch)
     for the case where that one-time row never made it into bronze - otherwise the first-ever insert for that
     key has no source for `created_at` at all, fails the NOT NULL constraint, and - since a failed batch never
     advances the watermark - permanently blocks the job on every future run.
   - When comparing a Spark row's UUID-typed column (a plain Python `str`) against a set of IDs fetched
     straight from Postgres via `fetch_incremental()`, cast those fetched values to `str()` - psycopg2 returns
     a Postgres `UUID` column as a `uuid.UUID` object, which is never equal to a `str` even for the same value.
   - If the job passes a Python list as a `psycopg2` parameter against a `uuid` column (e.g.
     `WHERE order_id = ANY(%s)`), cast it explicitly - `WHERE order_id = ANY(%s::uuid[])` - psycopg2 can't
     infer the array element type on its own and Postgres rejects the resulting `uuid = text` comparison.
   - `route_errors()` expects `rows: list[tuple]` of `(json.dumps(...), reason_code)` pairs, not a list of dicts.
5. **Airflow**: for a new silver domain, add a DAG (or a task in an existing one) with a `ShortCircuitOperator`
   (skip if no new bronze data) → `SparkSubmitOperator` → SQL-check → `outlets=[Asset(...)]`. For a new bronze
   domain, use the shared factory (`common/spark_ingestion_dag_factory.py`) rather than hand-writing a `with
   DAG(...)` block - every caller must pass `fileloc=__file__` (otherwise the DAG is attributed to the factory
   file, not the caller, and silently never registers) and must itself `from airflow import DAG` for a
   `dag: DAG = make_bronze_ingestion_dag(...)` type annotation (Airflow's DAG-discovery heuristic only parses a
   file that contains the literal text "airflow", which a file only importing from `common.*` never does - see
   [`docs/AIRFLOW3_PROJECT.md`](AIRFLOW3_PROJECT.md) §4). The `SparkSubmitOperator`'s `conf` must set **all
   three** of `spark.cores.max`, `spark.driver.memory`, and `spark.executor.memory` explicitly (see "Running a
   Spark job locally" above for why the memory settings are just as load-bearing as the cores cap, not
   optional) — copy the values from any existing DAG (`512m`/`512m`/`2` cores is the established default.
   `spark.driver.host="airflow-scheduler"` is also required so the driver's SparkUI link resolves correctly
   from a host browser — see [`docs/OBSERVABILITY.md`](OBSERVABILITY.md)). New DAG schedules should read from
   `common/pipeline_config.py`'s shared constants, not a hardcoded `timedelta(minutes=N)`.
6. **Gold**: add a `dbt` staging model, and either extend an existing fact/dimension or add a new one.
7. **Tests**: add schema tests to the relevant `.yml`, and a custom test if there's a business rule to enforce.

## Testing expectations

- Every Spark job file should be `python3 -m py_compile`-clean before assuming it's correct — this catches
  real syntax errors cheaply, but **does not** catch runtime issues (missing env vars, permission errors,
  Postgres constraint violations, PySpark schema/type mismatches) - these only surface by actually running the
  job against the live cluster.
- dbt models should pass `dbt parse` (no live DB needed) and, ideally, `dbt run`/`dbt test` against a real
  database before considering a change done.
- Airflow DAGs should show zero import errors (`airflow dags list-import-errors`) **and** actually appear in
  `airflow dags list` - a DAG that fails the discovery heuristic in step 5 above parses with zero errors yet
  never registers, so an import-error check alone isn't sufficient; confirm the `dag_id` shows up.
