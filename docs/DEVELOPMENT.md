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

| Field | Default | Affects |
|---|---|---|
| `warehouse.origin_lat`/`origin_lon` | Biel, CH | Fixed dispatch origin for every shipment. |
| `products.num_products` | 100 | Initial catalog size (seeded as `created` events at generator startup). |
| `products.initial_stock_min`/`initial_stock_max` | 200 / 1000 | Random range for each product's starting stock. |
| `products.restock_threshold_pct` | 20 | Below this % of `initial_stock`, `restock_check.py` triggers a replenishment. |
| `products.restock_target_pct` | 100 | Restock brings the product back up to this % of `initial_stock`. |
| `products.attribute_update_rate_per_hour` | 2 | Rate of price/category-change events — feeds the product SCD2 snapshot. Only affects periodic-batch ingestion load, not the streaming job. |
| `customers.num_customers` | 500 | Initial customer pool size. |
| `customers.attribute_update_rate_per_hour` | 5 | Address/segment-change rate — feeds the customer SCD2 snapshot. |
| `orders.target_concurrent_orders` | 50 | Ceiling on simultaneously-open orders the generator will maintain. |
| `orders.order_arrival_rate_per_minute` | 6 | How fast new orders appear — increases load on the periodic batch ingestion jobs, **not** the streaming job. |
| `orders.cancellation_probability` | 0.05 | Fraction of pending orders that get cancelled instead of progressing. |
| `payments.failure_probability` | 0.02 | Fraction of payments that fail instead of capturing. |
| `payments.processing_delay_seconds` | 5-60 | Simulated delay between authorization and capture/failure. |
| `trucks.num_trucks` | 12 | **Hard ceiling on concurrent `in_transit` shipments** — this is the one setting that directly affects the streaming job's load (bounds `truck_position_events` volume to `num_trucks × ping frequency`, regardless of order volume). |
| `trucks.avg_speed_kmh` | 80 | Drives the simulated `time_to_destination` (straight-line interpolation, no real routing). |
| `trucks.position_ping_interval_seconds` | 15 | Controls how often the live map updates — the other setting that directly affects streaming-job load. |
| `zones.<CH\|FR\|DE\|IT>` | see file | Bounding boxes for random destination selection per country. |
| `weather.locations` | 5 fixed cities | Consumed by `weather_enrichment_dag`, not the generator — kept in this file since it's the same kind of scenario-sizing parameter. |
| `weather.poll_interval_hours` | 1 | How often `weather_enrichment_dag` pulls from OpenWeatherMap. |
| `kafka.topics.*` | see file | Topic name mapping — change only if you also update every job that reads/writes that topic. |

## Adding a new domain

Following the established pattern (using an existing domain as a template, e.g. `payments`):

1. **Schema**: add the bronze table (`config/postgres/initdb/01-init-schema.sql`) — decide hypertable vs. plain
   table based on whether it's genuinely time-series. Add a `silver.<domain>_current` table + a matching
   `silver.error_<domain>` table. Grant `pipeline_rw` `INSERT` (not `UPDATE`) on the new bronze table in
   `02-create-role-and-grants.sh`.
2. **Generator**: add `data_generators/generators/<domain>.py`, wire it into `main.py`'s tick loop.
3. **Ingestion**: decide streaming vs. periodic batch (see [`docs/ARCHITECTURE.md`](ARCHITECTURE.md) for the
   reasoning that governs this choice — only genuinely sub-minute-freshness domains justify a persistent
   streaming job). For periodic batch, add a thin wrapper in `spark-batch-jobs/bronze_ingestion/` calling the
   shared `run_ingestion()` helper.
4. **Silver**: add `spark-batch-jobs/<domain>_to_silver.py`, following the watermark-read → validate → upsert
   → watermark-write pattern in `shared_utils.py`. If the job builds its DataFrame with
   `spark.createDataFrame([Row(...)])` (the pattern most existing `*_to_silver.py` jobs use), define an explicit
   `StructType` rather than relying on inference — if every row in one incremental batch happens to have `NULL`
   in some nullable source column (e.g. a batch of only "attribute updated" events that don't touch every
   field), inference fails outright with `[CANNOT_DETERMINE_TYPE]` rather than just guessing wrong. Confirmed by
   testing; fixed this way in `products_to_silver.py`, still a latent gap in the older `*_to_silver.py` jobs
   (deferred, not forgotten). Separately: if the job passes a Python list as a `psycopg2` parameter against a
   `uuid` column (e.g. `WHERE order_id = ANY(%s)`), cast it explicitly - `WHERE order_id = ANY(%s::uuid[])` -
   psycopg2 can't infer the array element type on its own and Postgres rejects the resulting `uuid = text`
   comparison (confirmed by testing, hit twice - `truck_positions_to_silver.py` and `inventory_to_silver.py`).
5. **Airflow**: add a DAG (or a task in an existing one) with a `ShortCircuitOperator` (skip if no new bronze
   data) → `SparkSubmitOperator` → SQL-check → `outlets=[Asset(...)]`. The `SparkSubmitOperator`'s `conf` must
   set **all three** of `spark.cores.max`, `spark.driver.memory`, and `spark.executor.memory` explicitly (see
   "Running a Spark job locally" above for why the memory settings are just as load-bearing as the cores cap,
   not optional) — copy the values from any existing DAG (`512m`/`512m`/`2` cores is the established default.
   `spark.driver.host="airflow-scheduler"` is also required so the driver's SparkUI link resolves correctly
   from a host browser — see [`docs/OBSERVABILITY.md`](OBSERVABILITY.md)).
6. **Gold**: add a `dbt` staging model, and either extend an existing fact/dimension or add a new one.
7. **Tests**: add schema tests to the relevant `.yml`, and a custom test if there's a business rule to enforce.

## Testing expectations

- Every Spark job file should be `python3 -m py_compile`-clean before assuming it's correct — this catches
  real syntax errors cheaply, but **does not** catch runtime issues (missing env vars, permission errors,
  Postgres constraint violations) — several real bugs in this project were only caught by actually running
  jobs against the live cluster, not by syntax-checking alone.
- dbt models should pass `dbt parse` (no live DB needed) and, ideally, `dbt run`/`dbt test` against a real
  database before considering a change done.
- Airflow DAGs should show zero import errors — `airflow dags list-import-errors`, or more directly, execute
  each file as a plain Python module inside the built Airflow image.
