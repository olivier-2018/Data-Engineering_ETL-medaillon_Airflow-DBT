# Architecture

## Overview

This is a medallion (bronze/silver/gold) pipeline for a simulated logistics scenario: a resale warehouse in
Biel, Switzerland, selling 100 products online with delivery across Switzerland, France, Germany, and Italy.

A synthetic data generator plays the role of "the real world" (orders arriving, trucks moving, payments
processing) to abstract away both IoT messages and MQTT broker but provide the pipeline with realistic data 
to ingest without needing real IoT hardware. 

## Component diagram

```
┌──────────────────┐
│  data-generator  │  (Python, confluent-kafka) — orders/payments/inventory/customers/
└────────┬─────────┘   products/truck-position lifecycle simulation
         │ publishes JSON
         ▼
┌──────────────────┐
│      Kafka       │  (KRaft mode, single broker, 6 topics) ◄── Kafka UI (browse/inspect)
└────────┬─────────┘
         │
    ┌────┴──────────────────────────────────┐
    │  Own service                          │ Airflow-scheduled
    ▼                                       ▼
┌────────────────────────────────┐   ┌────────────────────────────────────┐
| Spark Structured Streaming     |   | Spark incremental batches          |
| Folder:                        │   │ Folder:                            │
|  spark-streaming-jobs          |   | ./spark-batch-jobs/bronze_ingestion|
└──────────────┬─────────────────┘   └────────┬───────────────────────────┘
               │                              │
               ▼                              ▼
     ┌──────────────────────────────────────────────┐
     │   iot.* (bronze) — TimescaleDB hypertables   │
     │   immutable, append-only event logs          │
     └──────────────────┬───────────────────────────┘
                        │ Spark incremental batch (watermarked, Airflow-scheduled)
                        ▼
     ┌──────────────────────────────────────────────┐
     │   silver.* — validated, deduplicated,        │
     │   current-state + error tables               │
     └──────────────────┬───────────────────────────┘
                        │ dbt (snapshot → run → test, Airflow-scheduled)
                        ▼
     ┌──────────────────────────────────────────────┐
     │   gold.* — star schema, SCD2 dimensions      │
     └──────────────────┬───────────────────────────┘
                        │
                        ▼
              ┌────────────────────┐
              │  Grafana (+ Loki)  │  live dashboards, log search
              └────────────────────┘

OpenWeatherMap ──(hourly, Airflow PythonOperator, not Spark)──► iot.weather_observations (hypertable)
```

**Notes:**
- All Postgres access — bronze writes, silver reads/writes, everything — goes through **parameterized `psycopg2`**,
not Spark's JDBC connector. 
- `config/spark/Dockerfile` deliberately has no JDBC driver: it bakes in the Kafka
connector jars (checksum-verified against Maven Central at build time) and `psycopg2-binary`. 
- This is what makesidempotent `INSERT ... ON CONFLICT` appends and PostGIS `ST_MakePoint`/`ST_Y`/`ST_X` calls possible — Spark's
native JDBC writer can do neither.

## Why streaming vs. batch is split the way it is

Only **one** domain runs as persistent Spark Structured Streaming: truck position. Everything else — customer,
product, order, payment, and inventory events — runs as periodic incremental batch, orchestrated by Airflow.
This is a deliberate design decision and only aims at showcasing airflow with Spark incrementatiobatch jobs.

Limitations explained:
- Spark Standalone does **not support `--deploy-mode cluster` for Python applications**.  
- A persistent streaming job's driver, in client mode, blocks for the query's entire (indefinite) lifetime — completely wrong for an Airflow 
`SparkSubmitOperator` task, which would occupy a LocalExecutor slot forever.   
- The truck-position job runs as its **own docker-compose service** (`spark-streaming-truck-position`, `restart: unless-stopped`) specifically
because of this — Docker supervises it directly, and `bronze_streaming_supervisor_dag.py` only monitors/alerts
(checks Spark Master's REST API, raises `AirflowException` if the app isn't listed as running) rather than
attempting to resubmit it.

### Kafka Connect JDBC Sink was considered and rejected for the streaming job

Kafka Connect's JDBC Sink connector is the more idiomatic, lower-code way to do a pure Kafka→Postgres copy, and
was seriously evaluated as a replacement for the hand-rolled `foreachBatch` Spark job. It was rejected for three
concrete reasons:  
- (1) it would remove the project's only Spark Streaming example, working against the stated
Airflow+dbt+Spark learning goal;  
- (2) it can't express `ST_MakePoint(lon, lat)::geography` inline — the sink maps fields straight to columns, so PostGIS 
construction would need a generated-column/trigger workaround;  
- (3) the streaming job's silver `truck_positions_current` side-effect upsert (near-zero-latency live map refresh)
would need a second connector. 

## Resource-sharing model (one shared Spark cluster)

There is exactly **one** Spark Standalone cluster (`spark-master` + `spark-worker-1`/`2`) — every job in the
system, whether the persistent streaming job or an Airflow-triggered batch job, submits to the same
`spark://spark-master:7077` and competes for the same worker pool. Two things matter here:

- **Client-mode drivers run wherever `spark-submit` was invoked from.** The streaming job's driver lives in
  its own dedicated container. Every Airflow-triggered batch job's driver runs *inside `airflow-scheduler`*
  (only the executors land on the workers) — confirmed directly, not assumed.
- **`spark.cores.max` caps are set on every job** to prevent one greedy application from claiming the whole
  cluster (confirmed this happens by default: the streaming job, uncapped, once claimed all 4 available cores
  across both workers via Spark Master's own REST API). With `SPARK_WORKER_CORES=4/4` (8 total) and
  `spark.cores.max=2` on every job, the streaming job takes 2, leaving 6 for batch — 3 batch jobs can run fully
  concurrently before a 4th queues naturally at the Master level (no error, it just waits for capacity).
  No Airflow Pool is used to enforce this — by design, Airflow's own task/DAG scheduling stays unrestricted;
  the physical core ceiling does the throttling.

See [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) for the full resource budget and how these numbers were derived.

## Airflow DAG inventory

| DAG | Schedule | Purpose |
|---|---|---|
| `bronze_streaming_supervisor_dag` | every 5 min | Monitoring-only: alerts (fails the task) if the truck-position streaming app isn't running on Spark Master. Does not resubmit — Docker's `restart: unless-stopped` handles that. |
| `bronze_ingestion_dag` | every 10 min | 5 parallel client-mode `SparkSubmitOperator` tasks: Kafka→bronze for customer/product/order/payment/inventory events (batch Kafka reader, offsets tracked in `control.kafka_offsets`). |
| `weather_enrichment_dag` | hourly | `PythonOperator` pulls current + forecast from OpenWeatherMap for 5 fixed locations, writes to `iot.weather_observations`. Not Spark — there's no streaming source, just a REST API. |
| `silver_customers_products_dag` | every 15 min | Bronze→silver for the two low-frequency reference domains. |
| `silver_orders_dag`, `silver_payments_dag`, `silver_inventory_dag` | every 10 min | Bronze→silver for their respective domains, each with lifecycle/consistency validation. `silver_inventory_dag` also runs the automatic restock check afterward. |
| `silver_truck_positions_dag` | every 2 min | Bronze→silver validation/enrichment (bounding-box, orphan-order checks) — **not** the live map's primary data path, which is refreshed directly by the streaming job itself. |
| `gold_dbt_dag` | triggered when **all 6** silver Assets have updated since its last run (Airflow 3 Asset-list AND semantics) | `dbt snapshot` → `dbt run` → `dbt test` as three separate `BashOperator` tasks, then a gold row-count `SQLCheckOperator`. |

Deliberately mixed operator types across these DAGs (`BashOperator`, `PythonOperator`, `SparkSubmitOperator`,
`SQLThresholdCheckOperator`/`SQLCheckOperator`, `ShortCircuitOperator`, Asset-based scheduling) — a learning
goal, not incidental.

## Technology choices and why

| Choice | Why |
|---|---|
| TimescaleDB + PostGIS (custom `config/postgres/Dockerfile`, `FROM timescale/timescaledb-ha:pg15`) | Hypertables for the genuinely time-series bronze/silver domains; PostGIS for real geography (`GEOGRAPHY(POINT,4326)`, great-circle-aware) rather than plain lat/lon floats. The `-ha` image (not the leaner `timescale/timescaledb:pg15`) was required: the lean Alpine-based image's `apk add postgis` always tracks whatever Postgres major is *currently default* in that Alpine release, never Timescale's pinned older major — confirmed broken by actually building it. `-ha`'s extra bundled tooling (Patroni, pgvector) costs nothing at runtime since it's never activated (`CREATE EXTENSION` only for `timescaledb`/`postgis`), confirmed via `ps aux` and `pg_available_extensions`. |
| Kafka (KRaft mode, no Zookeeper) | Simpler ops than Kafka+ZooKeeper, still the real Kafka protocol/ecosystem. |
| `psycopg2`, not Spark JDBC | See above — needed for idempotent upserts and PostGIS function calls. |
| Airflow 3, LocalExecutor | No Celery/Redis needed at this scale; DAG Processor is a mandatory separate Airflow 3 component (not optional, unlike Airflow 2). |
| dbt-postgres, `delete+insert` incremental strategy | `merge` isn't reliably supported by dbt-postgres; `delete+insert` is well-supported and transactionally safe. |
| Grafana + Loki, not Elasticsearch/Kibana | Lighter for this project's actual needs (dashboards + basic log search), not a full APM/log-analytics platform. |
| `confluent-kafka`, not `kafka-python` | `kafka-python==2.0.2`'s vendored `six.moves` shim is broken under Python 3.12 — confirmed by testing. `confluent-kafka` is actively maintained with prebuilt wheels. |

## Known architectural risks, stated plainly

- **No triggerer.** Correct for the current operator set (nothing here is a deferrable operator/sensor), but
  becomes a hard requirement if a deferrable sensor or Asset watcher is added later — such a task would simply
  never resume without one, silently, not with an error.
- **Order/payment/inventory freshness lag.** Since those domains are periodic batch (10 min cadence) rather than
  streaming, cumulative bronze→silver→gold latency for order/revenue data can reach 30-40 minutes. Accepted for
  a demo; the most business-critical domain is also the least fresh in the pipeline.
- **`docker exec`/`kill` vs. crash semantics differ.** `restart: unless-stopped` does **not** bring a container
  back after an explicit `docker stop`/`kill` (Docker treats that as intentional) — only after the container's
  own process exits unexpectedly. Confirmed directly by testing both cases.
