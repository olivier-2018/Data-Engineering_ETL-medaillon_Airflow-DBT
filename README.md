# IoT Logistics ETL Pipeline with Kafka/Airflow3/dbt/Spark

A local, Docker-based demo to showcase a medallion (bronze/silver/gold) data pipeline, built around a simulated
warehouse-and-delivery scenario: a resale warehouse in **Biel/Bienne, Switzerland**, selling products online
with delivery to customers across Switzerland (40 reference cities, a configurable subset active at a time).

Synthetic customer/purchase-order/invoice/inventory/truck-GPS events flow through **Kafka** and get processed
by **Apache Spark** into the bronze layer using:
- a **persistent Structured Streaming** job for live truck tracking, and
- periodic **incremental batches** for customer, product, purchase order, invoice, inventory, and truck fleet
  data.


Data is stored in a **TimescaleDB + PostGIS** Postgres instance across bronze/silver/gold schemas, get transformed
silver→gold by **dbt**, and overall orchestrated by **Airflow 3**.  

**Grafana** and **Loki** (for logs) provides live dashboards, including a truck-position map.  

This project was created to showcase Airflow 3, dbt, and Spark hands-on — architecture choices favor demonstrating each tool's real capabilities (genuine Structured Streaming where it matters, genuine incremental batch where it doesn't, genuine dbt SCD2 snapshots) over minimizing moving parts.

## Architecture at a glance

```
Kafka (KRaft)
  ├─ truck_position_events ──► Spark Structured Streaming ──► bronze (hypertable) + silver "current position"
  │
  └─ 5 other domains        ──► Spark periodic batch        ──► bronze (hypertables + plain tables)
                                                                        │
                                                              Spark incremental batch (watermarked)
                                                                        ▼
                                                                silver (validated, deduplicated)
                                                                        │
                                                                dbt (snapshot → run → test)
                                                                        ▼
                                                              gold (star schema, SCD2 dimensions)

OpenWeatherMap ──► Airflow (hourly pull, not Spark) ──► iot.weather_observations (hypertable)

Airflow 3 (LocalExecutor) orchestrates every batch/silver/gold step via a mix of operator types.
Grafana + Loki visualize live data and logs; cAdvisor + Prometheus feed per-container CPU/memory metrics
into Grafana (opt-in — not started by `scripts/start.sh`, see below).
```

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full breakdown, including *why* each domain is streaming vs. batch, and [`docs/DATA_SCHEMA.md`](docs/DATA_SCHEMA.md) for the complete schema reference.

## Resource budget

Originally targeted at 16GB RAM / 7 logical cores; also confirmed running end-to-end on a smaller 13GB / 6-core
machine, though that's a tight fit (configured `mem_limit` ceilings sum to ~13GB — effectively the whole machine,
not a comfortable margin under it). See [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) for the full per-service
breakdown and the reasoning behind each number (several were tuned down after measuring real `docker stats`
output, not just guessed).

## Quickstart

```bash
scripts/init.sh    # one-time setup: .env, Fernet key, data directories
scripts/start.sh   # brings the stack up in dependency order
```

Then, in a separate step (synthetic load is opt-in, not automatic):

```bash
docker compose --profile generator up -d --build data-generator
```

Full walkthrough, required `.env` values, and a verification checklist: [`docs/SETUP.md`](docs/SETUP.md).

## UI & Dashboards

Once `scripts/start.sh` finishes, these are the web UIs available:

| UI | URL | What it's for |
|---|---|---|
| **Grafana** | http://localhost:3000 | The main dashboard UI — see below. Login with `GRAFANA_ADMIN_USER`/`GRAFANA_ADMIN_PASSWORD` from `.env`. |
| Airflow | http://localhost:8090 | DAG runs, task logs — every DAG starts unpaused automatically (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false`). Login is `admin` + an auto-generated password, not from `.env` — see [`docs/SETUP.md`](docs/SETUP.md#logging-into-the-airflow-ui). |
| Kafka UI | http://localhost:8089 | Browse topics/partitions, tail live messages, inspect consumer groups — no login (auth disabled for this local demo). |
| Spark Master | http://localhost:8080 | Cluster state — registered workers, running/completed applications, cores/memory in use. |
| Spark Worker 1 / 2 | http://localhost:8081 / http://localhost:8082 | Per-worker executor detail. |
| dbt docs | http://localhost:9000 | Generated dbt lineage graph/catalog - opt-in, not started by default (see [`docs/DBT_MODEL.md`](docs/DBT_MODEL.md#10-generated-documentation-lineage-graph-catalog)): `docker compose --profile documentation up -d dbt-docs` (after generating it at least once via `dbt docs generate`). |
| cAdvisor | http://localhost:8085 | Per-container CPU/memory/filesystem metrics, browsable directly - opt-in, not started by `scripts/start.sh`: `docker compose up -d cadvisor prometheus` (Prometheus scrapes cAdvisor; both feed the `prometheus` Grafana datasource). See [`docs/OBSERVABILITY.md`](docs/OBSERVABILITY.md) for a real OOM issue hit and fixed while wiring this up. |
| Prometheus | http://localhost:9090 | Query/explore the raw metrics cAdvisor exposes (`container_cpu_usage_seconds_total`, `container_memory_usage_bytes`, ...) directly, outside Grafana. |

### Grafana dashboards (folders: `business` / `operations` / `backend`)

12 dashboards are provisioned automatically across the three folders — no manual import needed: 5 in
`business` (Business Operations, Business Monitoring, Weather & Delivery Impact, Inventory & Restocking,
Executive Overview), 3 in `operations` (Live Truck Map, Zone & Customer Delay Risk, Fleet & Logistics
Efficiency), and 4 in `backend` (Pipeline Health, Backend Monitoring, Database Monitoring, Data Quality
Trends). See [`docs/OBSERVABILITY.md`](docs/OBSERVABILITY.md) for what each one shows.

Every panel's query has been verified against real data (see [`docs/OBSERVABILITY.md`](docs/OBSERVABILITY.md)
for exactly how). Grafana is also the way to browse **Loki** logs — Loki has no web UI of its own; use
Grafana's *Explore* view with a LogQL query like `{container="kafka"}`.

## Documentation

| Doc | Covers |
|---|---|
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Full system design, medallion data flow, streaming-vs-batch rationale, Airflow DAG inventory |
| [`docs/DATA_SCHEMA.md`](docs/DATA_SCHEMA.md) | Complete bronze/silver/gold schema reference, hypertables, PostGIS columns |
| [`docs/SETUP.md`](docs/SETUP.md) | Step-by-step quickstart, required `.env`/config values, troubleshooting |
| [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md) | Local dev workflow, `config.yaml` field reference, adding a new domain |
| [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) | Resource budget rationale, scaling notes, known limitations |
| [`docs/OBSERVABILITY.md`](docs/OBSERVABILITY.md) | Grafana dashboards, Loki log querying |
| [`docs/AIRFLOW3_PROJECT.md`](docs/AIRFLOW3_PROJECT.md) | This project's Airflow 3 setup: DAG inventory, the shared bronze-ingestion factory, Assets, schedule parametrization, concurrency model - written for learning, not just reference |
| [`docs/AIRFLOW3_BACKGROUND_OVERVIEW.md`](docs/AIRFLOW3_BACKGROUND_OVERVIEW.md) | Generic Airflow 3 tutorial/background (not specific to this repo) - concepts, DAG design patterns |
| [`docs/DBT_MODEL.md`](docs/DBT_MODEL.md) | dbt gold-layer processing, model-by-model - written for learning, not just reference |
| [`docs/DBT_BACKGROUND_OVERVIEW.md`](docs/DBT_BACKGROUND_OVERVIEW.md) | Generic dbt tutorial/background (not specific to this repo) - concepts, project structure, medallion architecture |

## Key features

- **Genuine Spark Structured Streaming** for the one domain where sub-minute freshness matters (truck GPS position), running as its own supervised docker-compose service — everything else is deliberately periodic incremental batch. This is a deliberate choice to showcase airflow and dbt, and since Spark Standalone can't cluster-deploy Python streaming jobs, forcing everything into "streaming" would be resume-driven engineering, not the right tool for the job (demo airflow, dbt and spark).
- **Immutable, append-only bronze layer** — every domain is an event log (purchase-order/invoice/truck lifecycle transitions are separate rows, not in-place updates), enforced at the database-privilege level (`pipeline_rw` has no `UPDATE` grant on `iot.*`).
- **Real PostGIS geography** for truck positions and weather locations, not just lat/lon floats — `ST_MakePoint`/ `ST_Y`/`ST_X` used throughout, verified against a live database.
- **Real dbt SCD2** (snapshots) for customer/product attribute history and purchase-order/invoice status transitions.
- **A mix of Airflow operator types** by design — `BashOperator`, `PythonOperator`, `SparkSubmitOperator`, SQL-check operators, `ShortCircuitOperator`, and Airflow 3 Asset-based cross-DAG scheduling — for learning breadth, not because every task needed a different operator.

## Known limitations

- Single-broker Kafka, single Postgres instance, no HA — appropriate for a local demo, not for production.
- Secrets live in `.env` (gitignored), not a vault — fine for a single-user local machine.
- `weather_enrichment_dag` needs a real `OPENWEATHERMAP_API_KEY` (free tier) to do anything.
- See [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) for the full list, including v1 scope simplifications
  (one product per order, one order per shipment, degenerate truck dimension).

## Acknowledgement

Built with Apache Kafka, Apache Spark, Apache Airflow, dbt, TimescaleDB, PostGIS, Grafana, and Loki — all
open-source.
