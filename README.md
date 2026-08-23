# IoT Logistics ETL Pipeline with Kafka/Airflow3/dbt/Spark

A local, Docker-based demo to showcase a medallion (bronze/silver/gold) data pipeline, built around a simulated
warehouse-and-delivery scenario: a resale warehouse in **Biel, Switzerland**, selling 100 products online with
fast delivery across Switzerland, France, Germany, and Italy.

Synthetic order/payment/inventory/truck-GPS events flow through **Kafka** and get processed by **Apache Spark** into the bronze layer using:  
- a **persistent Structured Streaming** job for live truck tracking, and  
- periodic **incremental batches** for customer, inventory, orders, payments, and products data.  


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
Grafana + Loki (+ cAdvisor/Prometheus, planned) visualize live data and logs.
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
docker compose --profile generator up -d data-generator
```

Full walkthrough, required `.env` values, and a verification checklist: [`docs/SETUP.md`](docs/SETUP.md).

## UI & Dashboards

Once `scripts/start.sh` finishes, these are the web UIs available:

| UI | URL | What it's for |
|---|---|---|
| **Grafana** | http://localhost:3000 | The main dashboard UI — see below. Login with `GRAFANA_ADMIN_USER`/`GRAFANA_ADMIN_PASSWORD` from `.env`. |
| Airflow | http://localhost:8090 | DAG runs, task logs, manually trigger/unpause DAGs (all DAGs start paused by default). Login is `admin` + an auto-generated password, not from `.env` — see [`docs/SETUP.md`](docs/SETUP.md#logging-into-the-airflow-ui). |
| Kafka UI | http://localhost:8089 | Browse topics/partitions, tail live messages, inspect consumer groups — no login (auth disabled for this local demo). |
| Spark Master | http://localhost:8080 | Cluster state — registered workers, running/completed applications, cores/memory in use. |
| Spark Worker 1 / 2 | http://localhost:8081 / http://localhost:8082 | Per-worker executor detail. |

### Grafana dashboards (folder: "IoT Logistics")

Three dashboards are provisioned automatically — no manual import needed:

- **Live Truck Map** — a Geomap panel plotting every truck's current position (`silver.truck_positions_current`,
  refreshed every 10s), fed directly by the persistent Structured Streaming job for near-real-time movement.
- **Pipeline Health** — per-domain silver watermark lag, error-row counts, and bronze ingestion volume — the
  first place to look if data seems stale or a job seems stuck.
- **Gold KPIs** — revenue by destination country, average time-to-destination, and products currently below
  their restock threshold.

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

## Key features

- **Genuine Spark Structured Streaming** for the one domain where sub-minute freshness matters (truck GPS position), running as its own supervised docker-compose service — everything else is deliberately periodic incremental batch. This is a deliberate choice to showcase airflow and dbt, and since Spark Standalone can't cluster-deploy Python streaming jobs, forcing everything into "streaming" would be resume-driven engineering, not the right tool for the job (demo airflow, dbt and spark).
- **Immutable, append-only bronze layer** — every domain is an event log (order status/payment/shipment lifecycle transitions are separate rows, not in-place updates), enforced at the database-privilege level (`pipeline_rw` has no `UPDATE` grant on `iot.*`).
- **Real PostGIS geography** for truck positions and weather locations, not just lat/lon floats — `ST_MakePoint`/ `ST_Y`/`ST_X` used throughout, verified against a live database.
- **Real dbt SCD2** (snapshots) for customer/product attribute history and order/payment status transitions.
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
