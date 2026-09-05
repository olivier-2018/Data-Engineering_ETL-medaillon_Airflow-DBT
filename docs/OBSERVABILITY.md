# Observability

## Spark Master UI and per-application detail

**http://localhost:8080** — cluster state: registered workers, cores/memory in use, and every running/completed
application. Each application's own name links out to its **driver's** SparkUI (Jobs/Stages/Storage/Environment/
Executors) — a separate page served by that specific driver process, not by Master itself.

| Application | Port | Notes |
|---|---|---|
| `bronze-truck-position-ingest` (the one persistent Structured Streaming job) | **4040** | Always up while the job runs — `http://localhost:4040/`. |
| Any batch job (`bronze_ingestion_dag`, every `silver_*_dag`) | **4041, 4042, 4043** | Assigned per concurrently-running job, starting at 4041 so it never collides with the streaming job's fixed 4040. |

The 3-port batch range isn't arbitrary — it's the actual concurrency ceiling: `SPARK_WORKER_CORES=4` on both
workers (8 total) minus the streaming job's permanent `spark.cores.max=2` leaves 6 free, and every batch job also
requests `spark.cores.max=2`, so **at most 3 batch jobs can hold executors at once**, regardless of how many DAGs
are scheduled around the same time — a 4th simply queues at Spark Master until a slot frees up (no error, just
waits). A batch job's SparkUI is only reachable while its driver process is alive, which for these jobs is
typically well under a minute — click through while a DAG is actively running, not after. Spark Worker 1/2 UIs
(http://localhost:8081, http://localhost:8082) show per-worker executor detail regardless of timing.

This took two separate fixes to get working from a host browser at all (both confirmed by testing, not assumed):
- `spark.driver.host` must stay each job's real Docker-network address (the container name for the streaming job,
  `airflow-scheduler` for batch jobs) — executors need this to reach the driver over RPC.
- `SPARK_PUBLIC_DNS=localhost`, set as a container **environment variable** (not a `spark.` conf), is what
  actually makes the "Application Detail UI" link Master shows resolve to `localhost:<port>` instead of an
  unreachable Docker-internal hostname — Spark's `WebUI` base class reports this env var, when set, as the
  public hostname independently of `spark.driver.host`.

## Kafka UI

**http://localhost:8089** — no login (auth is disabled for this local demo). Provisioned against the single
`kafka:9092` broker as cluster `local`.

Use it to:
- Browse the 8 topics (`iot.customer_events`, `iot.product_events`, `iot.purchase_order_events`,
  `iot.product_on_order_events`, `iot.invoice_events`, `iot.inventory_changes`, `iot.truck_fleet_events`,
  `iot.truck_position_events`), their partitions, and message counts.
- Open a topic's **Messages** tab to tail/inspect live JSON payloads as the generator (or a replay) publishes them
  — the fastest way to confirm the generator is actually producing before chasing a downstream ingestion bug.
- Inspect consumer groups/offsets for the batch ingestion jobs (`ingest_*` Spark jobs read via
  `startingOffsets`/`endingOffsets`, not a named consumer group, so groups here mostly reflect ad-hoc CLI/UI
  consumers, not the pipeline's own offset tracking — that lives in `control.kafka_offsets`, not Kafka's
  consumer-group protocol).

## Grafana

**http://localhost:3000** — login with `GRAFANA_ADMIN_USER`/`GRAFANA_ADMIN_PASSWORD` from `.env`.

Four datasources are provisioned automatically (`config/grafana/provisioning/datasources/datasources.yml`),
with **pinned, stable UIDs** (`iot_postgres`, `loki`, `prometheus`, `airflow_postgres`) — every dashboard JSON
file references these exact UIDs, so if you ever edit the datasource provisioning, keep the `uid:` fields
stable or every dashboard panel will show "Data source not found" (this happened during development). Note
`prometheus` only has actual data behind it once cAdvisor + Prometheus are running (opt-in, see above).

### Provisioned dashboards (folders: `business` / `operations` / `backend`)

12 dashboards, organized by the business/operations/backend goals laid out in `TODO_grafana-dev.md` (now fully
built out):

| Folder | Dashboard | What it shows |
|---|---|---|
| `business` | **Business Operations** | Day-to-day detail: total customers, % with an open PO, PO created-vs-delivered trend, PO status breakdown, sales by segment/region/category (selectable time grain via `$time_grain`), invoice status/aging/reminder breakdown, delivery fleet status, orders at risk, achieved-vs-estimated delivery, process time. |
| `business` | **Business Monitoring** | High-impact executive KPIs: customer count/growth trend, on-time delivery rate, avg delay, process-time percentiles (avg/p50/p90), sales by region/product at Y/Q/M grain — plus the two panels absorbed from the retired Gold KPIs dashboard (revenue by zone, restock threshold). |
| `business` | **Weather & Delivery Impact** | Delivery time by weather condition/temperature bucket, worst weather-linked delays, deliveries-by-condition trend — surfaces `gold.fact_weather_delivery_correlation`. Renders empty until a real `OPENWEATHERMAP_API_KEY` is set in `.env` (`iot.weather_observations` has no rows without one). |
| `business` | **Inventory & Restocking** | Stock-level trend per product (`$product` selector), restock cadence, top sellers vs. current stock, stockout events. |
| `business` | **Executive Overview** | Single-pane summary — revenue today, orders in flight, on-time delivery %, pipeline lag status, open SLA breaches, error count — every panel reuses a query already verified in another dashboard. |
| `operations` | **Live Truck Map** | A Geomap panel plotting `silver.truck_current_position` — the live view of where every truck currently is, color-coded by `truck_status`, on a fixed initial view (so zoom/pan survives the 10s auto-refresh). Below it, a table of trucks currently on a run (not `free`), with a live count of orders aboard each. |
| `operations` | **Zone & Customer Delay Risk** | SLA breach count, live at-risk orders, zone throughput/average-delay (14d, filterable via a multi-select `$zone` variable), customers with recurring delays (≥3 delivered orders, ranked by % late). |
| `operations` | **Fleet & Logistics Efficiency** | Fleet status mix, time spent in each `truck_status` over 24h (read from bronze `iot.truck_position_events` directly — silver only keeps current state), deliveries completed per truck, avg orders aboard per active run. |
| `backend` | **Pipeline Health** | `control.silver_watermarks` lag per domain (how far behind each silver job is), error-row counts per domain over the last 15 minutes, and bronze ingestion volume over the last 6 hours. |
| `backend` | **Backend Monitoring** | Bronze ingestion rate by domain, streaming data rate, Airflow DAG execution status (via the `airflow_postgres` datasource), per-container CPU/memory (selectable via a `$container` variable), Kafka offset freshness. |
| `backend` | **Database Monitoring** | Postgres size/connections/cache-hit-ratio/dead-tuple health, Airflow metadata DB health, Postgres container CPU/memory, 30-day ingestion history, TimescaleDB hypertable inventory. |
| `backend` | **Data Quality Trends** | Daily error-rate trend per domain, error rate as a % of ingestion volume, top rejection reason codes — the historical companion to Pipeline Health's point-in-time view. |

Every panel's query is verified directly against the live schema (not just checked for provisioning without
error) whenever the underlying tables change. Three new gold dbt models back the delivery-performance/sales
panels above — see [`docs/DBT_MODEL.md`](DBT_MODEL.md) for `fact_delivery_performance`, `fact_sales`, and
`fact_purchase_order_status_duration`.

### Adding or editing a dashboard

Dashboard JSON files live in `config/grafana/dashboards/<folder>/` (one subdirectory per Grafana folder) and
are mounted read-only into the Grafana container; the dashboard **provider**
(`config/grafana/provisioning/dashboards/dashboards.yml`, using `foldersFromFilesStructure: true`) auto-loads
anything found there — a new file just needs to exist in the right subdirectory, no docker-compose changes
required. Two things to get right:

1. Reference datasources by their **pinned UID** (`iot_postgres`/`loki`/`prometheus`/`airflow_postgres`), not by name.
2. After editing a dashboard's JSON file's content, `docker compose up -d grafana` alone will **not** pick up
   the change — Compose only recreates a container when the *service definition* changes, not when a
   bind-mounted file's contents change. Use `docker compose restart grafana` to force it to re-read.

## Loki — logs

**Loki has no web UI of its own.** It's a backend only (an ingestion API that Promtail pushes to, and a query
API) — the same relationship Prometheus has to Grafana for metrics. All log browsing happens through
**Grafana's Explore view** (compass icon, left nav):

1. Select the `loki` datasource.
2. Write a **LogQL** query, e.g.:
   - `{container="kafka"}` — all logs from the Kafka container
   - `{container="spark-master"} |= "ERROR"` — filter for the literal string "ERROR"
   - `{container=~"spark.*"}` — all Spark-related containers (regex label match)
3. Grafana renders both the matching log lines and a log-volume-over-time histogram automatically.

Promtail (the log shipper) auto-discovers every container on the host via the Docker socket
(`docker_sd_configs`) — no per-service configuration needed when a new service is added to `docker-compose.yml`.

## cAdvisor + Prometheus — per-container metrics

Postgres and Loki alone have no visibility into Docker's cgroups — neither can tell you a container's real CPU
or memory usage. Getting that into Grafana needs a metrics *exporter* (something that reads cgroup accounting
and re-exposes it) plus a time-series store to hold history: **cAdvisor** (http://localhost:8085, also browsable
directly — it ships its own minimal web UI) exports per-container `container_cpu_usage_seconds_total` /
`container_memory_usage_bytes` / filesystem metrics; **Prometheus** (http://localhost:9090) scrapes cAdvisor
every 15s (`config/prometheus/prometheus.yml`) and holds 24h of history. Both are confirmed working end-to-end
as of this writing — a `prometheus`-uid datasource is provisioned in Grafana alongside `iot_postgres`/`loki`.

**Opt-in, not part of `scripts/start.sh`**: start them explicitly with `docker compose up -d cadvisor prometheus`.

**A real OOM bug was hit and fixed while confirming this pair works**: cAdvisor's original `mem_limit: 200m`
was too tight for this host — `docker inspect cadvisor` showed `OOMKilled: true` roughly 10 minutes after every
start (this host's actual cgroup/overlay-mount count is higher than a minimal demo host, and cAdvisor's
in-memory stats cache scales with it). With no `restart:` policy configured, the container then stayed dead
silently — every per-container CPU/memory panel in Grafana would just show "No data," with no error surfaced
anywhere. Fixed by raising `mem_limit`/`memswap_limit` to 400m (measured steady-state after the fix: ~110-150MB,
comfortable headroom) and adding `restart: unless-stopped`, matching the policy already used by the Spark
streaming job and other long-running services. **Prometheus itself runs close to its own ceiling** (measured
~250MB of its 300m limit, ~84%) — not confirmed broken, but worth watching if scrape targets or retention grow;
raise `mem_limit` if it starts OOMing too.

Useful PromQL for exploring a specific container directly (Grafana panel or Prometheus's own UI):

```promql
rate(container_cpu_usage_seconds_total{name="postgres"}[5m])
container_memory_usage_bytes{name="grafana"}
```

`name` is the real Docker container name (confirmed via cAdvisor's raw `/metrics` output — cAdvisor also emits a
large set of `container_label_com_docker_compose_*` labels per series if you need to filter/group by those
instead).

Until this pair is started, per-container resource usage can still be checked manually via `docker stats`.
