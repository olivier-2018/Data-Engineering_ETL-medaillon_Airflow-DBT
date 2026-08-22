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
- Browse the 6 topics (`iot.customer_events`, `iot.product_events`, `iot.sales_order_events`,
  `iot.payment_events`, `iot.inventory_changes`, `iot.truck_position_events`), their partitions, and message counts.
- Open a topic's **Messages** tab to tail/inspect live JSON payloads as the generator (or a replay) publishes them
  — the fastest way to confirm the generator is actually producing before chasing a downstream ingestion bug.
- Inspect consumer groups/offsets for the batch ingestion jobs (`ingest_*` Spark jobs read via
  `startingOffsets`/`endingOffsets`, not a named consumer group, so groups here mostly reflect ad-hoc CLI/UI
  consumers, not the pipeline's own offset tracking — that lives in `control.kafka_offsets`, not Kafka's
  consumer-group protocol).

## Grafana

**http://localhost:3000** — login with `GRAFANA_ADMIN_USER`/`GRAFANA_ADMIN_PASSWORD` from `.env`.

Two datasources are provisioned automatically (`config/grafana/provisioning/datasources/datasources.yml`),
with **pinned, stable UIDs** (`iot_postgres`, `loki`) — every dashboard JSON file references these exact UIDs,
so if you ever edit the datasource provisioning, keep the `uid:` fields stable or every dashboard panel will
show "Data source not found" (this happened during development — see `docs/DEPLOYMENT.md`).

### Provisioned dashboards (folder: "IoT Logistics")

| Dashboard | What it shows |
|---|---|
| **Live Truck Map** | A Geomap panel plotting `silver.truck_positions_current` (filtered to non-delivered shipments) — the live view of where every truck currently is, refreshed on a 10s auto-refresh. Below it, a table of the 50 most recently updated shipments. |
| **Pipeline Health** | `control.watermarks` lag per domain (how far behind each silver job is), error-row counts per domain over the last 15 minutes, and bronze ingestion volume over the last 6 hours. |
| **Gold KPIs** | Revenue by destination country (last 24h), average time-to-destination across delivered shipments, and a table of products currently below their restock threshold. |

Every panel's query was verified by executing it directly through Grafana's own `/api/ds/query` endpoint (the
same code path the browser UI uses) against real data — not just checked for provisioning without error.

### Adding or editing a dashboard

Dashboard JSON files live in `config/grafana/dashboards/` and are mounted read-only into the Grafana container;
the dashboard **provider** (`config/grafana/provisioning/dashboards/dashboards.yml`) auto-loads anything in that
directory, so a new file just needs to exist there — no docker-compose changes required. Two things to get right:

1. Reference datasources by their **pinned UID** (`iot_postgres`/`loki`), not by name.
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

## Metrics (CPU/memory/health per container)

Postgres and Loki alone have no visibility into Docker's cgroups — neither can tell you a container's real CPU
or memory usage. Getting that into Grafana needs a metrics *exporter* (something that reads cgroup accounting
and re-exposes it) plus a time-series store to hold history. **cAdvisor + Prometheus were designed for this
purpose and staged in this repo's config during development, but are not yet confirmed/applied** — see
`docs/DEPLOYMENT.md` for current status. Until that's finalized, per-container resource usage in this project
is checked manually via `docker stats`.
