# Deployment & Resource Budget

## Target environment

Originally sized for a 16GB RAM / 7-logical-core machine (relaxed from an original 12GB target once it became
clear that genuine Spark Structured Streaming needed real headroom that 12GB couldn't comfortably provide).
**In practice, this has been run and verified end-to-end on a smaller 13GB RAM / 6-logical-core machine** — the
configured `mem_limit` ceilings across all active services (below) sum to ~13GB, which is effectively the
*entire* real RAM of that smaller host, not a comfortable margin under it. This worked without OOM issues at the
container level once every job's Spark memory settings were tuned correctly (see Known Issues below), but leaves
little slack for host-level overhead (OS, browser, IDE, etc.) — the 13GB host was observed using some swap under
concurrent load during heavy testing. **16GB is the safer recommendation if you have the choice**; treat 13GB as
"confirmed working, tight" rather than "comfortable."

## Current resource budget (measured, not just configured)

Every number below started as an estimate and was then **checked against real `docker stats` output** and
adjusted — several services were sized down after measuring actual idle/steady-state usage rather than left at
their original guessed values.

| Service | `mem_limit` | `cpus:` | Measured steady-state | Rationale |
|---|---|---|---|---|
| `kafka` | 1536m | 1.0 | Bursts to 99-102% of its quota (not sustained) | **Left at full quota deliberately** — it's actually ceiling-limited during bursts (message flushes); cutting it risks throttling delivery. |
| `data-generator` | 192m | 0.15 | ~16MB, <0.1% CPU | Trimmed hard from an original 512m/0.5 — genuinely near-idle. |
| `kafka-ui` | 384m | 0.3 | Spring Boot app, UI-only (no data plane) | Browses topics/messages/consumer groups against `kafka:9092`; auth disabled — fine for a single-user local demo, not for anything exposed further. |
| `spark-master` | 768m | 0.5 | Coordination only | Cluster-mode... no — client-mode driver JVMs for batch jobs run in `airflow-scheduler`; the *streaming* job's driver runs in its own dedicated container. Master only coordinates, hence the small footprint. |
| `spark-worker-1` | 2000m | 2.5 | Bursty (I/O-bound tasks) | `SPARK_WORKER_CORES=4` (Spark-internal scheduling slots) intentionally exceeds the real `cpus:` ceiling — these are I/O-bound jobs (waiting on Kafka/Postgres), so more scheduling slots than CPU quota lets Spark interleave more concurrent waiting tasks. |
| `spark-worker-2` | 1500m | 2.5 | Same pattern | Same reasoning, smaller memory allocation. |
| `spark-streaming-truck-position` | 1024m | 0.5 | ~484MB, bursty CPU (5s micro-batch cycle) | This **is** the client-mode driver process for the one persistent streaming job — runs as its own service, not submitted through Airflow, since Spark Standalone can't cluster-deploy Python apps (see [`docs/ARCHITECTURE.md`](ARCHITECTURE.md)). |
| `postgres` (Timescale+PostGIS) | 640m | 0.5 | Idle-ish, bursts under batch/dbt load | Hosts bronze/silver/gold/control — all of it, one instance. |
| `airflow-postgres` | 384m | 0.25 | ~44MB, near-zero CPU | Dedicated metadata DB, deliberately separate from the data-workload Postgres for isolation. |
| `airflow-api-server` | 768m | 0.3 | ~280MB, near-zero CPU most of the time | `mem_limit` is **not** reduced further despite low measured usage — an earlier investigation found even 1 worker sat close to 512m under real load; cutting it risks reintroducing a crash-loop (see Known Issues Encountered below). `cpus:` was trimmed based on measurement. |
| `airflow-scheduler` | 2048m | 2.0 | Varies with concurrent job count | Sized for up to 3 real concurrent client-mode driver JVMs (batch job drivers run here, not on the Spark workers) — see the resource-sharing model in [`docs/ARCHITECTURE.md`](ARCHITECTURE.md). |
| `airflow-dag-processor` | 640m | 0.5 | ~324MB, periodic re-parse bursts | Mandatory separate Airflow 3 component. |
| `grafana` | 256m | 0.2 | ~63MB, near-zero CPU | Trimmed from 400m/0.5 after measurement. |
| `loki` | 200m | 0.2 | ~65MB, ~1% CPU (compaction) | Trimmed from 300m/0.3. |
| `promtail` | 100m | 0.2 | ~49MB (49% of limit) | **Left unchanged** — proportionally less headroom than the other observability services; not a clear over-provisioning case. |

**Running total: ~12.95GB** across all currently-active services (kafka, kafka-ui, data-generator, the Spark
cluster + streaming job, both Postgres instances, all 4 Airflow components, Grafana/Loki/Promtail) — essentially
the full 13GB of the smaller verified host, ~11.6 CPU-ceiling-units against 6 real cores on that host (7 on the
original 16GB target). The CPU sum exceeding the physical core count is fine and intentional: Docker's `cpus:`
is a CFS bandwidth *ceiling*, not a rigid partition. Idle capacity from one container's unused ceiling is
available to others in real time; it only matters that no single container's *actual* usage needs exceed its
own ceiling at once. `cadvisor`/`prometheus` (below) are excluded from this total since they aren't started by
`scripts/start.sh`.

`cAdvisor`/`Prometheus` (for a dedicated service-health metrics dashboard) were discussed and staged in
`docker-compose.yml`/`config/prometheus/` during development, but **are not yet confirmed, applied, or
measured** as of this writing — treat them as proposed, not current, until this note is updated.

## How the CPU numbers were actually derived (not guessed)

Two Spark-specific facts drove the sizing, both confirmed by testing rather than assumed:

1. **`SPARK_WORKER_CORES` (Spark's internal scheduling-slot count) and Docker's `cpus:` (the real CPU-time
   ceiling) are different knobs.** This project deliberately runs them mismatched — `SPARK_WORKER_CORES=4` per
   worker (8 total) against a `cpus: 2.5` ceiling per worker (5 total) — because these are I/O-bound jobs
   (waiting on Kafka/Postgres, not computing), so oversubscribing scheduling slots relative to CPU quota lets
   Spark interleave more concurrent waiting tasks than the raw CPU budget would otherwise suggest.
2. **An uncapped Spark application will claim every available core.** Confirmed directly via Spark Master's own
   REST API (`http://localhost:8080/json/`): the streaming job, before `spark.cores.max` was added, claimed
   `"cores": 4` — the entire cluster at the time — leaving zero cores for any concurrent batch job. Every job in
   this project now sets `spark.cores.max=2` for exactly this reason.

## Known limitations

- Single-broker Kafka (KRaft, no replication) — a broker failure loses in-flight (unconsumed) messages. Fine
  for a local demo, not for production.
- Single Postgres instance, no failover/replication.
- Secrets live in `.env` (gitignored) rather than a vault — appropriate for a single-user local machine only.
- `weather_enrichment_dag` does nothing useful without a real `OPENWEATHERMAP_API_KEY`.
- No triggerer — fine for the current operator set (nothing deferrable), becomes a hard requirement the moment
  a deferrable sensor/Asset watcher is added.
- v1 scope simplifications, not gaps: one product per order, one order per shipment (no consolidation), a
  degenerate (non-SCD2) truck dimension, a simple direct restock event rather than a full supplier
  purchase-order lifecycle, no Google Maps ETA lookup (all explicitly deferred during planning, not oversights).

## Scaling notes (if this ever needed to run at higher throughput)

- The 3-concurrent-batch-job ceiling (`spark.cores.max=2` × 3 ≤ 6 remaining cores after the streaming job's 2)
  would need either more worker cores or a lower per-job cap to raise concurrency.
- `airflow-scheduler`'s memory would need to grow further if raising batch-job concurrency, since every
  concurrent batch job's driver JVM lives there.
- Kafka is already ceiling-limited under current load (bursts to 100%+ of its `cpus: 1.0`) — real throughput
  growth would need that raised first, not last.
- The 10-minute batch cadence for orders/payments/inventory is a deliberate freshness/resource trade-off (see
  [`docs/ARCHITECTURE.md`](ARCHITECTURE.md)) — tightening it would increase Airflow/Spark load proportionally.

## Known issues encountered and fixed during development

A representative sample (not exhaustive) of real, testing-discovered issues, kept here since they're the kind
of thing that recurs in similar setups:

- Docker healthchecks written with `curl` failed silently (`unhealthy`) in images that don't keep `curl` around
  after build time — switched every healthcheck in this repo to Python (`urllib.request`).
- A worker's healthcheck targeted its *host-side* published port instead of the container's actual internal
  listening port (`8082:8081` mapping — the healthcheck runs inside the container, where only `8081` exists).
- `apache-airflow-providers-standard==0.3.0`, pinned in the Airflow image build, was older than what
  `apache-airflow-core==3.0.3` requires — pip's resolver **silently downgraded Airflow itself** to satisfy the
  outdated pin, rather than erroring. Fixed by removing redundant pins for packages the base image already
  ships compatible versions of.
- Freshly-created bind-mounted host directories are owner-only-writable by default; every container here runs
  as a different, non-host-matching uid/gid — caused genuine `PermissionError`s on first boot across Airflow,
  Spark, and Grafana/Loki, until `scripts/init.sh` was made to set them world-writable.
- Loki 3.x requires `compactor.delete_request_store` explicitly whenever retention is enabled — omitting it is
  a hard config-validation failure, not a warning.
- Grafana dashboard JSON files referenced a datasource by a hardcoded `uid` that was never actually pinned in
  the datasource provisioning config, so Grafana auto-generated a random one that never matched — every panel
  would have failed with "Data source not found" in a real browser. Fixed by pinning explicit `uid:` values.
