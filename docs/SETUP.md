# Setup Guide

## Prerequisites

- Docker with Compose v2 (`docker compose version`)
- ~16GB RAM, 7 logical cores recommended; confirmed working end-to-end on 13GB/6 cores too, but that's a tight
  fit (see [`docs/DEPLOYMENT.md`](DEPLOYMENT.md) for the exact per-service budget and how it was derived)
- A free [OpenWeatherMap](https://openweathermap.org/api) API key if you want `weather_enrichment_dag` to work
  (optional — everything else runs without it)

## One-time setup

```bash
scripts/init.sh
```

This is idempotent (safe to re-run) and:
- Copies `.env.example` → `.env` if `.env` doesn't already exist (never overwrites an existing one)
- Generates `AIRFLOW_FERNET_KEY` if it's still empty
- Generates `AIRFLOW_API_SECRET_KEY`/`AIRFLOW_JWT_SECRET` if still empty — these must be **identical across every
  Airflow component** (the scheduler signs each task's internal Execution API JWT with these, and
  `airflow-api-server` verifies it); left as independent per-container defaults, every single task fails with
  `403 Forbidden` (confirmed by testing — this is what silently broke every batch DAG until diagnosed).
- Creates the `data-*` host directories every service needs
- Makes the directories that non-host-matching container users need to write into world-writable
  (`data-airflow-logs`, the Spark `data-spark-*` directories, `data-grafana`, `data-loki`, `dbt` — recursively,
  since a non-recursive chmod on `./dbt` alone leaves pre-existing subdirectories like `dbt/logs/`/`dbt/target/`
  just as unwritable) — this is a **real, confirmed-by-testing requirement**, not defensive boilerplate: every
  container here runs as a different uid/gid than your host user (Airflow: `uid=50000 gid=0`; Spark:
  `uid=185 gid=185`; etc.), and a freshly-created host directory is owner-only-writable by default, which causes
  genuine `PermissionError`s on first boot otherwise.

After running it, open `.env` and fill in `OPENWEATHERMAP_API_KEY` if you want weather data (the script prints
a reminder if it's still empty).

## Starting the stack

```bash
scripts/start.sh
```

Brings services up in dependency order with health-waits between stages: `kafka`/`postgres` →
`airflow-postgres`+`airflow-init` → the rest of the Airflow components → `grafana`/`loki`/`promtail`.
Prints the working UI URLs when done:

| Service | URL |
|---|---|
| Spark Master UI | http://localhost:8080 |
| Spark Worker 1 UI | http://localhost:8081 |
| Spark Worker 2 UI | http://localhost:8082 |
| Airflow UI | http://localhost:8090 |
| Kafka UI | http://localhost:8089 |
| Grafana | http://localhost:3000 |

Synthetic data generation is **not** started automatically — start it explicitly when you want load flowing:

```bash
docker compose --profile generator up -d --build data-generator
```

`--build` matters here specifically: unlike every other service in `scripts/start.sh`, this is the one command
not already wired through `--build` by that script (since it's profile-gated, this is the *only* place it's
started). Docker's build cache is content-hash based, not a naive mtime check, so `--build` is cheap when
nothing changed — but without it, `docker compose up` won't even check whether `data_generators/` source has
changed since the image was last built, silently running stale code indefinitely (confirmed: this is exactly
what happened with a customer-address generation fix that sat unbuilt for two days).

### Logging into the Airflow UI

Airflow 3's default auth backend (Simple Auth Manager) does **not** read credentials from `.env` — there is
nothing to configure. On first boot it generates a random `admin` password itself and persists it inside the
`airflow-api-server` container. Retrieve it with either:

```bash
docker compose logs airflow-api-server | grep "Simple auth manager"
# or, the persisted copy:
docker compose exec airflow-api-server cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

Log in at http://localhost:8090 with username `admin` and that password. It's stable across container
*restarts*, but a fresh `airflow-init` (e.g. after `scripts/reset.sh`) generates a new one — re-run the command
above if login stops working after a reset.

## Verification checklist

1. **Extensions and schema**:
   ```bash
   docker compose exec postgres psql -U postgres -d iot_database -c "\dx"
   docker compose exec postgres psql -U postgres -d iot_database -c "\d+ iot.truck_position_events"  # should show it's a hypertable
   ```
2. **Kafka topics** (created automatically the first time the generator/ingestion jobs touch them, or create
   manually):
   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
   ```
   Or browse them visually at http://localhost:8089 (Kafka UI) — no CLI needed.
3. **Generator publishing real data**:
   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 --topic iot.product_events --from-beginning --max-messages 2
   ```
   Or open a topic in Kafka UI and use its "Messages" tab to tail/inspect live payloads.
4. **Truck-position streaming job** (the one persistent Spark Structured Streaming job — should already be
   running as its own service):
   ```bash
   docker compose ps spark-streaming-truck-position
   docker compose exec postgres psql -U postgres -d iot_database -c "SELECT count(*) FROM iot.truck_position_events;"
   # run again a minute later - the count should have grown
   ```
5. **Airflow DAGs registered and parsing cleanly**:
   ```bash
   docker compose exec airflow-api-server airflow dags list
   docker compose exec airflow-api-server airflow dags list-import-errors  # should show nothing
   ```
   Every DAG starts **unpaused** automatically the moment it's first registered
   (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false` — overrides Airflow's own
   default, which is paused-by-default). This only applies at first registration,
   not retroactively — if a DAG was already registered while paused (e.g. before
   this setting existed), unpause it once manually:
   ```bash
   docker compose exec airflow-api-server airflow dags unpause bronze_ingestion_dag
   # ... one per DAG, or via the UI at http://localhost:8090
   ```
6. **Airflow health**:
   ```bash
   curl http://localhost:8090/api/v2/monitor/health
   # expect: metadatabase/scheduler/dag_processor all "healthy"; triggerer is null by design (none is run)
   ```
7. **Grafana datasources actually connected** (not just "provisioned"):
   ```bash
   curl -u admin:$GRAFANA_ADMIN_PASSWORD http://localhost:3000/api/datasources/uid/iot_postgres/health
   curl -u admin:$GRAFANA_ADMIN_PASSWORD http://localhost:3000/api/datasources/uid/loki/health
   ```
8. **dbt**, once silver has real data (after the batch DAGs have run at least once):
   ```bash
   docker compose exec airflow-scheduler bash -c "cd /opt/dbt && dbt snapshot && dbt run && dbt test"
   ```

## Stopping / resetting

Three levels, from lightest to most destructive:

```bash
scripts/pause.sh       # stops containers only (docker compose stop) - fastest resume, data preserved
scripts/terminate.sh   # stops AND removes containers/network (docker compose down) - data preserved
scripts/reset.sh       # DESTRUCTIVE - wipes volumes and data-* directories; requires typing "RESET" to confirm
```

`pause.sh` leaves containers in place (they'll show as "Exited" in `docker ps -a` - expected, not a problem) for
the fastest `scripts/start.sh` next time. `terminate.sh` additionally removes the containers and default network
for a clean `docker ps -a`, but still doesn't touch anything under `./data-*` (those are host bind mounts, not
Docker volumes) - `scripts/start.sh` recreates containers from there with no rebuild needed unless a Dockerfile
changed.

## Troubleshooting

Real issues hit and fixed during development — if you see one of these, here's what it was:

- **A container's Docker healthcheck shows `unhealthy` even though the service works fine when you curl it
  manually**: several services' healthchecks were originally written with `curl`, which isn't installed in
  their final images (`config/spark/Dockerfile` and `config/airflow/Dockerfile` deliberately don't keep it
  around past build time). All healthchecks in this repo now use Python (`urllib.request`, always present)
  instead — if you've modified a healthchecks, double-check `curl` is actually available before relying on it.
- **`airflow-api-server` crash-loops** (`INFO: Child process [N] died` repeating): its default
  `--workers 4` (or even 2) exceeds its memory budget on this scale. The compose command is pinned to
  `--workers 1`, which is sufficient for single-user local demo UI traffic.
- **A freshly-created `data-*` directory causes `PermissionError`/`mkdir: Permission denied` on first boot**:
  see the `scripts/init.sh` explanation above. If you deleted and recreated a `data-*` directory by hand
  without re-running `scripts/init.sh`, re-run it.
- **`mkdir of file:/tmp/spark-data/checkpoints/... failed`**: same root cause, specific to Spark's checkpoint
  volume.
- **`CREATE EXTENSION postgis` fails with "extension not available"**: only relevant if you've modified
  `config/postgres/Dockerfile` — the lean Alpine-based `timescale/timescaledb:pg15` image's `apk add postgis`
  is fundamentally incompatible (it always tracks whichever Postgres major is currently default in that Alpine
  release, never Timescale's pinned older one). Use `timescale/timescaledb-ha:pg15` instead, as this repo does.
- **Spark job errors with `ON CONFLICT DO UPDATE command cannot affect row a second time`**: Postgres rejects a
  single `INSERT ... VALUES (...), (...) ON CONFLICT DO UPDATE` if the VALUES list contains more than one row
  for the same conflict key. Deduplicate to the latest value per key before the upsert (already done everywhere
  in this repo's job code — if you add a new upsert path, remember this).
- **A Spark batch job silently gets zero executors and hangs**: check `spark.cores.max` — if too many
  concurrent jobs are requesting cores, the cluster can run out; extra submissions queue at Spark Master rather
  than erroring. See [`docs/ARCHITECTURE.md`](ARCHITECTURE.md#resource-sharing-model-one-shared-spark-cluster).
- **Every single Airflow task fails immediately** (`pid=None`, empty task logs, `httpx.ConnectError` or
  `403 Forbidden` in `docker compose logs airflow-scheduler`): `AIRFLOW_API_SECRET_KEY`/`AIRFLOW_JWT_SECRET` must
  be identical across every Airflow component — if they were manually edited to different values, or `.env` was
  created without running `scripts/init.sh`, every task's internal Execution API call fails auth. Re-run
  `scripts/init.sh` and recreate the Airflow containers.
- **A batch `SparkSubmitOperator` job fails with `Failed to find data source: kafka`**: the Airflow image's
  client-only Spark distribution needs the same checksum-verified Kafka connector jars baked into
  `config/airflow/Dockerfile` as the cluster's own image — if you rebuilt without picking up that layer,
  `docker compose build airflow-scheduler` again.
- **`PYTHON_VERSION_MISMATCH` between driver and executor**: the Spark cluster (Ubuntu Focal base) only has
  Python 3.8; the Airflow image is Python 3.11. `config/airflow/Dockerfile` installs an isolated Python 3.8 venv
  via `uv` used only for `PYSPARK_DRIVER_PYTHON` — don't set `PYSPARK_PYTHON` anywhere (that affects executors,
  which must keep using each worker's own native 3.8).
- **`Initial job has not accepted any resources; check your cluster UI...`**: this is a **memory**, not cores,
  problem if `spark.cores.max` math looks fine — Spark defaults `spark.executor.memory` to 1g if unset, which
  can exceed a worker's total `SPARK_WORKER_MEMORY` once more than one job's executors are running. Every job in
  this repo pins `spark.executor.memory` explicitly (512m — 256m hits Spark's hard ~450MB minimum and crashes
  with `INVALID_EXECUTOR_MEMORY`; the same floor applies to `spark.driver.memory`, `INVALID_DRIVER_MEMORY`).
- **`operator does not exist: uuid = text`**: a `psycopg2` parameter list of plain Python strings against a
  `uuid` column (e.g. `WHERE order_id = ANY(%s)`) — psycopg2 can't infer the array's Postgres type. Cast
  explicitly: `WHERE order_id = ANY(%s::uuid[])`.
- **A `*_to_silver.py` job fails with `[CANNOT_DETERMINE_TYPE]`**: `spark.createDataFrame(...)` with no explicit
  schema infers types from the row data — if every row in one incremental batch happens to have `NULL` in some
  nullable column, inference has nothing to go on. Define an explicit `StructType` instead of relying on
  inference (done for `products_to_silver.py`; the same latent risk still exists in the other `*_to_silver.py`
  jobs — deferred to a later pass).
- **`dbt_snapshot`/`dbt_run` fails with `PermissionError: '/opt/dbt/logs/dbt.log'`**: `scripts/init.sh`'s chmod on
  `./dbt` must be recursive — a non-recursive `chmod 777 dbt` doesn't touch pre-existing subdirectories from an
  earlier host-side `dbt` run (`dbt/logs/`, `dbt/target/`), which stay unwritable by the container's `airflow`
  user. Re-run `scripts/init.sh` (now recursive) or `chmod -R 777 dbt` directly.
- **Kafka topic data silently resets to offset 0 on every container recreate, even though `./data-kafka` is
  bind-mounted** — the single most disruptive bug found in this project, because it produced no error at all:
  every batch ingestion job would eventually fail with `AssertionError: Beginning offset N is after the ending
  offset 0` (a stale `control.kafka_offsets` watermark pointing past a "cluster" that no longer exists), which
  looked like a data-quality/watermark bug but was actually a wrong bind-mount target. Root cause, confirmed by
  directly inspecting the running container (`docker inspect`, `find -newer`, `/proc/1/cmdline`): the
  `apache/kafka:3.7.0` image's *actual* effective `log.dirs` is `/tmp/kafka-logs`, not `/tmp/kraft-combined-logs`
  — the latter is only what an unused template file (`/etc/kafka/docker/server.properties`) shows, not what the
  real generated config (`/opt/kafka/config/server.properties`) or the running broker uses. Every container
  recreate was silently bootstrapping a brand-new empty KRaft cluster in the container's own ephemeral
  filesystem. Fixed by pointing the bind mount at the real path (`./data-kafka:/tmp/kafka-logs`); verified by
  fully removing and recreating the `kafka` container and confirming topic offsets survived intact. Separately,
  `data-kafka` was also missing from `scripts/init.sh`'s permission-fixing chmod list (Kafka's image doesn't
  self-heal ownership the way the Postgres images here do) — fixed too, though it was masked by the mount-path
  bug the whole time (nothing was being written to the mounted path regardless of its permissions).
- **A batch ingestion job's stored Kafka offset watermark is stale** (points past what the topic can currently
  serve — e.g. after the bug above, or genuine retention expiry): `shared_ingestion_utils.py`'s `run_ingestion()`
  catches this specific case (`Py4JJavaError` containing `"is after the ending offset"`), logs a warning, deletes
  the stale `control.kafka_offsets` row, and skips cleanly — the next run re-ingests from `earliest`, which is
  safe since bronze appends are idempotent (`ON CONFLICT DO NOTHING`).
