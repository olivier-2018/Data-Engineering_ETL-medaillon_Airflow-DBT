#!/bin/bash
# One-time project setup. Idempotent - safe to re-run.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo "=== IoT Logistics Pipeline: init ==="

if [ ! -f .env ]; then
    echo "Creating .env from .env.example ..."
    cp .env.example .env
else
    echo ".env already exists - leaving it untouched."
fi

# Generate a Fernet key if the placeholder is still empty.
if grep -q '^AIRFLOW_FERNET_KEY=$' .env 2>/dev/null; then
    echo "Generating AIRFLOW_FERNET_KEY ..."
    FERNET_KEY=$(python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())")
    # macOS/BSD sed vs GNU sed both accept this form with an empty backup suffix given separately.
    sed -i.bak "s|^AIRFLOW_FERNET_KEY=$|AIRFLOW_FERNET_KEY=${FERNET_KEY}|" .env
    rm -f .env.bak
fi

# Generate the shared API secret key / JWT secret if still empty. These must be
# identical across every Airflow component (scheduler signs each task's internal
# Execution API JWT with these, airflow-api-server verifies it) - left as
# independent per-container defaults, every task fails with 403 Forbidden.
if grep -q '^AIRFLOW_API_SECRET_KEY=$' .env 2>/dev/null; then
    echo "Generating AIRFLOW_API_SECRET_KEY ..."
    API_SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
    sed -i.bak "s|^AIRFLOW_API_SECRET_KEY=$|AIRFLOW_API_SECRET_KEY=${API_SECRET_KEY}|" .env
    rm -f .env.bak
fi
if grep -q '^AIRFLOW_JWT_SECRET=$' .env 2>/dev/null; then
    echo "Generating AIRFLOW_JWT_SECRET ..."
    JWT_SECRET=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
    sed -i.bak "s|^AIRFLOW_JWT_SECRET=$|AIRFLOW_JWT_SECRET=${JWT_SECRET}|" .env
    rm -f .env.bak
fi

echo "Creating data-* host directories ..."
mkdir -p data-postgres data-airflow-postgres data-airflow-logs data-kafka \
         data-spark-logs data-spark-master data-spark-worker-1 data-spark-worker-2 \
         data-spark-checkpoints data-grafana data-loki

# Every container that writes into a bind-mounted host directory here runs
# as a different, non-host-matching uid/gid (Airflow: uid=50000 gid=0;
# Spark: uid=185 gid=185; etc.) - a freshly-created host directory is
# owner-only-writable by default, which causes real, confirmed-by-testing
# PermissionErrors ("/opt/airflow/logs/dag_processor",
# "mkdir of file:/tmp/spark-data/checkpoints/truck_position failed").
# World-writable is the simplest fix that works regardless of which
# container's uid/gid ends up writing here - acceptable for a local,
# single-user demo where these are just data/log directories, not code.
chmod 777 data-airflow-logs data-spark-logs data-spark-master \
          data-spark-worker-1 data-spark-worker-2 data-spark-checkpoints \
          data-grafana data-loki dbt
# Recursive: a non-recursive chmod on ./dbt alone doesn't touch pre-existing
# subdirectories/files from an earlier host-side `dbt` run (e.g. dbt/logs/,
# dbt/target/) - those stay at their original 755/644 and are just as
# unwritable to the container's airflow user, causing
# PermissionError: [Errno 13] '/opt/dbt/logs/dbt.log' the first time
# `dbt snapshot`/`run`/`test` runs inside airflow-scheduler (confirmed by
# testing).
chmod -R 777 dbt

if [ ! -w dbt ]; then
    echo "WARNING: could not make ./dbt world-writable (owned by another user?)." >&2
    echo "         dbt run/snapshot/test inside airflow-scheduler may fail with a" >&2
    echo "         PermissionError writing target/ or logs/ until this is fixed." >&2
fi

if grep -q '^OPENWEATHERMAP_API_KEY=$' .env 2>/dev/null; then
    echo
    echo "NOTE: OPENWEATHERMAP_API_KEY is empty in .env."
    echo "      Sign up at https://openweathermap.org/api and fill it in before"
    echo "      relying on weather_enrichment_dag - this script cannot generate"
    echo "      that value for you."
fi

echo
echo "=== init complete. Review .env, then run scripts/start.sh ==="
