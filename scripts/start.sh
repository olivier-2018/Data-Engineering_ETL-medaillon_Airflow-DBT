#!/bin/bash
#
# Brings the stack up in dependency order with health-waits between stages.
# But:
#  - Does NOT start data-generator - that's an intentional separate step.
#  - DOES start dbt-docs (Stage 8) - fresh docs site available available
#
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [ ! -f .env ]; then
    echo "ERROR: .env not found. Copy .env.example to .env and fill in the required values:" >&2
    echo "  cp .env.example .env" >&2
    exit 1
fi

wait_healthy() {
    local service="$1"
    local retries=30
    echo "Waiting for ${service} to become healthy ..."
    until [ "$(docker compose ps -q "$service" | xargs -r docker inspect -f '{{.State.Health.Status}}' 2>/dev/null)" = "healthy" ]; do
        retries=$((retries - 1))
        if [ "$retries" -le 0 ]; then
            echo "ERROR: ${service} did not become healthy in time." >&2
            docker compose logs --tail=50 "$service" >&2
            exit 1
        fi
        sleep 5
    done
    echo "${service} is healthy."
}

echo "=== Stage 1: Kafka + Postgres (iot data) ==="
# --build here also covers kafka-init (config/kafka/'s own Dockerfile):
# kafka-init is never named directly anywhere in this script (only reached
# transitively via kafka-ui's/data-generator's depends_on), and --build only
# rebuilds the services EXPLICITLY named in its own command - a dependency
# pulled in implicitly is not rebuilt just because the service that depends
# on it was. Building it explicitly here, before it's needed, forces a
# rebuild if config/kafka/ changed rather than silently running a stale
# image (confirmed as the same bug class that let a data-generator fix sit
# unbuilt for two days - see docs/SETUP.md).
docker compose build kafka-init
docker compose up -d --build kafka postgres
wait_healthy kafka
wait_healthy postgres

echo "=== Stage 2: Kafka UI ==="
docker compose up -d --build kafka-ui

echo "=== Stage 3: Spark cluster ==="
docker compose up -d --build spark-master
wait_healthy spark-master
docker compose up -d --build spark-worker-1 spark-worker-2
wait_healthy spark-worker-1
wait_healthy spark-worker-2

echo "=== Stage 4: Persistent truck-position streaming job ==="
# The one persistent Spark Structured Streaming job - not an Airflow-managed
# job (Spark Standalone can't cluster-deploy Python apps - see
# docs/ARCHITECTURE.md), its own always-on docker-compose service instead.
docker compose up -d --build spark-streaming-truck-position

echo "=== Stage 5: Airflow metadata DB + migration ==="
docker compose up -d airflow-postgres
wait_healthy airflow-postgres
docker compose up --build airflow-init

echo "=== Stage 6: Airflow components ==="
docker compose up -d --build airflow-api-server airflow-scheduler airflow-dag-processor
wait_healthy airflow-api-server

echo "=== Stage 7: Observability (Grafana + Loki/Promtail) ==="
docker compose up -d grafana loki promtail

echo "=== Stage 8: dbt docs (generates fresh docs, then serves them) ==="
docker compose up -d --build dbt-docs

cat <<EOF

=== Stack is up ===
Kafka UI:           http://localhost:8089
Airflow UI:         http://localhost:8090
Spark Master UI:    http://localhost:8080
Spark Worker 1 UI:  http://localhost:8081
Spark Worker 2 UI:  http://localhost:8082
Streaming job UI:   http://localhost:4040  (truck-position job's own SparkUI - up for as long as the job runs)
dbt docs:           http://localhost:9000
Grafana:            http://localhost:3000

To get Airflow credentials:
  docker compose exec airflow-api-server cat /opt/airflow/simple_auth_manager_passwords.json.generated

To start generating synthetic data:
  docker compose --profile generator up -d --build data-generator

To stop the generator only:
  docker compose stop data-generator

To start per-container CPU/memory metrics (cAdvisor + Prometheus, feeds Grafana's
"prometheus" datasource) - not started above, opt-in:
  docker compose up -d cadvisor prometheus
  cAdvisor:    http://localhost:8085
  Prometheus:  http://localhost:9090
EOF
