#!/bin/bash
# Brings the stack up in dependency order with health-waits between stages.
# Does NOT start data-generator - that's an intentional separate step
# (`docker compose up -d data-generator --profile generator`) so synthetic
# load is something you choose to turn on, not something that happens implicitly.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

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

cat <<EOF

=== Stack is up ===
Kafka UI:           http://localhost:8089
Airflow UI:         http://localhost:8090
Spark Master UI:    http://localhost:8080
Spark Worker 1 UI:  http://localhost:8081
Spark Worker 2 UI:  http://localhost:8082
dbt docs:           http://localhost:9000
Grafana:            http://localhost:3000

To get Airflow credentials:
  docker compose exec airflow-api-server cat /opt/airflow/simple_auth_manager_passwords.json.generated

To start generating synthetic data:
  docker compose --profile generator up -d data-generator

To generate and visualize dbt docs:
  docker compose --profile documentation up -d dbt-docs

To stop the generator only:
  docker compose stop data-generator
EOF
