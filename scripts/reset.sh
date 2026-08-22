#!/bin/bash
# DESTRUCTIVE: removes all containers, volumes, and the ./data-* directories -
# every bit of accumulated demo data (bronze/silver/gold, Airflow metadata,
# Kafka topics, Grafana dashboard state) is deleted. Requires typed
# confirmation, not just a [y/N] default-yes, given the blast radius.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo "=== IoT Logistics Pipeline: reset ==="
echo "This will PERMANENTLY DELETE:"
echo "  - All containers and Docker volumes for this project"
echo "  - Everything under ./data-* (postgres, kafka, spark checkpoints, grafana, ...)"
echo
read -r -p "Type RESET to confirm: " confirmation
if [ "$confirmation" != "RESET" ]; then
    echo "Aborted - no changes made."
    exit 1
fi

echo "Bringing the stack down and removing volumes ..."
docker compose --profile generator down -v

echo "Clearing ./data-* directories ..."
rm -rf data-postgres data-airflow-postgres data-airflow-logs data-kafka \
       data-spark-logs data-spark-master data-spark-worker-1 data-spark-worker-2 \
       data-spark-checkpoints data-grafana data-loki

echo
echo "=== Reset complete. Run scripts/init.sh then scripts/start.sh for a fresh run. ==="
