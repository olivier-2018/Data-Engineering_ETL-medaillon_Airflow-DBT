#!/bin/bash
# Non-destructive, fastest resume: stops all containers but does NOT remove
# them (they - and the network, and every ./data-* directory - stay exactly
# as they are). `docker ps -a` will still list them as "Exited", which is
# expected, not a sign anything went wrong. Restart with scripts/start.sh -
# no rebuild needed, all accumulated demo data survives.
#
# For a fuller teardown that also removes the containers/network (but still
# keeps ./data-*), use scripts/terminate.sh instead.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo "=== Pausing: stopping all containers (data preserved) ==="
docker compose --profile generator stop
echo "=== Paused. Data preserved in ./data-*. Resume with scripts/start.sh ==="
