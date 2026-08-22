#!/bin/bash
# Non-destructive to data, but removes containers: stops and removes every
# container plus the default network (`docker compose down`), while still
# preserving everything under ./data-* (those are host bind mounts, not
# Docker volumes, so `down` alone never touches them).
#
# Use this instead of scripts/pause.sh when you want a clean `docker ps -a`
# with nothing lingering as "Exited". Resuming after this needs
# scripts/start.sh again, which recreates containers (no image rebuild
# needed unless a Dockerfile changed).
#
# For a full wipe (containers + volumes + ./data-* contents), use
# scripts/reset.sh instead - that one is destructive and asks for typed
# confirmation.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo "=== Terminating: stopping and removing all containers (data preserved) ==="
docker compose --profile generator down
echo "=== Terminated. Data preserved in ./data-*. Restart with scripts/start.sh ==="
