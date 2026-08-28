#!/bin/bash
# Generates dbt's docs site, then serves it - one container, one process
# lifecycle. `dbt deps` first for the same reason gold_dbt_dag.py's
# dbt_deps task does: dbt/dbt_packages isn't checked into git and is wiped
# by scripts/reset.sh, so a fresh stack has no installed packages yet.
# Safe to re-run on every container start - dbt docs generate is
# idempotent, and if gold_dbt_dag hasn't produced any gold tables yet, the
# generated catalog is just sparse, not broken.
set -euo pipefail

cd /opt/dbt
dbt deps --profiles-dir /opt/dbt
dbt docs generate --profiles-dir /opt/dbt

# python3's own static server, not nginx - avoids needing two entirely
# different web-server tech stacks reconciled in one image just to serve
# a handful of static files for a local demo tool.
cd target
exec python3 -m http.server 80
