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

# This container writes into ./dbt/{logs,dbt_packages,target}, a bind mount
# also touched by airflow-scheduler's own dbt calls (gold_dbt_dag.py) under
# a different uid (50000 there vs. this image's own build-time uid). Files
# created here would otherwise default to non-world-writable, blocking that
# other uid from later opening them - e.g. PermissionError on
# dbt/logs/dbt.log. Note umask does NOT help here: dbt deps' package
# extraction (dbt_utils) explicitly chmods each extracted file to specific
# bits, which bypasses the process umask entirely - confirmed by testing.
# Explicit chmod after the fact is the only thing that reliably works.
# gold_dbt_dag.py's BashOperator tasks do the same after every dbt call.
chmod -R 777 /opt/dbt/logs /opt/dbt/dbt_packages /opt/dbt/target

# python3's own static server, not nginx - avoids needing two entirely
# different web-server tech stacks reconciled in one image just to serve
# a handful of static files for a local demo tool.
cd target
exec python3 -m http.server 80
