#!/bin/bash
# Least-privilege read-only role for Grafana's airflow_postgres datasource
# (DAG execution status / Airflow metadata DB health panels) - never the
# airflow admin role above, which owns/migrates every Airflow table.
# Runs as a .sh script (not .sql), same reason as
# config/postgres/initdb/03-create-role-and-grants.sh: password comes from
# the container's environment (GRAFANA_AIRFLOW_DB_USER/PASSWORD, set via
# .env), not hardcoded in a committed file.
#
# Only runs on a truly empty data dir (stock postgres image behavior) -
# i.e. a fresh clone or after scripts/reset.sh. Does NOT retroactively
# apply to an already-initialized airflow-postgres; that needs applying
# manually once (see docs/SETUP.md or the Grafana dashboard plan).
set -euo pipefail

: "${GRAFANA_AIRFLOW_DB_USER:?GRAFANA_AIRFLOW_DB_USER must be set}"
: "${GRAFANA_AIRFLOW_DB_PASSWORD:?GRAFANA_AIRFLOW_DB_PASSWORD must be set}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${GRAFANA_AIRFLOW_DB_USER}') THEN
            CREATE ROLE ${GRAFANA_AIRFLOW_DB_USER} LOGIN PASSWORD '${GRAFANA_AIRFLOW_DB_PASSWORD}';
        END IF;
    END
    \$\$;

    GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO ${GRAFANA_AIRFLOW_DB_USER};
    GRANT USAGE ON SCHEMA public TO ${GRAFANA_AIRFLOW_DB_USER};
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${GRAFANA_AIRFLOW_DB_USER};
    -- Airflow's own migrations create new tables over time (upgrades, new
    -- features) - this keeps the read-only grant current without a manual
    -- re-grant after every Airflow version bump.
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ${GRAFANA_AIRFLOW_DB_USER};
EOSQL
