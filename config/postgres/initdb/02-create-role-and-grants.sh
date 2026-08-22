#!/bin/bash
# Creates the least-privilege pipeline_rw role used by Spark/dbt/Airflow
# connections (never the postgres superuser). Runs as a .sh script (not
# .sql) specifically so the password can come from the container's
# environment (PIPELINE_DB_USER/PIPELINE_DB_PASSWORD, set via .env) instead
# of being hardcoded in a committed file.
set -euo pipefail

: "${PIPELINE_DB_USER:?PIPELINE_DB_USER must be set}"
: "${PIPELINE_DB_PASSWORD:?PIPELINE_DB_PASSWORD must be set}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${PIPELINE_DB_USER}') THEN
            CREATE ROLE ${PIPELINE_DB_USER} LOGIN PASSWORD '${PIPELINE_DB_PASSWORD}';
        END IF;
    END
    \$\$;

    GRANT USAGE ON SCHEMA iot, silver, gold, control TO ${PIPELINE_DB_USER};
    -- gold needs CREATE (not just USAGE) because dbt dynamically creates
    -- new tables/views there on every run/snapshot - unlike silver, whose
    -- tables are all pre-created by 01-init-schema.sql and only ever
    -- written to, never CREATEd, by the Spark jobs.
    GRANT CREATE ON SCHEMA gold TO ${PIPELINE_DB_USER};
    GRANT SELECT ON ALL TABLES IN SCHEMA iot TO ${PIPELINE_DB_USER};
    GRANT INSERT ON iot.product_events, iot.customer_events, iot.sales_order_events,
        iot.payment_events, iot.inventory_changes, iot.truck_position_events,
        iot.weather_observations TO ${PIPELINE_DB_USER};
    -- Deliberately no UPDATE grant on iot.* - bronze is append-only.

    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA silver TO ${PIPELINE_DB_USER};
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA gold TO ${PIPELINE_DB_USER};
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA control TO ${PIPELINE_DB_USER};
    GRANT USAGE ON ALL SEQUENCES IN SCHEMA silver TO ${PIPELINE_DB_USER};
    ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ${PIPELINE_DB_USER};
    ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT USAGE ON SEQUENCES TO ${PIPELINE_DB_USER};
EOSQL
