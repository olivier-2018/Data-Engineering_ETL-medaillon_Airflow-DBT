#!/bin/bash
# Creates the least-privilege pipeline_rw role (Spark/dbt/Airflow) and the
# generator_ro role (data_generators' startup DB-resume reads) - never the
# postgres superuser. Runs as a .sh script (not .sql) specifically so
# passwords can come from the container's environment (PIPELINE_DB_USER/
# PASSWORD, GENERATOR_DB_USER/PASSWORD, all set via .env) instead of being
# hardcoded in a committed file.
set -euo pipefail

: "${PIPELINE_DB_USER:?PIPELINE_DB_USER must be set}"
: "${PIPELINE_DB_PASSWORD:?PIPELINE_DB_PASSWORD must be set}"
: "${GENERATOR_DB_USER:?GENERATOR_DB_USER must be set}"
: "${GENERATOR_DB_PASSWORD:?GENERATOR_DB_PASSWORD must be set}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${PIPELINE_DB_USER}') THEN
            CREATE ROLE ${PIPELINE_DB_USER} LOGIN PASSWORD '${PIPELINE_DB_PASSWORD}';
        END IF;
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${GENERATOR_DB_USER}') THEN
            CREATE ROLE ${GENERATOR_DB_USER} LOGIN PASSWORD '${GENERATOR_DB_PASSWORD}';
        END IF;
    END
    \$\$;

    -- ============================= pipeline_rw ==========================
    GRANT USAGE ON SCHEMA iot, silver, gold, control, reference TO ${PIPELINE_DB_USER};
    -- gold needs CREATE (not just USAGE) because dbt dynamically creates
    -- new tables/views there on every run/snapshot - unlike silver/reference,
    -- whose tables are all pre-created by 01-init-schema.sql/
    -- 02-seed-reference-data.sql and only ever written to, never CREATEd,
    -- by the Spark jobs.
    GRANT CREATE ON SCHEMA gold TO ${PIPELINE_DB_USER};
    GRANT SELECT ON ALL TABLES IN SCHEMA iot TO ${PIPELINE_DB_USER};
    GRANT INSERT ON iot.product_events, iot.customer_events, iot.purchase_order_events,
        iot.product_on_order_events, iot.invoice_events, iot.inventory_changes,
        iot.truck_fleet_events, iot.truck_position_events, iot.weather_observations
        TO ${PIPELINE_DB_USER};
    -- Deliberately no UPDATE grant on iot.* - bronze is append-only.

    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA silver TO ${PIPELINE_DB_USER};
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA gold TO ${PIPELINE_DB_USER};
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA control TO ${PIPELINE_DB_USER};
    -- reference.* gets SELECT+UPDATE only (not INSERT/DELETE) - rows are
    -- seeded once by 02-seed-reference-data.sql; the only mutation this
    -- role ever needs is toggling is_active (e.g. activating more delivery
    -- cities later) via a plain SQL UPDATE, not adding/removing rows.
    GRANT SELECT, UPDATE ON ALL TABLES IN SCHEMA reference TO ${PIPELINE_DB_USER};
    GRANT USAGE ON ALL SEQUENCES IN SCHEMA silver TO ${PIPELINE_DB_USER};
    ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ${PIPELINE_DB_USER};
    ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT USAGE ON SEQUENCES TO ${PIPELINE_DB_USER};

    -- ============================= generator_ro =========================
    -- data_generators' only DB connection: read-only, and only against
    -- silver + reference - never iot (bronze), which it only ever writes to
    -- indirectly via Kafka. Used for startup state-resume reads (existing
    -- customers/open orders/invoices/fleet/stock/active zones - see the
    -- redesign plan §3/§3c) plus a handful of live, one-shot per-tick
    -- orphan-avoidance checks (db.py's customer_exists_in_silver() /
    -- purchase_order_ids_in_silver() / paid_purchase_order_ids_in_silver() -
    -- see TODO.md's "avoid orphans" items) - never a blocking dependency,
    -- just a cheap SELECT the caller skips its action on if negative.
    GRANT USAGE ON SCHEMA silver, reference TO ${GENERATOR_DB_USER};
    GRANT SELECT ON ALL TABLES IN SCHEMA silver TO ${GENERATOR_DB_USER};
    GRANT SELECT ON ALL TABLES IN SCHEMA reference TO ${GENERATOR_DB_USER};
EOSQL
