-- IoT Logistics Pipeline - schema bootstrap
-- Runs once, automatically, on first container start (mounted at
-- /docker-entrypoint-initdb.d/init.sql). Re-running requires a fresh volume.

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;

-- =====================================================================
-- SCHEMA: iot (bronze - immutable, append-only event logs)
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS iot;

-- --- Reference data: products -----------------------------------------
-- Append-only event log is the source of truth; iot.products is a
-- convenience view over the latest event per product_id.
CREATE TABLE iot.product_events (
    event_id        UUID PRIMARY KEY,
    product_id      UUID NOT NULL,
    event_type      VARCHAR(20) NOT NULL,  -- created, updated
    name            VARCHAR(200) NOT NULL,
    category        VARCHAR(100) NOT NULL,
    subcategory     VARCHAR(100),
    unit_price      DECIMAL(10,2) NOT NULL,
    weight_kg       DECIMAL(6,2),
    initial_stock   INT NOT NULL,
    event_at        TIMESTAMP NOT NULL,
    ingested_at     TIMESTAMP NOT NULL
);
CREATE INDEX idx_product_events_product_time ON iot.product_events(product_id, event_at DESC);
CREATE INDEX idx_product_events_ingested ON iot.product_events(ingested_at DESC);

CREATE VIEW iot.products AS
SELECT DISTINCT ON (product_id)
    product_id, name, category, subcategory, unit_price, weight_kg, initial_stock, event_at AS created_at
FROM iot.product_events
ORDER BY product_id, event_at DESC;

-- --- Reference data: customers -----------------------------------------
CREATE TABLE iot.customer_events (
    event_id        UUID PRIMARY KEY,
    customer_id     UUID NOT NULL,
    event_type      VARCHAR(20) NOT NULL,  -- created, updated
    name            VARCHAR(200) NOT NULL,
    country         VARCHAR(2) NOT NULL,   -- CH, FR, DE, IT
    city            VARCHAR(100),
    segment         VARCHAR(50),
    event_at        TIMESTAMP NOT NULL,
    ingested_at     TIMESTAMP NOT NULL
);
CREATE INDEX idx_customer_events_customer_time ON iot.customer_events(customer_id, event_at DESC);
CREATE INDEX idx_customer_events_ingested ON iot.customer_events(ingested_at DESC);

CREATE VIEW iot.customers AS
SELECT DISTINCT ON (customer_id)
    customer_id, name, country, city, segment, event_at AS created_at
FROM iot.customer_events
ORDER BY customer_id, event_at DESC;

-- --- Sales orders (event log, hypertable) -------------------------------
CREATE TABLE iot.sales_order_events (
    event_id             UUID NOT NULL,
    order_id             UUID NOT NULL,
    customer_id          UUID NOT NULL,
    product_id           UUID NOT NULL,
    quantity             INT NOT NULL,
    unit_price_snapshot  DECIMAL(10,2) NOT NULL,
    order_status         VARCHAR(20) NOT NULL,  -- pending, confirmed, picking, ready_for_dispatch, shipped, delivered, cancelled
    created_at           TIMESTAMP NOT NULL,     -- order's original creation time, carried on every event
    event_at             TIMESTAMP NOT NULL,
    ingested_at          TIMESTAMP NOT NULL,
    PRIMARY KEY (event_id, event_at)
);
SELECT create_hypertable('iot.sales_order_events', 'event_at');
CREATE INDEX idx_sales_order_events_order_time ON iot.sales_order_events(order_id, event_at DESC);
CREATE INDEX idx_sales_order_events_ingested ON iot.sales_order_events(ingested_at DESC);

-- --- Payments (event log, hypertable) -----------------------------------
CREATE TABLE iot.payment_events (
    event_id        UUID NOT NULL,
    order_id        UUID NOT NULL,
    payment_status  VARCHAR(20) NOT NULL,  -- authorized, captured, failed, refunded
    amount          DECIMAL(10,2) NOT NULL,
    event_at        TIMESTAMP NOT NULL,
    ingested_at     TIMESTAMP NOT NULL,
    PRIMARY KEY (event_id, event_at)
);
SELECT create_hypertable('iot.payment_events', 'event_at');
CREATE INDEX idx_payment_events_order_time ON iot.payment_events(order_id, event_at DESC);
CREATE INDEX idx_payment_events_ingested ON iot.payment_events(ingested_at DESC);

-- --- Inventory changes (event log, hypertable) --------------------------
CREATE TABLE iot.inventory_changes (
    event_id         UUID NOT NULL,
    product_id       UUID NOT NULL,
    quantity_delta   INT NOT NULL,
    current_stock    INT NOT NULL,
    change_reason    VARCHAR(50) NOT NULL,  -- purchase, sale, adjustment, return, restock
    changed_at       TIMESTAMP NOT NULL,
    ingested_at      TIMESTAMP NOT NULL,
    PRIMARY KEY (event_id, changed_at)
);
SELECT create_hypertable('iot.inventory_changes', 'changed_at');
CREATE INDEX idx_inventory_changes_product_time ON iot.inventory_changes(product_id, changed_at DESC);
CREATE INDEX idx_inventory_changes_ingested ON iot.inventory_changes(ingested_at DESC);

-- --- Truck position events (event log, hypertable, high-frequency) ------
CREATE TABLE iot.truck_position_events (
    event_id         UUID NOT NULL,
    shipment_id      UUID NOT NULL,
    truck_id         VARCHAR(20) NOT NULL,
    order_id         UUID NOT NULL,
    geog             GEOGRAPHY(POINT, 4326) NOT NULL,
    shipment_status  VARCHAR(20) NOT NULL,  -- ready_for_dispatch, loading, in_transit, delivered
    destination_country VARCHAR(2) NOT NULL,
    event_at         TIMESTAMP NOT NULL,
    ingested_at      TIMESTAMP NOT NULL,
    PRIMARY KEY (event_id, event_at)
);
SELECT create_hypertable('iot.truck_position_events', 'event_at');
CREATE INDEX idx_truck_position_shipment_time ON iot.truck_position_events(shipment_id, event_at DESC);
CREATE INDEX idx_truck_position_geog ON iot.truck_position_events USING GIST(geog);
CREATE INDEX idx_truck_position_ingested ON iot.truck_position_events(ingested_at DESC);

-- --- Weather observations (Airflow-populated, hypertable) ---------------
CREATE TABLE iot.weather_observations (
    id                     BIGSERIAL,
    location_name          VARCHAR(100) NOT NULL,
    lat                    DOUBLE PRECISION NOT NULL,
    lon                    DOUBLE PRECISION NOT NULL,
    observed_at            TIMESTAMP NOT NULL,
    temperature            DECIMAL(5,2),
    humidity               DECIMAL(5,2),
    pressure               DECIMAL(7,2),
    weather_condition      VARCHAR(100),
    forecast_horizon_hours INT,             -- null = current observation, else forecast lead time
    source                 VARCHAR(50) NOT NULL DEFAULT 'openweathermap',
    ingested_at            TIMESTAMP NOT NULL,
    PRIMARY KEY (id, observed_at)
);
SELECT create_hypertable('iot.weather_observations', 'observed_at');
CREATE INDEX idx_weather_obs_location_time ON iot.weather_observations(location_name, observed_at DESC);

-- =====================================================================
-- SCHEMA: silver (validated, deduplicated, current-state + error tables)
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE silver.customers_current (
    customer_id  UUID PRIMARY KEY,
    name         VARCHAR(200) NOT NULL,
    country      VARCHAR(2) NOT NULL,
    city         VARCHAR(100),
    segment      VARCHAR(50),
    updated_at   TIMESTAMP NOT NULL
);

CREATE TABLE silver.products_current (
    product_id     UUID PRIMARY KEY,
    name           VARCHAR(200) NOT NULL,
    category       VARCHAR(100) NOT NULL,
    subcategory    VARCHAR(100),
    unit_price     DECIMAL(10,2) NOT NULL,
    weight_kg      DECIMAL(6,2),
    initial_stock  INT NOT NULL,
    updated_at     TIMESTAMP NOT NULL
);

CREATE TABLE silver.sales_orders_current (
    order_id             UUID PRIMARY KEY,
    customer_id          UUID NOT NULL,
    product_id           UUID NOT NULL,
    quantity             INT NOT NULL,
    unit_price_snapshot  DECIMAL(10,2) NOT NULL,
    order_status         VARCHAR(20) NOT NULL,
    created_at           TIMESTAMP NOT NULL,
    updated_at           TIMESTAMP NOT NULL
);

CREATE TABLE silver.payments_current (
    order_id        UUID PRIMARY KEY,
    payment_status  VARCHAR(20) NOT NULL,
    amount          DECIMAL(10,2) NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE silver.inventory_current (
    product_id     UUID PRIMARY KEY,
    current_stock  INT NOT NULL,
    updated_at     TIMESTAMP NOT NULL
);

CREATE TABLE silver.inventory_history (
    event_id        UUID PRIMARY KEY,
    product_id      UUID NOT NULL,
    quantity_delta  INT NOT NULL,
    current_stock   INT NOT NULL,
    change_reason   VARCHAR(50) NOT NULL,
    changed_at      TIMESTAMP NOT NULL
);
CREATE INDEX idx_silver_inventory_history_product ON silver.inventory_history(product_id, changed_at DESC);

CREATE TABLE silver.truck_positions_current (
    shipment_id          UUID PRIMARY KEY,
    truck_id             VARCHAR(20) NOT NULL,
    order_id             UUID NOT NULL,
    geog                 GEOGRAPHY(POINT, 4326) NOT NULL,
    shipment_status      VARCHAR(20) NOT NULL,
    destination_country  VARCHAR(2) NOT NULL,
    updated_at           TIMESTAMP NOT NULL
);
CREATE INDEX idx_silver_truck_positions_geog ON silver.truck_positions_current USING GIST(geog);

CREATE TABLE silver.weather_observations (
    id                     BIGSERIAL PRIMARY KEY,
    location_name          VARCHAR(100) NOT NULL,
    lat                    DOUBLE PRECISION NOT NULL,
    lon                    DOUBLE PRECISION NOT NULL,
    observed_at            TIMESTAMP NOT NULL,
    temperature            DECIMAL(5,2),
    humidity               DECIMAL(5,2),
    pressure               DECIMAL(7,2),
    weather_condition      VARCHAR(100),
    forecast_horizon_hours INT
);

-- Error tables (one per domain) - rejected rows with a reason code
CREATE TABLE silver.error_customers (id BIGSERIAL PRIMARY KEY, raw_payload JSONB, reason_code VARCHAR(100), rejected_at TIMESTAMP NOT NULL DEFAULT now());
CREATE TABLE silver.error_products (id BIGSERIAL PRIMARY KEY, raw_payload JSONB, reason_code VARCHAR(100), rejected_at TIMESTAMP NOT NULL DEFAULT now());
CREATE TABLE silver.error_sales_orders (id BIGSERIAL PRIMARY KEY, raw_payload JSONB, reason_code VARCHAR(100), rejected_at TIMESTAMP NOT NULL DEFAULT now());
CREATE TABLE silver.error_payments (id BIGSERIAL PRIMARY KEY, raw_payload JSONB, reason_code VARCHAR(100), rejected_at TIMESTAMP NOT NULL DEFAULT now());
CREATE TABLE silver.error_inventory_changes (id BIGSERIAL PRIMARY KEY, raw_payload JSONB, reason_code VARCHAR(100), rejected_at TIMESTAMP NOT NULL DEFAULT now());
CREATE TABLE silver.error_truck_positions (id BIGSERIAL PRIMARY KEY, raw_payload JSONB, reason_code VARCHAR(100), rejected_at TIMESTAMP NOT NULL DEFAULT now());

-- =====================================================================
-- SCHEMA: gold (schema + grants only - dbt materializes tables here)
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS gold;

-- =====================================================================
-- SCHEMA: control (watermarks + Kafka offset tracking)
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS control;

-- Bronze -> silver incremental watermark (Postgres-to-Postgres jobs, §7)
CREATE TABLE control.watermarks (
    table_name                 VARCHAR(100) PRIMARY KEY,
    last_processed_ingested_at TIMESTAMP NOT NULL,
    updated_at                 TIMESTAMP NOT NULL DEFAULT now()
);
INSERT INTO control.watermarks (table_name, last_processed_ingested_at) VALUES
    ('sales_order_events', '1970-01-01'),
    ('payment_events', '1970-01-01'),
    ('inventory_changes', '1970-01-01'),
    ('truck_position_events', '1970-01-01'),
    ('customer_events', '1970-01-01'),
    ('product_events', '1970-01-01');

-- Kafka -> bronze offset tracking (periodic batch ingestion jobs, §6a)
CREATE TABLE control.kafka_offsets (
    topic       VARCHAR(200) NOT NULL,
    partition   INT NOT NULL,
    last_offset BIGINT NOT NULL,
    updated_at  TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition)
);

-- Role creation + grants deliberately live in 02-create-role-and-grants.sh,
-- not here: that script substitutes the pipeline_rw password from the
-- container's environment, whereas a plain .sql file mounted into
-- /docker-entrypoint-initdb.d gets no env-var substitution from the
-- postgres entrypoint (only .sh scripts do) - hardcoding a password here
-- would mean committing a secret to the repo.
