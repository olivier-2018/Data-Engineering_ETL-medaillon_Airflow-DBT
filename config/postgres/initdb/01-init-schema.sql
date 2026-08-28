-- IoT Logistics Pipeline - schema bootstrap
-- Runs once, automatically, on first container start (mounted at
-- /docker-entrypoint-initdb.d/init.sql). Re-running requires a fresh volume.
--
-- Business-logic redesign (registration -> purchase order -> invoice ->
-- consolidated truck delivery -> restock). See TODO_improve_business_logic.md
-- for the source spec and the plan file for the full design rationale.

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;

-- =====================================================================
-- SCHEMA: reference (static/lookup data - not event-sourced, not a
-- generator hyperparameter; seeded directly via 03-seed-reference-data.sql)
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS reference;

CREATE TABLE reference.delivery_zones (
    zone_id      SERIAL PRIMARY KEY,
    city         VARCHAR(100) NOT NULL,
    canton       VARCHAR(10),
    country      VARCHAR(2) NOT NULL DEFAULT 'CH',
    lat          DOUBLE PRECISION NOT NULL,
    lon          DOUBLE PRECISION NOT NULL,
    is_warehouse BOOLEAN NOT NULL DEFAULT false,
    is_active    BOOLEAN NOT NULL DEFAULT true,
    created_at   TIMESTAMP NOT NULL DEFAULT now(),
    updated_at   TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX idx_delivery_zones_active ON reference.delivery_zones(is_active);

CREATE TABLE reference.product_categories (
    category_id   SERIAL PRIMARY KEY,
    name          VARCHAR(100) NOT NULL UNIQUE,
    subcategories TEXT[] NOT NULL DEFAULT '{}'  -- native Postgres array, not a CSV string
);

CREATE TABLE reference.weather_stations (
    station_id SERIAL PRIMARY KEY,
    name       VARCHAR(100) NOT NULL,
    lat        DOUBLE PRECISION NOT NULL,
    lon        DOUBLE PRECISION NOT NULL
);

-- =====================================================================
-- SCHEMA: iot (bronze - immutable, append-only event logs)
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS iot;

-- --- Reference data: products -----------------------------------------
-- Append-only event log is the source of truth; iot.products is a
-- convenience view over the latest event per product_id (raw debug view -
-- silver.products_current is the business-authoritative current state).
CREATE TABLE iot.product_events (
    event_id           UUID PRIMARY KEY,
    product_id         UUID NOT NULL,
    event_type         VARCHAR(20) NOT NULL,  -- created, updated, refill
    name               VARCHAR(200) NOT NULL,
    brand              VARCHAR(100),
    model              VARCHAR(100),
    category           VARCHAR(100) NOT NULL,
    subcategory        VARCHAR(100),
    unit_price         DECIMAL(10,2) NOT NULL,
    weight_kg          DECIMAL(6,2),
    nominal_capacity   INT NOT NULL,          -- max qty the warehouse holds for this product
    refill_qty         INT,                   -- only set on event_type='refill'
    refill_unit_price  DECIMAL(10,2),         -- only set on event_type='refill' (40% below unit_price)
    event_at           TIMESTAMP NOT NULL,
    ingested_at        TIMESTAMP NOT NULL
);
CREATE INDEX idx_product_events_product_time ON iot.product_events(product_id, event_at DESC);
CREATE INDEX idx_product_events_ingested ON iot.product_events(ingested_at DESC);

CREATE VIEW iot.products AS
SELECT DISTINCT ON (product_id)
    product_id, name, brand, model, category, subcategory, unit_price, weight_kg,
    nominal_capacity, event_at AS created_at
FROM iot.product_events
ORDER BY product_id, event_at DESC;

-- --- Reference data: customers -----------------------------------------
CREATE TABLE iot.customer_events (
    event_id          UUID PRIMARY KEY,
    customer_id       UUID NOT NULL,
    event_type        VARCHAR(20) NOT NULL,  -- created, updated, account_verified, account_disabled, account_enabled
    name              VARCHAR(200) NOT NULL,
    email             VARCHAR(255),
    address           VARCHAR(300),
    tel               VARCHAR(50),
    country           VARCHAR(2) NOT NULL,   -- CH only for this redesign, kept generic for later expansion
    city              VARCHAR(100),
    segment           VARCHAR(50),
    verified_account  BOOLEAN,               -- only meaningful on account_verified events
    disabled_account  BOOLEAN,               -- only meaningful on account_disabled/account_enabled events
    event_at          TIMESTAMP NOT NULL,
    ingested_at       TIMESTAMP NOT NULL
);
CREATE INDEX idx_customer_events_customer_time ON iot.customer_events(customer_id, event_at DESC);
CREATE INDEX idx_customer_events_ingested ON iot.customer_events(ingested_at DESC);

CREATE VIEW iot.customers AS
SELECT DISTINCT ON (customer_id)
    customer_id, name, email, address, tel, country, city, segment, event_at AS created_at
FROM iot.customer_events
ORDER BY customer_id, event_at DESC;

-- --- Purchase orders (event log, hypertable) ----------------------------
-- Header-only: line items (which products/qty) live in product_on_order_events.
CREATE TABLE iot.purchase_order_events (
    event_id             UUID NOT NULL,
    purchase_order_id    UUID NOT NULL,
    customer_id          UUID NOT NULL,
    status               VARCHAR(20) NOT NULL,  -- created, invoiced, paid, on-hold, loaded, in-transit, delivered, closed, cancelled
    delivery_address     VARCHAR(300),
    contact_tel          VARCHAR(50),
    invoice_address      VARCHAR(300),
    vat_number           VARCHAR(50),
    target_delivery_date TIMESTAMP,
    truck_id             VARCHAR(20),           -- set once loaded onto a truck
    zone_id              INT,                   -- reference.delivery_zones.zone_id (dispatch grouping key)
    event_at             TIMESTAMP NOT NULL,
    ingested_at          TIMESTAMP NOT NULL,
    PRIMARY KEY (event_id, event_at)
);
SELECT create_hypertable('iot.purchase_order_events', 'event_at');
CREATE INDEX idx_purchase_order_events_order_time ON iot.purchase_order_events(purchase_order_id, event_at DESC);
CREATE INDEX idx_purchase_order_events_truck ON iot.purchase_order_events(truck_id, event_at DESC);
CREATE INDEX idx_purchase_order_events_ingested ON iot.purchase_order_events(ingested_at DESC);

-- --- Purchase order line items (event log, hypertable) ------------------
CREATE TABLE iot.product_on_order_events (
    event_id            UUID NOT NULL,
    product_on_order_id UUID NOT NULL,
    purchase_order_id   UUID NOT NULL,
    product_id          UUID NOT NULL,
    qty_on_order        INT NOT NULL,
    customer_comment    VARCHAR(500),
    event_at            TIMESTAMP NOT NULL,
    ingested_at         TIMESTAMP NOT NULL,
    PRIMARY KEY (event_id, event_at)
);
SELECT create_hypertable('iot.product_on_order_events', 'event_at');
CREATE INDEX idx_product_on_order_events_po ON iot.product_on_order_events(purchase_order_id, event_at DESC);
CREATE INDEX idx_product_on_order_events_ingested ON iot.product_on_order_events(ingested_at DESC);

-- --- Invoices (event log, hypertable) - folds in what payments used to be
CREATE TABLE iot.invoice_events (
    event_id           UUID NOT NULL,
    invoice_id         UUID NOT NULL,
    purchase_order_id  UUID NOT NULL,
    customer_id        UUID NOT NULL,
    amount             DECIMAL(10,2) NOT NULL,
    status             VARCHAR(20) NOT NULL,  -- created, pending, settled, cancelled
    payment_reminder   INT NOT NULL DEFAULT 0,
    due_at             TIMESTAMP,
    event_at           TIMESTAMP NOT NULL,
    ingested_at        TIMESTAMP NOT NULL,
    PRIMARY KEY (event_id, event_at)
);
SELECT create_hypertable('iot.invoice_events', 'event_at');
CREATE INDEX idx_invoice_events_invoice_time ON iot.invoice_events(invoice_id, event_at DESC);
CREATE INDEX idx_invoice_events_po ON iot.invoice_events(purchase_order_id, event_at DESC);
CREATE INDEX idx_invoice_events_ingested ON iot.invoice_events(ingested_at DESC);

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

-- --- Truck fleet (event log, low-volume - seeded once + rare updates) ---
CREATE TABLE iot.truck_fleet_events (
    event_id    UUID PRIMARY KEY,
    truck_id    VARCHAR(20) NOT NULL,
    event_type  VARCHAR(20) NOT NULL,  -- created, updated
    name        VARCHAR(100) NOT NULL,
    brand       VARCHAR(100),
    model       VARCHAR(100),
    size        VARCHAR(50),
    capacity    INT NOT NULL,          -- number of products the truck can carry
    weight_kg   DECIMAL(8,2),
    event_at    TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP NOT NULL
);
CREATE INDEX idx_truck_fleet_events_truck_time ON iot.truck_fleet_events(truck_id, event_at DESC);

-- --- Truck position events (event log, hypertable, high-frequency) ------
-- A ping is the truck's own position, not one order's - the truck-to-orders
-- manifest is tracked via purchase_order_events.truck_id, not here.
CREATE TABLE iot.truck_position_events (
    event_id        UUID NOT NULL,
    truck_id        VARCHAR(20) NOT NULL,
    geog            GEOGRAPHY(POINT, 4326) NOT NULL,
    truck_status    VARCHAR(20) NOT NULL,  -- free, loading, in_transit, returning
    current_zone_id INT,                   -- reference.delivery_zones.zone_id of the current/nearest stop, if any
    event_at        TIMESTAMP NOT NULL,
    ingested_at     TIMESTAMP NOT NULL,
    PRIMARY KEY (event_id, event_at)
);
SELECT create_hypertable('iot.truck_position_events', 'event_at');
CREATE INDEX idx_truck_position_truck_time ON iot.truck_position_events(truck_id, event_at DESC);
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
-- Tables below are created in dependency order (referenced table first) so
-- each one's FK constraints can sit directly under its own CREATE TABLE
-- instead of in one big block at the end. FKs are silver-only, deliberately
-- not added on iot.* (bronze): bronze is an immutable append-only log
-- ingested independently per-topic (different Kafka partitions/batch
-- cycles), so a child event can legitimately land before its parent event's
-- batch has run - a hard FK there would make ingestion order-fragile for no
-- benefit, since bronze is never queried for referential correctness.
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE silver.customers_current (
    customer_id       UUID PRIMARY KEY,
    name              VARCHAR(200) NOT NULL,
    email             VARCHAR(255) UNIQUE,
    address           VARCHAR(300),
    tel               VARCHAR(50),
    country           VARCHAR(2) NOT NULL,
    city              VARCHAR(100),
    segment           VARCHAR(50),
    verified_account  BOOLEAN NOT NULL DEFAULT false,
    disabled_account  BOOLEAN NOT NULL DEFAULT false,
    created_at        TIMESTAMP NOT NULL,
    updated_at        TIMESTAMP NOT NULL
);

CREATE TABLE silver.products_current (
    product_id        UUID PRIMARY KEY,
    name              VARCHAR(200) NOT NULL,
    brand             VARCHAR(100),
    model             VARCHAR(100),
    category          VARCHAR(100) NOT NULL,
    subcategory       VARCHAR(100),
    unit_price        DECIMAL(10,2) NOT NULL,
    weight_kg         DECIMAL(6,2),
    nominal_capacity  INT NOT NULL,
    restock_required  BOOLEAN NOT NULL DEFAULT false,  -- materialized: current_stock < threshold_pct * nominal_capacity
    updated_at        TIMESTAMP NOT NULL
);

-- --- truck_fleet_current created before purchase_orders_current: the
-- latter's truck_id FK (below) needs this table to already exist.
CREATE TABLE silver.truck_fleet_current (
    truck_id    VARCHAR(20) PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    brand       VARCHAR(100),
    model       VARCHAR(100),
    size        VARCHAR(50),
    capacity    INT NOT NULL,
    weight_kg   DECIMAL(8,2),
    updated_at  TIMESTAMP NOT NULL
);

CREATE TABLE silver.purchase_orders_current (
    purchase_order_id    UUID PRIMARY KEY,
    customer_id          UUID NOT NULL,
    status               VARCHAR(20) NOT NULL,
    delivery_address     VARCHAR(300),
    contact_tel          VARCHAR(50),
    invoice_address      VARCHAR(300),
    vat_number           VARCHAR(50),
    target_delivery_date TIMESTAMP,
    truck_id             VARCHAR(20),
    zone_id              INT,  -- reference.delivery_zones.zone_id (dispatch grouping key, resumed as-is on restart)
    created_at           TIMESTAMP NOT NULL,
    updated_at           TIMESTAMP NOT NULL
);
CREATE INDEX idx_silver_purchase_orders_truck ON silver.purchase_orders_current(truck_id);
CREATE INDEX idx_silver_purchase_orders_status ON silver.purchase_orders_current(status);
CREATE INDEX idx_silver_purchase_orders_zone ON silver.purchase_orders_current(zone_id);
-- Defense-in-depth only (not the primary check): every Spark bronze->silver
-- job already does its own orphan-detection LEFT JOIN and routes unmatched
-- rows to the domain's error_* table before attempting the upsert, so under
-- correct operation these never actually fire.
ALTER TABLE silver.purchase_orders_current
    ADD CONSTRAINT fk_purchase_orders_customer
        FOREIGN KEY (customer_id) REFERENCES silver.customers_current(customer_id),
    ADD CONSTRAINT fk_purchase_orders_truck
        FOREIGN KEY (truck_id) REFERENCES silver.truck_fleet_current(truck_id),
    ADD CONSTRAINT fk_purchase_orders_zone
        FOREIGN KEY (zone_id) REFERENCES reference.delivery_zones(zone_id);

CREATE TABLE silver.product_on_orders_current (
    product_on_order_id UUID PRIMARY KEY,
    purchase_order_id   UUID NOT NULL,
    product_id          UUID NOT NULL,
    qty_on_order        INT NOT NULL,
    customer_comment    VARCHAR(500),
    created_at          TIMESTAMP NOT NULL,
    updated_at          TIMESTAMP NOT NULL
);
CREATE INDEX idx_silver_product_on_orders_po ON silver.product_on_orders_current(purchase_order_id);
ALTER TABLE silver.product_on_orders_current
    ADD CONSTRAINT fk_product_on_orders_purchase_order
        FOREIGN KEY (purchase_order_id) REFERENCES silver.purchase_orders_current(purchase_order_id),
    ADD CONSTRAINT fk_product_on_orders_product
        FOREIGN KEY (product_id) REFERENCES silver.products_current(product_id);

CREATE TABLE silver.invoices_current (
    invoice_id         UUID PRIMARY KEY,
    purchase_order_id  UUID NOT NULL,
    customer_id        UUID NOT NULL,
    amount             DECIMAL(10,2) NOT NULL,
    status             VARCHAR(20) NOT NULL,
    payment_reminder   INT NOT NULL DEFAULT 0,
    due_payment_date   TIMESTAMP,
    created_at         TIMESTAMP NOT NULL,
    updated_at         TIMESTAMP NOT NULL
);
CREATE INDEX idx_silver_invoices_po ON silver.invoices_current(purchase_order_id);
CREATE INDEX idx_silver_invoices_status ON silver.invoices_current(status);
ALTER TABLE silver.invoices_current
    ADD CONSTRAINT fk_invoices_purchase_order
        FOREIGN KEY (purchase_order_id) REFERENCES silver.purchase_orders_current(purchase_order_id),
    ADD CONSTRAINT fk_invoices_customer
        FOREIGN KEY (customer_id) REFERENCES silver.customers_current(customer_id);

CREATE TABLE silver.inventory_current (
    product_id     UUID PRIMARY KEY,
    current_stock  INT NOT NULL,
    updated_at     TIMESTAMP NOT NULL
);
ALTER TABLE silver.inventory_current
    ADD CONSTRAINT fk_inventory_current_product
        FOREIGN KEY (product_id) REFERENCES silver.products_current(product_id);

CREATE TABLE silver.inventory_history (
    event_id        UUID PRIMARY KEY,
    product_id      UUID NOT NULL,
    quantity_delta  INT NOT NULL,
    current_stock   INT NOT NULL,
    change_reason   VARCHAR(50) NOT NULL,
    changed_at      TIMESTAMP NOT NULL
);
CREATE INDEX idx_silver_inventory_history_product ON silver.inventory_history(product_id, changed_at DESC);
ALTER TABLE silver.inventory_history
    ADD CONSTRAINT fk_inventory_history_product
        FOREIGN KEY (product_id) REFERENCES silver.products_current(product_id);

CREATE TABLE silver.truck_current_position (
    truck_id        VARCHAR(20) PRIMARY KEY,
    geog            GEOGRAPHY(POINT, 4326) NOT NULL,
    truck_status    VARCHAR(20) NOT NULL,
    current_zone_id INT,
    updated_at      TIMESTAMP NOT NULL
);
CREATE INDEX idx_silver_truck_current_position_geog ON silver.truck_current_position USING GIST(geog);
ALTER TABLE silver.truck_current_position
    ADD CONSTRAINT fk_truck_current_position_truck
        FOREIGN KEY (truck_id) REFERENCES silver.truck_fleet_current(truck_id),
    ADD CONSTRAINT fk_truck_current_position_zone
        FOREIGN KEY (current_zone_id) REFERENCES reference.delivery_zones(zone_id);

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
CREATE TABLE silver.error_purchase_orders (id BIGSERIAL PRIMARY KEY, raw_payload JSONB, reason_code VARCHAR(100), rejected_at TIMESTAMP NOT NULL DEFAULT now());
CREATE TABLE silver.error_product_on_orders (id BIGSERIAL PRIMARY KEY, raw_payload JSONB, reason_code VARCHAR(100), rejected_at TIMESTAMP NOT NULL DEFAULT now());
CREATE TABLE silver.error_invoices (id BIGSERIAL PRIMARY KEY, raw_payload JSONB, reason_code VARCHAR(100), rejected_at TIMESTAMP NOT NULL DEFAULT now());
CREATE TABLE silver.error_inventory_changes (id BIGSERIAL PRIMARY KEY, raw_payload JSONB, reason_code VARCHAR(100), rejected_at TIMESTAMP NOT NULL DEFAULT now());
CREATE TABLE silver.error_truck_fleet (id BIGSERIAL PRIMARY KEY, raw_payload JSONB, reason_code VARCHAR(100), rejected_at TIMESTAMP NOT NULL DEFAULT now());
CREATE TABLE silver.error_truck_positions (id BIGSERIAL PRIMARY KEY, raw_payload JSONB, reason_code VARCHAR(100), rejected_at TIMESTAMP NOT NULL DEFAULT now());

-- =====================================================================
-- SCHEMA: gold (schema + grants only - dbt materializes tables here)
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS gold;

-- =====================================================================
-- SCHEMA: control (silver watermarks + Kafka offset tracking)
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS control;

-- Bronze -> silver incremental watermark (Postgres-to-Postgres jobs, §5).
-- Named silver_watermarks (not just "watermarks") to make clear this tracks
-- bronze->silver progress specifically - a distinct mechanism from
-- kafka_offsets below, which tracks Kafka->bronze progress instead.
CREATE TABLE control.silver_watermarks (
    table_name                 VARCHAR(100) PRIMARY KEY,
    last_processed_ingested_at TIMESTAMP NOT NULL,
    updated_at                 TIMESTAMP NOT NULL DEFAULT now()
);
INSERT INTO control.silver_watermarks (table_name, last_processed_ingested_at) VALUES
    ('purchase_order_events', '1970-01-01'),
    ('product_on_order_events', '1970-01-01'),
    ('invoice_events', '1970-01-01'),
    ('inventory_changes', '1970-01-01'),
    ('truck_position_events', '1970-01-01'),
    ('truck_fleet_events', '1970-01-01'),
    ('customer_events', '1970-01-01'),
    ('product_events', '1970-01-01');

-- Kafka -> bronze offset tracking (periodic batch ingestion jobs, §4)
CREATE TABLE control.kafka_offsets (
    topic       VARCHAR(200) NOT NULL,
    partition   INT NOT NULL,
    last_offset BIGINT NOT NULL,
    updated_at  TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition)
);

-- Role creation + grants deliberately live in 02-create-role-and-grants.sh,
-- not here: that script substitutes the pipeline_rw/generator_ro passwords
-- from the container's environment, whereas a plain .sql file mounted into
-- /docker-entrypoint-initdb.d gets no env-var substitution from the
-- postgres entrypoint (only .sh scripts do) - hardcoding a password here
-- would mean committing a secret to the repo.
