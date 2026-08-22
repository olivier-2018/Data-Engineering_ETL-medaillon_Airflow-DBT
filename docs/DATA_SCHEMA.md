# Data Schema Reference

Full schema reference for the logistics domain model. Source of truth: `config/postgres/initdb/01-init-schema.sql`
(schemas/tables) + `02-create-role-and-grants.sh` (roles/privileges) + the `dbt/` project (gold layer).

## Schema overview

| Schema | Purpose | Who writes to it |
|---|---|---|
| `iot` | Bronze — immutable, append-only event logs | Spark (streaming + batch ingestion jobs) |
| `silver` | Validated, deduplicated, current-state + error tables | Spark (bronze→silver batch jobs) |
| `gold` | Star schema, SCD2 dimensions | dbt |
| `control` | Watermarks (silver jobs) + Kafka offsets (bronze ingestion jobs) | Spark |

`pipeline_rw` (the role every Spark/dbt/Airflow connection uses — never the `postgres` superuser) has
`SELECT`+`INSERT` only on `iot.*` — **no `UPDATE` grant**, enforcing bronze immutability at the privilege level,
confirmed directly: `UPDATE iot.truck_position_events ...` as `pipeline_rw` returns `permission denied`.

## Bronze (`iot` schema)

| Table | Hypertable? | Time column | Notes |
|---|---|---|---|
| `iot.product_events` | No (plain table) | — | Append-only; `iot.products` is a `DISTINCT ON` view over the latest event per `product_id`. |
| `iot.customer_events` | No (plain table) | — | Same pattern; `iot.customers` view. |
| `iot.sales_order_events` | **Yes** | `event_at` | One row per lifecycle transition, not one row per order — `order_id` is a non-unique reference column. |
| `iot.payment_events` | **Yes** | `event_at` | Separate domain from order status (distinct from "acknowledge the order" in the business narrative). |
| `iot.inventory_changes` | **Yes** | `changed_at` | Includes both organic sale-driven decrements and automatic restock events. |
| `iot.truck_position_events` | **Yes**, high-frequency | `event_at` | The one domain ingested via persistent Spark Structured Streaming; `geog GEOGRAPHY(POINT,4326)`, GIST-indexed. |
| `iot.weather_observations` | **Yes** | `observed_at` | Populated by Airflow (`weather_enrichment_dag`), not Spark — genuinely time-series regardless of ingestion mechanism. |

Reference tables and their two low-frequency event logs are **not** hypertables — hypertable-ness tracks whether
a table is genuinely time-series, independent of whether Spark ingests it via streaming or periodic batch.

### `iot.product_events`
`event_id` (PK), `product_id`, `event_type` (`created`/`updated`), `name`, `category`, `subcategory`,
`unit_price`, `weight_kg`, `initial_stock` (baseline for the 20%-threshold restock check), `event_at`, `ingested_at`.

### `iot.customer_events`
`event_id` (PK), `customer_id`, `event_type`, `name`, `country` (`CH`/`FR`/`DE`/`IT`), `city`, `segment`,
`event_at`, `ingested_at`.

### `iot.sales_order_events`
`event_id`, `order_id`, `customer_id`, `product_id`, `quantity`, `unit_price_snapshot`, `order_status`
(`pending → confirmed → picking → ready_for_dispatch → shipped → delivered`, or `cancelled`), `created_at`
(the order's original creation time, carried on every event), `event_at`, `ingested_at`.
PK: `(event_id, event_at)`. **v1 simplification**: one product per order.

### `iot.payment_events`
`event_id`, `order_id`, `payment_status` (`authorized → captured`, or `failed`/`refunded`), `amount`,
`event_at`, `ingested_at`. PK: `(event_id, event_at)`.

### `iot.inventory_changes`
`event_id`, `product_id`, `quantity_delta`, `current_stock`, `change_reason`
(`purchase`/`sale`/`adjustment`/`return`/`restock`), `changed_at`, `ingested_at`. PK: `(event_id, changed_at)`.
Single-warehouse model — no `warehouse_id` column (this scenario has exactly one warehouse, in Biel).

### `iot.truck_position_events`
`event_id`, `shipment_id`, `truck_id` (drawn from the finite, config-driven truck fleet), `order_id`
(**v1: 1 shipment = 1 order**), `geog GEOGRAPHY(POINT,4326)`, `shipment_status`
(`ready_for_dispatch → loading → in_transit → delivered`), `destination_country`, `event_at`, `ingested_at`.
PK: `(event_id, event_at)`. No route/waypoint data — destination is chosen once at dispatch; position is
simulated via straight-line interpolation, not a real routing engine.

### `iot.weather_observations`
`id`, `location_name`, `lat`, `lon`, `observed_at`, `temperature`, `humidity`, `pressure`,
`weather_condition`, `forecast_horizon_hours` (`NULL` = current observation, else forecast lead time in hours),
`source` (default `'openweathermap'`), `ingested_at`. PK: `(id, observed_at)`.

## Silver (`silver` schema)

Current-state tables (one row per natural key, "latest wins"), plus one error table per domain
(`raw_payload JSONB`, `reason_code`, `rejected_at`) for rows that fail validation.

| Table | Key | Notes |
|---|---|---|
| `silver.customers_current` | `customer_id` | Feeds the dbt `customer_snapshot` (SCD2). |
| `silver.products_current` | `product_id` | Feeds the dbt `product_snapshot` (SCD2). |
| `silver.sales_orders_current` | `order_id` | Latest event per order; lifecycle-transition-validated. |
| `silver.payments_current` | `order_id` | Payment-status-transition-validated. |
| `silver.inventory_current` | `product_id` | Latest stock level per product. |
| `silver.inventory_history` | `event_id` | Append-only, stock-consistency-validated (via a `lag()` window check). |
| `silver.truck_positions_current` | `shipment_id` | Refreshed **twice**: directly by the streaming job (near-zero latency, primary path for the live map) and by `truck_positions_to_silver.py` (validated/enriched historical view + error routing — not the map's primary path). |
| `silver.weather_observations` | — | Normalized pass-through from bronze. |
| `silver.error_customers`, `error_products`, `error_sales_orders`, `error_payments`, `error_inventory_changes`, `error_truck_positions` | — | One per domain. |

## Control (`control` schema)

- **`control.watermarks`**: `table_name` (PK), `last_processed_ingested_at`, `updated_at`. Tracks bronze→silver
  incremental progress (Postgres timestamps) — seeded at `1970-01-01` for each of the 6 domains needing it.
- **`control.kafka_offsets`**: `topic`, `partition` (composite PK), `last_offset`, `updated_at`. Tracks the 5
  periodic-batch Kafka ingestion jobs' progress (Kafka offsets, not timestamps — a genuinely different
  bookkeeping scheme since bronze ingestion reads Kafka directly, while bronze→silver reads already-landed
  Postgres rows).

## Gold (`gold` schema — dbt-materialized)

Schema itself contains only grants; every table is created by `dbt run`/`dbt snapshot`.

**Snapshots (real SCD2, `strategy: timestamp`)**: `customer_snapshot`, `product_snapshot`,
`order_status_snapshot`, `payment_status_snapshot`.

**Dimensions**: `dim_datetime` (via `dbt_utils.date_spine`), `dim_customer`/`dim_product` (views over their
snapshots + a surrogate key), `dim_truck` (degenerate — derived from `DISTINCT truck_id` actually observed in
the data, not a config-driven count), `dim_destination_country` (static `CH`/`FR`/`DE`/`IT`).

**Facts** (all `materialized: incremental`, `incremental_strategy: delete+insert`): `fact_sales_orders`,
`fact_payments`, `fact_shipments` (dispatch/delivery timing computed directly from the **bronze** event log,
not silver — `silver.truck_positions_current` only retains the latest position per shipment, not the
first-seen timestamp needed for `time_to_destination`), `fact_inventory_snapshot` (periodic snapshot fact, one
row per product per day), `fact_weather_delivery_correlation` (joins weather at the destination country's
representative location against each delivered shipment's actual delivery window).

There is no real dimension source data (no customer/product master tables independent of the event streams) —
`dim_customer`/`dim_product` are genuinely derived from the event logs via their snapshots, not degenerate in
the usual sense, since attribute *history* is tracked. `dim_truck`/`dim_destination_country` are the
genuinely degenerate ones.

## Notes on schema evolution during implementation

The original schema design (documented in an earlier planning pass) had `sales_orders`/`logistics_shipments` as
mutable, single-row-per-entity tables (an `order_id` primary key, updated in place via `updated_at`). This was
revised before implementation to the immutable event-log design above, specifically so bronze itself preserves
full history rather than requiring downstream reconstruction — a deliberate medallion best-practice choice, not
an incidental one.
