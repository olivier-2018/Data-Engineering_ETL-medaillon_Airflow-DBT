# Data Schema Reference

Full schema reference for the logistics domain model. Source of truth: `config/postgres/initdb/01-init-schema.sql`
(schemas/tables), `02-seed-reference-data.sql` (reference data), `03-create-role-and-grants.sh` (roles/privileges),
and the `dbt/` project (gold layer).

## Schema overview

| Schema | Purpose | Who writes to it |
|---|---|---|
| `reference` | Static/lookup data (delivery zones, product categories, weather stations) - not event-sourced | Seeded once via `02-seed-reference-data.sql`; `is_active` toggled manually |
| `iot` | Bronze — immutable, append-only event logs | Spark (streaming + batch ingestion jobs), Airflow (`weather_enrichment_dag`, `restock_check.py`) |
| `silver` | Validated, deduplicated, current-state + error tables | Spark (bronze→silver batch jobs, the truck-position streaming job) |
| `gold` | Star schema, SCD2 dimensions | dbt |
| `control` | Bronze→silver watermarks + Kafka offsets | Spark |

`pipeline_rw` (the role every Spark/dbt/Airflow connection uses — never the `postgres` superuser) has
`SELECT`+`INSERT` only on `iot.*` — no `UPDATE`/`DELETE` grant, enforcing bronze immutability at the privilege
level. It has full `SELECT`/`INSERT`/`UPDATE`/`DELETE` on `silver`/`gold`/`control`, and `SELECT`+`UPDATE` on
`reference` (so `is_active` can be toggled via plain SQL). A separate `generator_ro` role (used only by
`data_generators/`, for its startup state-resume reads) has `SELECT` only on `silver`/`reference` — it never
touches `iot` at all; Kafka is the generator's only write path into the pipeline.

## Reference (`reference` schema)

| Table | Key | Notes |
|---|---|---|
| `reference.delivery_zones` | `zone_id` | 40 Swiss cities, a configurable subset `is_active` at a time. Biel/Bienne's row has `is_warehouse = true` (the single source of truth for the warehouse's own coordinates). |
| `reference.product_categories` | `category_id` | 8 categories; `subcategories` is a native Postgres `TEXT[]`, not a CSV string. |
| `reference.weather_stations` | `station_id` | 5 fixed observation points, used by `fact_weather_delivery_correlation` to find each delivery zone's nearest station. |

## Bronze (`iot` schema)

| Table | Hypertable? | Time column | Notes |
|---|---|---|---|
| `iot.customer_events` | No (plain table) | — | Append-only; `iot.customers` is a `DISTINCT ON` view over the latest event per `customer_id`. |
| `iot.product_events` | No (plain table) | — | Same pattern; `iot.products` view. |
| `iot.purchase_order_events` | **Yes** | `event_at` | Header-only — line items live in `product_on_order_events`. One row per lifecycle transition, not one row per order. |
| `iot.product_on_order_events` | **Yes** | `event_at` | Line items: `purchase_order_id` × `product_id` × `qty_on_order`. |
| `iot.invoice_events` | **Yes** | `event_at` | Folds in what a separate payment domain used to cover. |
| `iot.inventory_changes` | **Yes** | `changed_at` | Fed by two independent writers: the generator (Kafka, `change_reason='sale'`) and `restock_check.py` (direct psycopg2, `change_reason='restock'`, bypassing Kafka). |
| `iot.truck_fleet_events` | No (plain table) | — | Low-volume: seeded once per truck + rare `updated` events. |
| `iot.truck_position_events` | **Yes**, high-frequency | `event_at` | The one domain ingested via persistent Spark Structured Streaming. A ping is the truck's own position, not one order's — the truck-to-orders manifest is tracked via `purchase_order_events.truck_id`, not here. |
| `iot.weather_observations` | **Yes** | `observed_at` | Populated by Airflow (`weather_enrichment_dag`), not Spark. |

### `iot.customer_events`
`event_id` (PK), `customer_id`, `event_type` (`created`/`updated`/`account_verified`/`account_disabled`/
`account_enabled`), `name`, `email`, `address`, `tel`, `country` (`CH` only for this scenario, kept generic),
`city`, `segment`, `verified_account`, `disabled_account`, `event_at`, `ingested_at`.

### `iot.product_events`
`event_id` (PK), `product_id`, `event_type` (`created`/`updated`/`refill`), `name`, `brand`, `model`,
`category`, `subcategory`, `unit_price`, `weight_kg`, `nominal_capacity` (warehouse capacity ceiling),
`refill_qty`/`refill_unit_price` (only set on `refill`), `event_at`, `ingested_at`.

### `iot.purchase_order_events`
`event_id`, `purchase_order_id`, `customer_id`, `status` (`created → invoiced → paid → on-hold → loaded →
in-transit → delivered → closed`, or `cancelled` from any of the first four), `delivery_address`,
`contact_tel`, `invoice_address`, `vat_number`, `target_delivery_date`, `truck_id` (set once loaded),
`zone_id` (`reference.delivery_zones.zone_id`, the dispatch grouping key), `event_at`, `ingested_at`.
PK: `(event_id, event_at)`. No `created_at` column — a `created` event fires exactly once, ever, per order.

### `iot.product_on_order_events`
`event_id`, `product_on_order_id`, `purchase_order_id`, `product_id`, `qty_on_order`, `customer_comment`,
`event_at`, `ingested_at`. PK: `(event_id, event_at)`. Immutable/single-event per line item — never edited
after order creation.

### `iot.invoice_events`
`event_id`, `invoice_id`, `purchase_order_id`, `customer_id`, `amount`, `status` (`created → pending →
settled`, or `cancelled` from `created`/`pending`), `payment_reminder` (0-3), `due_at`, `event_at`,
`ingested_at`. PK: `(event_id, event_at)`.

### `iot.inventory_changes`
`event_id`, `product_id`, `quantity_delta`, `current_stock`, `change_reason`
(`purchase`/`sale`/`adjustment`/`return`/`restock`), `changed_at`, `ingested_at`. PK: `(event_id, changed_at)`.
Single-warehouse model — no `warehouse_id` column.

### `iot.truck_fleet_events`
`event_id` (PK), `truck_id`, `event_type` (`created`/`updated`), `name`, `brand`, `model`, `size`, `capacity`
(product units the truck can carry), `weight_kg`, `event_at`, `ingested_at`.

### `iot.truck_position_events`
`event_id`, `truck_id`, `geog GEOGRAPHY(POINT,4326)` (GIST-indexed), `truck_status`
(`free`/`loading`/`in_transit`/`returning`), `current_zone_id` (nearest/current stop, if any), `event_at`,
`ingested_at`. PK: `(event_id, event_at)`. Position is simulated via straight-line interpolation between
stops, not a real routing engine.

### `iot.weather_observations`
`id`, `location_name`, `lat`, `lon`, `observed_at`, `temperature`, `humidity`, `pressure`,
`weather_condition`, `forecast_horizon_hours` (`NULL` = current observation, else forecast lead time in hours),
`source` (default `'openweathermap'`), `ingested_at`. PK: `(id, observed_at)`.

## Silver (`silver` schema)

Current-state tables (one row per natural key, "latest wins"), plus one error table per domain
(`raw_payload JSONB`, `reason_code`, `rejected_at`) for rows that fail validation.

| Table | Key | Notes |
|---|---|---|
| `silver.customers_current` | `customer_id` | `email` is `UNIQUE`. Feeds the dbt `customer_snapshot` (SCD2). |
| `silver.products_current` | `product_id` | `restock_required` is materialized here (`current_stock < restock_threshold_pct% of nominal_capacity`). Feeds `product_snapshot` (SCD2). |
| `silver.truck_fleet_current` | `truck_id` | Near-static dimension. Feeds `truck_fleet_snapshot` (SCD2). |
| `silver.purchase_orders_current` | `purchase_order_id` | FKs to `customers_current`/`truck_fleet_current`/`reference.delivery_zones`. Lifecycle-transition-validated. |
| `silver.product_on_orders_current` | `product_on_order_id` | FKs to `purchase_orders_current`/`products_current`. |
| `silver.invoices_current` | `invoice_id` | FKs to `purchase_orders_current`/`customers_current`. Status + reminder-count validated. |
| `silver.inventory_current` | `product_id` | Latest stock level per product. |
| `silver.inventory_history` | `event_id` | Append-only, stock-consistency-validated (via a `lag()` window check). |
| `silver.truck_current_position` | `truck_id` | Refreshed **twice**: directly by the streaming job (near-zero latency, primary path for the live map) and by `truck_positions_to_silver.py` (validated/enriched historical view + error routing — not the map's primary path). |
| `silver.weather_observations` | — | Normalized pass-through from bronze. |
| `silver.error_customers`, `error_products`, `error_purchase_orders`, `error_product_on_orders`, `error_invoices`, `error_inventory_changes`, `error_truck_fleet`, `error_truck_positions` | — | One per domain. |

FKs are silver-only, deliberately not present on `iot.*` (bronze) — bronze is an immutable append-only log
ingested independently per-topic, so a child event can legitimately land before its parent event's batch has
run; a hard FK there would make ingestion order-fragile for no benefit.

## Control (`control` schema)

- **`control.silver_watermarks`**: `table_name` (PK), `last_processed_ingested_at`, `updated_at`. Tracks
  bronze→silver incremental progress (Postgres timestamps) for each of the 8 domains needing it.
- **`control.kafka_offsets`**: `topic`, `partition` (composite PK), `last_offset`, `updated_at`. Tracks the 7
  periodic-batch Kafka ingestion jobs' progress (Kafka offsets, not timestamps — a distinct bookkeeping scheme
  from `silver_watermarks`, since bronze ingestion reads Kafka directly while bronze→silver reads already-landed
  Postgres rows).

## Gold (`gold` schema — dbt-materialized)

Schema itself contains only grants; every table is created by `dbt run`/`dbt snapshot`. Full model-by-model
detail (materializations, tests, business-question mapping) is in [`docs/DBT_MODEL.md`](DBT_MODEL.md).

**Snapshots (real SCD2, `strategy: timestamp`)**: `customer_snapshot`, `product_snapshot`,
`purchase_order_status_snapshot`, `invoice_snapshot`, `truck_fleet_snapshot`.

**Dimensions**: `dim_datetime` (via `dbt_utils.date_spine`), `dim_customer`/`dim_product` (views over their
snapshots + a surrogate key), `dim_truck` (a real dimension sourced from `truck_fleet_snapshot`),
`dim_delivery_zone` (view over all 40 reference zones, not just the currently-active subset).

**Facts** (all `materialized: incremental`, `incremental_strategy: delete+insert` unless noted): `fact_purchase_orders`
(header-level), `fact_product_on_orders` (line-item grain — order × product), `fact_invoices`,
`fact_shipments` (dispatch/delivery timing computed directly from the **bronze** `iot.purchase_order_events`
log, not silver — `silver.purchase_orders_current` only retains an order's latest status, not the
`in-transit`/`delivered` transition timestamps needed for `time_to_destination_seconds`), `fact_inventory_snapshot`
(periodic snapshot fact, one row per product per day, includes refill-price economics),
`fact_weather_delivery_correlation` (joins weather at each delivery zone's nearest observation station against
the order's actual delivery time), `fact_sales` (denormalized order×product mart pre-joined to
customer/product/zone for the Grafana business dashboards — see [`docs/DBT_MODEL.md`](DBT_MODEL.md) for why its
customer/product joins resolve through the natural key rather than the fact tables' own surrogate keys),
`fact_delivery_performance` (one row per delivered order — process time, delay-vs-target, on-time flag),
`fact_purchase_order_status_duration` (`materialized: view` — time spent in each PO status, read directly off
`purchase_order_status_snapshot`'s SCD2 history).
