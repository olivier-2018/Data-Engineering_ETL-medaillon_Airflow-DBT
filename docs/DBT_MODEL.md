# dbt — Gold Layer Processing

This is a from-first-principles walkthrough of what dbt does in this pipeline, why it's structured this way,
and exactly which file handles what — written for learning, not just reference. If you're new to dbt, read
top to bottom; if you already know dbt, jump to [Model inventory](#model-inventory) or
[How Airflow invokes dbt](#how-airflow-invokes-dbt).

## 1. Where dbt sits in the pipeline

```
Kafka → Spark (bronze ingestion) → iot.* (bronze, immutable)
                                        │
                          Spark (bronze→silver, spark-batch-jobs/silver_processing/)
                                        ▼
                              silver.* (validated, current-state)
                                        │
                                       dbt  ◄── you are here
                                        ▼
                              gold.* (star schema, SCD2 dimensions)
```

dbt owns exactly one hop: **silver → gold**. It never touches Kafka, never reads bronze directly (with one
documented exception, §6), and never writes to `silver.*`. Everything upstream of `silver.*` is Spark's job;
everything from `silver.*` onward to business-ready analytics is dbt's job. This split exists so each tool
does the thing it's actually good at: Spark for validating/deduplicating high-volume event streams with
window functions, dbt for expressing the *business logic* of a star schema (SCD2 history, incremental fact
tables, declarative tests) in SQL that's easy to read, review, and extend.

## 2. Why dbt, and why this project uses it the way it does

dbt is not a query engine — it's a SQL templating + orchestration layer that turns a folder of `SELECT`
statements into materialized tables/views, runs them in dependency order (inferred from `ref()`/`source()`
calls, not hand-maintained), and can test the results. Three dbt-specific mechanisms this project leans on:

- **Snapshots** — dbt's built-in SCD2 (slowly-changing dimension type 2) implementation. Point a snapshot at a
  silver current-state table, tell it which column marks "when did this row last change"
  (`strategy: timestamp`, `updated_at`), and dbt automatically maintains `dbt_valid_from`/`dbt_valid_to`
  columns tracking every historical version of a row — without you writing any SCD2 merge logic by hand.
- **Incremental models** — a fact table that would be wasteful to fully rebuild every run instead only
  processes rows that changed since the last run (`is_incremental()` + a `WHERE updated_at > (SELECT
  max(updated_at) FROM {{ this }})` filter), using `incremental_strategy: delete+insert` (dbt-postgres doesn't
  reliably support `merge`, per this project's own confirmed testing).
- **Tests** — declarative (`not_null`, `unique`, `accepted_values`, `relationships` in `.yml` files) and
  custom singular SQL tests (`tests/*.sql`, a query that should return zero rows) that run via `dbt test` and
  fail the pipeline loudly if a business invariant is violated.

This project deliberately invokes dbt via plain `BashOperator` calls to the `dbt` CLI from Airflow (`dbt
snapshot`, `dbt run`, `dbt test` as three separate tasks — not `dbt build`, and not the `astronomer-cosmos`
Airflow provider), specifically so the DAG shows three distinct, individually-retriable/observable steps and
so the project demonstrates plain-CLI dbt orchestration rather than a framework that hides it.

## 3. Project structure

```
dbt/
├── dbt_project.yml       # project config: paths, per-folder materialization defaults
├── profiles.yml          # connection info (env-var driven - pipeline_rw, never a superuser)
├── packages.yml          # dbt_utils (surrogate keys, date_spine)
├── models/
│   ├── staging/          # 1:1 views over silver.*/reference.* - thin pass-through, no business logic
│   ├── gold_marts/
│   │   ├── dimensions/   # dim_* - built from snapshots (SCD2) or plain views (degenerate/static dims)
│   │   └── facts/        # fact_* - incremental, delete+insert
│   └── (snapshots are their own top-level dbt concept, see below)
├── snapshots/            # SCD2 history for customers/products/purchase-order status/invoices/truck fleet
└── tests/                # custom singular SQL tests (schema tests live inline in .yml files instead)
```


### `dbt_project.yml` — key settings

```yaml
name: 'iot_gold'
profile: 'iot_gold'
models:
  iot_gold:
    staging:
      +materialized: view       # thin pass-throughs - no reason to materialize a physical table
    gold_marts:
      dimensions:
        +materialized: incremental
      facts:
        +materialized: incremental
snapshots:
  iot_gold:
    +target_schema: gold        # snapshots land directly in gold, not a separate snapshot schema
```

### `profiles.yml` — connection

Env-var driven (`PIPELINE_DB_USER`/`PIPELINE_DB_PASSWORD`/`POSTGRES_HOST`/`POSTGRES_DB`), `threads: 2`. Never
the `postgres` superuser — dbt connects as `pipeline_rw`, the same least-privilege role every Spark job uses,
which has `CREATE` on `gold` (needed since dbt dynamically creates tables there) but only `SELECT` on `silver`
and `reference` (dbt never writes upstream of gold).

## 4. Model inventory

### Staging (`models/staging/`, all `materialized: view`)

One staging model per silver/reference table — a thin, 1:1 pass-through with no joins, no business logic. The
point of a staging layer is purely naming/typing consistency, so every downstream model references a stable
`stg_*` name instead of reaching into `silver.*` directly (if a silver table's shape ever changes, only the
staging model needs updating, not every fact/dimension that used it).

| Model | Source | Notes |
|---|---|---|
| `stg_customers` | `silver.customers_current` | includes `verified_account`/`disabled_account` |
| `stg_products` | `silver.products_current` | includes `nominal_capacity`/`restock_required` |
| `stg_purchase_orders` | `silver.purchase_orders_current` | header only - no product/qty (see line items below) |
| `stg_product_on_orders` | `silver.product_on_orders_current` | the line-item grain (order × product × qty) |
| `stg_invoices` | `silver.invoices_current` | folds in what a separate payment domain used to cover |
| `stg_inventory` | `silver.inventory_current` | current stock level per product - feeds `fact_inventory_snapshot` |
| `stg_truck_fleet` | `silver.truck_fleet_current` | near-static dimension source |
| `stg_truck_positions` | `silver.truck_current_position` | unpacks `geog` via `ST_Y`/`ST_X` |
| `stg_delivery_zones` | `reference.delivery_zones` | one of two staging models over `reference`, not `silver` |
| `stg_weather_stations` | `reference.weather_stations` | the 5 fixed observation points; used by `fact_weather_delivery_correlation` to find each zone's nearest station |

`sources.yml` declares every source table's freshness thresholds (`warn_after`/`error_after`), checked via
`dbt source freshness` — not currently wired into a DAG task, but available for manual/future use.

### Snapshots (`snapshots/`, all `strategy: timestamp`, `target_schema: gold`)

Real SCD2 — every historical version of a row is preserved, queryable via `dbt_valid_from`/`dbt_valid_to`.

| Snapshot | Source | `unique_key` | Tracks |
|---|---|---|---|
| `customer_snapshot` | `stg_customers` | `customer_id` | profile/verification/disabled history |
| `product_snapshot` | `stg_products` | `product_id` | price/category/restock-flag history |
| `purchase_order_status_snapshot` | `stg_purchase_orders` | `purchase_order_id` | the 8-state lifecycle |
| `invoice_snapshot` | `stg_invoices` | `invoice_id` | status + reminder-count history |
| `truck_fleet_snapshot` | `stg_truck_fleet` | `truck_id` | fleet attribute history (rarely changes) |

Truck **position** deliberately has no snapshot — it's not a slowly-changing dimension, it's a high-frequency
event stream already fully preserved in bronze (`iot.truck_position_events`); a snapshot would just duplicate
that history at a coarser grain for no benefit.

### Dimensions (`models/gold_marts/dimensions/`)

| Dimension | Built from | Materialization | Notes |
|---|---|---|---|
| `dim_customer` | `customer_snapshot` | view | surrogate key = `customer_id` + `dbt_valid_from` |
| `dim_product` | `product_snapshot` | view | same surrogate-key pattern |
| `dim_truck` | `truck_fleet_snapshot` | table | a *real* dimension now (was degenerate pre-redesign) |
| `dim_delivery_zone` | `stg_delivery_zones` | view | all 40 zones, not just currently-active ones - a fact
  referencing a since-deactivated zone must still resolve |
| `dim_datetime` | `dbt_utils.date_spine` | table | standard calendar dimension |

### Facts (`models/gold_marts/facts/`, all `materialized: incremental`, `incremental_strategy: delete+insert`)

| Fact | Grain | Notes |
|---|---|---|
| `fact_purchase_orders` | one row per order | header-level: customer, status, dates |
| `fact_product_on_orders` | one row per (order × product) | the new line-item grain - per-product revenue/volume |
| `fact_invoices` | one row per invoice | amount, status, reminder count, settle latency |
| `fact_shipments` | one row per order (its dispatch→delivery leg) | sourced from **bronze** `iot.purchase_order_events` directly (see §6) for exact `in-transit`/`delivered` timestamps, joined to `stg_purchase_orders` for `truck_id`/`zone_id`; "orders per truck run" is a `GROUP BY truck_id` at query time, not a baked-in stop grain (see §6 for why) |
| `fact_inventory_snapshot` | one row per (product × day) | periodic snapshot, includes refill-cost economics |
| `fact_weather_delivery_correlation` | one row per delivered shipment | joins weather at the destination's nearest observation |

## 5. Gold marts → business questions

The four marts requested (see `TODO_improve_business_logic.md`) aren't separate models — they're just how you
query the facts/dimensions above:

- **Sales/profit**: `fact_invoices` (revenue) + `fact_product_on_orders` (revenue by product/category) +
  `fact_inventory_snapshot`'s refill-cost data (cost side), grouped by `dim_customer`'s region + `dim_datetime`.
- **Customer growth**: `dim_customer`/`customer_snapshot` history — new customers per period,
  verified-vs-unverified funnel, disabled-account churn.
- **Delivery ops**: `fact_shipments` + `dim_truck` + `dim_delivery_zone` — this is where consolidation
  efficiency becomes visible (`avg_orders_per_truck_run`, `avg_distance_per_order`).
- **Outstanding invoices / cash-flow**: `fact_invoices` filtered to `pending`/`payment_reminder > 0`.

## 6. The one place dbt reads bronze directly

`fact_shipments` queries `{{ source('iot', 'purchase_order_events') }}` directly, bypassing silver, because
`silver.purchase_orders_current` only ever holds an order's *latest* status (it's a current-state table, by
design) — the precise `in-transit`/`delivered` transition timestamps needed for `time_to_destination_seconds`
only exist in bronze's immutable event log. This is a documented, deliberate exception, not an architectural
leak.

Note this fact deliberately does **not** try to reconstruct discrete "truck stop" boundaries from
`iot.truck_position_events`. Each ping's `current_zone_id` is set the instant a truck arrives at a stop but then
persists unchanged through the *entire next leg* (it's overwritten only at the *following* stop, or cleared on
the return-to-warehouse leg) — so grouping pings by `(truck_id, current_zone_id)` would span far more than the
actual dwell time at that stop, and distinguishing one visit to a city from a later visit on a different truck
run requires detecting run boundaries in the ping stream, which is fragile and adds a synthetic grain with no
analytical payoff a `purchase_order_id`-grain fact can't already answer via `GROUP BY truck_id`/`zone_id`/date.
The per-order `in-transit`/`delivered` transitions in `iot.purchase_order_events` are simpler *and* more
precise, since they're emitted at the exact instant each order is delivered.

## 7. Tests

**Schema tests** (declared in `dimensions.yml`/`facts.yml`, run via `dbt test`): `not_null`/`unique` on every
primary/surrogate key, `accepted_values` on enumerated columns (e.g. `dim_customer.country` ∈ `{CH}` today,
`fact_purchase_orders.status` ∈ the 9 known statuses), `relationships` (foreign-key-style checks between fact
and dimension tables).

**Custom singular tests** (`tests/*.sql`, each a query that must return zero rows to pass):

| Test | Checks |
|---|---|
| `assert_purchase_order_status_transitions_valid.sql` | the 8-state lifecycle never regresses, across full snapshot history |
| `assert_invoice_status_transitions_valid.sql` | same idea for invoice status/reminders |
| `assert_no_negative_stock.sql` | `stg_products`/inventory never shows negative stock |
| `assert_truck_capacity_not_exceeded.sql` | sum of `qty_on_order` for orders sharing a `truck_id` never exceeds that truck's `capacity` |

These mirror checks Spark's `silver_processing/*.py` jobs already do at ingestion time — deliberately
redundant (defense-in-depth): a bug in the Spark-side check shouldn't be able to silently corrupt gold without
a second, independent check catching it too.

## 8. How Airflow invokes dbt

`airflow/dags/gold_dbt_dag.py`: scheduled on the **AND** of 7 of the 8 silver-domain Airflow Assets (Airflow
3's asset-list scheduling — it only fires once every *trigger* domain has updated since gold's last run, not
on any individual domain's schedule). `truck_fleet_current` is deliberately excluded — see
[`docs/AIRFLOW3_PROJECT.md`](AIRFLOW3_PROJECT.md) §7 for why a near-static domain can't be part of this AND
condition. Five separate tasks, run in sequence:

```
dbt_deps  →  dbt_run_staging  →  dbt_snapshot  →  dbt_run  →  dbt_test  →  check_gold_row_counts (SQLCheckOperator)
```

`dbt_deps` installs `dbt_packages` (not checked into git, wiped by `scripts/reset.sh`) - idempotent and fast
once packages are already present, so running it every cycle is a non-issue. `dbt_run_staging` (`dbt run
--select staging`) runs next so the staging views snapshots `ref()` (§4 above) always exist before `dbt
snapshot` runs against them. Separate tasks (not one `dbt build` call) so a failure in, say, `dbt test` is
visible as its own failed task in the Airflow UI, independent of whether earlier steps succeeded - and so each
step can be retried/re-run individually without repeating the others.

## 9. Data interface summary

**Inputs** (read-only): `silver.*` (every current-state table), `reference.delivery_zones`,
`reference.weather_stations`, and `iot.purchase_order_events` (bronze, §6 exception only).

**Outputs** (owned/written): everything in `gold.*` — snapshots, dimensions, facts. Nothing outside `gold` is
ever written by dbt.

**Credentials**: `pipeline_rw` via `profiles.yml`, env-var driven, same role every Spark job connects as.

## 10. Generated documentation (lineage graph, catalog)

Beyond this handwritten doc, dbt generates its own browsable documentation site directly from the
project: every model/source/snapshot/test node, the full `ref()`/`source()` lineage graph, and (via
`dbt docs generate`) a **catalog** of each model's actual columns/types read back from the warehouse.
The lineage graph is the most useful part for this project — it visually confirms the same silver →
staging → snapshots → dimensions/facts dependency chain described in §3/§4 above, entirely derived from
`ref()`/`source()` calls, so it can never drift out of sync with the models the way a hand-drawn diagram
could.

**Generating it** needs the dbt CLI + a warehouse connection, so it runs via `airflow-scheduler` (models
should already be built at least once — run/snapshot first if starting fresh):

```bash
docker compose run --rm airflow-scheduler bash -c \
  "cd /opt/dbt && dbt run --select staging && dbt snapshot && dbt run && dbt docs generate"
```

This writes `manifest.json`, `catalog.json`, and `index.html` into `dbt/target/` (gitignored, regenerated
on demand — not checked in).

**Serving it** is a separate concern from generating it — viewing static HTML/JSON needs neither the dbt
CLI nor a warehouse connection, so it's a dedicated `dbt-docs` service (`nginx:alpine`, mounting
`dbt/target/` read-only) under its own `documentation` compose profile, the same opt-in pattern as the
`generator` profile:

```bash
docker compose --profile documentation up -d dbt-docs
```

Then open `http://localhost:9000`. Stop it with `docker compose --profile documentation down dbt-docs`
(or just `docker compose stop dbt-docs`) when done — it's not part of the default `docker compose up`.
Since `dbt/target/` is a bind mount, re-running `dbt docs generate` updates the files the already-running
`dbt-docs` container serves immediately — no restart needed, just refresh the browser.

Note: most models/columns in this project don't yet have `description:` fields in their schema YAML, so
the generated site's node details are sparse today (lineage and column types/tests still show correctly
regardless — those come from the DAG and the live catalog, not from descriptions). Worth filling in
incrementally if the docs site becomes a real day-to-day reference, not required for the lineage graph
itself to be useful.
