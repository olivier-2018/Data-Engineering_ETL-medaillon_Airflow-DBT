## Table of Contents
- [DBT overview](#dbt-overview)
  - [High-Level Overview of DBT](#highlevel-overview-of-dbt)
  - [More detailed Overview of DBT — A Quick Tutorial](#more-detailed-overview-of-dbt-a-quick-tutorial)
- [DBT Project Structure and Best-practice](#dbt-project-structure-and-best-practice)
  - [High-Level Overview of DBT Project Structure](#highlevel-overview-of-dbt-project-structure)
  - [More Detailed Overview of DBT Project Structure](#more-detailed-overview-of-dbt-project-structure)
  - [Deep Dive: How Everything Fits Together](#deep-dive-how-everything-fits-together)
  - [Summary Table](#summary-table)
  - [Final Takeaway - DBT Project Best-Practice](#final-takeaway-dbt-project-best-practice)
- [Medallion architecture with DBT](#medallion-architecture-with-dbt)
  - [High-Level Overview](#highlevel-overview)
  - [DBT Medallion Folder Structure (Detailed)](#dbt-medallion-folder-structure-detailed)
  - [How the Layers Work Together](#how-the-layers-work-together)
  - [Example Folder Structure (Full)](#example-folder-structure-full)
  - [Where Tests & Documentation Live](#where-tests-documentation-live)
  - [Materialization Strategy](#materialization-strategy)
  - [Final Takeaway - Medaillon architecture with DBT](#final-takeaway-medaillon-architecture-with-dbt)

# DBT overview

DBT is one of those tools that looks simple on the surface but quietly reshapes how teams build analytics. This is a conceptual + practical tutorial.


## High‑Level Overview of DBT  

**DBT (Data Build Tool)** is a framework for transforming data in your warehouse using **SQL + software engineering best practices**.

Think of it as:

> **SQL + version control + modularity + testing + documentation + lineage**

DBT does *not* ingest data.  
DBT does *not* orchestrate pipelines.  
DBT does *not* store data.

DBT **only transforms data** — but it does so in a way that makes your warehouse feel like a real software project.

### What DBT gives you:
- **Models** → SQL files that define tables/views  
- **Dependencies** → `ref()` creates DAGs automatically  
- **Testing** → schema tests + data tests  
- **Documentation** → auto‑generated docs site  
- **Environment management** → dev/staging/prod  
- **Macros** → reusable SQL logic  
- **Packages** → reusable DBT libraries  
- **Materializations** → table, view, incremental, ephemeral  

DBT is the backbone of modern medallion architectures (bronze → silver → gold).

### 🧭 DBT Mental Model  
DBT turns your SQL into a **directed acyclic graph (DAG)**.

- Each SQL file = a node  
- Each `ref()` = a dependency 
- DBT builds the graph and runs transformations in the correct order  

**Dependencies** tells dbt which model you depend on, ensures the model is built in the correct order, and automatically resolves the schema + relation name.

This is why DBT feels magical — you write SQL, DBT handles orchestration.

## 🔧 More detailed Overview of DBT — A Quick Tutorial  
Below is a practical, senior‑level walkthrough of how DBT is used in real projects.

### 1. Install DBT  
For Postgres:
```bash
pip install dbt-postgres
```

For Databricks:
```bash
pip install dbt-databricks
```

Initialize a project:
```bash
dbt init my_project
```

This creates:

```
models/
macros/
tests/
dbt_project.yml
```

### 2. Configure Your Warehouse Connection  
In `profiles.yml`:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: olivier
      password: secret
      dbname: analytics
      schema: public
      threads: 4
```

DBT uses this to connect to your warehouse.

## 3. Create Your First Model  
Inside `models/` create:

### `models/silver_orders.sql`
```sql
select
    order_id,
    customer_id,
    total_amount,
    created_at::date as order_date
from {{ source('raw', 'orders') }}
```

DBT will turn this SQL file into a **view** or **table** depending on materialization.


## 4. Define Sources  
In `models/sources.yml`:

```yaml
version: 2

sources:
  - name: raw
    tables:
      - name: orders
```

This tells DBT where raw data lives.

## 5. Add Tests  
In `models/silver_orders.yml`:

```yaml
version: 2

models:
  - name: silver_orders
    columns:
      - name: order_id
        tests:
          - not_null
          - unique
      - name: total_amount
        tests:
          - not_null
          - greater_than:
              min_value: 0
```

DBT will automatically generate SQL to test these constraints.

### Kinds of DBT tests

**1. Generic (schema) tests — the most commonly used.** —  Declared in YAML. Four
ship with DBT core and cover most of a project's test coverage: `not_null`, `unique`, `accepted_values` (column only contains values from a fixed list), `relationships` (foreign-key integrity). 

See following packages to add more:  
 - `dbt_utils` (`unique_combination_of_columns` for composite keys, `expression_is_true` for arbitrary boolean checks)  
 - `dbt_expectations` (ranges, regex/format, distribution checks) — reach for these before writing custom SQL.

Example:
```yaml
# 1. Generic tests — models/schema.yml
models:
  - name: orders
    columns:
      - name: order_id
        tests: [not_null, unique]
      - name: status
        tests:
          - accepted_values:
              values: ['pending', 'shipped', 'delivered']
      - name: customer_id
        tests:
          - relationships:
              to: ref('dim_customers')
              field: customer_id
```

**2. Singular tests** — plain SQL files in `tests/*.sql` that must return zero rows to pass. Used for
business rules that span rows/tables and don't reduce to a per-column rule, e.g. "an order's status must
never regress" or "line-item totals must sum to the order total." Used sparingly — most checks are
better expressed as a generic test.

Example:
```sql
-- 2. Singular test — tests/assert_line_items_match_order_total.sql
select order_id
from {{ ref('orders') }}
where order_total != (select sum(line_amount) from {{ ref('order_lines') }} ol where ol.order_id = orders.order_id)
```

**3. Unit tests** (`unit_tests:`, DBT ≥ 1.8) — feed a model fake input rows and assert the exact output,
testing the SQL logic itself rather than real warehouse data. Good for tricky `CASE WHEN`/macro logic;
newer and less universally used than the other two.

Example:
```yaml
# 3. Unit test — models/schema.yml
unit_tests:
  - name: test_order_status_mapping
    model: stg_orders
    given:
      - input: source('raw', 'orders')
        rows: [{status_code: 1, expected: 'pending'}]
    expect:
      rows: [{status: 'pending'}]
```

**4. Source freshness** (`freshness:` on a source, checked via `dbt source freshness`) — not a `dbt test`,
but the same idea applied to "has this data landed recently enough."

Useful knobs regardless of test type: `severity: warn` (log without failing the run), `store_failures:
true` (persist failing rows for inspection), and tags (`dbt test --select tag:critical` for a fast CI subset)

Example:

```yaml
# 4. Source freshness — models/sources.yml
sources:
  - name: raw
    tables:
      - name: orders
        loaded_at_field: updated_at
        freshness:
          warn_after: {count: 6, period: hour}
          error_after: {count: 12, period: hour}
```

## 6. Build the Model  
Run:

```bash
dbt run
```

DBT will:

- Parse your SQL  
- Build the DAG  
- Execute transformations in order  
- Materialize tables/views  

Run tests:

```bash
dbt test
```


## 7. Create Gold Models  
Example:

### `models/gold_sales_summary.sql`
```sql
select
    order_date,
    sum(total_amount) as daily_revenue,
    count(*) as orders_count
from {{ ref('silver_orders') }}
group by order_date
```

Notice the `ref()` — this is how DBT builds dependencies.


## 8. Documentation  
Generate docs:

```bash
dbt docs generate
dbt docs serve
```

You get:

- Lineage graph  
- Model descriptions  
- Tests  
- Sources  
- Dependencies  

This is one of DBT’s strongest features.

## 9. Materializations  

A **materialization** is *how* dbt turns a `SELECT` into something that actually exists in the
warehouse — the strategy dbt uses to persist a model's query result. Every model is compiled to a
`CREATE ...` (or nothing at all) statement based on which materialization it's given; the model's SQL
itself never changes, only how dbt wraps it.

- **`view`** — wraps the SQL in `CREATE VIEW`. No data is duplicated; the query re-runs every time the
  view is read. Default materialization, cheapest to build, slowest to query. Good fit for thin
  pass-through/staging models.
- **`table`** — wraps the SQL in `CREATE TABLE AS`, fully rebuilt every run. Data is physically stored,
  so reads are fast, but every `dbt run` recomputes the whole table from scratch. Good fit for
  dimensions and anything small/cheap enough to fully rebuild each time.
- **`incremental`** — like `table`, but after the first run only *new/changed* rows are processed
  (via an `is_incremental()` filter you write) and merged into the existing table, instead of a full
  rebuild. Essential once a table is too large to recompute every run — the standard choice for
  high-volume fact tables.
- **`ephemeral`** — not built in the warehouse at all; inlined as a CTE into whatever model(s) `ref()`
  it. Useful for a small reusable snippet of logic that doesn't need its own table/view.

In `dbt_project.yml`:

```yaml
models:
  my_project:
    silver:
      materialized: incremental
    gold:
      materialized: table
```

Incremental is key for large IoT datasets.


## 🔥 10. Incremental Model Example  
```sql
{{ config(materialized='incremental', unique_key='order_id') }}

select *
from {{ source('raw', 'orders') }}

{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

This is how DBT handles incremental loads.


---
---

# DBT Project Structure and Best-practice

A DBT project has a very specific shape because it’s designed to make SQL development feel like real software engineering: modular, testable, documented, and dependency‑driven. Once you understand the structure, DBT becomes predictable and powerful.

Below is a **high‑level overview**, followed by a **deep drill‑down into each folder**, and finally a **workflow explanation** showing how everything fits together.


## ⭐ High‑Level Overview of DBT Project Structure
A DBT project is organized around **models**, **sources**, **tests**, **macros**, and **configuration**.  
Everything lives inside a predictable directory tree:

```
dbt_project.yml
models/
macros/
tests/
seeds/
snapshots/
analysis/
```

Each folder has a specific purpose, and DBT uses this structure to automatically build a DAG, generate documentation, run tests, and materialize tables.


## 🧭 More Detailed Overview of DBT Project Structure

### 📁 1. `dbt_project.yml` — The Project Brain  
This file defines:

- model paths  
- materialization defaults  
- naming conventions  
- tests configuration  
- seeds configuration  
- snapshots configuration  

It’s the **central configuration file**.

Example, matching the `staging/ → intermediate/ → marts/` layout used throughout this doc:

```yaml
name: 'my_project'
version: '1.0.0'
config-version: 2

profile: 'my_project'

model-paths: ["models"]
seed-paths: ["seeds"]
snapshot-paths: ["snapshots"]
test-paths: ["tests"]
macro-paths: ["macros"]

target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  my_project:
    staging:
      +materialized: view
    intermediate:
      +materialized: view
    marts:
      +materialized: table

seeds:
  my_project:
    +schema: seeds

snapshots:
  my_project:
    +target_schema: snapshots
```

Each `+materialized` (and other `+`-prefixed keys) applies to every model in that folder unless a
model overrides it with its own `{{ config(...) }}` block — folder-level defaults, not hard rules.


### 📁 2. `models/` — Your SQL Transformations  
This is where your **silver** and **gold** SQL models live.

Inside `models/`, teams usually create subfolders:

```
models/
  staging/
  intermediate/
  marts/
```

Each SQL file becomes a **table**, **view**, or **incremental model** depending on configuration.

### 📁 3. `models/*.yml` — Schema + Tests + Documentation  
Every model has a YAML file describing:

- columns  
- tests  
- descriptions  
- tags  
- metadata  

Example:

```yaml
version: 2
models:
  - name: orders_silver
    description: "Cleaned and standardized orders"
    columns:
      - name: order_id
        tests:
          - not_null
          - unique
```

This is how DBT enforces quality.


### 📁 4. `macros/` — Reusable SQL Logic  
Macros let you write reusable SQL functions using Jinja.

Example:

```sql
{% macro safe_divide(a, b) %}
  case when {{ b }} = 0 then null else {{ a }} / {{ b }} end
{% endmacro %}
```

Macros are the **programming layer** of DBT.

### 📁 5. `tests/` — Custom Data Tests  
These are SQL queries that return failing rows.

Example:

```sql
select *
from {{ ref('orders_silver') }}
where total_amount < 0
```

DBT runs these automatically.


### 📁 6. `seeds/` — CSV Files Loaded as Tables  
Seeds are static reference data:

- country codes  
- currency mappings  
- product categories  

DBT loads them into the warehouse.

---

### 📁 7. `snapshots/` — Slowly Changing Dimensions (SCD Type 2)  
Snapshots track changes over time.

Example:

```sql
{% snapshot customers_snapshot %}
  {{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='timestamp',
      updated_at='updated_at'
    )
  }}
  select * from {{ source('raw', 'customers') }}
{% endsnapshot %}
```
**Notes:**
- **`{{ source('raw', 'customers') }}`** — resolves to the actual raw table (schema + table name declared
in `sources.yml`, e.g. `raw.customers`), the same way `ref()` resolves to another model. Using `source()` (rather than a bare table name) is what lets dbt track this snapshot as a **DAG node** downstream of
that source, include it in lineage graphs, and apply source freshness checks.
- **`{% snapshot %} ... {% endsnapshot %}`** — a distinct block type from a regular model (not just SQL in `models/`). Each time `dbt snapshot` runs, dbt takes the `select` result and diffs it against the snapshot's existing rows (matched on `unique_key`): 
  - unchanged rows are left alone; 
  - a row whose tracked columns differ gets its current record closed (`dbt_valid_to` set to now) and a new record inserted
(`dbt_valid_from` = now, `dbt_valid_to` = null) — that's the SCD Type 2 mechanic, giving you every
historical version of a row instead of just the latest one. 
  - `strategy: 'timestamp'` + `updated_at` tells
dbt which column marks "this row changed" (an alternative `strategy: 'check'` + `check_cols` diffs
specific columns directly when no reliable updated-at timestamp exists). Unlike a normal model, a
snapshot is only ever appended to — it's never rebuilt from scratch by `dbt run`, only extended by
`dbt snapshot`.

This is how DBT handles historical tracking.


### 📁 8. `analysis/` — Ad‑hoc SQL Queries  
These are not part of the DAG.  
Useful for exploration or temporary queries.


## 🔍 Deep Dive: How Everything Fits Together

### 🧱 1. **Sources → Staging Models**  
Sources define raw tables:

```yaml
sources:
  - name: raw
    tables:
      - name: orders
```

Staging models clean them:

```
models/staging/stg_orders.sql
```

These models:

- rename columns  
- enforce types  
- remove duplicates  
- standardize timestamps  

### 🧩 2. **Staging → Intermediate Models**  
Intermediate models combine staging models:

```
models/intermediate/int_orders_with_customers.sql
```

These models:

- join staging tables  
- apply business logic  
- enrich data  

### 📊 3. **Intermediate → Marts (Gold Models)**  
Gold models are your semantic layer:

```
models/marts/sales/sales_daily_summary.sql
```

These models:

- aggregate  
- roll up  
- produce dashboard‑ready tables  

### 🧪 4. **Tests**  
DBT automatically runs:

- schema tests (unique, not_null, accepted_values)  
- custom SQL tests  

Tests live in:

```
models/*.yml
tests/*.sql
```

### 📘 5. **Documentation + Lineage**  
DBT generates a full documentation site:

- model descriptions  
- column descriptions  
- lineage graph  
- tests  
- sources  

This is one of DBT’s strongest features.


### 🔄 6. **Materializations**  
Defined in `dbt_project.yml`:

```yaml
models:
  staging:
    materialized: view
  intermediate:
    materialized: table
  marts:
    materialized: incremental
```

Materializations control how DBT builds your models.


## 🧭 Summary Table

| Folder | Purpose | Key Concept |
|--------|---------|-------------|
| `dbt_project.yml` | Global config | Materializations, paths |
| `models/` | SQL transformations | DAG via `ref()` |
| `models/*.yml` | Tests + docs | Schema enforcement |
| `macros/` | Reusable SQL | Jinja templating |
| `tests/` | Custom tests | Data quality |
| `seeds/` | CSV → tables | Reference data |
| `snapshots/` | SCD Type 2 | Historical tracking |
| `analysis/` | Ad‑hoc SQL | Exploration |


## 🎯 Final Takeaway - DBT Project Best-Practice
A DBT project is structured like a real software project:

- **SQL models** are your code  
- **YAML files** define tests and documentation  
- **Macros** add programmability  
- **Sources** define raw inputs  
- **Snapshots** track history  
- **Seeds** load reference data  
- **Materializations** control how models are built  
- **The DAG** ties everything together  

Once you understand the structure, DBT becomes predictable, scalable, and extremely powerful.


---
---

# ⭐ Medallion architecture with DBT

A DBT medallion folder structure is simply the **DBT way** of implementing the classic **Bronze → Silver → Gold** architecture.  
DBT doesn’t enforce this structure — *you design it* — but the pattern is now industry‑standard because it keeps projects clean, scalable, and predictable.

Below is the **high‑level structure**, then a **deep drill‑down**, and finally a **workflow explanation** showing how everything fits together.


##  High‑Level Overview  
A DBT medallion project is usually organized like this:

```
models/
  bronze/      → raw ingestion + minimal cleanup
  silver/      → standardized, cleaned, conformed data
  gold/        → business-ready semantic models
  marts/       → optional: domain-specific gold models
  staging/     → optional: source-level cleaning
```

This structure mirrors the medallion architecture used in Databricks, Snowflake, BigQuery, and Fabric.


## 🧭 DBT Medallion Folder Structure (Detailed)


### 🥉 1. **Bronze Layer**  
Bronze models represent **raw data** loaded from external systems.

Folder:

```
models/bronze/
```

Contents:

- Raw tables from ingestion (Kafka, CDC, S3, Postgres, IoT streams)
- Minimal transformations:
  - rename columns
  - cast types
  - basic deduplication
  - flatten JSON
- No business logic

Example file:

```
models/bronze/bronze_orders.sql
```

Bronze is your **landing zone**.


### 🥈 2. **Silver Layer**  
Silver models represent **cleaned, standardized, conformed** data.

Folder:

```
models/silver/
```

Contents:

- Cleaned tables
- Standardized timestamps
- Normalized enums
- Deduplicated records
- Joined reference data
- Incremental models

Silver is your **analytics-ready foundation**.

Example:

```
models/silver/silver_orders.sql
models/silver/silver_customers.sql
```

Silver is where most data quality tests live.

### 🥇 3. **Gold Layer**  
Gold models represent **business-ready semantic tables**.

Folder:

```
models/gold/
```

Contents:

- Fact tables
- Dimension tables
- Aggregates
- Business metrics
- Dashboard-ready tables

Example:

```
models/gold/fct_sales_daily.sql
models/gold/dim_customers.sql
```

Gold is your **semantic layer** for BI tools.


### 🏛️ 4. **Marts Layer**  
Some teams split gold into domain-specific marts:

```
models/marts/
  sales/
  marketing/
  logistics/
  finance/
```

This is optional but extremely useful for large organizations.


### 🧼 5. **Staging Layer**  
Some teams add a staging layer between bronze and silver:

```
models/staging/
```

Staging models:

- rename columns to DBT naming conventions  
- cast types  
- remove duplicates  
- apply basic cleaning  

Staging is often used when bronze is *too raw*.

## 🔧 How the Layers Work Together  
Here’s the typical flow:

1. **Bronze**  
   - Raw ingestion  
   - Minimal cleanup  
   - No business logic  

2. **Staging (optional)**  
   - Standardize column names  
   - Cast types  
   - Basic cleaning  

3. **Silver**  
   - Conform schemas  
   - Join reference tables  
   - Normalize values  
   - Incremental loads  

4. **Gold**  
   - Facts  
   - Dimensions  
   - Aggregates  
   - Business metrics  

5. **Marts (optional)**  
   - Domain-specific gold models  

DBT builds the DAG automatically using `ref()`.

## 🧩 Example Folder Structure (Full)

```
models/
  bronze/
    bronze_orders.sql
    bronze_customers.sql

  staging/
    stg_orders.sql
    stg_customers.sql

  silver/
    silver_orders.sql
    silver_customers.sql
    silver_order_items.sql

  gold/
    dim_customers.sql
    fct_sales_daily.sql
    fct_sales_monthly.sql

  marts/
    sales/
      sales_dashboard.sql
    marketing/
      campaign_performance.sql
```

## 📘 Where Tests & Documentation Live  
Tests and documentation live in YAML files next to the models:

```
models/silver/silver_orders.yml
models/gold/fct_sales_daily.yml
```

These define:

- column descriptions  
- tests (unique, not_null, accepted_values)  
- tags  
- metadata  

## 🧪 Materialization Strategy  
Typical medallion materializations:

```
models:
  bronze:
    materialized: view
  staging:
    materialized: view
  silver:
    materialized: incremental
  gold:
    materialized: table
```

Silver is often incremental because it handles large volumes.

Gold is often table because BI tools query it heavily.


## 🎯 Final Takeaway - Medaillon architecture with DBT
A DBT medallion folder structure is simply the DBT implementation of the Bronze → Silver → Gold architecture:

- **Bronze** = raw  
- **Silver** = cleaned  
- **Gold** = business-ready  
- **Marts** = domain-specific gold  
- **Staging** = optional cleaning layer  

This structure keeps your project clean, scalable, and easy to navigate — and DBT automatically builds the DAG from it.

