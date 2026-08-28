{{
    config(
        materialized='incremental',
        unique_key=['product_id', 'snapshot_date'],
        incremental_strategy='delete+insert',
    )
}}

-- Periodic snapshot fact: one row per (product, day), refreshed each dbt
-- run - re-running on the same day updates that day's row, a new day
-- inserts a new one. restock_required is now materialized directly in
-- silver.products_current (products_to_silver.py computes it against
-- config.yaml's restock_threshold_pct at write time) rather than
-- re-derived here against a hardcoded 20% - one source of truth for the
-- threshold. refill_unit_price/refill discount economics come from the
-- product's most recent 'refill' bronze event, joined via silver's
-- nominal_capacity/unit_price (current price already reflects any refill
-- discount applied at write time in the generator, restock_check.py).
select
    i.product_id,
    current_date as snapshot_date,
    cast(to_char(current_date, 'YYYYMMDD') as int) as date_key,
    i.current_stock,
    p.nominal_capacity,
    p.unit_price,
    p.restock_required,
    i.updated_at
from {{ ref('stg_inventory') }} i
left join {{ ref('stg_products') }} p on p.product_id = i.product_id
