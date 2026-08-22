{{
    config(
        materialized='incremental',
        unique_key=['product_id', 'snapshot_date'],
        incremental_strategy='delete+insert',
    )
}}

-- Periodic snapshot fact: one row per (product, day), refreshed each dbt
-- run - re-running on the same day updates that day's row, a new day
-- inserts a new one.
select
    i.product_id,
    current_date as snapshot_date,
    cast(to_char(current_date, 'YYYYMMDD') as int) as date_key,
    i.current_stock,
    p.initial_stock,
    (i.current_stock < p.initial_stock * 0.2) as is_below_restock_threshold,
    i.updated_at
from {{ ref('stg_inventory') }} i
left join {{ ref('stg_products') }} p on p.product_id = i.product_id
