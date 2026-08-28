{{
    config(
        materialized='incremental',
        unique_key='product_on_order_id',
        incremental_strategy='delete+insert',
    )
}}

-- New line-item grain (order x product), enabling per-product revenue/
-- volume analysis the header-only fact_purchase_orders can't answer alone.
-- Revenue uses the product's CURRENT unit_price (dim_product filtered to
-- dbt_valid_to is null) since product_on_order_events carries no price
-- snapshot at order time - a known simplification, not a price-at-order-
-- time calculation.
with line_items as (
    select * from {{ ref('stg_product_on_orders') }}
    {% if is_incremental() %}
    where updated_at > (select coalesce(max(updated_at), '1970-01-01') from {{ this }})
    {% endif %}
),

current_products as (
    select * from {{ ref('dim_product') }} where dbt_valid_to is null
)

select
    li.product_on_order_id,
    li.purchase_order_id,
    cp.product_key,
    li.qty_on_order,
    cp.unit_price,
    li.qty_on_order * cp.unit_price as line_amount,
    cast(to_char(li.created_at, 'YYYYMMDD') as int) as order_date_key,
    li.updated_at
from line_items li
left join current_products cp on cp.product_id = li.product_id
