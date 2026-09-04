{{
    config(
        materialized='incremental',
        unique_key='product_on_order_id',
        incremental_strategy='delete+insert',
    )
}}

-- Denormalized sales mart: pre-joins fact_product_on_orders (line-item grain)
-- against its order header, product, customer, and zone dimensions so dashboard
-- panels can slice by product/category/customer/region/time without repeating
-- the same join set in every panel. Time-grain flexibility (Y/Q/M/W/D) is left
-- to query-time date_trunc() against order_date_key/updated_at, not baked in here.
with lines as (
    select * from {{ ref('fact_product_on_orders') }}
    {% if is_incremental() %}
    where updated_at > (select coalesce(max(updated_at), '1970-01-01') from {{ this }})
    {% endif %}
),

orders as (
    select purchase_order_id, customer_key, zone_id, order_date_key from {{ ref('fact_purchase_orders') }}
),

current_products as (
    select product_key, category, subcategory, brand from {{ ref('dim_product') }} where dbt_valid_to is null
),

current_customers as (
    select customer_key, segment from {{ ref('dim_customer') }} where dbt_valid_to is null
),

zones as (
    select zone_id, city as zone_city, canton from {{ ref('dim_delivery_zone') }}
)

select
    li.product_on_order_id,
    li.purchase_order_id,
    o.customer_key,
    cc.segment as customer_segment,
    o.zone_id,
    z.zone_city,
    z.canton,
    li.product_key,
    cp.category,
    cp.subcategory,
    cp.brand,
    o.order_date_key,
    li.qty_on_order,
    li.unit_price,
    li.line_amount,
    li.updated_at
from lines li
left join orders o on o.purchase_order_id = li.purchase_order_id
left join current_products cp on cp.product_key = li.product_key
left join current_customers cc on cc.customer_key = o.customer_key
left join zones z on z.zone_id = o.zone_id
