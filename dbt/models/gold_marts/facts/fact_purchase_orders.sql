{{
    config(
        materialized='incremental',
        unique_key='purchase_order_id',
        incremental_strategy='delete+insert',
    )
}}

-- Header-level fact: one row per order. Line-item detail (product/qty/
-- revenue) lives in fact_product_on_orders instead - splitting the two
-- lets header attributes (status, dates, zone) be queried without
-- fanning out across an order's product lines.
with orders as (
    select * from {{ ref('stg_purchase_orders') }}
    {% if is_incremental() %}
    where updated_at > (select coalesce(max(updated_at), '1970-01-01') from {{ this }})
    {% endif %}
),

current_customers as (
    select * from {{ ref('dim_customer') }} where dbt_valid_to is null
)

select
    o.purchase_order_id,
    cc.customer_key,
    o.zone_id,
    o.truck_id,
    cast(to_char(o.created_at, 'YYYYMMDD') as int) as order_date_key,
    cast(to_char(o.target_delivery_date, 'YYYYMMDD') as int) as target_delivery_date_key,
    o.status,
    o.created_at,
    o.updated_at
from orders o
left join current_customers cc on cc.customer_id = o.customer_id
