{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
    )
}}

with orders as (
    select * from {{ ref('stg_sales_orders') }}
    {% if is_incremental() %}
    where updated_at > (select coalesce(max(updated_at), '1970-01-01') from {{ this }})
    {% endif %}
),

current_customers as (
    select * from {{ ref('dim_customer') }} where dbt_valid_to is null
),

current_products as (
    select * from {{ ref('dim_product') }} where dbt_valid_to is null
)

select
    o.order_id,
    cc.customer_key,
    cp.product_key,
    cast(to_char(o.created_at, 'YYYYMMDD') as int) as order_date_key,
    o.quantity,
    o.unit_price_snapshot,
    o.quantity * o.unit_price_snapshot as order_amount,
    o.order_status,
    o.updated_at
from orders o
left join current_customers cc on cc.customer_id = o.customer_id
left join current_products cp on cp.product_id = o.product_id
