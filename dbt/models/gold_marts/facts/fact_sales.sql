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
--
-- customer_key/product_key on fact_purchase_orders/fact_product_on_orders are
-- surrogate keys captured at that fact row's own last incremental refresh, not
-- re-resolved when the referenced dimension later moves to a new SCD2 version
-- independently (customers.attribute_update_rate_per_hour/products.attribute_
-- update_rate_per_hour in config.yaml keep both dimensions churning). Comparing
-- them directly against dim_customer/dim_product's dbt_valid_to IS NULL row
-- fails for any customer/product whose dimension has since rolled forward -
-- confirmed empirically at ~75% of rows before this fix. customer_key_lookup/
-- product_key_lookup resolve through the stable natural key (customer_id/
-- product_id) instead of comparing two potentially-different-vintage surrogate
-- keys, and the fact's own customer_key/product_key columns below are the
-- CURRENT surrogate key (from current_customers/current_products), not the
-- possibly-stale one carried on fact_purchase_orders/fact_product_on_orders -
-- so a downstream join back to dim_customer/dim_product from this mart doesn't
-- need to know about this quirk at all.
with lines as (
    select * from {{ ref('fact_product_on_orders') }}
    {% if is_incremental() %}
    where updated_at > (select coalesce(max(updated_at), '1970-01-01') from {{ this }})
    {% endif %}
),

orders as (
    select purchase_order_id, customer_key, zone_id, order_date_key from {{ ref('fact_purchase_orders') }}
),

customer_key_lookup as (
    select customer_key, customer_id from {{ ref('dim_customer') }}
),

product_key_lookup as (
    select product_key, product_id from {{ ref('dim_product') }}
),

current_products as (
    select product_id, product_key, category, subcategory, brand from {{ ref('dim_product') }} where dbt_valid_to is null
),

current_customers as (
    select customer_id, customer_key, segment from {{ ref('dim_customer') }} where dbt_valid_to is null
),

zones as (
    select zone_id, city as zone_city, canton from {{ ref('dim_delivery_zone') }}
)

select
    li.product_on_order_id,
    li.purchase_order_id,
    cc.customer_key,
    cc.segment as customer_segment,
    o.zone_id,
    z.zone_city,
    z.canton,
    cp.product_key,
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
left join customer_key_lookup ckl on ckl.customer_key = o.customer_key
left join current_customers cc on cc.customer_id = ckl.customer_id
left join product_key_lookup pkl on pkl.product_key = li.product_key
left join current_products cp on cp.product_id = pkl.product_id
left join zones z on z.zone_id = o.zone_id
