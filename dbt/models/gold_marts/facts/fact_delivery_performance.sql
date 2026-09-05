{{
    config(
        materialized='incremental',
        unique_key='purchase_order_id',
        incremental_strategy='delete+insert',
    )
}}

-- Delivery-performance mart: compares each delivered order's target_delivery_date/
-- created_at against fact_shipments' already-computed delivery_time. Reads the raw
-- timestamps from stg_purchase_orders rather than fact_purchase_orders, since the
-- latter only stores the _date_key int (YYYYMMDD) - not enough precision for a
-- sub-day delay/process-time calculation.
--
-- customer_key is resolved from stg_purchase_orders.customer_id (the stable
-- natural key) against dim_customer's CURRENT row, not copied from
-- fact_purchase_orders.customer_key - that column is a surrogate key captured
-- at fact_purchase_orders' own last refresh, which goes stale the moment the
-- customer's SCD2 dimension moves to a new version independently (same root
-- cause fixed in fact_sales.sql; confirmed this model would hit the identical
-- failure mode once customer dimension churn resumes).
--
-- Grain is one row per delivered order (is_delivered = true in fact_shipments);
-- undelivered orders are excluded until fact_shipments has a delivery_time for
-- them. Inherits fact_shipments' known incremental-overwrite limitation (see that
-- model's comment) since it's incremental off the same delivery_time column.
with shipments as (
    select * from {{ ref('fact_shipments') }}
    where is_delivered
    {% if is_incremental() %}
    and updated_at > (select coalesce(max(updated_at), '1970-01-01') from {{ this }})
    {% endif %}
),

orders_raw as (
    select purchase_order_id, customer_id, created_at, target_delivery_date
    from {{ ref('stg_purchase_orders') }}
),

header as (
    select purchase_order_id, order_date_key, target_delivery_date_key
    from {{ ref('fact_purchase_orders') }}
),

current_customers as (
    select customer_id, customer_key from {{ ref('dim_customer') }} where dbt_valid_to is null
)

select
    s.purchase_order_id,
    cc.customer_key,
    s.truck_key,
    s.zone_id,
    h.order_date_key,
    h.target_delivery_date_key,
    o.created_at as order_created_at,
    o.target_delivery_date,
    s.delivery_time,
    extract(epoch from (s.delivery_time - o.created_at)) as process_time_seconds,
    extract(epoch from (s.delivery_time - o.target_delivery_date)) as delay_seconds,
    extract(epoch from (s.delivery_time - o.target_delivery_date)) / 86400.0 as delay_days,
    (s.delivery_time <= o.target_delivery_date) as is_on_time,
    now() as updated_at
from shipments s
join orders_raw o on o.purchase_order_id = s.purchase_order_id
left join header h on h.purchase_order_id = s.purchase_order_id
left join current_customers cc on cc.customer_id = o.customer_id
where o.target_delivery_date is not null
