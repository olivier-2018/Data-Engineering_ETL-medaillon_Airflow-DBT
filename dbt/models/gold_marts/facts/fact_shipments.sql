{{
    config(
        materialized='incremental',
        unique_key='purchase_order_id',
        incremental_strategy='delete+insert',
    )
}}

-- Dispatch/delivery timing is read directly from the bronze event log
-- (iot.purchase_order_events, see docs/DBT.md §6), not from silver, since
-- silver only retains an order's LATEST status - the exact in-transit/
-- delivered transition timestamps needed for time_to_destination_seconds
-- only exist in bronze's full history.
--
-- Grain is one row per order, not a synthetic "truck stop" surrogate:
-- reconstructing discrete stop boundaries from truck_position_events'
-- current_zone_id (which persists through the whole next travel leg, not
-- just the arrival instant) is fragile and adds a grain with no
-- analytical payoff a GROUP BY truck_id/zone_id/date can't already give.
--
-- Known limitation: if an order's in-transit and delivered events land in
-- different dbt runs, an incremental pass could see only one of the two
-- and delete+insert would overwrite a previously-complete row with an
-- incomplete one. Accepted for now (same pattern the pre-redesign
-- fact_shipments already used); revisit if it causes visible gaps.
with order_events as (
    select
        purchase_order_id,
        min(event_at) filter (where status = 'in-transit') as dispatch_time,
        max(event_at) filter (where status = 'delivered') as delivery_time
    from {{ source('iot', 'purchase_order_events') }}
    {% if is_incremental() %}
    where ingested_at > (select coalesce(max(updated_at), '1970-01-01') from {{ this }})
    {% endif %}
    group by purchase_order_id
),

orders as (
    select purchase_order_id, truck_id, zone_id from {{ ref('stg_purchase_orders') }}
),

current_trucks as (
    select * from {{ ref('dim_truck') }} where dbt_valid_to is null
)

select
    oe.purchase_order_id,
    o.truck_id,
    ct.truck_key,
    o.zone_id,
    oe.dispatch_time,
    oe.delivery_time,
    case
        when oe.delivery_time is not null and oe.dispatch_time is not null
        then extract(epoch from (oe.delivery_time - oe.dispatch_time))
    end as time_to_destination_seconds,
    (oe.delivery_time is not null) as is_delivered,
    now() as updated_at
from order_events oe
join orders o on o.purchase_order_id = oe.purchase_order_id
left join current_trucks ct on ct.truck_id = o.truck_id
