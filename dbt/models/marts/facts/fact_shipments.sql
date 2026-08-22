{{
    config(
        materialized='incremental',
        unique_key='shipment_id',
        incremental_strategy='delete+insert',
    )
}}

-- Dispatch/delivery timing is computed directly from the bronze event log
-- (iot.truck_position_events), not from silver.truck_positions_current,
-- since silver only retains the LATEST position per shipment - the full
-- history needed for time_to_destination lives only in bronze.
with shipment_events as (
    select
        shipment_id,
        truck_id,
        order_id,
        destination_country,
        min(event_at) filter (where shipment_status = 'loading') as dispatch_time,
        max(event_at) filter (where shipment_status = 'delivered') as delivery_time
    from {{ source('iot', 'truck_position_events') }}
    {% if is_incremental() %}
    where ingested_at > (select coalesce(max(updated_at), '1970-01-01') from {{ this }})
    {% endif %}
    group by shipment_id, truck_id, order_id, destination_country
),

current_trucks as (
    select * from {{ ref('dim_truck') }}
),

countries as (
    select * from {{ ref('dim_destination_country') }}
)

select
    se.shipment_id,
    se.order_id,
    ct.truck_id,
    co.country_code as destination_country_key,
    se.dispatch_time,
    se.delivery_time,
    case
        when se.delivery_time is not null and se.dispatch_time is not null
        then extract(epoch from (se.delivery_time - se.dispatch_time))
    end as time_to_destination_seconds,
    (se.delivery_time is not null) as is_delivered,
    now() as updated_at
from shipment_events se
left join current_trucks ct on ct.truck_id = se.truck_id
left join countries co on co.country_code = se.destination_country
