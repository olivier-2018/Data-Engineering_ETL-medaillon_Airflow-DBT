{{
    config(
        materialized='incremental',
        unique_key='purchase_order_id',
        incremental_strategy='delete+insert',
    )
}}

-- Correlates weather at each delivery zone's NEAREST observation station
-- with that order's actual delivery time. Replaces the pre-redesign
-- country->representative-city hardcoded map (destination_country_key/
-- dim_destination_country are both gone now that the whole simulation is
-- Switzerland-only, see plan decision #3) with a real nearest-station
-- lookup by squared lat/lon distance - accurate enough for Switzerland's
-- small geographic extent, and it now works per-zone instead of per-
-- country, which is more precise given all 40 zones share one country.
with delivered_shipments as (
    select * from {{ ref('fact_shipments') }}
    where is_delivered
    {% if is_incremental() %}
        and updated_at > (select coalesce(max(updated_at), '1970-01-01') from {{ this }})
    {% endif %}
),

zones as (
    select * from {{ ref('dim_delivery_zone') }}
),

stations as (
    select * from {{ ref('stg_weather_stations') }}
),

nearest_station as (
    select
        z.zone_id,
        s.name as station_name,
        row_number() over (
            partition by z.zone_id
            order by (power(z.lat - s.lat, 2) + power(z.lon - s.lon, 2))
        ) as rn
    from zones z
    cross join stations s
),

nearest_weather as (
    select
        ds.purchase_order_id,
        w.temperature,
        w.humidity,
        w.pressure,
        w.weather_condition,
        row_number() over (
            partition by ds.purchase_order_id
            order by abs(extract(epoch from (w.observed_at - ds.delivery_time)))
        ) as rn
    from delivered_shipments ds
    join nearest_station ns on ns.zone_id = ds.zone_id and ns.rn = 1
    join {{ source('iot', 'weather_observations') }} w
        on w.location_name = ns.station_name
        and w.forecast_horizon_hours is null
)

select
    ds.purchase_order_id,
    ds.zone_id,
    ds.delivery_time,
    nw.temperature,
    nw.humidity,
    nw.pressure,
    nw.weather_condition,
    ds.time_to_destination_seconds,
    now() as updated_at
from delivered_shipments ds
left join nearest_weather nw on nw.purchase_order_id = ds.purchase_order_id and nw.rn = 1
