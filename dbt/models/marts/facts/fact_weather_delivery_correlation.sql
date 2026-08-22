{{
    config(
        materialized='incremental',
        unique_key='shipment_id',
        incremental_strategy='delete+insert',
    )
}}

-- Correlates weather at the destination country's representative location
-- (Biel/Bern=CH, Paris=FR, Berlin=DE, Milan=IT - see data_generators/config.yaml
-- weather.locations) with each delivered shipment's actual delivery window -
-- the "weather <-> logistics" bridge fact, feasible now that weather is
-- tracked per real location rather than a single generic reading.
with country_location as (
    select * from (
        values
            ('CH', 'Bern'),
            ('FR', 'Paris'),
            ('DE', 'Berlin'),
            ('IT', 'Milan')
    ) as t(destination_country_key, location_name)
),

delivered_shipments as (
    select * from {{ ref('fact_shipments') }}
    where is_delivered
    {% if is_incremental() %}
        and updated_at > (select coalesce(max(updated_at), '1970-01-01') from {{ this }})
    {% endif %}
),

nearest_weather as (
    select
        ds.shipment_id,
        w.temperature,
        w.humidity,
        w.pressure,
        w.weather_condition,
        row_number() over (
            partition by ds.shipment_id
            order by abs(extract(epoch from (w.observed_at - ds.delivery_time)))
        ) as rn
    from delivered_shipments ds
    join country_location cl on cl.destination_country_key = ds.destination_country_key
    join {{ source('iot', 'weather_observations') }} w
        on w.location_name = cl.location_name
        and w.forecast_horizon_hours is null  -- current observations only
)

select
    ds.shipment_id,
    ds.destination_country_key,
    ds.delivery_time,
    nw.temperature,
    nw.humidity,
    nw.pressure,
    nw.weather_condition,
    ds.time_to_destination_seconds,
    now() as updated_at
from delivered_shipments ds
left join nearest_weather nw on nw.shipment_id = ds.shipment_id and nw.rn = 1
