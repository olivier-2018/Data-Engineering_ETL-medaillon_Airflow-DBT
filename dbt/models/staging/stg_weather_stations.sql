select
    station_id,
    name,
    lat,
    lon
from {{ source('reference', 'weather_stations') }}
