select
    truck_id,
    st_y(geog::geometry) as lat,
    st_x(geog::geometry) as lon,
    truck_status,
    current_zone_id,
    updated_at
from {{ source('silver', 'truck_current_position') }}
