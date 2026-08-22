select
    shipment_id,
    truck_id,
    order_id,
    st_y(geog::geometry) as lat,
    st_x(geog::geometry) as lon,
    shipment_status,
    destination_country,
    updated_at
from {{ source('silver', 'truck_positions_current') }}
