select
    truck_id,
    name,
    brand,
    model,
    size,
    capacity,
    weight_kg,
    updated_at
from {{ source('silver', 'truck_fleet_current') }}
