select
    zone_id,
    city,
    canton,
    country,
    lat,
    lon,
    is_warehouse,
    is_active,
    created_at,
    updated_at
from {{ source('reference', 'delivery_zones') }}
