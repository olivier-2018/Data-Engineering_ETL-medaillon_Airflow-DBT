{{ config(materialized='view') }}

-- Plain view, no SCD2 snapshot needed - zones are static enough that
-- is_active as a current-state flag is sufficient. Sourced from the full
-- 40-city reference list (not just the currently-active subset), so a
-- fact referencing a city later deactivated still resolves.
select
    zone_id,
    city,
    canton,
    country,
    lat,
    lon,
    is_warehouse,
    is_active
from {{ ref('stg_delivery_zones') }}
