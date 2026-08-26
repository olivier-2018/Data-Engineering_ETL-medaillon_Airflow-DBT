{{ config(materialized='table') }}

-- A real dimension now (was degenerate pre-redesign, derived from distinct
-- truck_id in the position stream) - the fleet is a proper SCD2-tracked
-- source (truck_fleet_snapshot) with actual attributes (brand/model/
-- capacity/weight), not just an id with nothing else known about it.
select
    {{ dbt_utils.generate_surrogate_key(['truck_id', 'dbt_valid_from']) }} as truck_key,
    truck_id,
    name,
    brand,
    model,
    size,
    capacity,
    weight_kg,
    dbt_valid_from,
    dbt_valid_to
from {{ ref('truck_fleet_snapshot') }}
