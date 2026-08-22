{{ config(materialized='table') }}

-- Degenerate dimension: the fleet is derived from trucks actually observed
-- in the data rather than hardcoded from config.yaml's num_trucks, so it
-- stays correct even if the fleet size changes between runs.
select distinct truck_id
from {{ ref('stg_truck_positions') }}
