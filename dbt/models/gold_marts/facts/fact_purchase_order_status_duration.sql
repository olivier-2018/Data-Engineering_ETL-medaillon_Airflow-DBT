{{ config(materialized='view') }}

-- Thin derived view over purchase_order_status_snapshot's SCD2 history: each row
-- in the snapshot already IS one status's full validity window, so duration-in-
-- status is just dbt_valid_to - dbt_valid_from (or now() for the still-current
-- status row). A view, not incremental: it's a cheap read over an already-
-- materialized snapshot table, and incremental delete+insert here would need a
-- synthetic composite key (purchase_order_id + dbt_valid_from) for no real
-- performance benefit at this data volume.
select
    purchase_order_id,
    status,
    dbt_valid_from as status_entered_at,
    coalesce(dbt_valid_to, now()) as status_exited_at,
    extract(epoch from (coalesce(dbt_valid_to, now()) - dbt_valid_from)) as status_duration_seconds
from {{ ref('purchase_order_status_snapshot') }}
