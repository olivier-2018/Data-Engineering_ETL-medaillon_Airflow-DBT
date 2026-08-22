-- Cross-layer sanity check on the accumulated snapshot history (a different
-- check than Spark's per-batch validation in sales_orders_to_silver.py,
-- which only sees one incremental batch at a time): across the FULL history
-- in order_status_snapshot, order_status rank must never decrease for the
-- same order_id (except cancelled, which is terminal from any early rank).
with ranked as (
    select
        order_id,
        order_status,
        dbt_valid_from,
        case order_status
            when 'pending' then 0
            when 'confirmed' then 1
            when 'picking' then 2
            when 'ready_for_dispatch' then 3
            when 'shipped' then 4
            when 'delivered' then 5
            when 'cancelled' then 99
            else -1
        end as status_rank
    from {{ ref('order_status_snapshot') }}
),

with_lag as (
    select
        *,
        lag(status_rank) over (partition by order_id order by dbt_valid_from) as prev_rank
    from ranked
)

select *
from with_lag
where prev_rank is not null
  and status_rank != 99  -- cancelled is always a valid terminal transition
  and status_rank < prev_rank
