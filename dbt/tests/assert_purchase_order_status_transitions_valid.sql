-- Cross-layer sanity check on the accumulated snapshot history (a different
-- check than Spark's per-batch validation in purchase_orders_to_silver.py,
-- which only sees one incremental batch at a time): across the FULL history
-- in purchase_order_status_snapshot, status rank must never decrease for the
-- same purchase_order_id (except cancelled, which is terminal from any
-- earlier rank <= 3, mirroring _CANCELLABLE_MAX_RANK in
-- purchase_orders_to_silver.py).
with ranked as (
    select
        purchase_order_id,
        status,
        dbt_valid_from,
        case status
            when 'created' then 0
            when 'invoiced' then 1
            when 'paid' then 2
            when 'on-hold' then 3
            when 'loaded' then 4
            when 'in-transit' then 5
            when 'delivered' then 6
            when 'closed' then 7
            when 'cancelled' then 99
            else -1
        end as status_rank
    from {{ ref('purchase_order_status_snapshot') }}
),

with_lag as (
    select
        *,
        lag(status_rank) over (partition by purchase_order_id order by dbt_valid_from) as prev_rank
    from ranked
)

select *
from with_lag
where prev_rank is not null
  and status_rank != 99  -- cancelled is always a valid terminal transition
  and status_rank < prev_rank
