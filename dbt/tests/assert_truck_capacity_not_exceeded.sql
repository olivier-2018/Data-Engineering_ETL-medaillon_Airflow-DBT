-- Defense-in-depth check on consolidation: the sum of qty_on_order for
-- orders sharing a truck run must never exceed that truck's capacity.
-- fact_shipments doesn't carry an explicit run id (see docs/DBT.md §6 -
-- deliberately order-grain, not a synthetic stop/run grain), so a run is
-- approximated here by bucketing dispatch_time to the minute: orders
-- dispatched together on the same truck within the same minute were
-- loaded as part of the same Dispatcher batch. This is a practical
-- approximation, not an exact run reconstruction - acceptable for a
-- defense-in-depth check, not a source of truth.
with truck_loads as (
    select
        fs.truck_key,
        date_trunc('minute', fs.dispatch_time) as run_bucket,
        sum(fpo.qty_on_order) as total_qty
    from {{ ref('fact_shipments') }} fs
    join {{ ref('fact_product_on_orders') }} fpo on fpo.purchase_order_id = fs.purchase_order_id
    where fs.dispatch_time is not null
    group by fs.truck_key, date_trunc('minute', fs.dispatch_time)
)

select tl.*, dt.capacity
from truck_loads tl
join {{ ref('dim_truck') }} dt on dt.truck_key = tl.truck_key
where tl.total_qty > dt.capacity
