-- Same cross-layer idea as assert_order_status_transitions_valid.sql,
-- applied to payment_status_snapshot's accumulated history.
with ranked as (
    select
        order_id,
        payment_status,
        dbt_valid_from,
        case payment_status
            when 'authorized' then 0
            when 'captured' then 1
            when 'failed' then 1
            when 'refunded' then 2
            else -1
        end as status_rank
    from {{ ref('payment_status_snapshot') }}
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
  and status_rank < prev_rank
