-- Same cross-layer idea as assert_purchase_order_status_transitions_valid.sql,
-- applied to invoice_snapshot's accumulated history, plus a check that
-- payment_reminder never decreases within the 'pending' status (it only
-- ever increments on a reminder, per invoices_to_silver.py).
with ranked as (
    select
        invoice_id,
        status,
        payment_reminder,
        dbt_valid_from,
        case status
            when 'created' then 0
            when 'pending' then 1
            when 'settled' then 2
            when 'cancelled' then 99
            else -1
        end as status_rank
    from {{ ref('invoice_snapshot') }}
),

with_lag as (
    select
        *,
        lag(status_rank) over (partition by invoice_id order by dbt_valid_from) as prev_rank,
        lag(payment_reminder) over (partition by invoice_id order by dbt_valid_from) as prev_reminder
    from ranked
)

select *
from with_lag
where prev_rank is not null
  and (
    (status_rank != 99 and status_rank < prev_rank)
    or (payment_reminder < prev_reminder)
  )
