{{
    config(
        materialized='incremental',
        unique_key='invoice_id',
        incremental_strategy='delete+insert',
    )
}}

-- Replaces fact_payments (payments folded into the invoice lifecycle, see
-- plan decision #6). settle_latency_seconds is null until an invoice
-- actually reaches 'settled'.
with invoices as (
    select * from {{ ref('stg_invoices') }}
    {% if is_incremental() %}
    where updated_at > (select coalesce(max(updated_at), '1970-01-01') from {{ this }})
    {% endif %}
),

current_customers as (
    select * from {{ ref('dim_customer') }} where dbt_valid_to is null
)

select
    i.invoice_id,
    i.purchase_order_id,
    cc.customer_key,
    i.amount,
    i.status,
    i.payment_reminder,
    cast(to_char(i.created_at, 'YYYYMMDD') as int) as invoice_date_key,
    i.due_payment_date,
    case
        when i.status = 'settled'
        then extract(epoch from (i.updated_at - i.created_at))
    end as settle_latency_seconds,
    i.created_at,
    i.updated_at
from invoices i
left join current_customers cc on cc.customer_id = i.customer_id
