{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
    )
}}

select
    p.order_id,
    p.payment_status,
    p.amount,
    cast(to_char(p.updated_at, 'YYYYMMDD') as int) as payment_date_key,
    p.updated_at
from {{ ref('stg_payments') }} p
{% if is_incremental() %}
where p.updated_at > (select coalesce(max(updated_at), '1970-01-01') from {{ this }})
{% endif %}
