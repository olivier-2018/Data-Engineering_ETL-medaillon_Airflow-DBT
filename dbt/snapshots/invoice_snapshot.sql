{% snapshot invoice_snapshot %}
{{
    config(
        target_schema='gold',
        unique_key='invoice_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}
select * from {{ ref('stg_invoices') }}
{% endsnapshot %}
