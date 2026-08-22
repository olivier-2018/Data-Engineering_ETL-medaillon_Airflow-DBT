{% snapshot order_status_snapshot %}
{{
    config(
        target_schema='gold',
        unique_key='order_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}
select * from {{ source('silver', 'sales_orders_current') }}
{% endsnapshot %}
