{% snapshot purchase_order_status_snapshot %}
{{
    config(
        target_schema='gold',
        unique_key='purchase_order_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}
select * from {{ ref('stg_purchase_orders') }}
{% endsnapshot %}
