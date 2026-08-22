{% snapshot product_snapshot %}
{{
    config(
        target_schema='gold',
        unique_key='product_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}
select * from {{ source('silver', 'products_current') }}
{% endsnapshot %}
