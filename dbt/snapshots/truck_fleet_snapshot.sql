{% snapshot truck_fleet_snapshot %}
{{
    config(
        target_schema='gold',
        unique_key='truck_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}
select * from {{ ref('stg_truck_fleet') }}
{% endsnapshot %}
