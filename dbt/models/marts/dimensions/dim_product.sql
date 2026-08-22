{{ config(materialized='view') }}

select
    {{ dbt_utils.generate_surrogate_key(['product_id', 'dbt_valid_from']) }} as product_key,
    product_id,
    name,
    category,
    subcategory,
    unit_price,
    weight_kg,
    initial_stock,
    dbt_valid_from,
    dbt_valid_to
from {{ ref('product_snapshot') }}
