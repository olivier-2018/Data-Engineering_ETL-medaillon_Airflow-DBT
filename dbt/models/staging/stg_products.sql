select
    product_id,
    name,
    category,
    subcategory,
    unit_price,
    weight_kg,
    initial_stock,
    updated_at
from {{ source('silver', 'products_current') }}
