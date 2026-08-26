select
    product_id,
    name,
    brand,
    model,
    category,
    subcategory,
    unit_price,
    weight_kg,
    nominal_capacity,
    restock_required,
    updated_at
from {{ source('silver', 'products_current') }}
