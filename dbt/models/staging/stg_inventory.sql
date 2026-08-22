select
    product_id,
    current_stock,
    updated_at
from {{ source('silver', 'inventory_current') }}
