select
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price_snapshot,
    order_status,
    created_at,
    updated_at
from {{ source('silver', 'sales_orders_current') }}
