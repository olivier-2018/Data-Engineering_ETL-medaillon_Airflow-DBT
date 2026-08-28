select
    product_on_order_id,
    purchase_order_id,
    product_id,
    qty_on_order,
    customer_comment,
    created_at,
    updated_at
from {{ source('silver', 'product_on_orders_current') }}
