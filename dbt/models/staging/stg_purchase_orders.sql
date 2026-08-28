select
    purchase_order_id,
    customer_id,
    status,
    delivery_address,
    contact_tel,
    invoice_address,
    vat_number,
    target_delivery_date,
    truck_id,
    zone_id,
    created_at,
    updated_at
from {{ source('silver', 'purchase_orders_current') }}
