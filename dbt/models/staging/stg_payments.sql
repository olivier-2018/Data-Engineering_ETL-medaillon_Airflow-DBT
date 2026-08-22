select
    order_id,
    payment_status,
    amount,
    updated_at
from {{ source('silver', 'payments_current') }}
