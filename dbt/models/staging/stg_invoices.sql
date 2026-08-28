select
    invoice_id,
    purchase_order_id,
    customer_id,
    amount,
    status,
    payment_reminder,
    due_payment_date,
    created_at,
    updated_at
from {{ source('silver', 'invoices_current') }}
