select
    customer_id,
    name,
    country,
    city,
    segment,
    updated_at
from {{ source('silver', 'customers_current') }}
