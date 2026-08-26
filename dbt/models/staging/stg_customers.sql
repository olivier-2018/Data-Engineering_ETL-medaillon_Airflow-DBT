select
    customer_id,
    name,
    email,
    address,
    tel,
    country,
    city,
    segment,
    verified_account,
    disabled_account,
    created_at,
    updated_at
from {{ source('silver', 'customers_current') }}
