{{ config(materialized='view') }}

-- SCD2 dimension: the real historical accumulation already happened in the
-- customer_snapshot (dbt snapshot, timestamp strategy) - this model just
-- adds a surrogate key on top for use as a fact-table foreign key.
select
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }} as customer_key,
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
    dbt_valid_from,
    dbt_valid_to
from {{ ref('customer_snapshot') }}
