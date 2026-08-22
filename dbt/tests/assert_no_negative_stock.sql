-- Fails if any product's current stock has gone negative - dbt tests pass
-- when the query returns zero rows.
select product_id, current_stock
from {{ ref('stg_inventory') }}
where current_stock < 0
