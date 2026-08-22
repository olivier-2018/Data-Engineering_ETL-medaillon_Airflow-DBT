{{ config(materialized='table') }}

with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast('2027-12-31' as date)"
    ) }}
)

select
    cast(to_char(date_day, 'YYYYMMDD') as int) as date_key,
    date_day as date,
    extract(hour from date_day) as hour,
    extract(dow from date_day) as day_of_week,
    extract(week from date_day) as week,
    extract(month from date_day) as month,
    extract(quarter from date_day) as quarter,
    extract(year from date_day) as year,
    (extract(dow from date_day) in (0, 6)) as is_weekend
from spine
