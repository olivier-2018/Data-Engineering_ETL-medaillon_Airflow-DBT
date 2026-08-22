{{ config(materialized='table') }}

-- Static reference dimension: the 4 delivery countries in scope.
select * from (
    values
        ('CH', 'Switzerland'),
        ('FR', 'France'),
        ('DE', 'Germany'),
        ('IT', 'Italy')
) as t(country_code, country_name)
