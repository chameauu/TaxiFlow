with fact as (
    select
        count(*) as trip_count,
        cast(sum(total_amount) as decimal(18, 2)) as total_amount
    from {{ ref('fct_trips') }}
),

staging as (
    select
        count(*) as trip_count,
        cast(sum(total_amount) as decimal(18, 2)) as total_amount
    from {{ ref('stg_yellow_trips') }}
)

select
    fact.trip_count as fact_trip_count,
    staging.trip_count as staging_trip_count,
    fact.total_amount as fact_total_amount,
    staging.total_amount as staging_total_amount
from fact
cross join staging
where fact.trip_count != staging.trip_count
   or abs(fact.total_amount - staging.total_amount) > 0.01
