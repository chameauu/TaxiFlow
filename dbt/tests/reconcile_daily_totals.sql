with fact as (
    select
        count(*) as trip_count,
        cast(sum(total_amount) as decimal(18, 2)) as total_amount
    from {{ ref('fct_trips') }}
),

daily as (
    select
        cast(sum(trip_count) as bigint) as trip_count,
        cast(sum(total_revenue) as decimal(18, 2)) as total_amount
    from {{ ref('agg_daily_trips') }}
)

select
    fact.trip_count as fact_trip_count,
    daily.trip_count as daily_trip_count,
    fact.total_amount as fact_total_amount,
    daily.total_amount as daily_total_amount
from fact
cross join daily
where fact.trip_count != daily.trip_count
   or abs(fact.total_amount - daily.total_amount) > 0.01
