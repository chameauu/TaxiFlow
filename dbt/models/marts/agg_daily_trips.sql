select
    concat(cast(pickup_date as varchar), '|', service) as daily_key,
    pickup_date,
    service,
    count(*) as trip_count,
    cast(avg(passenger_count) as double) as avg_passenger_count,
    cast(sum(total_amount) as decimal(18, 2)) as total_revenue,
    cast(sum(fare_amount) as decimal(18, 2)) as fare_revenue,
    cast(sum(tip_amount) as decimal(18, 2)) as tip_revenue,
    count(distinct pickup_location_id) as pickup_zone_count
from {{ ref('fct_trips') }}
group by pickup_date, service
