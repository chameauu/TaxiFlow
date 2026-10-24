select
    concat(
        cast(pickup_day_of_week as varchar), '|',
        cast(pickup_hour as varchar), '|',
        cast(pickup_location_id as varchar), '|',
        service
    ) as demand_key,
    pickup_day_of_week,
    pickup_hour,
    pickup_location_id,
    pickup_zone,
    pickup_borough,
    service,
    count(*) as trip_count,
    cast(avg(passenger_count) as double) as avg_passenger_count,
    cast(sum(total_amount) as decimal(18, 2)) as total_revenue
from {{ ref('fct_trips') }}
group by
    pickup_day_of_week,
    pickup_hour,
    pickup_location_id,
    pickup_zone,
    pickup_borough,
    service
