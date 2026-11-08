select
    pickup_day_of_week,
    pickup_hour,
    pickup_borough,
    service,
    sum(trip_count) as trip_count,
    sum(total_revenue) as total_revenue
from gold.agg_hourly_demand
group by
    pickup_day_of_week,
    pickup_hour,
    pickup_borough,
    service
order by pickup_day_of_week, pickup_hour
