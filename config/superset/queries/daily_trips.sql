select
    pickup_date,
    service,
    trip_count,
    avg_passenger_count,
    total_revenue,
    fare_revenue,
    tip_revenue,
    pickup_zone_count
from gold.agg_daily_trips
order by pickup_date, service
