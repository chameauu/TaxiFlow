select
    pickup_date,
    pickup_zone,
    pickup_borough,
    vendor_id,
    service,
    trip_count,
    avg_trip_distance,
    total_revenue,
    tip_revenue
from gold.agg_zone_daily
order by pickup_date desc, total_revenue desc
