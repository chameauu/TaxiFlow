select
    concat(
        cast(pickup_date as varchar), '|',
        cast(pickup_location_id as varchar), '|',
        cast(vendor_id as varchar), '|',
        service
    ) as zone_daily_key,
    pickup_date,
    pickup_location_id,
    pickup_zone,
    pickup_borough,
    vendor_id,
    service,
    count(*) as trip_count,
    cast(avg(passenger_count) as double) as avg_passenger_count,
    cast(avg(trip_distance) as double) as avg_trip_distance,
    cast(sum(total_amount) as decimal(18, 2)) as total_revenue,
    cast(sum(tip_amount) as decimal(18, 2)) as tip_revenue
from {{ ref('fct_trips') }}
group by
    pickup_date,
    pickup_location_id,
    pickup_zone,
    pickup_borough,
    vendor_id,
    service
