with trips as (
    select
        trip_id,
        vendor_id,
        pickup_at,
        dropoff_at,
        passenger_count,
        trip_distance,
        ratecode_id,
        store_and_fwd_flag,
        pickup_location_id,
        dropoff_location_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        service,
        source_file,
        source_row_number,
        ingested_at,
        content_hash,
        pickup_year,
        pickup_month
    from {{ ref('stg_yellow_trips') }}
),

zones as (
    select
        location_id,
        borough,
        zone_name,
        service_zone
    from {{ ref('stg_taxi_zones') }}
)

select
    trips.trip_id,
    trips.vendor_id,
    trips.pickup_at,
    trips.dropoff_at,
    trips.passenger_count,
    trips.trip_distance,
    trips.ratecode_id,
    trips.store_and_fwd_flag,
    trips.pickup_location_id,
    trips.dropoff_location_id,
    trips.payment_type,
    trips.fare_amount,
    trips.extra,
    trips.mta_tax,
    trips.tip_amount,
    trips.tolls_amount,
    trips.improvement_surcharge,
    trips.total_amount,
    trips.congestion_surcharge,
    trips.airport_fee,
    trips.service,
    trips.source_file,
    trips.source_row_number,
    trips.ingested_at,
    trips.content_hash,
    trips.pickup_year,
    trips.pickup_month,
    pickup_zone.zone_name as pickup_zone,
    coalesce(pickup_zone.borough, 'Unknown') as pickup_borough,
    dropoff_zone.zone_name as dropoff_zone,
    coalesce(dropoff_zone.borough, 'Unknown') as dropoff_borough
from trips
left join zones as pickup_zone
    on trips.pickup_location_id = pickup_zone.location_id
left join zones as dropoff_zone
    on trips.dropoff_location_id = dropoff_zone.location_id
