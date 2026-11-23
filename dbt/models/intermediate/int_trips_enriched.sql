-- Unified trip stream: batch TLC rows and CDC live-state rows, one model.
--
-- record_source tags the origin so reconciliation and quality can compare
-- each source against its own bronze layer. The two trip_id namespaces
-- cannot collide: batch ids are sha256 hex, CDC ids are 'cdc-*' text.
-- Batch-only columns stay null for CDC rows and vice versa; no second
-- dedupe here (bronze already dedupes batch rows; CDC staging reduces
-- events to the live state).

with trips as (
    select
        trip_id,
        'batch' as record_source,
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
        pickup_month,
        cast(null as timestamp(3) with time zone) as updated_at,
        cast(null as bigint) as source_ts_ms,
        cast(null as date) as event_date,
        false as is_deleted
    from {{ ref('stg_yellow_trips') }}

    union all

    select
        trip_id,
        'cdc' as record_source,
        vendor_id,
        pickup_at,
        dropoff_at,
        passenger_count,
        trip_distance,
        cast(null as bigint) as ratecode_id,
        cast(null as varchar) as store_and_fwd_flag,
        pickup_location_id,
        dropoff_location_id,
        payment_type,
        fare_amount,
        cast(null as decimal(12, 2)) as extra,
        cast(null as decimal(12, 2)) as mta_tax,
        tip_amount,
        cast(null as decimal(12, 2)) as tolls_amount,
        cast(null as decimal(12, 2)) as improvement_surcharge,
        total_amount,
        cast(null as decimal(12, 2)) as congestion_surcharge,
        cast(null as decimal(12, 2)) as airport_fee,
        service,
        cast(null as varchar) as source_file,
        cast(null as bigint) as source_row_number,
        ingested_at,
        cast(null as varchar) as content_hash,
        cast(null as integer) as pickup_year,
        cast(null as integer) as pickup_month,
        updated_at,
        source_ts_ms,
        event_date,
        is_deleted
    from {{ ref('stg_cdc_trips') }}
    -- Freeze the CDC window at run start (see macros/cdc_cutoff_ms.sql) so the
    -- fact table and its reconciliation read the same deterministic set even
    -- while the stream keeps appending.
    where source_ts_ms <= {{ cdc_cutoff_ms() }}
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
    trips.record_source,
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
    trips.updated_at,
    trips.source_ts_ms,
    trips.event_date,
    trips.is_deleted,
    pickup_zone.zone_name as pickup_zone,
    coalesce(pickup_zone.borough, 'Unknown') as pickup_borough,
    dropoff_zone.zone_name as dropoff_zone,
    coalesce(dropoff_zone.borough, 'Unknown') as dropoff_borough
from trips
left join zones as pickup_zone
    on trips.pickup_location_id = pickup_zone.location_id
left join zones as dropoff_zone
    on trips.dropoff_location_id = dropoff_zone.location_id
