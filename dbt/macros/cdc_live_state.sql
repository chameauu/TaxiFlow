-- CDC live-state semantics, shared by the production staging model and the
-- committed fixture test (dbt/tests/assert_cdc_fixture_semantics.sql).
--
-- Rules:
--   * the latest event per trip_id wins, ordered by __source_ts_ms (arrival
--     order in Delta is not event order);
--   * delete events (__op = 'd', __deleted = 'true') remove the trip from the
--     live state, so a later insert can legitimately resurrect it.
--
-- The relation must expose the bronze CDC columns. Keeping the rules in one
-- macro means the fixture exercises the same SQL the pipeline runs.
{% macro cdc_live_state(relation) %}
with events as (
    select
        trip_id,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount,
        payment_type,
        pickup_zone_id,
        dropoff_zone_id,
        service,
        updated_at,
        __op,
        __deleted,
        __source_ts_ms,
        event_date,
        ingested_at,
        is_deleted
    from {{ relation }}
),

ranked as (
    select
        trip_id,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount,
        payment_type,
        pickup_zone_id,
        dropoff_zone_id,
        service,
        updated_at,
        __op,
        __deleted,
        __source_ts_ms,
        event_date,
        ingested_at,
        is_deleted,
        row_number() over (
            partition by trip_id
            order by __source_ts_ms desc
        ) as event_rank
    from events
),

live as (
    select
        trip_id,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount,
        payment_type,
        pickup_zone_id,
        dropoff_zone_id,
        service,
        updated_at,
        __source_ts_ms,
        event_date,
        ingested_at,
        is_deleted
    from ranked
    where event_rank = 1
      and __op != 'd'
      -- __deleted is varchar in bronze and can be inferred as boolean by
      -- seeds; compare as text and treat a null flag as "not deleted".
      and coalesce(lower(cast(__deleted as varchar)), 'false') != 'true'
)

select
    cast(trip_id as varchar) as trip_id,
    cast(vendor_id as integer) as vendor_id,
    cast(pickup_datetime as timestamp(3) with time zone) as pickup_at,
    cast(dropoff_datetime as timestamp(3) with time zone) as dropoff_at,
    cast(passenger_count as bigint) as passenger_count,
    cast(trip_distance as decimal(10, 2)) as trip_distance,
    cast(payment_type as bigint) as payment_type,
    cast(fare_amount as decimal(12, 2)) as fare_amount,
    cast(tip_amount as decimal(12, 2)) as tip_amount,
    cast(total_amount as decimal(12, 2)) as total_amount,
    cast(pickup_zone_id as integer) as pickup_location_id,
    cast(dropoff_zone_id as integer) as dropoff_location_id,
    cast(service as varchar) as service,
    cast(updated_at as timestamp(3) with time zone) as updated_at,
    cast(ingested_at as timestamp(3) with time zone) as ingested_at,
    cast(is_deleted as boolean) as is_deleted,
    cast(__source_ts_ms as bigint) as source_ts_ms,
    cast(event_date as date) as event_date
from live
{% endmacro %}
