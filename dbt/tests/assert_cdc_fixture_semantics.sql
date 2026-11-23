-- Fixture assertion for the CDC live-state semantics.
--
-- Runs the same cdc_live_state macro the production staging model uses, over
-- committed events (insert / update / delete / tombstone / out-of-order
-- arrival / resurrection), and compares the result to the committed expected
-- state. Any mismatch returns a row and fails the test.

with live as (
    {{ cdc_live_state(ref('cdc_events_fixture')) }}
),

expected as (
    select
        cast(trip_id as varchar) as trip_id,
        cast(total_amount as decimal(12, 2)) as total_amount
    from {{ ref('cdc_events_expected') }}
),

mismatches as (
    select
        'unexpected_live_trip' as mismatch,
        live.trip_id
    from live
    left join expected on live.trip_id = expected.trip_id
    where expected.trip_id is null

    union all

    select
        'missing_live_trip' as mismatch,
        expected.trip_id
    from expected
    left join live on expected.trip_id = live.trip_id
    where live.trip_id is null

    union all

    select
        'total_amount_mismatch' as mismatch,
        expected.trip_id
    from expected
    join live on expected.trip_id = live.trip_id
    where live.total_amount != expected.total_amount
)

select mismatch, trip_id
from mismatches
