select
    trip_id,
    pickup_location_id,
    dropoff_location_id,
    pickup_borough,
    dropoff_borough
from {{ ref('fct_trips') }}
where pickup_borough is null
   or dropoff_borough is null
