select
    trip_id,
    pickup_date
from {{ ref('fct_trips') }}
where pickup_date < date '2024-01-01'
   or pickup_date >= date '2024-02-01'
