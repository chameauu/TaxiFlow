-- The pickup period is a batch contract: TLC slices are pinned to 2024-01 in
-- the manifest. CDC trips carry the generator's wall clock (today), so they
-- are excluded here and validated by the CDC-specific tests instead.
select
    trip_id,
    pickup_date
from {{ ref('fct_trips') }}
where record_source = 'batch'
  and (pickup_date < date '2024-01-01'
       or pickup_date >= date '2024-02-01')
