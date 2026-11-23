-- Trips by origin: batch TLC backfill versus the CDC live state.
select
    record_source,
    count(*) as trip_count
from gold.fct_trips
group by record_source
order by record_source
