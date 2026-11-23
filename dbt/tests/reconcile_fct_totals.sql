-- Source-aware reconciliation (Phase 7 task 3).
--
-- Batch is static: per-source count and sum must match exactly.
-- CDC keeps moving while dbt runs: bronze MERGE updates existing rows (moving
-- their latest __source_ts_ms past any cutoff) and deletes retire trips, so an
-- exact fact-side count would race with the stream. Instead, the frozen window
-- (macros/cdc_cutoff_ms.sql) must be fully present in the fact table with the
-- right amounts: any dropped, duplicated, or mis-cast CDC row fails.

with fact_batch as (
    select
        count(*) as trip_count,
        cast(sum(total_amount) as decimal(18, 2)) as total_amount
    from {{ ref('fct_trips') }}
    where record_source = 'batch'
),

staging_batch as (
    select
        count(*) as trip_count,
        cast(sum(total_amount) as decimal(18, 2)) as total_amount
    from {{ ref('stg_yellow_trips') }}
),

window_trips as (
    select
        trip_id,
        total_amount
    from {{ ref('stg_cdc_trips') }}
    where source_ts_ms <= {{ cdc_cutoff_ms() }}
),

cdc_amounts as (
    select
        (select count(*) from window_trips) as staging_count,
        (select cast(sum(total_amount) as decimal(18, 2)) from window_trips) as staging_total,
        (select count(*)
         from {{ ref('fct_trips') }} f
         join window_trips w
             on f.record_source = 'cdc'
            and f.trip_id = w.trip_id) as fact_count,
        (select cast(sum(f.total_amount) as decimal(18, 2))
         from {{ ref('fct_trips') }} f
         join window_trips w
             on f.record_source = 'cdc'
            and f.trip_id = w.trip_id) as fact_total
)

select
    'batch_row_count' as issue,
    cast(null as varchar) as trip_id,
    concat(
        'fact=', cast(f.trip_count as varchar),
        ' staging=', cast(s.trip_count as varchar)
    ) as detail
from fact_batch f
cross join staging_batch s
where f.trip_count != s.trip_count

union all

select
    'batch_total_amount' as issue,
    cast(null as varchar) as trip_id,
    concat(
        'fact=', cast(f.total_amount as varchar),
        ' staging=', cast(s.total_amount as varchar)
    ) as detail
from fact_batch f
cross join staging_batch s
where abs(f.total_amount - s.total_amount) > 0.01

union all

select
    'cdc_trip_missing_from_fact' as issue,
    w.trip_id as trip_id,
    'trip is inside the frozen window but absent from fct_trips' as detail
from window_trips w
where not exists (
    select 1
    from {{ ref('fct_trips') }} f
    where f.record_source = 'cdc'
      and f.trip_id = w.trip_id
)

union all

select
    'cdc_window_row_count' as issue,
    cast(null as varchar) as trip_id,
    concat(
        'fact=', cast(a.fact_count as varchar),
        ' staging=', cast(a.staging_count as varchar)
    ) as detail
from cdc_amounts a
where a.fact_count != a.staging_count

union all

select
    'cdc_window_total_amount' as issue,
    cast(null as varchar) as trip_id,
    concat(
        'fact=', cast(a.fact_total as varchar),
        ' staging=', cast(a.staging_total as varchar)
    ) as detail
from cdc_amounts a
where abs(a.fact_total - a.staging_total) > 0.01
