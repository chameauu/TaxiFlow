with source as (
    select
        quarantine_id,
        service,
        source_file,
        source_row_number,
        quarantine_reason,
        pickup_datetime,
        raw_record,
        ingested_at
    from {{ source('bronze', 'tlc_quarantine') }}
)

select
    cast(quarantine_id as varchar) as quarantine_id,
    cast(service as varchar) as service,
    cast(source_file as varchar) as source_file,
    cast(source_row_number as bigint) as source_row_number,
    cast(quarantine_reason as varchar) as quarantine_reason,
    with_timezone(cast(pickup_datetime as timestamp), 'America/New_York') as pickup_at,
    cast(raw_record as varchar) as raw_record,
    cast(ingested_at as timestamp(3) with time zone) as ingested_at
from source
