with source as (
    select
        locationid,
        borough,
        zone,
        service_zone
    from {{ ref('taxi_zone_lookup') }}
)

select
    cast(locationid as integer) as location_id,
    cast(borough as varchar) as borough,
    cast(zone as varchar) as zone_name,
    cast(service_zone as varchar) as service_zone
from source
where locationid is not null
