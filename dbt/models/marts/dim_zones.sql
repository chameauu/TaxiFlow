select
    location_id,
    borough,
    zone_name,
    service_zone
from {{ ref('stg_taxi_zones') }}
