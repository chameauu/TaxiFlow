select
    concat(cast(payment_type as varchar), '|', pickup_borough) as tip_key,
    payment_type,
    pickup_borough,
    count(*) as trip_count,
    cast(sum(fare_amount) as decimal(18, 2)) as fare_amount,
    cast(sum(tip_amount) as decimal(18, 2)) as tip_amount,
    cast(sum(tip_amount) / nullif(sum(fare_amount), 0) as double) as tip_rate
from {{ ref('fct_trips') }}
group by payment_type, pickup_borough
