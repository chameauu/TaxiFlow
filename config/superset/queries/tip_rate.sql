select
    payment_type,
    pickup_borough,
    sum(trip_count) as trip_count,
    sum(fare_amount) as fare_amount,
    sum(tip_amount) as tip_amount,
    sum(tip_amount) / nullif(sum(fare_amount), 0) as tip_rate
from gold.agg_tip_rate
group by payment_type, pickup_borough
order by tip_rate desc
