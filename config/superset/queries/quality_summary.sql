select
    bronze_trip_rows,
    fact_trip_rows,
    quarantine_rows,
    quarantine_reason_count,
    max_bronze_ingested_at,
    batch_data_age_seconds,
    cdc_lag_seconds
from gold.quality_summary
