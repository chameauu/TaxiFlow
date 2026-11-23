select
    (select count(*) from {{ ref('stg_yellow_trips') }}) as bronze_trip_rows,
    (select max(ingested_at) from {{ ref('stg_yellow_trips') }}) as max_bronze_ingested_at,
    (select count(*) from {{ ref('stg_tlc_quarantine') }}) as quarantine_rows,
    (select count(*) from {{ ref('fct_trips') }}) as fact_trip_rows,
    (select count(*) from {{ ref('stg_cdc_trips') }}) as cdc_trip_rows,
    (select count(*) from {{ ref('fct_quality_reasons') }}) as quarantine_reason_count,
    date_diff(
        'second',
        (select max(ingested_at) from {{ ref('stg_yellow_trips') }}),
        current_timestamp
    ) as batch_data_age_seconds,
    -- Event-time age of the newest CDC event: source commit -> bronze, the
    -- same clock the quality gate enforces (< 60 s).
    (
        select date_diff(
            'second',
            from_unixtime(max(__source_ts_ms) / 1000.0),
            current_timestamp
        )
        from {{ source('bronze', 'cdc_taxi_trips') }}
    ) as cdc_lag_seconds
