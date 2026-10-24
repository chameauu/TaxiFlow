select
    (select count(*) from {{ ref('stg_yellow_trips') }}) as bronze_trip_rows,
    (select max(ingested_at) from {{ ref('stg_yellow_trips') }}) as max_bronze_ingested_at,
    (select count(*) from {{ ref('stg_tlc_quarantine') }}) as quarantine_rows,
    (select count(*) from {{ ref('fct_trips') }}) as fact_trip_rows,
    (select count(*) from {{ ref('fct_quality_reasons') }}) as quarantine_reason_count,
    date_diff(
        'second',
        (select max(ingested_at) from {{ ref('stg_yellow_trips') }}),
        current_timestamp
    ) as freshness_seconds
