-- Shared CDC event-time cutoff for dbt runs.
--
-- The synthetic stream keeps appending while dbt is running, so two reads of
-- the live pipeline seconds apart disagree by a handful of rows. The unified
-- model (int_trips_enriched) and its reconciliation therefore both freeze the
-- CDC window at run start minus this buffer; events inside the buffer join on
-- the next run. The buffer must exceed the observed Debezium -> Delta latency
-- (about 5 s in the Phase 4 drills).
{% macro cdc_cutoff_ms() %}
cast(
    to_unixtime(cast('{{ run_started_at }}' as timestamp(3) with time zone)) * 1000
        - 60000
    as bigint
)
{% endmacro %}
