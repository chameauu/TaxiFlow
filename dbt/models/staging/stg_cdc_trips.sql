-- CDC staging: Debezium events reduced to the live trip state.
--
-- The reduction rules live in the cdc_live_state macro so the committed
-- fixture test (dbt/tests/assert_cdc_fixture_semantics.sql) exercises the
-- exact SQL this model runs: latest event per trip_id by __source_ts_ms,
-- deletes and tombstones removed from the live state.
--
-- iot timestamps are UTC wall clock; Debezium emits epoch millis that Spark's
-- from_avro decodes into timestamp(3) with time zone, so bronze already
-- carries UTC instants. The TLC New York wall-clock rule is batch-only (see
-- stg_yellow_trips).

{{ cdc_live_state(source('bronze', 'cdc_taxi_trips')) }}
