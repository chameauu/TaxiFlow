"""Unit tests for the CDC configuration contract."""

from __future__ import annotations

import pytest

from taxiflow.streaming.contract import (
    CDC_TABLE_NAME,
    COMPATIBILITY,
    TOPIC,
    VALUE_SUBJECT,
    CdcError,
    cdc_checkpoint_path,
    cdc_table_path,
    load_avro_schema,
    load_connector_config,
)


def test_cdc_paths_are_stable_and_nested_under_the_bronze_prefix() -> None:
    assert TOPIC == "taxiflow.iot.taxi_nyc_time_series"
    assert f"{TOPIC}-value" == VALUE_SUBJECT
    assert COMPATIBILITY == "BACKWARD_TRANSITIVE"
    assert CDC_TABLE_NAME == "cdc_taxi_trips"
    assert cdc_table_path() == "s3a://taxiflow/bronze/cdc_taxi_trips"
    assert cdc_checkpoint_path() == "s3a://taxiflow/bronze/_checkpoints/cdc_taxi_trips"
    assert cdc_table_path("custom") == "s3a://custom/bronze/cdc_taxi_trips"


def test_connector_config_substitutes_environment_without_leaking_defaults() -> None:
    payload = load_connector_config(
        environ={
            "POSTGRES_USER": "taxiflow_test",
            "POSTGRES_PASSWORD": "not-a-real-password",
            "POSTGRES_DB": "taxiflow_test",
        }
    )
    config = payload["config"]

    assert config["database.user"] == "taxiflow_test"
    assert config["database.password"] == "not-a-real-password"
    assert config["database.dbname"] == "taxiflow_test"
    assert config["key.converter"] == "io.confluent.connect.avro.AvroConverter"
    assert config["value.converter"] == "io.confluent.connect.avro.AvroConverter"


def test_missing_connector_environment_fails_fast() -> None:
    with pytest.raises(CdcError, match="POSTGRES_USER"):
        load_connector_config(environ={})


def test_avro_contract_keeps_the_source_generated_trip_key() -> None:
    schema = load_avro_schema()
    fields = {field["name"]: field for field in schema["fields"]}

    assert fields["trip_id"]["type"] == "string"
    assert fields["__source_ts_ms"]["type"] == "long"
    assert fields["__op"]["type"] == "string"
    assert fields["__deleted"]["type"] == ["null", "string"]
