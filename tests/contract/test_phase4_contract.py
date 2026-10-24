"""Static contract for the Phase 4 CDC bronze layer."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.contract

PROJECT_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_PATH = PROJECT_ROOT / "compose.yaml"
MAKEFILE_PATH = PROJECT_ROOT / "Makefile"
CONNECTOR_PATH = PROJECT_ROOT / "config" / "debezium" / "taxi-iot-connector.json"
AVRO_PATH = PROJECT_ROOT / "config" / "avro" / "taxi_trip_event.avsc"
POSTGRES_INIT = PROJECT_ROOT / "config" / "postgres" / "init" / "02-iot.sql"
STREAM_JOB = PROJECT_ROOT / "src" / "taxiflow" / "jobs" / "stream_cdc_bronze.py"
REPORT_JOB = PROJECT_ROOT / "src" / "taxiflow" / "jobs" / "report_cdc.py"
STREAMING_ROOT = PROJECT_ROOT / "src" / "taxiflow" / "streaming"
DEBEZIUM_DOCKERFILE = PROJECT_ROOT / "docker" / "debezium" / "Dockerfile"


def load_compose() -> dict[str, Any]:
    data = yaml.safe_load(COMPOSE_PATH.read_text())
    assert isinstance(data, dict)
    return data


def load_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text())
    assert isinstance(data, dict)
    return data


def test_avro_contract_defines_the_stable_trip_key_and_event_metadata() -> None:
    schema = load_json(AVRO_PATH)
    fields = {field["name"]: field for field in schema["fields"]}

    assert schema["type"] == "record"
    assert schema["name"] == "TaxiTripEvent"
    assert fields["trip_id"]["type"] == "string"
    assert fields["service"]["type"] == "string"
    assert fields["pickup_datetime"]["type"] == "long"
    assert fields["pickup_datetime"]["logicalType"] == "timestamp-millis"
    assert fields["__op"]["type"] == "string"
    assert fields["__source_ts_ms"]["type"] == "long"
    assert fields["trip_distance"]["type"] == ["null", "string"]
    assert fields["fare_amount"]["type"] == ["null", "string"]
    assert fields["total_amount"]["type"] == ["null", "string"]


def test_connector_uses_avro_and_a_documented_compatibility_policy() -> None:
    connector = load_json(CONNECTOR_PATH)
    config = connector["config"]

    assert connector["name"] == "taxiflow-iot"
    assert config["connector.class"] == "io.debezium.connector.postgresql.PostgresConnector"
    assert config["table.include.list"] == "iot.taxi_nyc_time_series"
    assert config["topic.prefix"] == "taxiflow"
    assert config["plugin.name"] == "pgoutput"
    assert config["slot.name"] == "taxiflow_iot"
    assert config["key.converter"] == "io.confluent.connect.avro.AvroConverter"
    assert config["value.converter"] == "io.confluent.connect.avro.AvroConverter"
    assert config["key.converter.schema.registry.url"] == "http://schema-registry:8081"
    assert config["value.converter.schema.registry.url"] == "http://schema-registry:8081"
    assert config["decimal.handling.mode"] == "string"
    assert config["time.precision.mode"] == "connect"
    assert config["transforms.unwrap.type"] == "io.debezium.transforms.ExtractNewRecordState"
    assert config["transforms.unwrap.delete.handling.mode"] == "rewrite"
    assert config["transforms.unwrap.add.fields"] == "op,source.ts_ms"
    assert config["database.user"] == "${POSTGRES_USER}"
    assert config["database.password"] == "${POSTGRES_PASSWORD}"
    assert config["database.dbname"] == "${POSTGRES_DB}"


def test_debezium_worker_builds_avro_converter_into_the_image() -> None:
    services = load_compose()["services"]
    debezium = services["debezium"]
    environment = debezium["environment"]
    dockerfile = DEBEZIUM_DOCKERFILE.read_text()

    assert debezium["build"]["context"] == "./docker/debezium"
    assert debezium["image"] == "taxiflow/debezium:2.5.1-avro"
    assert environment["KEY_CONVERTER"] == "io.confluent.connect.avro.AvroConverter"
    assert environment["VALUE_CONVERTER"] == "io.confluent.connect.avro.AvroConverter"
    assert environment["CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL"] == (
        "http://schema-registry:8081"
    )
    assert environment["CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL"] == (
        "http://schema-registry:8081"
    )
    assert "kafka-connect-avro-converter" in dockerfile
    assert "kafka-connect-avro-data" in dockerfile
    assert "kafka-schema-converter" in dockerfile
    assert "kafka-schema-serializer" in dockerfile
    assert "kafka-avro-serializer" in dockerfile
    assert "/kafka/connect/confluent-avro" in dockerfile


def test_postgres_source_has_replica_identity_and_a_stable_source_key() -> None:
    sql = POSTGRES_INIT.read_text()

    assert "CREATE SCHEMA IF NOT EXISTS iot" in sql
    assert "CREATE TABLE IF NOT EXISTS iot.taxi_nyc_time_series" in sql
    assert "trip_id text PRIMARY KEY" in sql
    assert "REPLICA IDENTITY FULL" in sql
    assert "emit_synthetic_trip" in sql
    assert "delete_synthetic_trip" in sql


def test_stream_job_partitions_by_event_date_and_merges_on_trip_id() -> None:
    text = STREAM_JOB.read_text()

    assert "event_date" in text
    assert "partitionBy" in text
    assert "DeltaTable.forPath" in text
    assert "trip_id" in text
    assert "__source_ts_ms" in text
    assert "checkpointLocation" in text
    assert "from pyspark.sql.avro.functions import from_avro" in text
    assert "JsonConverter" not in text


def test_makefile_exposes_cdc_lifecycle_targets() -> None:
    makefile = MAKEFILE_PATH.read_text()

    for target in (
        "cdc-init",
        "cdc-seed",
        "cdc-register",
        "cdc-start",
        "cdc-stop",
        "cdc-report",
    ):
        assert re.search(rf"^{target}:", makefile, re.MULTILINE)
    assert "taxiflow cdc register" in makefile
    assert "stream_cdc_bronze.py" in makefile
    assert "stream-generator" in makefile


def test_compose_exposes_a_long_running_stream_worker_and_generator() -> None:
    services = load_compose()["services"]

    assert "spark-stream" in services
    assert services["spark-stream"]["profiles"] == ["stream"]
    assert "stream_cdc_bronze.py" in " ".join(services["spark-stream"]["command"])
    assert "stream-generator" in services
    assert "postgres" in services["stream-generator"]["depends_on"]


def test_report_job_exposes_freshness_and_latest_state() -> None:
    report = REPORT_JOB.read_text()
    metrics = (STREAMING_ROOT / "metrics.py").read_text()
    contract = (STREAMING_ROOT / "contract.py").read_text()

    assert "collect_metrics" in report
    assert "cdc_table_path" in report
    assert 'F.max("ingested_at")' in metrics
    assert "freshness_seconds" in metrics
    assert "is_deleted" in metrics
    assert "cdc_taxi_trips" in contract


def test_streaming_package_exposes_registration_and_schema_helpers() -> None:
    assert (STREAMING_ROOT / "__init__.py").is_file()
    assert (STREAMING_ROOT / "contract.py").is_file()
    assert (STREAMING_ROOT / "registry.py").is_file()
