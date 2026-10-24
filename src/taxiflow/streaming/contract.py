"""Shared CDC contract for the Postgres → Debezium → Kafka → Delta path."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path
from string import Template
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONNECTOR_CONFIG_PATH = PROJECT_ROOT / "config" / "debezium" / "taxi-iot-connector.json"
AVRO_SCHEMA_PATH = PROJECT_ROOT / "config" / "avro" / "taxi_trip_event.avsc"

TOPIC = "taxiflow.iot.taxi_nyc_time_series"
KEY_SUBJECT = f"{TOPIC}-key"
VALUE_SUBJECT = f"{TOPIC}-value"
COMPATIBILITY = "BACKWARD_TRANSITIVE"
CDC_TABLE_NAME = "cdc_taxi_trips"


class CdcError(RuntimeError):
    """Raised when the CDC contract or control plane is invalid."""


def load_avro_schema(path: Path = AVRO_SCHEMA_PATH) -> dict[str, Any]:
    """Load the reviewed CDC Avro contract."""
    data = json.loads(path.read_text())
    if not isinstance(data, dict) or data.get("type") != "record":
        raise CdcError(f"{path} is not an Avro record schema")
    return data


def load_connector_config(
    path: Path = CONNECTOR_CONFIG_PATH,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Load the connector JSON and substitute environment-backed credentials."""
    environment = dict(os.environ if environ is None else environ)
    try:
        text = Template(path.read_text()).substitute(environment)
    except KeyError as exc:
        raise CdcError(
            f"connector config requires environment variable {exc.args[0]}"
        ) from exc
    data = json.loads(text)
    if not isinstance(data, dict) or "config" not in data:
        raise CdcError(f"{path} is not a connector payload")
    config = data["config"]
    for key in (
        "database.user",
        "database.password",
        "database.dbname",
        "key.converter.schema.registry.url",
        "value.converter.schema.registry.url",
    ):
        if not config.get(key):
            raise CdcError(f"connector config is missing {key}")
    return data


def cdc_table_path(bucket: str = "taxiflow") -> str:
    """Return the single bronze CDC table location."""
    return f"s3a://{bucket}/bronze/{CDC_TABLE_NAME}"


def cdc_checkpoint_path(bucket: str = "taxiflow") -> str:
    """Return the checkpoint location nested under the table prefix."""
    return f"s3a://{bucket}/bronze/_checkpoints/{CDC_TABLE_NAME}"
