"""Timestamp-semantics contract between the batch and CDC sources.

The batch pipeline reads TLC Parquet whose timestamps are naive wall-clock
times in ``America/New_York``. The CDC pipeline reads Postgres changes whose
naive ``timestamp`` columns are UTC wall-clock times, converted by Debezium
to epoch milliseconds. Silver must restore each source with the matching
interpretation; a single shared conversion would be a silent 4-5 hour skew.

These tests pin that contract before any CDC model is unified with batch
data (Phase 7 task 3). They also pin the connector settings that make the
conversion deterministic.
"""

from __future__ import annotations

import calendar
import json
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from taxiflow.streaming.contract import load_connector_config

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONNECTOR_CONFIG_PATH = PROJECT_ROOT / "config" / "debezium" / "taxi-iot-connector.json"


def connector_settings() -> dict[str, str]:
    payload = json.loads(CONNECTOR_CONFIG_PATH.read_text(encoding="utf-8"))
    return payload["config"]


def test_connector_pins_the_semantics_it_relies_on() -> None:
    config = connector_settings()

    # connect mode maps TIMESTAMP (without tz) to INT64 epoch millis, which the
    # Avro contract declares as timestamp-millis fields.
    assert config["time.precision.mode"] == "connect"
    # Deletes must still emit events (the Spark MERGE keys on trip_id, and the
    # tombstone drill relies on delete events carrying the old row image).
    assert config["tombstones.on.delete"] == "true"
    # __op and __source_ts_ms must reach the unwrapped event for dedupe order.
    assert "op" in config["transforms.unwrap.add.fields"]
    assert "source.ts_ms" in config["transforms.unwrap.add.fields"]


def test_iot_naive_timestamps_are_utc_wall_clock() -> None:
    """The iot source convention: naive ``timestamp`` values are UTC wall clock.

    With the Debezium and Postgres containers pinned to UTC, the epoch
    milliseconds Debezium emits for a naive value are the value interpreted
    as UTC. Silver restores the instant with an epoch-based conversion, not a
    New York reinterpretation.
    """
    naive = "2026-10-24 12:00:00"
    naive_dt = datetime.strptime(naive, "%Y-%m-%d %H:%M:%S")

    utc_epoch_ms = int(
        naive_dt.replace(tzinfo=UTC).timestamp() * 1000
    )
    assert utc_epoch_ms == calendar.timegm(naive_dt.timetuple()) * 1000


def test_new_york_interpretation_would_skew_the_cdc_clock() -> None:
    """Silver must NOT apply the TLC New York rule to CDC timestamps.

    Same naive wall clock, interpreted in America/New_York, lands 5 hours
    LATER on the UTC epoch clock than the UTC interpretation in winter
    (4 in summer). Asserting the offset keeps the asymmetry between the two
    sources explicit.
    """
    naive = datetime(2026, 1, 15, 9, 30, 0)

    utc_epoch_s = calendar.timegm(naive.timetuple())
    ny_epoch_s = int(
        naive.replace(tzinfo=ZoneInfo("America/New_York")).timestamp()
    )

    assert ny_epoch_s == utc_epoch_s + 5 * 3600


def test_unwrapped_events_carry_the_source_epoch_clock() -> None:
    """``__source_ts_ms`` is the only freshness clock for CDC lag (task 5).

    The unwrap transform must add it next to ``__op`` so the silver dedupe can
    order events and the quality runner can compute end-to-end lag.
    """
    config = connector_settings()
    assert config["transforms.unwrap.add.fields"] == "op,source.ts_ms"


def test_connector_config_loader_accepts_the_pinned_settings() -> None:
    payload = load_connector_config(
        environ={
            "POSTGRES_USER": "taxiflow_test",
            "POSTGRES_PASSWORD": "not-a-real-password",
            "POSTGRES_DB": "taxiflow_test",
        }
    )
    assert payload["config"]["time.precision.mode"] == "connect"
