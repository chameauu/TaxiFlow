"""Clock-pin contract for the CDC timestamp parity rule (Phase 7 task 3).

The batch loader treats naive TLC timestamps as America/New_York wall clock.
The CDC path instead receives epoch milliseconds produced by Debezium from
naive Postgres timestamps, so the conversion base must be pinned: the server
and the worker both run in UTC, and the naive iot values are documented as
UTC wall clock. Without these pins the two sources would enter silver with
different time semantics.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.contract

PROJECT_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_PATH = PROJECT_ROOT / "docker-compose.yaml"
UTC_PIN = "timezone=UTC"


def test_postgres_pins_the_server_clock_to_utc() -> None:
    compose = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    command = compose["services"]["postgres"]["command"]
    assert command[-2:] == ["-c", UTC_PIN], (
        "postgres must pin timezone=UTC so Debezium's naive-timestamp to "
        "epoch-millis conversion has a documented base"
    )


def test_debezium_pins_the_worker_clock_to_utc() -> None:
    compose = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    environment = compose["services"]["debezium"]["environment"]
    assert environment.get("TZ") == "UTC", (
        "debezium must pin TZ=UTC so connect-mode timestamp conversions "
        "cannot drift with the worker's default zone"
    )
