"""End-to-end CDC delete contract against the local stack.

A trip inserted into the OLTP table must appear in gold once the stream and a
dbt build catch up, and it must disappear again after the source row is
deleted. This is the behaviour the CDC reduction exists to guarantee; the
fixture test (dbt/tests/assert_cdc_fixture_semantics.sql) pins the SQL rules,
and this test pins the live path.
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASE = ("docker", "compose", "--profile", "core", "--profile", "stream")
TRIP_ID = "cdc-it-delete-contract"
POLL_SECONDS = 180
INTERVAL = 5
# dbt freezes the CDC window at run start minus 60 s (dbt/macros/cdc_cutoff_ms),
# so a fresh event only enters silver on a build that starts after the buffer.
CDC_BUFFER_SECONDS = 65


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [*BASE, *args],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=check,
    )


def require_stack() -> None:
    try:
        running = compose("ps", "--status", "running", "--services", check=False)
    except FileNotFoundError:
        if os.environ.get("TAXIFLOW_REQUIRE_STACK") == "1":
            pytest.fail("docker compose is not available")
        pytest.skip("docker compose is not available")
    required = {"postgres", "kafka", "schema-registry", "spark-stream", "trino", "minio"}
    missing = required - set(running.stdout.split())
    if missing:
        if os.environ.get("TAXIFLOW_REQUIRE_STACK") == "1":
            pytest.fail(f"stack services are not running: {sorted(missing)}")
        pytest.skip(f"stack services are not running: {sorted(missing)}")


def psql(sql: str) -> subprocess.CompletedProcess[str]:
    return compose(
        "exec",
        "-T",
        "postgres",
        "psql",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        "taxiflow",
        "-d",
        "taxiflow",
        "-c",
        sql,
    )


def trino(sql: str) -> str:
    result = compose(
        "exec",
        "-T",
        "trino",
        "trino",
        "--catalog",
        "delta",
        "--schema",
        "gold",
        "--output-format",
        "TSV_HEADER",
        "--execute",
        sql,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    return result.stdout


def wait_for(predicate, message: str) -> None:
    deadline = time.monotonic() + POLL_SECONDS
    last = ""
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return
        time.sleep(INTERVAL)
    pytest.fail(f"{message} (last seen: {last!r})")


def fact_count() -> int:
    output = trino(
        "select count(*) as n from fct_trips "
        f"where trip_id = '{TRIP_ID}' and record_source = 'cdc'"
    )
    lines = [line for line in output.splitlines() if line.strip()]
    return int(lines[-1])


def bronze_events() -> int:
    output = compose(
        "exec",
        "-T",
        "trino",
        "trino",
        "--catalog",
        "delta",
        "--schema",
        "default",
        "--output-format",
        "TSV_HEADER",
        "--execute",
        f"select count(*) as n from cdc_taxi_trips where trip_id = '{TRIP_ID}'",
        check=False,
    )
    assert output.returncode == 0, output.stderr[-2000:]
    lines = [line for line in output.stdout.splitlines() if line.strip()]
    return int(lines[-1])


def bronze_delete_events() -> int:
    output = compose(
        "exec",
        "-T",
        "trino",
        "trino",
        "--catalog",
        "delta",
        "--schema",
        "default",
        "--output-format",
        "TSV_HEADER",
        "--execute",
        "select count(*) as n from cdc_taxi_trips "
        f"where trip_id = '{TRIP_ID}' and __op = 'd'",
        check=False,
    )
    assert output.returncode == 0, output.stderr[-2000:]
    lines = [line for line in output.stdout.splitlines() if line.strip()]
    return int(lines[-1])


def dbt_build() -> None:
    # Models only: sibling data tests (for example reconcile_daily_totals)
    # compare against marts this selection does not rebuild.
    result = subprocess.run(
        [
            "uv",
            "run",
            "dbt",
            "run",
            "--select",
            "+fct_trips",
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt",
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-2000:]


def test_deleted_source_trip_leaves_gold() -> None:
    require_stack()
    psql(f"delete from iot.taxi_nyc_time_series where trip_id = '{TRIP_ID}'")
    psql(
        "insert into iot.taxi_nyc_time_series "
        "(trip_id, vendor_id, pickup_datetime, dropoff_datetime, passenger_count, "
        " trip_distance, fare_amount, tip_amount, total_amount, payment_type, "
        " pickup_zone_id, dropoff_zone_id, service) values "
        f"('{TRIP_ID}', 1, now() - interval '1 hour', now() - interval '40 minutes', "
        " 2, 4.20, 30.00, 4.00, 34.00, 1, 161, 237, 'yellow')"
    )
    try:
        wait_for(lambda: bronze_events() >= 1, "insert never reached bronze")
        time.sleep(CDC_BUFFER_SECONDS)
        dbt_build()
        wait_for(lambda: fact_count() == 1, "insert never reached gold")
    finally:
        psql(f"delete from iot.taxi_nyc_time_series where trip_id = '{TRIP_ID}'")

    wait_for(lambda: bronze_delete_events() >= 1, "delete never reached bronze")
    time.sleep(CDC_BUFFER_SECONDS)
    dbt_build()
    assert fact_count() == 0, "deleted trip is still present in gold.fct_trips"
