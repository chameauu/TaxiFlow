"""Runtime replay contract for the CDC Spark job against the local stack."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PACKAGES = (
    "io.delta:delta-spark_2.12:3.1.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
    "org.apache.spark:spark-avro_2.12:3.5.1"
)
BASE = ("docker", "compose", "--profile", "core", "--profile", "stream")
TEST_TABLE = "s3a://taxiflow/bronze/_pytest/cdc_taxi_trips"


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    """Run a Compose command from the project root."""
    return subprocess.run(
        [*BASE, *args],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=check,
    )


def require_stack() -> None:
    """Skip unless the local stack is expected to be available."""
    try:
        running = compose("ps", "--status", "running", "--services", check=False)
    except FileNotFoundError:
        if os.environ.get("TAXIFLOW_REQUIRE_STACK") == "1":
            pytest.fail("docker compose is not available")
        pytest.skip("docker compose is not available")
    if "minio" not in running.stdout.split():
        if os.environ.get("TAXIFLOW_REQUIRE_STACK") == "1":
            pytest.fail("minio is not running; start the core profile first")
        pytest.skip("minio is not running")


def test_cdc_merge_is_idempotent_and_keeps_latest_state() -> None:
    require_stack()
    compose("up", "-d", "--wait", "minio")

    result = compose(
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "spark-stream",
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[2]",
        "--driver-memory",
        "2g",
        "--conf",
        "spark.sql.shuffle.partitions=8",
        "--packages",
        PACKAGES,
        "--conf",
        "spark.jars.ivy=/tmp/.ivy2",
        "/opt/taxiflow/tests/integration/cdc_replay.py",
        "--table-path",
        TEST_TABLE,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-4000:]
    lines = [
        line.removeprefix("TAXIFLOW_CDC_REPLAY=")
        for line in result.stdout.splitlines()
        if line.startswith("TAXIFLOW_CDC_REPLAY=")
    ]
    assert len(lines) == 1, result.stdout[-2000:]
    metrics = json.loads(lines[0])

    assert metrics["first"]["row_count"] == 2
    assert metrics["replay"]["row_count"] == 2
    assert metrics["replay"]["distinct_trip_ids"] == 2
    assert metrics["replay"]["duplicate_rows"] == 0
    assert metrics["update"]["row_count"] == 2
    assert metrics["update"]["duplicate_rows"] == 0
    assert metrics["delete"]["deleted_rows"] == 1
    assert metrics["trip_1_fare"] == "10.00"
    assert metrics["trip_2_fare"] == "25.00"
    assert metrics["trip_2_deleted"] is True
