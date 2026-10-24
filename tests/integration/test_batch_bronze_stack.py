"""Replay contract for the Spark batch bronze job against the local stack."""

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
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)
BASE = (
    "docker",
    "compose",
    "--profile",
    "core",
    "--profile",
    "batch",
)
TEST_ROOT = "s3a://taxiflow/bronze/_pytest"


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    """Run a Compose command from the project root."""
    return subprocess.run(
        [*BASE, *args],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=check,
    )


def run_batch() -> dict[str, object]:
    """Run one fixture month and return the emitted metrics."""
    result = compose(
        "run",
        "--rm",
        "--no-deps",
        "spark",
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[2]",
        "--driver-memory",
        "2g",
        "--packages",
        PACKAGES,
        "--conf",
        "spark.jars.ivy=/tmp/.ivy2",
        "/opt/taxiflow/src/taxiflow/jobs/batch_bronze.py",
        "--root",
        "/opt/taxiflow",
        "--manifest",
        "/opt/taxiflow/tests/fixtures/tlc/tlc_manifest.json",
        "--service",
        "yellow",
        "--year",
        "2024",
        "--month",
        "1",
        "--table-path",
        f"{TEST_ROOT}/tlc_trips",
        "--quarantine-path",
        f"{TEST_ROOT}/tlc_quarantine",
        "--report-path",
        f"{TEST_ROOT}/_schema_drift",
        "--write-partitions",
        "1",
        check=False,
    )
    assert result.returncode == 0, result.stderr[-4000:]
    lines = [
        line.removeprefix("TAXIFLOW_METRICS=")
        for line in result.stdout.splitlines()
        if line.startswith("TAXIFLOW_METRICS=")
    ]
    assert len(lines) == 1, result.stdout[-2000:]
    return json.loads(lines[0])


def require_stack() -> None:
    """Skip unless the local stack is expected to be available."""
    try:
        compose("ps", "--status", "running", "--services", check=False)
    except FileNotFoundError:
        if os.environ.get("TAXIFLOW_REQUIRE_STACK") == "1":
            pytest.fail("docker compose is not available")
        pytest.skip("docker compose is not available")


def test_batch_replay_keeps_counts_sums_and_trip_ids_unchanged() -> None:
    require_stack()
    compose("up", "-d", "--wait", "minio")

    first = run_batch()
    second = run_batch()

    stable_keys = {
        "read_rows",
        "written_rows",
        "quarantined_rows",
        "deduplicated_rows",
        "trip_id_count",
        "content_hash_count",
        "total_amount_sum",
        "fare_amount_sum",
        "trip_distance_sum",
    }
    assert {key: first[key] for key in stable_keys} == {
        key: second[key] for key in stable_keys
    }
    assert first["read_rows"] == 6
    assert first["written_rows"] == 4
    assert first["quarantined_rows"] == 2
    assert first["deduplicated_rows"] == 1
    assert first["trip_id_count"] == 4
    assert first["unknown_columns"] == []
    assert first["missing_columns"] == []
