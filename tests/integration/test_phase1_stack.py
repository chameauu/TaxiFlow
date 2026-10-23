"""Runtime contract for the Phase 1 stack.

The suite requires a running stack. Set TAXIFLOW_REQUIRE_STACK=1 to turn a
missing service into a failure instead of a skip.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from urllib.request import urlopen

import pytest

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROFILES = (
    "--profile",
    "core",
    "--profile",
    "stream",
    "--profile",
    "orchestration",
    "--profile",
    "bi",
)
HTTP_ENDPOINTS = {
    "minio": "http://localhost:9000/minio/health/live",
    "trino": "http://localhost:8084/v1/info",
    "schema-registry": "http://localhost:8081/subjects",
    "debezium": "http://localhost:8083/connectors",
    "airflow": "http://localhost:8080/health",
    "superset": "http://localhost:8088/health",
}


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    """Run a Compose command from the project root."""
    return subprocess.run(
        ["docker", "compose", *PROFILES, *args],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=check,
    )


def require_running(service: str) -> None:
    """Require a service to be running, or skip when the stack is optional."""
    result = compose("ps", "--status", "running", "--services", check=False)
    if service in result.stdout.split():
        return
    if os.environ.get("TAXIFLOW_REQUIRE_STACK") == "1":
        pytest.fail(f"{service} is not running; start the stack with make up-all")
    pytest.skip(f"{service} is not running")


def http_ok(url: str) -> None:
    """Assert that an HTTP endpoint returns 200."""
    with urlopen(url, timeout=10) as response:
        assert response.status == 200, url


def test_http_endpoints() -> None:
    for service, url in HTTP_ENDPOINTS.items():
        require_running(service)
        http_ok(url)


def test_postgres_databases_and_logical_wal() -> None:
    require_running("postgres")
    result = compose(
        "exec",
        "-T",
        "postgres",
        "sh",
        "-c",
        'psql -U "$POSTGRES_USER" -d postgres -tAc '
        "\"SELECT datname FROM pg_database WHERE datname IN "
        "('taxiflow','metastore','airflow','superset') ORDER BY 1\"",
    )
    assert set(result.stdout.split()) == {"taxiflow", "metastore", "airflow", "superset"}

    wal_level = compose(
        "exec",
        "-T",
        "postgres",
        "sh",
        "-c",
        'psql -U "$POSTGRES_USER" -d postgres -tAc "SHOW wal_level"',
    )
    assert wal_level.stdout.strip() == "logical"


def test_minio_created_the_lake_bucket() -> None:
    require_running("minio")
    compose(
        "exec",
        "-T",
        "minio",
        "sh",
        "-c",
        "test -d /bitnami/minio/data/taxiflow",
    )


def test_kafka_broker_is_reachable() -> None:
    require_running("kafka")
    compose(
        "exec",
        "-T",
        "kafka",
        "kafka-broker-api-versions",
        "--bootstrap-server",
        "localhost:9092",
    )


def test_trino_sees_the_delta_catalog() -> None:
    require_running("trino")
    result = compose(
        "exec",
        "-T",
        "trino",
        "trino",
        "--execute",
        "SHOW CATALOGS",
        "--output-format",
        "TSV",
    )
    assert "delta" in result.stdout.split()


def test_trino_reaches_minio_by_service_name() -> None:
    require_running("trino")
    require_running("minio")
    compose(
        "exec",
        "-T",
        "trino",
        "curl",
        "--fail",
        "--silent",
        "http://minio:9000/minio/health/live",
    )


def test_airflow_can_see_the_health_dag() -> None:
    require_running("airflow")
    result = compose(
        "exec",
        "-T",
        "airflow",
        "airflow",
        "dags",
        "list",
    )
    assert "taxiflow_health" in result.stdout
