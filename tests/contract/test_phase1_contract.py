"""Static contract for the Phase 1 local platform.

These tests read compose.yaml, the Trino catalog, and .env.example. They do not
start containers; the integration suite proves the running stack.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.contract

PROJECT_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_PATH = PROJECT_ROOT / "compose.yaml"
ENV_EXAMPLE_PATH = PROJECT_ROOT / ".env.example"
TRINO_DELTA_PATH = PROJECT_ROOT / "config" / "trino" / "catalog" / "delta.properties"
MAKEFILE_PATH = PROJECT_ROOT / "Makefile"
CI_PATH = PROJECT_ROOT / ".github" / "workflows" / "ci.yml"

CORE_SERVICES = {"minio", "postgres", "hive-metastore", "trino"}
STREAM_SERVICES = {"zookeeper", "kafka", "schema-registry", "debezium"}
ORCHESTRATION_SERVICES = {"airflow-init", "airflow"}
BI_SERVICES = {"superset-init", "superset"}
BATCH_SERVICES = {"spark"}
REQUIRED_SERVICES = (
    CORE_SERVICES | STREAM_SERVICES | ORCHESTRATION_SERVICES | BI_SERVICES | BATCH_SERVICES
)

LONG_RUNNING_SERVICES = REQUIRED_SERVICES - {"airflow-init", "superset-init"}
ONE_SHOT_SERVICES = {"airflow-init", "superset-init"}

STATEFUL_VOLUMES = {
    "minio": {"minio-data"},
    "postgres": {"postgres-data"},
    "trino": {"trino-data"},
    "zookeeper": {"zookeeper-data", "zookeeper-log"},
    "kafka": {"kafka-data"},
    "airflow": {"airflow-logs"},
}

REQUIRED_ENV = {
    "TAXIFLOW_ENV",
    "MINIO_ROOT_USER",
    "MINIO_ROOT_PASSWORD",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_DB",
    "S3_BUCKET",
    "METASTORE_DB",
    "AIRFLOW_DB",
    "SUPERSET_DB",
    "AIRFLOW_FERNET_KEY",
    "AIRFLOW_WEBSERVER_SECRET_KEY",
    "AIRFLOW_ADMIN_USER",
    "AIRFLOW_ADMIN_PASSWORD",
    "SUPERSET_SECRET_KEY",
    "SUPERSET_ADMIN_USER",
    "SUPERSET_ADMIN_EMAIL",
    "SUPERSET_ADMIN_PASSWORD",
}

SECRET_ENV_KEYS = {
    "MINIO_ROOT_PASSWORD",
    "POSTGRES_PASSWORD",
    "AIRFLOW_FERNET_KEY",
    "AIRFLOW_WEBSERVER_SECRET_KEY",
    "AIRFLOW_ADMIN_PASSWORD",
    "SUPERSET_SECRET_KEY",
    "SUPERSET_ADMIN_PASSWORD",
}

IMAGE_PIN_RE = re.compile(r"(?::[A-Za-z0-9][A-Za-z0-9._-]*|@sha256:[0-9a-f]{64})$")


def load_compose() -> dict[str, Any]:
    """Load compose.yaml as a mapping."""
    data = yaml.safe_load(COMPOSE_PATH.read_text())
    assert isinstance(data, dict), "compose.yaml must be a mapping"
    return data


def services() -> dict[str, dict[str, Any]]:
    """Return the Compose services mapping."""
    return load_compose()["services"]


def env_pairs(service: dict[str, Any]) -> dict[str, str]:
    """Normalize a service environment list or mapping to a mapping."""
    environment = service.get("environment", {})
    if isinstance(environment, list):
        pairs = {}
        for item in environment:
            key, _, value = item.partition("=")
            pairs[key] = value
        return pairs
    assert isinstance(environment, dict), "service environment must be a list or mapping"
    return {str(key): str(value) for key, value in environment.items()}


def volume_sources(service: dict[str, Any]) -> set[str]:
    """Return the source names of a service's volumes."""
    sources = set()
    for volume in service.get("volumes", []):
        assert isinstance(volume, str), "use short volume syntax for contract checks"
        source = volume.split(":", 1)[0]
        if source and not source.startswith((".", "/")):
            sources.add(source)
    return sources


def parse_env_example() -> dict[str, str]:
    """Parse .env.example without executing it."""
    values = {}
    for raw_line in ENV_EXAMPLE_PATH.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        assert separator, f"invalid .env.example line: {raw_line!r}"
        values[key] = value
    return values


def test_all_required_services_exist() -> None:
    assert set(services()) == REQUIRED_SERVICES


def test_each_service_has_the_expected_profiles() -> None:
    expected = {
        **{name: {"core"} for name in CORE_SERVICES if name != "postgres"},
        **{name: {"stream"} for name in STREAM_SERVICES},
        **{name: {"orchestration"} for name in ORCHESTRATION_SERVICES},
        **{name: {"bi"} for name in BI_SERVICES},
        **{name: {"batch"} for name in BATCH_SERVICES},
        "postgres": {"core", "orchestration", "bi"},
    }
    for name, service in services().items():
        assert set(service.get("profiles", [])) == expected[name], name


def test_one_named_network_is_used_by_every_service() -> None:
    compose = load_compose()
    assert compose.get("networks", {}).keys() == {"taxiflow"}
    for name, service in services().items():
        assert service.get("networks") == ["taxiflow"], name


def test_images_are_pinned_and_not_latest() -> None:
    for name, service in services().items():
        image = str(service.get("image", ""))
        assert image, f"{name} has no image"
        assert "latest" not in image, f"{name} uses latest: {image}"
        assert IMAGE_PIN_RE.search(image), f"{name} image is not pinned: {image}"


def test_long_running_services_have_healthchecks() -> None:
    for name in LONG_RUNNING_SERVICES:
        assert "healthcheck" in services()[name], f"{name} has no healthcheck"


def test_one_shot_services_do_not_restart() -> None:
    for name in ONE_SHOT_SERVICES:
        assert services()[name].get("restart") == "no", name


def test_stateful_services_have_named_volumes() -> None:
    compose_volumes = set(load_compose().get("volumes", {}))
    for name, expected in STATEFUL_VOLUMES.items():
        assert expected <= volume_sources(services()[name]), f"{name} missing a named volume"
        assert expected <= compose_volumes, f"{name} uses undeclared volume {expected}"


def test_dependencies_use_service_names_and_exist() -> None:
    known = set(services())
    for name, service in services().items():
        depends = service.get("depends_on", {})
        dependencies = set(depends)
        assert dependencies <= known, f"{name} depends on unknown service {dependencies - known}"


def test_no_loopback_or_reference_credentials_in_compose() -> None:
    text = COMPOSE_PATH.read_text()
    assert "127.0.0.1" not in text
    assert "minio_access_key" not in text
    assert "minio_secret_key" not in text
    assert "k6/k6" not in text
    assert "change-me" not in text


def test_secret_values_come_from_environment() -> None:
    for name, service in services().items():
        environment = env_pairs(service)
        for key in SECRET_ENV_KEYS & environment.keys():
            assert environment[key].startswith("${"), f"{name}.{key} is a literal secret"


def test_trino_delta_catalog_contract() -> None:
    text = TRINO_DELTA_PATH.read_text()
    assert "connector.name=delta_lake" in text
    assert "hive.metastore.uri=thrift://hive-metastore:9083" in text
    assert "hive.s3.endpoint=http://minio:9000" in text
    assert "hive.s3.path-style-access=true" in text
    assert "hive.s3.ssl.enabled=false" in text


def test_lake_warehouse_is_under_the_taxiflow_bucket() -> None:
    hive = env_pairs(services()["hive-metastore"])
    assert hive["HIVE_METASTORE_WAREHOUSE_DIR"] == "s3://${S3_BUCKET}/warehouse/"
    minio = env_pairs(services()["minio"])
    assert minio["MINIO_DEFAULT_BUCKETS"] == "${S3_BUCKET}"


def test_env_example_documents_required_values() -> None:
    values = parse_env_example()
    assert values.keys() >= REQUIRED_ENV


def test_env_example_has_placeholders_not_real_secrets() -> None:
    values = parse_env_example()
    for key in SECRET_ENV_KEYS:
        assert values[key].startswith("change-me"), f"{key} must be an obvious placeholder"


def test_makefile_exposes_the_phase_one_commands() -> None:
    text = MAKEFILE_PATH.read_text()
    for target in ("up", "up-all", "down", "reset", "logs", "ps", "images", "pull", "verify"):
        assert f"{target}:" in text, target


def test_ci_generates_env_before_compose_validation() -> None:
    text = CI_PATH.read_text()
    assert "make env" in text
    assert "make verify" in text
