"""Static contract for the Phase 2 batch bronze slice."""

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
MANIFEST_PATH = PROJECT_ROOT / "config" / "tlc_manifest.json"
JOBS_PATH = PROJECT_ROOT / "src" / "taxiflow" / "jobs"


def load_compose() -> dict[str, Any]:
    data = yaml.safe_load(COMPOSE_PATH.read_text())
    assert isinstance(data, dict)
    return data


def spark_service() -> dict[str, Any]:
    return load_compose()["services"]["spark"]


def test_spark_mounts_the_project_read_only_and_a_persistent_ivy_cache() -> None:
    volumes = spark_service().get("volumes", [])

    assert "./:/opt/taxiflow:ro" in volumes
    assert "spark-ivy:/tmp/.ivy2" in volumes


def test_spark_receives_lake_credentials_and_the_package_path() -> None:
    environment = spark_service().get("environment", {})

    assert environment["PYTHONPATH"] == "/opt/taxiflow/src"
    assert environment["S3A_ENDPOINT"] == "http://minio:9000"
    assert "${MINIO_ROOT_USER}" in environment["MINIO_ROOT_USER"]
    assert "${MINIO_ROOT_PASSWORD}" in environment["MINIO_ROOT_PASSWORD"]


def test_makefile_exposes_seed_batch_and_report_targets() -> None:
    makefile = MAKEFILE_PATH.read_text()

    assert re.search(r"^seed:", makefile, re.MULTILINE)
    assert re.search(r"^batch:", makefile, re.MULTILINE)
    assert re.search(r"^batch-report:", makefile, re.MULTILINE)
    assert "batch_bronze.py --root /opt/taxiflow" in makefile


def test_production_manifest_declares_exactly_one_verified_month() -> None:
    manifest = json.loads(MANIFEST_PATH.read_text())

    assert manifest["version"] == 1
    assert len(manifest["files"]) == 1
    entry = manifest["files"][0]
    assert entry["service"] == "yellow"
    assert (entry["year"], entry["month"]) == (2024, 1)
    assert entry["size_bytes"] == 49_961_641
    assert re.fullmatch(r"[0-9a-f]{64}", entry["sha256"])
    assert entry["rows"] == 2_964_624


def test_committed_jobs_do_not_infer_the_parquet_schema() -> None:
    offenders = []
    for path in JOBS_PATH.glob("*.py"):
        if "inferSchema" in path.read_text():
            offenders.append(path.name)

    assert not offenders
