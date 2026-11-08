"""Static contract for the Phase 6 serving layer."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.contract

PROJECT_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_PATH = PROJECT_ROOT / "docker-compose.yaml"
MAKEFILE_PATH = PROJECT_ROOT / "Makefile"
ENV_EXAMPLE_PATH = PROJECT_ROOT / ".env.example"
SUPERSET_DOCKERFILE = PROJECT_ROOT / "docker" / "superset" / "Dockerfile"
SUPERSET_ROOT = PROJECT_ROOT / "config" / "superset"
DASHBOARD_ROOT = SUPERSET_ROOT / "dashboard"
QUERY_ROOT = SUPERSET_ROOT / "queries"


def load_compose() -> dict[str, Any]:
    data = yaml.safe_load(COMPOSE_PATH.read_text())
    assert isinstance(data, dict)
    return data


def load_yaml(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text())
    assert isinstance(data, dict)
    return data


def test_superset_image_installs_the_trino_sqlalchemy_driver() -> None:
    dockerfile = SUPERSET_DOCKERFILE.read_text()

    assert "FROM apache/superset:4.1.1" in dockerfile
    assert "trino[sqlalchemy]==0.330.0" in dockerfile
    assert "psycopg2-binary==2.9.9" in dockerfile


def test_superset_init_creates_an_admin_and_imports_the_dashboard() -> None:
    services = load_compose()["services"]
    init = services["superset-init"]
    command = " ".join(init["command"])
    environment = init["environment"]

    assert "import_dashboard.sh" in command
    script = (SUPERSET_ROOT / "import_dashboard.sh").read_text()
    assert "superset db upgrade" in script
    assert "superset init" in script
    assert "superset fab create-admin" in script
    assert "superset import-dashboards" in script
    assert any(
        "config/superset:/opt/taxiflow/config/superset:ro" in volume
        for volume in init["volumes"]
    )
    for key in (
        "SUPERSET_ADMIN_USER",
        "SUPERSET_ADMIN_EMAIL",
        "SUPERSET_ADMIN_PASSWORD",
        "TRINO_HOST",
        "TRINO_PORT",
        "TRINO_USER",
        "TRINO_CATALOG",
    ):
        assert key in environment


def test_dashboard_bundle_is_versioned_and_importable() -> None:
    metadata = load_yaml(DASHBOARD_ROOT / "metadata.yaml")

    assert metadata["version"] == "1.0.0"
    assert metadata["type"] == "Dashboard"
    assert metadata["timestamp"]

    for part in ("dashboards", "charts", "datasets", "databases"):
        assert (DASHBOARD_ROOT / part).is_dir(), part

    assert list((DASHBOARD_ROOT / "dashboards").glob("*.yaml"))
    assert len(list((DASHBOARD_ROOT / "charts").glob("*.yaml"))) >= 5
    assert len(list((DASHBOARD_ROOT / "datasets").rglob("*.yaml"))) >= 5
    assert len(list((DASHBOARD_ROOT / "databases").glob("*.yaml"))) == 1


def test_dashboard_database_is_the_trino_delta_catalog() -> None:
    database_path = next((DASHBOARD_ROOT / "databases").glob("*.yaml"))
    database = load_yaml(database_path)

    assert database["database_name"] == "TaxiFlow Trino"
    assert database["sqlalchemy_uri"].startswith("trino://taxiflow@trino:8080/delta")
    assert database["expose_in_sqllab"] is True
    assert database["allow_dml"] is False


def test_dashboard_charts_cover_the_four_core_questions() -> None:
    charts = [
        load_yaml(path) for path in (DASHBOARD_ROOT / "charts").glob("*.yaml")
    ]
    names = {chart["slice_name"] for chart in charts}

    assert {
        "Daily trips and revenue",
        "Zone and vendor performance",
        "Demand by hour and weekday",
        "Tip rate by payment type and borough",
        "Data quality and freshness",
    } <= names
    assert all(chart["version"] == "1.0.0" for chart in charts)
    assert all(chart["dataset_uuid"] for chart in charts)


def test_dashboard_queries_cover_the_core_questions_and_are_safe() -> None:
    expected = {
        "daily_trips.sql": "gold.agg_daily_trips",
        "zone_vendor.sql": "gold.agg_zone_daily",
        "demand_heatmap.sql": "gold.agg_hourly_demand",
        "tip_rate.sql": "gold.agg_tip_rate",
        "quality_summary.sql": "gold.quality_summary",
    }

    for name, table in expected.items():
        path = QUERY_ROOT / name
        assert path.is_file(), name
        sql = path.read_text()
        assert table in sql
        assert not re.search(r"\bselect\s+\*", sql, re.IGNORECASE)
        assert not re.search(r"\bsum\s*\(\s*passenger_count\b", sql, re.IGNORECASE)
        assert not re.search(r"\bsum\s*\(\s*trip_distance\b", sql, re.IGNORECASE)


def test_makefile_exposes_serving_bootstrap_and_verification() -> None:
    makefile = MAKEFILE_PATH.read_text()

    assert re.search(r"^superset-bootstrap:", makefile, re.MULTILINE)
    assert re.search(r"^serving-verify:", makefile, re.MULTILINE)
    assert "superset-init" in makefile
    assert "test_phase6_serving_stack.py" in makefile


def test_phase6_environment_is_documented() -> None:
    env_example = ENV_EXAMPLE_PATH.read_text()

    for key in (
        "SUPERSET_ADMIN_USER",
        "SUPERSET_ADMIN_EMAIL",
        "SUPERSET_ADMIN_PASSWORD",
        "SUPERSET_TRINO_URI",
    ):
        assert f"{key}=" in env_example


def test_serving_documentation_records_refresh_expectations() -> None:
    path = SUPERSET_ROOT / "README.md"
    assert path.is_file()
    text = path.read_text().lower()

    assert "make dbt-build" in text
    assert "not real-time" in text
    assert "refresh" in text
