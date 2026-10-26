"""Static contract for the Phase 5 orchestration and quality layer."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.contract

PROJECT_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_PATH = PROJECT_ROOT / "compose.yaml"
MAKEFILE_PATH = PROJECT_ROOT / "Makefile"
AIRFLOW_ROOT = PROJECT_ROOT / "airflow"
AIRFLOW_DOCKERFILE = PROJECT_ROOT / "docker" / "airflow" / "Dockerfile"
DAG_PATH = AIRFLOW_ROOT / "dags" / "taxiflow_pipeline.py"
QUALITY_ROOT = PROJECT_ROOT / "src" / "taxiflow" / "quality"
SUITE_ROOT = QUALITY_ROOT / "suites"
OPS_SQL = PROJECT_ROOT / "config" / "postgres" / "init" / "03-ops.sql"


def load_compose() -> dict[str, Any]:
    data = yaml.safe_load(COMPOSE_PATH.read_text())
    assert isinstance(data, dict)
    return data


def load_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text())
    assert isinstance(data, dict)
    return data


def test_airflow_uses_a_self_contained_project_image() -> None:
    assert AIRFLOW_DOCKERFILE.is_file()
    dockerfile = AIRFLOW_DOCKERFILE.read_text()
    services = load_compose()["services"]
    airflow = services["airflow"]

    assert "FROM apache/airflow:2.9.3-python3.11" in dockerfile
    assert "openjdk-17" in dockerfile
    assert "uv" in dockerfile
    assert "UV_PROJECT_ENVIRONMENT" in dockerfile
    assert "great-expectations" in dockerfile or "quality" in dockerfile
    assert "pyspark" in dockerfile or "orchestration" in dockerfile

    assert airflow["build"]["context"] == "."
    assert airflow["build"]["dockerfile"] == "docker/airflow/Dockerfile"
    assert airflow["image"] == "taxiflow/airflow:2.9.3"
    assert airflow["environment"]["TRINO_HOST"] == "trino"
    assert airflow["environment"]["TRINO_PORT"] == "8080"
    assert airflow["environment"]["POSTGRES_HOST"] == "postgres"
    assert airflow["environment"]["TAXIFLOW_PROJECT_ROOT"] == "/opt/taxiflow"
    assert any("airflow-webserver.pid" in part for part in airflow["command"])
    assert any(":/opt/taxiflow:ro" in volume for volume in airflow["volumes"])


def test_phase5_dag_orders_batch_dbt_and_quality_before_promotion() -> None:
    assert DAG_PATH.is_file()
    dag = DAG_PATH.read_text()

    assert "with DAG(" in dag
    assert 'schedule="@daily"' in dag
    assert "catchup=False" in dag
    assert "max_active_runs=1" in dag
    assert "on_failure_callback" in dag
    assert "BashOperator" in dag
    for task_id in ("batch", "dbt_build", "quality", "promote"):
        assert f'task_id="{task_id}"' in dag
    assert "batch >> dbt_build >> quality >> promote" in dag
    assert "run_batch.sh" in dag
    assert "run_dbt.sh" in dag
    assert "run_quality.sh" in dag


def test_airflow_scripts_are_executable_and_fail_loudly() -> None:
    for name in ("run_batch.sh", "run_dbt.sh", "run_quality.sh"):
        path = AIRFLOW_ROOT / "scripts" / name
        assert path.is_file(), name
        assert os.access(path, os.X_OK), name
        text = path.read_text()
        assert "set -euo pipefail" in text
        assert "taxiflow" in text


def test_makefile_exposes_quality_and_batch_orchestration() -> None:
    makefile = MAKEFILE_PATH.read_text()

    assert re.search(r"^quality:", makefile, re.MULTILINE)
    assert "taxiflow quality run" in makefile
    assert "make quality" in makefile
    assert "make batch" in makefile


def test_ops_schema_records_runs_and_alerts() -> None:
    assert OPS_SQL.is_file()
    sql = OPS_SQL.read_text()

    assert "CREATE SCHEMA IF NOT EXISTS ops" in sql
    assert "CREATE TABLE IF NOT EXISTS ops.pipeline_runs" in sql
    assert "CREATE TABLE IF NOT EXISTS ops.pipeline_alerts" in sql
    for column in (
        "run_id",
        "kind",
        "status",
        "read_rows",
        "written_rows",
        "quarantined_rows",
        "deduplicated_rows",
        "checks_total",
        "checks_failed",
        "details",
    ):
        assert column in sql


def test_gx_suites_are_committed_with_key_expectations() -> None:
    for name in ("bronze_tlc_trips.json", "silver_stg_yellow_trips.json"):
        path = SUITE_ROOT / name
        assert path.is_file(), name
        suite = load_json(path)
        expectation_types = {item["expectation_type"] for item in suite["expectations"]}
        assert "expect_column_values_to_not_be_null" in expectation_types
        assert "expect_column_values_to_be_unique" in expectation_types
        assert "expect_column_values_to_be_in_set" in expectation_types

    bronze = load_json(SUITE_ROOT / "bronze_tlc_trips.json")
    silver = load_json(SUITE_ROOT / "silver_stg_yellow_trips.json")
    assert any(
        item["kwargs"].get("column") == "trip_id" for item in bronze["expectations"]
    )
    assert any(
        item["kwargs"].get("column") == "pickup_at" for item in silver["expectations"]
    )


def test_phase5_environment_is_documented() -> None:
    env_example = (PROJECT_ROOT / ".env.example").read_text()

    for key in (
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "TAXIFLOW_BATCH_YEAR",
        "TAXIFLOW_BATCH_MONTH",
        "TAXIFLOW_QUALITY_MAX_FRESHNESS_SECONDS",
    ):
        assert f"{key}=" in env_example
