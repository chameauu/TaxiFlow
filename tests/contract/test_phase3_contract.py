"""Static contract for the Phase 3 dbt silver/gold layer."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.contract

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_ROOT = PROJECT_ROOT / "dbt"
MODEL_ROOT = DBT_ROOT / "models"


def load_yaml(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text())
    assert isinstance(data, dict)
    return data


def test_dbt_project_uses_the_trino_profile_and_layer_schemas() -> None:
    project = load_yaml(DBT_ROOT / "dbt_project.yml")
    profile = load_yaml(DBT_ROOT / "profiles.yml")

    assert project["profile"] == "taxiflow"
    assert project["models"]["taxiflow"]["staging"]["+schema"] == "silver"
    assert project["models"]["taxiflow"]["intermediate"]["+materialized"] == "table"
    assert project["models"]["taxiflow"]["intermediate"]["+on_table_exists"] == "drop"
    assert project["models"]["taxiflow"]["marts"]["+schema"] == "gold"
    assert project["models"]["taxiflow"]["marts"]["+on_table_exists"] == "drop"
    assert project["seeds"]["taxiflow"]["taxi_zone_lookup"]["+schema"] == "reference"

    output = profile["taxiflow"]["outputs"]["dev"]
    assert output["type"] == "trino"
    assert output["database"] == "{{ env_var('TRINO_CATALOG', 'delta') }}"
    assert output["schema"] == "{{ env_var('DBT_SCHEMA', 'analytics') }}"


def test_required_models_and_reconciliation_tests_exist() -> None:
    expected_models = {
        "staging/stg_yellow_trips.sql",
        "staging/stg_taxi_zones.sql",
        "staging/stg_tlc_quarantine.sql",
        "intermediate/int_trips_enriched.sql",
        "marts/dim_zones.sql",
        "marts/fct_trips.sql",
        "marts/agg_daily_trips.sql",
        "marts/agg_zone_daily.sql",
        "marts/agg_hourly_demand.sql",
        "marts/agg_tip_rate.sql",
        "marts/fct_quality_reasons.sql",
        "marts/quality_summary.sql",
    }
    actual_models = {
        str(path.relative_to(MODEL_ROOT)) for path in MODEL_ROOT.rglob("*.sql")
    }
    assert expected_models <= actual_models

    expected_tests = {
        "reconcile_fct_totals.sql",
        "reconcile_daily_totals.sql",
        "reconcile_quarantine_totals.sql",
        "assert_pickup_period.sql",
        "assert_no_null_zone_borough.sql",
    }
    actual_tests = {path.name for path in (DBT_ROOT / "tests").glob("*.sql")}
    assert expected_tests <= actual_tests


def test_schema_declares_keys_relationships_and_accepted_values() -> None:
    schema = (MODEL_ROOT / "schema.yml").read_text()

    assert "unique" in schema
    assert "not_null" in schema
    assert "relationships" in schema
    assert "accepted_values" in schema
    assert "ref('dim_zones')" in schema
    assert "pickup_location_id" in schema
    assert "dropoff_location_id" in schema


def test_models_do_not_use_select_star_or_hardcoded_catalog_names() -> None:
    offenders: list[str] = []
    for path in MODEL_ROOT.rglob("*.sql"):
        text = path.read_text()
        if re.search(r"(?i)\bselect\s+\*|\b\w+\.\*", text):
            offenders.append(f"{path.name}: select star")
        if re.search(r"\bdelta\.(default|silver|gold|reference)\b", text):
            offenders.append(f"{path.name}: hardcoded catalog")

    assert not offenders


def test_taxi_zone_seed_is_pinned_and_complete() -> None:
    seed = DBT_ROOT / "seeds" / "taxi_zone_lookup.csv"
    payload = seed.read_bytes()

    assert len(payload) == 12_331
    assert hashlib.sha256(payload).hexdigest() == (
        "1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed"
    )
    assert len(payload.decode().splitlines()) == 266


def test_makefile_exposes_registration_and_dbt_build_targets() -> None:
    makefile = (PROJECT_ROOT / "Makefile").read_text()

    assert re.search(r"^register-sources:", makefile, re.MULTILINE)
    assert re.search(r"^dbt-debug:", makefile, re.MULTILINE)
    assert re.search(r"^dbt-build:", makefile, re.MULTILINE)
    assert "dbt build --project-dir dbt --profiles-dir dbt" in makefile
    assert "delta.system.register_table" in makefile


def test_dbt_environment_variables_are_documented() -> None:
    env_example = (PROJECT_ROOT / ".env.example").read_text()

    for key in (
        "TRINO_HOST",
        "TRINO_PORT",
        "TRINO_USER",
        "TRINO_CATALOG",
        "TRINO_BRONZE_SCHEMA",
        "DBT_SCHEMA",
    ):
        assert f"{key}=" in env_example
