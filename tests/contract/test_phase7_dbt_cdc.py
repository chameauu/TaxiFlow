"""Static contract for the Phase 7 CDC-into-dbt integration (task 3).

These tests read the dbt project files only. They never run dbt; the
integration proof is `dbt build` against the live stack and the Phase 7
exit gate. The contract pins the semantics the unified model must keep:

- CDC staging applies Debezium event semantics (latest event per trip_id,
  deletes excluded from the live state) before unification — not a plain
  union of raw events.
- CDC timestamps are restored as UTC instants; the TLC New York rule is
  batch-only.
- `record_source` tags every unified row and the reconciliation tests
  compare batch and CDC separately instead of one mixed total.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.contract

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_MODELS = PROJECT_ROOT / "dbt" / "models"
DBT_TESTS = PROJECT_ROOT / "dbt" / "tests"
DBT_ROOT = PROJECT_ROOT / "dbt"
CDC_MACRO = DBT_ROOT / "macros" / "cdc_live_state.sql"
SOURCES_YML = DBT_MODELS / "sources.yml"
SCHEMA_YML = DBT_MODELS / "schema.yml"
STG_CDC_SQL = DBT_MODELS / "staging" / "stg_cdc_trips.sql"
INT_SQL = DBT_MODELS / "intermediate" / "int_trips_enriched.sql"
FCT_SQL = DBT_MODELS / "marts" / "fct_trips.sql"
RECONCILE_FCT_SQL = DBT_TESTS / "reconcile_fct_totals.sql"
MAKEFILE_PATH = PROJECT_ROOT / "Makefile"


def text_of(path: Path) -> str:
    assert path.exists(), f"{path} is missing"
    return path.read_text(encoding="utf-8")


def test_sources_declare_the_cdc_bronze_table() -> None:
    sources = text_of(SOURCES_YML)
    assert "cdc_taxi_trips" in sources, (
        "sources.yml must declare the CDC bronze Delta table"
    )


def test_stg_cdc_trips_applies_event_semantics() -> None:
    sql = text_of(STG_CDC_SQL)

    assert "source('bronze', 'cdc_taxi_trips')" in sql, (
        "stg_cdc_trips must read the CDC bronze Delta table"
    )
    # The reduction rules live in the shared cdc_live_state macro so the
    # committed fixture test exercises the same SQL (see test_phase7_cdc_fixture).
    assert "cdc_live_state(source('bronze', 'cdc_taxi_trips'))" in sql
    macro = CDC_MACRO.read_text()
    assert "__source_ts_ms" in macro and "row_number" in macro, (
        "the macro must reduce events to the latest per trip_id by source ts"
    )
    assert "__op" in macro, "the macro must evaluate the event operation"
    assert "is_deleted" in macro, (
        "the macro must keep a live-state flag for deletes"
    )


def test_stg_cdc_trips_restores_utc_instants_not_new_york() -> None:
    sql = text_of(STG_CDC_SQL) + "\n" + CDC_MACRO.read_text()
    executable = "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )

    assert "America/New_York" not in executable, (
        "CDC timestamps are UTC epoch instants; the TLC New York rule is "
        "batch-only and applying it here would skew the unified model"
    )
    assert "from_unixtime" not in executable, (
        "bronze already carries timestamp(3) with time zone (from_avro decodes "
        "Avro timestamp-millis); re-dividing epoch millis here is a type error"
    )
    for column in ("pickup_at", "dropoff_at", "updated_at"):
        assert f"timestamp(3) with time zone) as {column}" in executable, (
            f"stg_cdc_trips must cast {column} to timestamp(3) with time zone"
        )


def test_int_trips_enriched_unifies_both_sources_with_record_source() -> None:
    sql = text_of(INT_SQL)

    assert "record_source" in sql, (
        "the unified intermediate must tag each row with its source"
    )
    assert "stg_yellow_trips" in sql, "batch trips must stay in the unified model"
    assert "stg_cdc_trips" in sql, "CDC trips must enter the unified model"
    assert "union all" in sql.lower(), (
        "the two sources must be combined with an explicit union"
    )


def test_fct_trips_carries_record_source() -> None:
    sql = text_of(FCT_SQL)
    assert "record_source" in sql, (
        "the fact table must expose record_source so quality and marts can "
        "reason about origin"
    )


def test_reconciliation_is_source_aware() -> None:
    sql = text_of(RECONCILE_FCT_SQL)
    assert "record_source" in sql, (
        "fct totals must reconcile per source: batch against batch staging, "
        "CDC against CDC staging — a single mixed total would mask a wrong "
        "union"
    )


def test_schema_yml_tests_record_source() -> None:
    yml = text_of(SCHEMA_YML)
    assert "record_source" in yml, (
        "schema.yml must document and test record_source (not_null and "
        "accepted_values batch/cdc)"
    )


def test_register_sources_includes_the_cdc_table() -> None:
    makefile = text_of(MAKEFILE_PATH)
    recipe_start = makefile.find("register-sources:")
    assert recipe_start >= 0, "register-sources target must exist"
    recipe_end = makefile.find("\n\n", recipe_start)
    recipe = makefile[recipe_start : recipe_end if recipe_end > 0 else None]
    assert "cdc_taxi_trips" in recipe, (
        "register-sources must register the CDC bronze table with Trino"
    )
