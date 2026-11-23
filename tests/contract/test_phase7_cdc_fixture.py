"""Static contract for the committed CDC event fixture.

The fixture exists so the op/tombstone semantics of ``stg_cdc_trips`` are
proven on a real engine against committed events, not only inferred from
whatever the synthetic stream happens to contain. The same macro must drive
the production model and the fixture assertion; otherwise the test would
validate a copy of the logic instead of the logic itself.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

pytestmark = pytest.mark.contract

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT = PROJECT_ROOT / "dbt"
MACRO = DBT / "macros" / "cdc_live_state.sql"
MODEL = DBT / "models" / "staging" / "stg_cdc_trips.sql"
EVENTS = DBT / "seeds" / "cdc_events_fixture.csv"
EXPECTED = DBT / "seeds" / "cdc_events_expected.csv"
FIXTURE_TEST = DBT / "tests" / "assert_cdc_fixture_semantics.sql"
PROJECT = DBT / "dbt_project.yml"


def test_fixture_files_are_committed() -> None:
    for path in (EVENTS, EXPECTED):
        assert path.is_file(), f"missing fixture: {path}"
    header = EVENTS.read_text().splitlines()[0]
    for column in ("trip_id", "__op", "__deleted", "__source_ts_ms"):
        assert column in header, f"fixture is missing {column}"


def test_fixture_covers_every_event_kind() -> None:
    rows = list(csv.DictReader(EVENTS.read_text().splitlines()))
    ops = {row["__op"] for row in rows}
    assert ops == {"c", "u", "d"}, f"fixture ops are {sorted(ops)}"
    assert any(row["__deleted"] == "true" for row in rows), "no tombstone event"
    # A later __source_ts_ms event must precede an older one in file order, so
    # rank-by-timestamp (not file order) is required.
    stamps = [int(row["__source_ts_ms"]) for row in rows]
    assert stamps != sorted(stamps), "fixture must contain out-of-order events"


def test_macro_owns_the_liveness_rules() -> None:
    text = MACRO.read_text()
    assert "{% macro cdc_live_state(" in text
    for rule in (
        "row_number() over",
        "partition by trip_id",
        "order by __source_ts_ms desc",
        "__op != 'd'",
        "__deleted",
    ):
        assert rule in text, f"macro is missing rule: {rule}"


def test_model_uses_the_shared_macro() -> None:
    text = MODEL.read_text()
    assert "cdc_live_state(source('bronze', 'cdc_taxi_trips'))" in text
    # The model must not re-implement the ranking next to the macro call.
    assert "row_number() over" not in text.split("{%")[0]


def test_fixture_test_reuses_the_macro_and_seeds() -> None:
    text = FIXTURE_TEST.read_text()
    assert "cdc_live_state(ref('cdc_events_fixture'))" in text
    assert "ref('cdc_events_expected')" in text


def test_fixture_seeds_are_declared() -> None:
    text = PROJECT.read_text()
    for seed in ("cdc_events_fixture", "cdc_events_expected"):
        assert seed in text, f"seed {seed} is not declared in dbt_project.yml"
