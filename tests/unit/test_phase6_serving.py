"""Phase 6 serving unit tests for committed SQL safety checks."""

from __future__ import annotations

from pathlib import Path

import pytest

from taxiflow.serving.sqlcheck import validate_query_sql


def test_query_safety_accepts_explicit_columns_and_additive_money() -> None:
    sql = """
        select
            pickup_date,
            service,
            trip_count,
            total_revenue
        from gold.agg_daily_trips
        order by pickup_date
    """

    assert validate_query_sql(sql) == []


def test_query_safety_rejects_select_star() -> None:
    problems = validate_query_sql("select * from gold.agg_daily_trips")

    assert "SELECT * is forbidden" in problems


@pytest.mark.parametrize("column", ["passenger_count", "trip_distance"])
def test_query_safety_rejects_summable_per_trip_measures(column: str) -> None:
    problems = validate_query_sql(f"select sum({column}) as total from gold.fct_trips")

    assert any(column in problem for problem in problems)


def test_every_committed_serving_query_passes_safety_checks() -> None:
    query_root = Path(__file__).resolve().parents[2] / "config" / "superset" / "queries"
    paths = sorted(query_root.glob("*.sql"))

    assert paths
    for path in paths:
        assert validate_query_sql(path.read_text()) == [], path.name
