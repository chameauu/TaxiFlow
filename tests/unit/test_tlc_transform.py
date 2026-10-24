"""Unit tests for deterministic TLC identity and period helpers."""

from __future__ import annotations

from datetime import datetime

from taxiflow.tlc.transform import (
    content_hash,
    is_in_period,
    period_bounds,
    trip_id,
)

ROW = [
    "1",
    "2024-01-01T00:00:00",
    "2024-01-01T00:10:00",
    "1",
    "2",
    "1",
    "1.5",
    "10.0",
]


def test_content_hash_is_stable_for_the_same_values() -> None:
    assert content_hash(ROW) == content_hash(list(ROW))


def test_content_hash_changes_when_a_source_value_changes() -> None:
    changed = list(ROW)
    changed[0] = "2"

    assert content_hash(ROW) != content_hash(changed)


def test_content_hash_distinguishes_null_from_empty_string() -> None:
    assert content_hash([None]) != content_hash([""])


def test_trip_id_is_stable_and_decomposes_into_provenance() -> None:
    first = trip_id("yellow", "data/raw/yellow.parquet", "abc123", 1)
    second = trip_id("yellow", "data/raw/yellow.parquet", "abc123", 1)

    assert first == second
    assert len(first) == 64
    assert first != trip_id("yellow", "data/raw/other.parquet", "abc123", 1)
    assert first != trip_id("yellow", "data/raw/yellow.parquet", "abc123", 2)


def test_period_bounds_roll_over_the_year() -> None:
    assert period_bounds(2024, 12) == (
        datetime(2024, 12, 1),
        datetime(2025, 1, 1),
    )


def test_is_in_period_excludes_the_next_month_boundary() -> None:
    assert is_in_period(datetime(2024, 1, 1), 2024, 1)
    assert is_in_period(datetime(2024, 1, 31, 23, 59, 59), 2024, 1)
    assert not is_in_period(datetime(2024, 2, 1), 2024, 1)
    assert not is_in_period(datetime(2023, 12, 31, 23, 59, 59), 2024, 1)
    assert not is_in_period(None, 2024, 1)
