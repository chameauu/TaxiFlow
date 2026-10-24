"""Unit tests for batch bronze pure helpers and timezone policy."""

from __future__ import annotations

from taxiflow.jobs.batch_bronze import (
    SPARK_SESSION_TIMEZONE,
    bronze_column_names,
    build_replace_where,
)
from taxiflow.tlc.manifest import TlcFile
from taxiflow.tlc.schema import TLC_SCHEMAS, plan_columns

ENTRY = TlcFile(
    service="yellow",
    year=2024,
    month=1,
    url="https://example.invalid/yellow.parquet",
    path="data/raw/yellow.parquet",
    size_bytes=1,
    sha256="0" * 64,
    rows=1,
)


def test_replace_where_is_deterministic_and_month_scoped() -> None:
    assert build_replace_where(ENTRY) == (
        "service = 'yellow' AND pickup_year = 2024 AND pickup_month = 1"
    )


def test_bronze_columns_keep_schema_order_then_provenance() -> None:
    plan = plan_columns(
        TLC_SCHEMAS["yellow"],
        [spec.name for spec in TLC_SCHEMAS["yellow"].columns],
    )

    columns = bronze_column_names(plan)

    assert columns[: len(plan.schema.columns)] == plan.schema.column_names
    assert columns[len(plan.schema.columns) :] == (
        "service",
        "source_file",
        "source_row_number",
        "ingested_at",
        "content_hash",
        "trip_id",
        "pickup_year",
        "pickup_month",
    )


def test_bronze_session_timezone_is_utc_to_preserve_wall_clock_values() -> None:
    assert SPARK_SESSION_TIMEZONE == "UTC"
