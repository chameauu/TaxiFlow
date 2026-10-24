"""Unit tests for explicit TLC schemas and schema-drift planning."""

from __future__ import annotations

import pytest

from taxiflow.tlc.schema import (
    TLC_SCHEMAS,
    SchemaError,
    normalize_column_name,
    plan_columns,
)

ALL_YELLOW_COLUMNS = [spec.name for spec in TLC_SCHEMAS["yellow"].columns]


def test_yellow_schema_has_explicit_partition_and_time_columns() -> None:
    schema = TLC_SCHEMAS["yellow"]

    assert schema.service == "yellow"
    assert schema.pickup_column == "tpep_pickup_datetime"
    assert schema.dropoff_column == "tpep_dropoff_datetime"
    assert {spec.name for spec in schema.columns} >= {
        "VendorID",
        "tpep_pickup_datetime",
        "PULocationID",
        "total_amount",
        "Airport_fee",
    }


def test_green_schema_has_its_own_timestamp_contract() -> None:
    schema = TLC_SCHEMAS["green"]

    assert schema.pickup_column == "lpep_pickup_datetime"
    assert schema.dropoff_column == "lpep_dropoff_datetime"
    assert "trip_type" in {spec.name for spec in schema.columns}


def test_normalize_column_name_is_case_and_whitespace_insensitive() -> None:
    assert normalize_column_name("  TPE PICKUP ") == "tpe pickup"
    assert normalize_column_name("Airport_Fee") == "airport_fee"


def test_plan_columns_matches_case_insensitively_and_reports_missing_and_unknown() -> None:
    actual = [name.upper() for name in ALL_YELLOW_COLUMNS if name != "Airport_fee"]
    actual.append("new_tlc_column")

    plan = plan_columns(TLC_SCHEMAS["yellow"], actual)

    assert dict(plan.mapping)["Airport_fee"] is None
    assert plan.missing == ("Airport_fee",)
    assert plan.unknown == ("new_tlc_column",)
    assert dict(plan.mapping)["tpep_pickup_datetime"] == "TPEP_PICKUP_DATETIME"


def test_plan_columns_rejects_duplicate_normalized_names() -> None:
    with pytest.raises(SchemaError, match="duplicate"):
        plan_columns(TLC_SCHEMAS["yellow"], [*ALL_YELLOW_COLUMNS, "vendorid"])


def test_plan_columns_treats_renamed_column_as_missing_plus_unknown() -> None:
    actual = [
        "pickup_datetime" if name == "tpep_pickup_datetime" else name
        for name in ALL_YELLOW_COLUMNS
    ]

    plan = plan_columns(TLC_SCHEMAS["yellow"], actual)

    assert "tpep_pickup_datetime" in plan.missing
    assert "pickup_datetime" in plan.unknown
