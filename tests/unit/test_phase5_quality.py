"""Phase 5 data-quality and run-metadata unit tests."""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd
import pytest

from taxiflow.quality.metadata import RunRecord, ensure_schema_statements
from taxiflow.quality.reconcile import QualityError, Snapshot, evaluate_snapshot
from taxiflow.quality.suites import load_suite, validate_dataframe


def bronze_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "trip_id": ["a", "b"],
            "service": ["yellow", "yellow"],
            "tpep_pickup_datetime": [
                datetime(2024, 1, 1, 8, 0),
                datetime(2024, 1, 1, 9, 0),
            ],
            "tpep_dropoff_datetime": [
                datetime(2024, 1, 1, 8, 15),
                datetime(2024, 1, 1, 9, 20),
            ],
            "trip_distance": [1.5, 2.0],
            "total_amount": [12.5, 18.0],
            "pickup_year": [2024, 2024],
            "pickup_month": [1, 1],
            "ingested_at": [
                datetime(2024, 1, 2, tzinfo=UTC),
                datetime(2024, 1, 2, tzinfo=UTC),
            ],
        }
    )


def silver_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "trip_id": ["a", "b"],
            "service": ["yellow", "yellow"],
            "pickup_at": [
                datetime(2024, 1, 1, 13, 0, tzinfo=UTC),
                datetime(2024, 1, 1, 14, 0, tzinfo=UTC),
            ],
            "pickup_location_id": [132, 161],
            "pickup_borough": ["Queens", "Manhattan"],
            "total_amount": [12.5, 18.0],
            "pickup_year": [2024, 2024],
            "ingested_at": [
                datetime(2024, 1, 2, tzinfo=UTC),
                datetime(2024, 1, 2, tzinfo=UTC),
            ],
        }
    )


def snapshot(**overrides: object) -> Snapshot:
    values: dict[str, object] = {
        "bronze_rows": 10,
        "silver_rows": 10,
        "fact_rows": 10,
        "quarantine_rows": 2,
        "quarantine_reason_rows": 2,
        "bronze_trip_ids": 10,
        "silver_trip_ids": 10,
        "fact_trip_ids": 10,
        "bronze_total_amount": 100.25,
        "silver_total_amount": 100.25,
        "fact_total_amount": 100.25,
        "freshness_seconds": 12,
    }
    values.update(overrides)
    return Snapshot(**values)


def test_quality_suites_load_and_validate_fixture_data() -> None:
    assert load_suite("bronze_tlc_trips").expectation_suite_name == "bronze_tlc_trips"
    assert load_suite("silver_stg_yellow_trips").expectation_suite_name == (
        "silver_stg_yellow_trips"
    )

    bronze = validate_dataframe(bronze_frame(), "bronze_tlc_trips")
    silver = validate_dataframe(silver_frame(), "silver_stg_yellow_trips")

    assert bronze.success is True
    assert silver.success is True
    assert bronze.evaluated >= 4
    assert silver.evaluated >= 4


def test_quality_suite_rejects_duplicate_or_null_keys() -> None:
    duplicate = bronze_frame()
    duplicate.loc[1, "trip_id"] = "a"

    result = validate_dataframe(duplicate, "bronze_tlc_trips")

    assert result.success is False
    assert any("trip_id" in failure for failure in result.failed)


def test_reconciliation_accepts_a_consistent_snapshot() -> None:
    checks = evaluate_snapshot(snapshot(), max_freshness_seconds=60)

    assert checks
    assert all(check.passed for check in checks)
    assert {check.name for check in checks} >= {
        "bronze_rows",
        "silver_rows",
        "fact_rows",
        "trip_ids",
        "total_amount",
        "quarantine_rows",
        "freshness",
    }


@pytest.mark.parametrize(
    ("overrides", "failed_name"),
    [
        ({"fact_rows": 9}, "fact_rows"),
        ({"silver_rows": 8}, "silver_rows"),
        ({"fact_trip_ids": 9}, "trip_ids"),
        ({"fact_total_amount": 99.0}, "total_amount"),
        ({"quarantine_reason_rows": 1}, "quarantine_rows"),
        ({"freshness_seconds": 61}, "freshness"),
        ({"bronze_rows": 0}, "bronze_rows"),
    ],
)
def test_reconciliation_rejects_a_broken_snapshot(
    overrides: dict[str, object], failed_name: str
) -> None:
    checks = evaluate_snapshot(snapshot(**overrides), max_freshness_seconds=60)
    failed = {check.name for check in checks if not check.passed}

    assert failed_name in failed


def test_quality_error_lists_every_failed_check() -> None:
    with pytest.raises(QualityError) as exc:
        evaluate_snapshot(
            snapshot(fact_rows=9, freshness_seconds=61),
            max_freshness_seconds=60,
            raise_on_failure=True,
        )

    assert "fact_rows" in str(exc.value)
    assert "freshness" in str(exc.value)


def test_run_metadata_creates_the_ops_schema_and_tables() -> None:
    statements = "\n".join(ensure_schema_statements())

    assert "CREATE SCHEMA IF NOT EXISTS ops" in statements
    assert "CREATE TABLE IF NOT EXISTS ops.pipeline_runs" in statements
    assert "CREATE TABLE IF NOT EXISTS ops.pipeline_alerts" in statements


def test_run_record_serializes_nested_details() -> None:
    record = RunRecord(
        run_id="abc123",
        kind="quality",
        status="success",
        started_at=datetime(2024, 1, 2, tzinfo=UTC),
        finished_at=datetime(2024, 1, 2, 0, 1, tzinfo=UTC),
        read_rows=12,
        written_rows=10,
        quarantined_rows=2,
        deduplicated_rows=0,
        checks_total=7,
        checks_failed=0,
        details={"table": "delta.gold.fct_trips"},
    )

    params = record.as_params()

    assert params["run_id"] == "abc123"
    assert params["read_rows"] == 12
    assert '"table"' in params["details"]
