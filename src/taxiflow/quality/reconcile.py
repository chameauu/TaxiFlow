"""Layer-to-layer reconciliation checks for TaxiFlow quality gates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class QualityError(RuntimeError):
    """Raised when one or more quality checks do not pass."""


@dataclass(frozen=True)
class Snapshot:
    """One consistent set of counts and sums across bronze, silver, and gold.

    Batch and CDC are reconciled separately. Batch is static, so gold must
    equal bronze exactly. The CDC stream keeps appending while the runner
    reads, so every CDC metric is frozen at the gold fact's own event-time
    window (the max cdc source_ts_ms present in fct_trips): the check is that
    the frozen window is fully present in gold with matching amounts, which
    catches dropped, duplicated, or mis-cast rows without racing the stream.
    Trips whose latest event lands after the last dbt build are invisible
    here by design; they join on the next run.
    """

    bronze_rows: int
    silver_rows: int
    bronze_trip_ids: int
    silver_trip_ids: int
    bronze_total_amount: float
    silver_total_amount: float
    fact_batch_rows: int
    fact_batch_trip_ids: int
    fact_batch_total_amount: float
    cdc_rows: int
    cdc_trip_ids: int
    cdc_total_amount: float
    cdc_bronze_live_rows: int
    fact_cdc_window_rows: int
    fact_cdc_window_total_amount: float
    quarantine_rows: int
    quarantine_reason_rows: int
    batch_data_age_seconds: int | None
    cdc_lag_seconds: int | None


@dataclass(frozen=True)
class QualityCheck:
    """One pass/fail assertion with enough context to debug a failed run."""

    name: str
    passed: bool
    expected: Any
    actual: Any
    detail: str = ""


def _check(
    name: str,
    passed: bool,
    expected: Any,
    actual: Any,
    detail: str = "",
) -> QualityCheck:
    return QualityCheck(
        name=name,
        passed=passed,
        expected=expected,
        actual=actual,
        detail=detail,
    )


def evaluate_snapshot(
    snapshot: Snapshot,
    *,
    max_freshness_seconds: int = 86_400,
    max_cdc_lag_seconds: int = 60,
    raise_on_failure: bool = False,
) -> tuple[QualityCheck, ...]:
    """Evaluate every cross-layer invariant and optionally raise a summary."""
    checks = (
        _check(
            "bronze_rows",
            snapshot.bronze_rows > 0,
            "> 0",
            snapshot.bronze_rows,
            "bronze must contain at least one accepted trip",
        ),
        _check(
            "silver_rows",
            snapshot.silver_rows == snapshot.bronze_rows,
            snapshot.bronze_rows,
            snapshot.silver_rows,
            "batch staging must preserve every accepted batch bronze row",
        ),
        _check(
            "cdc_rows",
            snapshot.cdc_rows == snapshot.cdc_bronze_live_rows,
            snapshot.cdc_bronze_live_rows,
            snapshot.cdc_rows,
            "CDC staging must equal the live state derived from CDC bronze "
            "(latest event per trip_id, deletes removed)",
        ),
        _check(
            "fact_batch_rows",
            snapshot.fact_batch_rows == snapshot.bronze_rows,
            snapshot.bronze_rows,
            snapshot.fact_batch_rows,
            "gold fact must preserve every accepted batch trip",
        ),
        _check(
            "trip_ids",
            (
                snapshot.bronze_trip_ids == snapshot.bronze_rows
                and snapshot.silver_trip_ids == snapshot.bronze_rows
                and snapshot.fact_batch_trip_ids == snapshot.bronze_rows
                and snapshot.cdc_trip_ids == snapshot.cdc_rows
                and snapshot.fact_cdc_window_rows == snapshot.cdc_rows
            ),
            {
                "bronze": snapshot.bronze_rows,
                "silver": snapshot.bronze_rows,
                "fact_batch": snapshot.bronze_rows,
                "cdc_window": snapshot.cdc_rows,
                "fact_cdc_window": snapshot.cdc_rows,
            },
            {
                "bronze": snapshot.bronze_trip_ids,
                "silver": snapshot.silver_trip_ids,
                "fact_batch": snapshot.fact_batch_trip_ids,
                "cdc_window": snapshot.cdc_trip_ids,
                "fact_cdc_window": snapshot.fact_cdc_window_rows,
            },
            "every accepted trip must have one stable trip_id in every layer",
        ),
        _check(
            "total_amount",
            (
                _amounts_match(
                    snapshot.bronze_total_amount, snapshot.silver_total_amount
                )
                and _amounts_match(
                    snapshot.fact_batch_total_amount,
                    snapshot.bronze_total_amount,
                )
                and _amounts_match(
                    snapshot.fact_cdc_window_total_amount,
                    snapshot.cdc_total_amount,
                )
            ),
            "equal within 0.01",
            {
                "bronze": snapshot.bronze_total_amount,
                "silver": snapshot.silver_total_amount,
                "fact_batch": snapshot.fact_batch_total_amount,
                "cdc_window": snapshot.cdc_total_amount,
                "fact_cdc_window": snapshot.fact_cdc_window_total_amount,
            },
            "money must reconcile exactly (within one cent) across layers "
            "and across both sources",
        ),
        _check(
            "quarantine_rows",
            snapshot.quarantine_rows == snapshot.quarantine_reason_rows,
            snapshot.quarantine_rows,
            snapshot.quarantine_reason_rows,
            "quarantine counts must equal the sum of reason groups",
        ),
        _check(
            "batch_data_age",
            (
                snapshot.batch_data_age_seconds is not None
                and 0 <= snapshot.batch_data_age_seconds <= max_freshness_seconds
            ),
            f"<= {max_freshness_seconds}s",
            snapshot.batch_data_age_seconds,
            "newest batch ingest time must be recent enough for the batch gate",
        ),
        _check(
            "cdc_lag",
            (
                snapshot.cdc_lag_seconds is None
                or 0 <= snapshot.cdc_lag_seconds <= max_cdc_lag_seconds
            ),
            f"<= {max_cdc_lag_seconds}s (skipped when no CDC rows exist)",
            snapshot.cdc_lag_seconds,
            "CDC event time must stay inside the streaming freshness budget",
        ),
    )
    if raise_on_failure:
        failed = [check for check in checks if not check.passed]
        if failed:
            summary = "; ".join(
                f"{check.name}: expected {check.expected}, actual {check.actual}"
                for check in failed
            )
            raise QualityError(f"quality checks failed: {summary}")
    return checks


def _amounts_match(*amounts: float, tolerance: float = 0.01) -> bool:
    if not amounts:
        return True
    first = amounts[0]
    return all(abs(value - first) <= tolerance for value in amounts[1:])
