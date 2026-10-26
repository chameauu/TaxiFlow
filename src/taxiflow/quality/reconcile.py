"""Layer-to-layer reconciliation checks for TaxiFlow quality gates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class QualityError(RuntimeError):
    """Raised when one or more quality checks do not pass."""


@dataclass(frozen=True)
class Snapshot:
    """One consistent set of counts and sums across bronze, silver, and gold."""

    bronze_rows: int
    silver_rows: int
    fact_rows: int
    quarantine_rows: int
    quarantine_reason_rows: int
    bronze_trip_ids: int
    silver_trip_ids: int
    fact_trip_ids: int
    bronze_total_amount: float
    silver_total_amount: float
    fact_total_amount: float
    freshness_seconds: int | None


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
            "silver staging must preserve every accepted bronze row",
        ),
        _check(
            "fact_rows",
            snapshot.fact_rows == snapshot.bronze_rows,
            snapshot.bronze_rows,
            snapshot.fact_rows,
            "gold fact must preserve every accepted bronze row",
        ),
        _check(
            "trip_ids",
            (
                snapshot.bronze_trip_ids == snapshot.bronze_rows
                and snapshot.silver_trip_ids == snapshot.bronze_rows
                and snapshot.fact_trip_ids == snapshot.bronze_rows
            ),
            snapshot.bronze_rows,
            {
                "bronze": snapshot.bronze_trip_ids,
                "silver": snapshot.silver_trip_ids,
                "gold": snapshot.fact_trip_ids,
            },
            "every accepted trip must have one stable trip_id in every layer",
        ),
        _check(
            "total_amount",
            _amounts_match(
                snapshot.bronze_total_amount,
                snapshot.silver_total_amount,
                snapshot.fact_total_amount,
            ),
            "equal within 0.01",
            {
                "bronze": snapshot.bronze_total_amount,
                "silver": snapshot.silver_total_amount,
                "gold": snapshot.fact_total_amount,
            },
            "money must reconcile exactly (within one cent) across layers",
        ),
        _check(
            "quarantine_rows",
            snapshot.quarantine_rows == snapshot.quarantine_reason_rows,
            snapshot.quarantine_rows,
            snapshot.quarantine_reason_rows,
            "quarantine counts must equal the sum of reason groups",
        ),
        _check(
            "freshness",
            (
                snapshot.freshness_seconds is not None
                and 0 <= snapshot.freshness_seconds <= max_freshness_seconds
            ),
            f"<= {max_freshness_seconds}s",
            snapshot.freshness_seconds,
            "silver ingest time must be recent enough for the batch gate",
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
