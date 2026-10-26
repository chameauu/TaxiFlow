"""Great Expectations suites and DataFrame validation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SUITE_ROOT = Path(__file__).resolve().parent / "suites"
SUITE_FILES = {
    "bronze_tlc_trips": SUITE_ROOT / "bronze_tlc_trips.json",
    "silver_stg_yellow_trips": SUITE_ROOT / "silver_stg_yellow_trips.json",
}


class SuiteError(RuntimeError):
    """Raised when a suite is missing or cannot be loaded."""


@dataclass(frozen=True)
class SuiteResult:
    """Compact result of one Great Expectations suite run."""

    name: str
    success: bool
    evaluated: int
    successful: int
    failed: tuple[str, ...]
    statistics: dict[str, Any]


def load_suite(name: str) -> Any:
    """Load a committed GX suite by its stable short name."""
    from great_expectations.core import ExpectationConfiguration, ExpectationSuite

    path = SUITE_FILES.get(name)
    if path is None:
        raise SuiteError(f"unknown suite: {name}")
    try:
        payload = json.loads(path.read_text())
    except FileNotFoundError as exc:
        raise SuiteError(f"missing suite file: {path}") from exc
    except json.JSONDecodeError as exc:
        raise SuiteError(f"invalid suite JSON: {path}: {exc}") from exc

    suite = ExpectationSuite(expectation_suite_name=payload["expectation_suite_name"])
    for expectation in payload.get("expectations", []):
        suite.add_expectation(ExpectationConfiguration(**expectation))
    return suite


def validate_dataframe(dataframe: Any, name: str) -> SuiteResult:
    """Run one GX suite against a pandas DataFrame sample."""
    from great_expectations.dataset import PandasDataset

    suite = load_suite(name)
    dataset = PandasDataset(dataframe, profiler=None)
    dataset._expectation_suite = suite
    result = dataset.validate(result_format="SUMMARY")
    failures = tuple(
        f"{item.expectation_config.kwargs.get('column', 'table')}:"
        f"{item.expectation_config.expectation_type}"
        for item in result.results
        if not item.success
    )
    statistics = dict(result.statistics)
    return SuiteResult(
        name=suite.expectation_suite_name,
        success=bool(result.success),
        evaluated=int(statistics.get("evaluated_expectations", 0)),
        successful=int(statistics.get("successful_expectations", 0)),
        failed=failures,
        statistics=statistics,
    )
