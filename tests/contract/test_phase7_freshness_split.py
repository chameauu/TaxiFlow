"""Static contract for the split freshness metrics (Phase 7 task 5).

The batch pipeline and the CDC stream have different clocks and different
budgets. Reporting one number under both labels hid that the streaming SLA was
never measured. These tests pin one metric per clock.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from taxiflow.__main__ import build_parser

pytestmark = pytest.mark.contract

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUNNER = PROJECT_ROOT / "src" / "taxiflow" / "quality" / "runner.py"
RECONCILE = PROJECT_ROOT / "src" / "taxiflow" / "quality" / "reconcile.py"
SUMMARY = PROJECT_ROOT / "dbt" / "models" / "marts" / "quality_summary.sql"
MAKEFILE = PROJECT_ROOT / "Makefile"
ENV_EXAMPLE = PROJECT_ROOT / ".env.example"


def test_runner_reports_both_clocks() -> None:
    text = RUNNER.read_text()
    assert "AS batch_data_age_seconds" in text
    assert "AS cdc_lag_seconds" in text
    # The CDC lag must be measured from event time, not ingest time.
    assert "from_unixtime" in text


def test_reconcile_enforces_the_cdc_budget_only_when_events_exist() -> None:
    text = RECONCILE.read_text()
    assert '"cdc_lag"' in text
    assert "cdc_lag_seconds is None" in text


def test_gold_summary_exposes_both_metrics() -> None:
    text = SUMMARY.read_text()
    assert "as batch_data_age_seconds" in text
    assert "as cdc_lag_seconds" in text


def test_quality_command_accepts_the_cdc_budget() -> None:
    args = build_parser().parse_args(
        ["quality", "run", "--cdc-max-lag-seconds", "45", "--no-record"]
    )
    assert args.cdc_max_lag_seconds == 45
    assert "TAXIFLOW_CDC_MAX_LAG_SECONDS" in MAKEFILE.read_text()
    assert "TAXIFLOW_CDC_MAX_LAG_SECONDS" in ENV_EXAMPLE.read_text()
