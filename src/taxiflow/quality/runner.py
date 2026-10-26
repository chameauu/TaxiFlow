"""Run bronze/silver quality suites and cross-layer reconciliation."""

from __future__ import annotations

import json
import os
import re
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from taxiflow.quality.metadata import (
    MetadataError,
    RunRecord,
    record_failure,
    record_run,
)
from taxiflow.quality.reconcile import (
    QualityCheck,
    QualityError,
    Snapshot,
    evaluate_snapshot,
)
from taxiflow.quality.suites import SuiteResult, validate_dataframe

IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


@dataclass(frozen=True)
class QualityConfig:
    """Environment-backed configuration for one quality run."""

    trino_host: str = "localhost"
    trino_port: int = 8084
    trino_user: str = "taxiflow"
    trino_catalog: str = "delta"
    bronze_schema: str = "default"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    max_freshness_seconds: int = 86_400
    sample_limit: int = 50_000

    @classmethod
    def from_env(cls) -> QualityConfig:
        return cls(
            trino_host=os.environ.get("TRINO_HOST", "localhost"),
            trino_port=int(os.environ.get("TRINO_PORT", "8084")),
            trino_user=os.environ.get("TRINO_USER", "taxiflow"),
            trino_catalog=os.environ.get("TRINO_CATALOG", "delta"),
            bronze_schema=os.environ.get("TRINO_BRONZE_SCHEMA", "default"),
            silver_schema=os.environ.get("DBT_SILVER_SCHEMA", "silver"),
            gold_schema=os.environ.get("DBT_GOLD_SCHEMA", "gold"),
            max_freshness_seconds=int(
                os.environ.get("TAXIFLOW_QUALITY_MAX_FRESHNESS_SECONDS", "86400")
            ),
            sample_limit=int(os.environ.get("TAXIFLOW_QUALITY_SAMPLE_LIMIT", "50000")),
        )

    def table(self, schema: str, name: str) -> str:
        for value in (self.trino_catalog, schema, name):
            if not IDENTIFIER.fullmatch(value):
                raise QualityError(f"unsafe Trino identifier: {value!r}")
        return f"{self.trino_catalog}.{schema}.{name}"


@dataclass(frozen=True)
class QualityReport:
    """Complete result of one quality gate."""

    run_id: str
    started_at: datetime
    finished_at: datetime
    snapshot: Snapshot
    checks: tuple[QualityCheck, ...]
    suites: tuple[SuiteResult, ...]

    @property
    def success(self) -> bool:
        return all(check.passed for check in self.checks) and all(
            suite.success for suite in self.suites
        )

    @property
    def failed_checks(self) -> tuple[QualityCheck, ...]:
        return tuple(check for check in self.checks if not check.passed)

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "success": self.success,
            "snapshot": asdict(self.snapshot),
            "checks": [asdict(check) for check in self.checks],
            "suites": [asdict(suite) for suite in self.suites],
        }


SNAPSHOT_SQL = """
SELECT
    (SELECT count(*) FROM {bronze_trips}) AS bronze_rows,
    (SELECT count(DISTINCT trip_id) FROM {bronze_trips}) AS bronze_trip_ids,
    (SELECT coalesce(sum(CAST(total_amount AS double)), 0.0)
       FROM {bronze_trips}) AS bronze_total_amount,
    (SELECT count(*) FROM {silver_trips}) AS silver_rows,
    (SELECT count(DISTINCT trip_id) FROM {silver_trips}) AS silver_trip_ids,
    (SELECT coalesce(sum(CAST(total_amount AS double)), 0.0)
       FROM {silver_trips}) AS silver_total_amount,
    (SELECT count(*) FROM {gold_trips}) AS fact_rows,
    (SELECT count(DISTINCT trip_id) FROM {gold_trips}) AS fact_trip_ids,
    (SELECT coalesce(sum(CAST(total_amount AS double)), 0.0)
       FROM {gold_trips}) AS fact_total_amount,
    (SELECT count(*) FROM {quarantine}) AS quarantine_rows,
    (SELECT coalesce(sum(CAST(quarantined_rows AS bigint)), 0)
       FROM {quality_reasons}) AS quarantine_reason_rows,
    (SELECT date_diff('second', max(ingested_at), current_timestamp)
       FROM {silver_trips}) AS freshness_seconds
"""

BRONZE_SAMPLE_COLUMNS = (
    "trip_id",
    "service",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "total_amount",
    "pickup_year",
    "pickup_month",
    "ingested_at",
)
SILVER_SAMPLE_COLUMNS = (
    "trip_id",
    "service",
    "pickup_at",
    "pickup_location_id",
    "total_amount",
    "pickup_year",
    "ingested_at",
)


def build_snapshot_sql(config: QualityConfig) -> str:
    """Render the validated reconciliation query for one catalog layout."""
    return SNAPSHOT_SQL.format(
        bronze_trips=config.table(config.bronze_schema, "tlc_trips"),
        silver_trips=config.table(config.silver_schema, "stg_yellow_trips"),
        gold_trips=config.table(config.gold_schema, "fct_trips"),
        quarantine=config.table(config.bronze_schema, "tlc_quarantine"),
        quality_reasons=config.table(config.gold_schema, "fct_quality_reasons"),
    )


def connect_trino(config: QualityConfig) -> Any:
    """Open one Trino DB-API connection."""
    import trino

    try:
        return trino.dbapi.connect(
            host=config.trino_host,
            port=config.trino_port,
            user=config.trino_user,
            catalog=config.trino_catalog,
            http_scheme="http",
        )
    except Exception as exc:  # pragma: no cover - exercised against the stack
        raise QualityError(f"cannot connect to Trino: {exc}") from exc


def query_snapshot(connection: Any, config: QualityConfig) -> Snapshot:
    """Read all reconciliation counts and sums in one query."""
    cursor = connection.cursor()
    try:
        cursor.execute(build_snapshot_sql(config))
        row = cursor.fetchone()
        columns = [description[0] for description in cursor.description]
    finally:
        cursor.close()
    if row is None:
        raise QualityError("reconciliation query returned no rows")
    values = dict(zip(columns, row, strict=True))
    return Snapshot(
        bronze_rows=int(values["bronze_rows"] or 0),
        silver_rows=int(values["silver_rows"] or 0),
        fact_rows=int(values["fact_rows"] or 0),
        quarantine_rows=int(values["quarantine_rows"] or 0),
        quarantine_reason_rows=int(values["quarantine_reason_rows"] or 0),
        bronze_trip_ids=int(values["bronze_trip_ids"] or 0),
        silver_trip_ids=int(values["silver_trip_ids"] or 0),
        fact_trip_ids=int(values["fact_trip_ids"] or 0),
        bronze_total_amount=float(values["bronze_total_amount"] or 0.0),
        silver_total_amount=float(values["silver_total_amount"] or 0.0),
        fact_total_amount=float(values["fact_total_amount"] or 0.0),
        freshness_seconds=(
            int(values["freshness_seconds"])
            if values["freshness_seconds"] is not None
            else None
        ),
    )


def fetch_sample(
    connection: Any,
    table: str,
    columns: tuple[str, ...],
    limit: int,
) -> Any:
    """Fetch a bounded sample for GX while keeping an explicit column list."""
    import pandas as pd

    projected = ", ".join(columns)
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT {projected} FROM {table} LIMIT {int(limit)}")
        rows = cursor.fetchall()
        names = [description[0] for description in cursor.description]
    finally:
        cursor.close()
    return pd.DataFrame(rows, columns=names)


def run_suites(connection: Any, config: QualityConfig) -> tuple[SuiteResult, ...]:
    """Run both committed GX suites against bounded table samples."""
    bronze = fetch_sample(
        connection,
        config.table(config.bronze_schema, "tlc_trips"),
        BRONZE_SAMPLE_COLUMNS,
        config.sample_limit,
    )
    silver = fetch_sample(
        connection,
        config.table(config.silver_schema, "stg_yellow_trips"),
        SILVER_SAMPLE_COLUMNS,
        config.sample_limit,
    )
    return (
        validate_dataframe(bronze, "bronze_tlc_trips"),
        validate_dataframe(silver, "silver_stg_yellow_trips"),
    )


def suite_checks(suites: tuple[SuiteResult, ...]) -> tuple[QualityCheck, ...]:
    """Turn GX suite results into the same check shape as SQL reconciliation."""
    return tuple(
        QualityCheck(
            name=f"gx_{suite.name}",
            passed=suite.success,
            expected="all expectations pass",
            actual={
                "evaluated": suite.evaluated,
                "failed": list(suite.failed),
            },
            detail=", ".join(suite.failed),
        )
        for suite in suites
    )


def run_quality(
    config: QualityConfig | None = None,
    *,
    record: bool = True,
) -> QualityReport:
    """Execute the quality gate and optionally persist run metadata."""
    effective = config or QualityConfig.from_env()
    started_at = datetime.now(UTC)
    run_id = uuid.uuid4().hex
    try:
        connection = connect_trino(effective)
        try:
            snapshot = query_snapshot(connection, effective)
            suites = run_suites(connection, effective)
        finally:
            connection.close()
        checks = evaluate_snapshot(
            snapshot,
            max_freshness_seconds=effective.max_freshness_seconds,
        ) + suite_checks(suites)
        report = QualityReport(
            run_id=run_id,
            started_at=started_at,
            finished_at=datetime.now(UTC),
            snapshot=snapshot,
            checks=checks,
            suites=suites,
        )
        if record:
            record_run(_record_from_report(report))
        return report
    except Exception as exc:
        if record:
            try:
                record_failure(
                    kind="quality",
                    run_id=run_id,
                    message=f"quality gate failed: {exc}",
                )
            except MetadataError as metadata_exc:
                print(f"warning: could not record quality failure: {metadata_exc}", file=sys.stderr)
        if isinstance(exc, QualityError):
            raise
        raise QualityError(f"quality gate failed: {exc}") from exc


def _record_from_report(report: QualityReport) -> RunRecord:
    failed = [check.name for check in report.checks if not check.passed]
    failed += [
        suite.name for suite in report.suites if not suite.success
    ]
    return RunRecord(
        run_id=report.run_id,
        kind="quality",
        status="success" if report.success else "failed",
        started_at=report.started_at,
        finished_at=report.finished_at,
        read_rows=report.snapshot.bronze_rows + report.snapshot.quarantine_rows,
        written_rows=report.snapshot.bronze_rows,
        quarantined_rows=report.snapshot.quarantine_rows,
        deduplicated_rows=report.snapshot.bronze_rows
        - report.snapshot.bronze_trip_ids,
        checks_total=len(report.checks),
        checks_failed=len(failed),
        details={"failed_checks": failed, "report": report.as_dict()},
    )


def print_report(report: QualityReport) -> None:
    """Write a stable machine-readable report to stdout."""
    print(json.dumps(report.as_dict(), indent=2, sort_keys=True))
