"""Replay-safe run metadata and failure alerts for TaxiFlow."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4


class MetadataError(RuntimeError):
    """Raised when run metadata cannot be read or written."""


def ensure_schema_statements() -> tuple[str, ...]:
    """Return the idempotent SQL that creates the operations schema."""
    return (
        "CREATE SCHEMA IF NOT EXISTS ops",
        """
        CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
            run_id text PRIMARY KEY,
            kind text NOT NULL,
            status text NOT NULL,
            started_at timestamptz NOT NULL,
            finished_at timestamptz,
            read_rows bigint,
            written_rows bigint,
            quarantined_rows bigint,
            deduplicated_rows bigint,
            checks_total integer,
            checks_failed integer,
            details jsonb NOT NULL DEFAULT '{}'::jsonb,
            airflow_run_id text
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ops.pipeline_alerts (
            alert_id bigserial PRIMARY KEY,
            run_id text,
            kind text NOT NULL,
            task_id text,
            severity text NOT NULL DEFAULT 'error',
            message text NOT NULL,
            details jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now()
        )
        """,
        "CREATE INDEX IF NOT EXISTS pipeline_runs_started_at_idx "
        "ON ops.pipeline_runs (started_at DESC)",
        "CREATE INDEX IF NOT EXISTS pipeline_alerts_created_at_idx "
        "ON ops.pipeline_alerts (created_at DESC)",
    )


@dataclass(frozen=True)
class RunRecord:
    """One pipeline execution, including the counts needed for reconciliation."""

    run_id: str
    kind: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    read_rows: int | None = None
    written_rows: int | None = None
    quarantined_rows: int | None = None
    deduplicated_rows: int | None = None
    checks_total: int | None = None
    checks_failed: int | None = None
    details: dict[str, Any] = field(default_factory=dict)
    airflow_run_id: str | None = None

    def as_params(self) -> dict[str, Any]:
        """Serialize the record for psycopg named parameters."""
        return {
            "run_id": self.run_id,
            "kind": self.kind,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "read_rows": self.read_rows,
            "written_rows": self.written_rows,
            "quarantined_rows": self.quarantined_rows,
            "deduplicated_rows": self.deduplicated_rows,
            "checks_total": self.checks_total,
            "checks_failed": self.checks_failed,
            "details": json.dumps(self.details, sort_keys=True, default=str),
            "airflow_run_id": self.airflow_run_id,
        }


def _postgres_settings() -> dict[str, Any]:
    return {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "dbname": os.environ.get("POSTGRES_DB", "taxiflow"),
        "user": os.environ.get("POSTGRES_USER", "taxiflow"),
        "password": os.environ.get("POSTGRES_PASSWORD", ""),
    }


def connect_postgres() -> Any:
    """Open one Postgres connection from environment configuration."""
    import psycopg

    try:
        return psycopg.connect(**_postgres_settings(), connect_timeout=10)
    except Exception as exc:  # pragma: no cover - exercised against the stack
        raise MetadataError(f"cannot connect to Postgres: {exc}") from exc


def ensure_schema(connection: Any) -> None:
    """Create the ops schema and tables if they do not exist."""
    with connection.cursor() as cursor:
        for statement in ensure_schema_statements():
            cursor.execute(statement)
    connection.commit()


def insert_run(connection: Any, record: RunRecord) -> None:
    """Insert or update one run by its stable run id."""
    sql = """
        INSERT INTO ops.pipeline_runs (
            run_id, kind, status, started_at, finished_at, read_rows,
            written_rows, quarantined_rows, deduplicated_rows, checks_total,
            checks_failed, details, airflow_run_id
        ) VALUES (
            %(run_id)s, %(kind)s, %(status)s, %(started_at)s, %(finished_at)s,
            %(read_rows)s, %(written_rows)s, %(quarantined_rows)s,
            %(deduplicated_rows)s, %(checks_total)s, %(checks_failed)s,
            %(details)s, %(airflow_run_id)s
        )
        ON CONFLICT (run_id) DO UPDATE SET
            status = EXCLUDED.status,
            finished_at = EXCLUDED.finished_at,
            read_rows = EXCLUDED.read_rows,
            written_rows = EXCLUDED.written_rows,
            quarantined_rows = EXCLUDED.quarantined_rows,
            deduplicated_rows = EXCLUDED.deduplicated_rows,
            checks_total = EXCLUDED.checks_total,
            checks_failed = EXCLUDED.checks_failed,
            details = EXCLUDED.details,
            airflow_run_id = EXCLUDED.airflow_run_id
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, record.as_params())
    connection.commit()


def insert_alert(
    connection: Any,
    *,
    kind: str,
    message: str,
    run_id: str | None = None,
    task_id: str | None = None,
    severity: str = "error",
    details: dict[str, Any] | None = None,
) -> None:
    """Persist one failure alert for the dashboard and operators."""
    sql = """
        INSERT INTO ops.pipeline_alerts (
            run_id, kind, task_id, severity, message, details
        ) VALUES (%(run_id)s, %(kind)s, %(task_id)s, %(severity)s,
                  %(message)s, %(details)s)
    """
    params = {
        "run_id": run_id,
        "kind": kind,
        "task_id": task_id,
        "severity": severity,
        "message": message,
        "details": json.dumps(details or {}, sort_keys=True, default=str),
    }
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
    connection.commit()


def record_run(record: RunRecord) -> None:
    """Record one run in Postgres, creating the schema on first use."""
    connection = connect_postgres()
    try:
        ensure_schema(connection)
        insert_run(connection, record)
    finally:
        connection.close()


def record_payload(
    *,
    kind: str,
    payload: dict[str, Any],
    status: str = "success",
    airflow_run_id: str | None = None,
) -> RunRecord:
    """Normalize a batch/quality JSON payload into a RunRecord."""
    now = datetime.now(UTC)
    started_at = _parse_timestamp(payload.get("started_at") or payload.get("ingested_at"))
    record = RunRecord(
        run_id=str(payload.get("run_id") or uuid4().hex),
        kind=kind,
        status=status,
        started_at=started_at or now,
        finished_at=_parse_timestamp(payload.get("finished_at")) or now,
        read_rows=_as_int(payload.get("read_rows")),
        written_rows=_as_int(payload.get("written_rows")),
        quarantined_rows=_as_int(payload.get("quarantined_rows")),
        deduplicated_rows=_as_int(payload.get("deduplicated_rows")),
        checks_total=_as_int(payload.get("checks_total")),
        checks_failed=_as_int(payload.get("checks_failed")),
        details=payload,
        airflow_run_id=airflow_run_id,
    )
    record_run(record)
    return record


def record_failure(
    *,
    kind: str,
    message: str,
    task_id: str | None = None,
    run_id: str | None = None,
    airflow_run_id: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    """Record a failed run and a matching alert without hiding the original error."""
    now = datetime.now(UTC)
    failure_id = run_id or uuid4().hex
    record = RunRecord(
        run_id=failure_id,
        kind=kind,
        status="failed",
        started_at=now,
        finished_at=now,
        details={"message": message, **(details or {})},
        airflow_run_id=airflow_run_id,
    )
    connection = connect_postgres()
    try:
        ensure_schema(connection)
        insert_run(connection, record)
        insert_alert(
            connection,
            run_id=failure_id,
            kind=kind,
            task_id=task_id,
            message=message,
            details=details,
        )
    finally:
        connection.close()


def parse_metrics_line(output: str) -> dict[str, Any]:
    """Extract the batch job's machine-readable metrics line."""
    for line in reversed(output.splitlines()):
        if line.startswith("TAXIFLOW_METRICS="):
            payload = line.removeprefix("TAXIFLOW_METRICS=")
            data = json.loads(payload)
            if not isinstance(data, dict):
                raise MetadataError("TAXIFLOW_METRICS payload must be an object")
            return data
    raise MetadataError("batch output did not contain TAXIFLOW_METRICS=")


def list_runs(limit: int = 10) -> list[dict[str, Any]]:
    """Return the most recent runs for local inspection."""
    connection = connect_postgres()
    try:
        ensure_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT run_id, kind, status, started_at, finished_at, read_rows,
                       written_rows, quarantined_rows, deduplicated_rows,
                       checks_total, checks_failed, details
                FROM ops.pipeline_runs
                ORDER BY started_at DESC
                LIMIT %(limit)s
                """,
                {"limit": limit},
            )
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
    finally:
        connection.close()


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    raise MetadataError(f"unsupported timestamp: {value!r}")


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)
