"""Readback metrics for the CDC bronze table."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def collect_metrics(spark: Any, table_path: str) -> dict[str, Any]:
    """Return row, latest-state, and freshness metrics for a Delta CDC table."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    # Read one committed snapshot: the streaming writer may commit between
    # separate actions, which would make row_count and distinct_keys disagree.
    version = DeltaTable.forPath(spark, table_path).history(1).select("version").first()[0]
    table = (
        spark.read.format("delta")
        .option("versionAsOf", version)
        .load(table_path)
    )
    row_count = table.count()
    distinct_keys = table.select("trip_id").distinct().count()
    deleted_rows = table.filter(F.col("is_deleted")).count()
    aggregate = table.agg(
        F.max("ingested_at").alias("max_ingested_at"),
        F.max("event_date").alias("max_event_date"),
        F.max("__source_ts_ms").alias("max_source_ts_ms"),
    ).first()
    latest = aggregate["max_ingested_at"]
    if latest is not None and latest.tzinfo is None:
        latest = latest.replace(tzinfo=timezone.utc)  # noqa: UP017
    now = datetime.now(timezone.utc)  # noqa: UP017
    freshness = None if latest is None else max(0.0, (now - latest).total_seconds())
    return {
        "row_count": row_count,
        "distinct_trip_ids": distinct_keys,
        "duplicate_rows": row_count - distinct_keys,
        "deleted_rows": deleted_rows,
        "max_ingested_at": latest.isoformat() if latest else None,
        "max_event_date": str(aggregate["max_event_date"]),
        "max_source_ts_ms": aggregate["max_source_ts_ms"],
        "freshness_seconds": None if freshness is None else round(freshness, 3),
        "table_path": table_path,
        "snapshot_version": version,
        "measured_at": now.isoformat(),
    }
