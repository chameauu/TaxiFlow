"""Replay a deterministic CDC batch and verify MERGE idempotency.

This script runs inside the Spark container. It is intentionally not named
``test_*`` so pytest does not collect it; ``test_cdc_stream_stack.py`` invokes it
through Docker Compose.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from typing import Any

from taxiflow.jobs.stream_cdc_bronze import _merge_batch, build_spark
from taxiflow.streaming.metrics import collect_metrics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table-path", required=True)
    return parser.parse_args()


def delete_path(spark: Any, path: str) -> None:
    """Remove the test table so the replay starts from a known state."""
    jvm = spark._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    filesystem = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    if filesystem.exists(hadoop_path):
        filesystem.delete(hadoop_path, True)


def event_schema() -> Any:
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("trip_id", StringType(), False),
            StructField("vendor_id", IntegerType(), True),
            StructField("pickup_datetime", TimestampType(), True),
            StructField("dropoff_datetime", TimestampType(), True),
            StructField("passenger_count", IntegerType(), True),
            StructField("trip_distance", StringType(), True),
            StructField("fare_amount", StringType(), True),
            StructField("tip_amount", StringType(), True),
            StructField("total_amount", StringType(), True),
            StructField("payment_type", IntegerType(), True),
            StructField("pickup_zone_id", IntegerType(), True),
            StructField("dropoff_zone_id", IntegerType(), True),
            StructField("service", StringType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("__op", StringType(), True),
            StructField("__source_ts_ms", LongType(), True),
            StructField("__deleted", StringType(), True),
            StructField("event_date", DateType(), True),
            StructField("ingested_at", TimestampType(), True),
            StructField("is_deleted", BooleanType(), True),
        ]
    )


def event(
    trip_id: str,
    fare_amount: str,
    source_ts_ms: int,
    *,
    deleted: bool = False,
) -> tuple[Any, ...]:
    """Build one row in the exact shape emitted by the streaming job."""
    return (
        trip_id,
        1,
        datetime(2026, 10, 24, 12, 0),
        datetime(2026, 10, 24, 12, 20),
        1,
        "1.25",
        fare_amount,
        "1.00",
        fare_amount,
        1,
        100,
        200,
        "yellow",
        datetime(2026, 10, 24, 12, 30),
        "d" if deleted else "c",
        source_ts_ms,
        "true" if deleted else "false",
        date(2026, 10, 24),
        datetime(2026, 10, 24, 20, 0),
        deleted,
    )


def latest_state(spark: Any, table_path: str) -> dict[str, dict[str, Any]]:
    rows = spark.read.format("delta").load(table_path).orderBy("trip_id").collect()
    return {row["trip_id"]: row.asDict(recursive=True) for row in rows}


def main() -> int:
    args = parse_args()
    spark = build_spark("taxiflow-cdc-replay")
    try:
        delete_path(spark, args.table_path)
        first = spark.createDataFrame(
            [
                event("trip-1", "10.00", 1_000),
                event("trip-1", "9.00", 900),
                event("trip-2", "20.00", 2_000),
            ],
            event_schema(),
        )
        _merge_batch(first, 0, spark, args.table_path)
        metrics_first = collect_metrics(spark, args.table_path)

        _merge_batch(first, 0, spark, args.table_path)
        metrics_replay = collect_metrics(spark, args.table_path)

        update = spark.createDataFrame(
            [
                event("trip-1", "99.00", 500),
                event("trip-2", "25.00", 3_000),
            ],
            event_schema(),
        )
        _merge_batch(update, 1, spark, args.table_path)
        metrics_update = collect_metrics(spark, args.table_path)

        deletion = spark.createDataFrame(
            [event("trip-2", "25.00", 4_000, deleted=True)],
            event_schema(),
        )
        _merge_batch(deletion, 2, spark, args.table_path)
        metrics_delete = collect_metrics(spark, args.table_path)
        state = latest_state(spark, args.table_path)

        expected = {
            "first": (2, 2, 0),
            "replay": (2, 2, 0),
            "update": (2, 2, 0),
            "delete": (2, 2, 0),
        }
        observed = {
            "first": (
                metrics_first["row_count"],
                metrics_first["distinct_trip_ids"],
                metrics_first["duplicate_rows"],
            ),
            "replay": (
                metrics_replay["row_count"],
                metrics_replay["distinct_trip_ids"],
                metrics_replay["duplicate_rows"],
            ),
            "update": (
                metrics_update["row_count"],
                metrics_update["distinct_trip_ids"],
                metrics_update["duplicate_rows"],
            ),
            "delete": (
                metrics_delete["row_count"],
                metrics_delete["distinct_trip_ids"],
                metrics_delete["duplicate_rows"],
            ),
        }
        for name, expected_counts in expected.items():
            if observed[name] != expected_counts:
                raise AssertionError(f"{name}: expected {expected_counts}, got {observed[name]}")
        if state["trip-1"]["fare_amount"] != "10.00":
            raise AssertionError("an older source event overwrote trip-1")
        if state["trip-2"]["fare_amount"] != "25.00":
            raise AssertionError("the newer source event was not applied to trip-2")
        if state["trip-2"]["is_deleted"] is not True:
            raise AssertionError("the delete event did not become a tombstone")
        if metrics_delete["deleted_rows"] != 1:
            raise AssertionError("the delete tombstone was not counted")

        print(
            "TAXIFLOW_CDC_REPLAY="
            + json.dumps(
                {
                    "first": metrics_first,
                    "replay": metrics_replay,
                    "update": metrics_update,
                    "delete": metrics_delete,
                    "trip_1_fare": state["trip-1"]["fare_amount"],
                    "trip_2_fare": state["trip-2"]["fare_amount"],
                    "trip_2_deleted": state["trip-2"]["is_deleted"],
                },
                default=str,
                sort_keys=True,
            )
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
