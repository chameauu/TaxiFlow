"""Read back Delta bronze tables and print reconciliation metrics as JSON."""

from __future__ import annotations

import argparse
import json
import os

from taxiflow.jobs.batch_bronze import build_spark


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse report arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=os.environ.get("S3_BUCKET", "taxiflow"))
    parser.add_argument("--table-path")
    parser.add_argument("--quarantine-path")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Read both tables and print stable metrics for replay comparisons."""
    from pyspark.sql import functions as F

    args = parse_args(argv)
    table_path = args.table_path or f"s3a://{args.bucket}/bronze/tlc_trips"
    quarantine_path = (
        args.quarantine_path or f"s3a://{args.bucket}/bronze/tlc_quarantine"
    )

    spark = build_spark("taxiflow-bronze-report")
    try:
        bronze = spark.read.format("delta").load(table_path)
        quarantine = spark.read.format("delta").load(quarantine_path)
        row = bronze.agg(
            F.count("*").alias("row_count"),
            F.countDistinct("trip_id").alias("trip_id_count"),
            F.countDistinct("content_hash").alias("content_hash_count"),
            F.sum("total_amount").alias("total_amount_sum"),
            F.sum("fare_amount").alias("fare_amount_sum"),
            F.sum("trip_distance").alias("trip_distance_sum"),
        ).first()
        metrics = {
            "row_count": int(row["row_count"]),
            "trip_id_count": int(row["trip_id_count"]),
            "content_hash_count": int(row["content_hash_count"]),
            "total_amount_sum": float(row["total_amount_sum"] or 0.0),
            "fare_amount_sum": float(row["fare_amount_sum"] or 0.0),
            "trip_distance_sum": float(row["trip_distance_sum"] or 0.0),
            "quarantine_count": quarantine.count(),
            "table_path": table_path,
            "quarantine_path": quarantine_path,
        }
    finally:
        spark.stop()

    print("TAXIFLOW_METRICS=" + json.dumps(metrics, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
