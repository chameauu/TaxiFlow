"""Read back the CDC bronze table and expose freshness."""

from __future__ import annotations

import argparse
import json
import os
import sys

from taxiflow.jobs.stream_cdc_bronze import build_spark
from taxiflow.streaming.contract import cdc_table_path
from taxiflow.streaming.metrics import collect_metrics


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse report arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=os.environ.get("S3_BUCKET", "taxiflow"))
    parser.add_argument("--table-path")
    parser.add_argument("--max-freshness-seconds", type=float)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Print CDC row, latest-state, and max(ingested_at) metrics."""
    args = parse_args(argv)
    table_path = args.table_path or cdc_table_path(args.bucket)
    spark = build_spark("taxiflow-cdc-report")
    try:
        metrics = collect_metrics(spark, table_path)
    finally:
        spark.stop()
    print("TAXIFLOW_CDC_METRICS=" + json.dumps(metrics, sort_keys=True))
    if args.max_freshness_seconds is not None:
        freshness = metrics["freshness_seconds"]
        if freshness is None or freshness > args.max_freshness_seconds:
            print(
                f"error: freshness {freshness}s exceeds {args.max_freshness_seconds}s",
                file=sys.stderr,
            )
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
