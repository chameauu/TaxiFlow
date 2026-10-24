"""Idempotent Spark batch ingestion from TLC Parquet into Delta bronze."""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from taxiflow.tlc.manifest import TlcFile, load_manifest, select_file, verify_file
from taxiflow.tlc.schema import TLC_SCHEMAS, ColumnPlan, plan_columns
from taxiflow.tlc.transform import NULL_TOKEN, SEP, period_bounds

PARTITION_COLUMNS = ("service", "pickup_year", "pickup_month")
SPARK_SESSION_TIMEZONE = "UTC"
BRONZE_PROVENANCE_COLUMNS = (
    "service",
    "source_file",
    "source_row_number",
    "ingested_at",
    "content_hash",
    "trip_id",
    "pickup_year",
    "pickup_month",
)
QUARANTINE_COLUMNS = (
    "quarantine_id",
    "service",
    "source_file",
    "source_row_number",
    "quarantine_reason",
    "pickup_datetime",
    "raw_record",
    "ingested_at",
)


class BatchError(RuntimeError):
    """Raised when a batch run cannot satisfy its contract."""


def build_replace_where(entry: TlcFile) -> str:
    """Return the deterministic partition predicate for one source month."""
    return (
        f"service = '{entry.service}' "
        f"AND pickup_year = {entry.year} "
        f"AND pickup_month = {entry.month}"
    )


def bronze_column_names(plan: ColumnPlan) -> tuple[str, ...]:
    """Return the exact bronze column order for a source schema."""
    return plan.schema.column_names + BRONZE_PROVENANCE_COLUMNS


def _spark_type(name: str) -> Any:
    from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, TimestampType

    mapping = {
        "int": IntegerType(),
        "long": LongType(),
        "double": DoubleType(),
        "string": StringType(),
        "timestamp": TimestampType(),
    }
    try:
        return mapping[name]
    except KeyError as exc:
        raise BatchError(f"unsupported Spark type: {name}") from exc


def build_spark(app_name: str = "taxiflow-batch-bronze") -> Any:
    """Build one configured SparkSession for the batch entry point."""
    from pyspark.sql import SparkSession

    user = os.environ.get("MINIO_ROOT_USER")
    password = os.environ.get("MINIO_ROOT_PASSWORD")
    if not user or not password:
        raise BatchError("MINIO_ROOT_USER and MINIO_ROOT_PASSWORD must be set")

    endpoint = os.environ.get("S3A_ENDPOINT", "http://minio:9000")
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Bronze preserves TLC wall-clock values without a UTC shift. Silver
        # interprets them as America/New_York and converts explicitly.
        .config("spark.sql.session.timeZone", SPARK_SESSION_TIMEZONE)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", user)
        .config("spark.hadoop.fs.s3a.secret.key", password)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
    )
    return builder.getOrCreate()


def read_source(spark: Any, entry: TlcFile, root: Path) -> tuple[Any, ColumnPlan]:
    """Read Parquet with the explicit contract schema and report drift."""
    schema = TLC_SCHEMAS[entry.service]
    path = verify_file(entry, root)
    actual_columns = spark.read.parquet(str(path)).columns
    plan = plan_columns(schema, actual_columns)
    dataframe = spark.read.schema(schema.spark_struct()).parquet(str(path))
    return dataframe, plan


def apply_contract(dataframe: Any, plan: ColumnPlan) -> Any:
    """Select and cast every contract column, filling missing columns with typed nulls."""
    from pyspark.sql import functions as F

    mapping = dict(plan.mapping)
    selected = []
    for spec in plan.schema.columns:
        actual = mapping[spec.name]
        target_type = _spark_type(spec.spark_type)
        if actual is None:
            selected.append(F.lit(None).cast(target_type).alias(spec.name))
        else:
            selected.append(F.col(actual).cast(target_type).alias(spec.name))
    return dataframe.select(*selected)


def _canonical_column(spec: Any) -> Any:
    from pyspark.sql import functions as F

    column = F.col(spec.name)
    if spec.spark_type == "timestamp":
        return F.date_format(column, "yyyy-MM-dd'T'HH:mm:ss")
    return column.cast("string")


def add_bronze_columns(
    dataframe: Any,
    plan: ColumnPlan,
    entry: TlcFile,
    ingested_at: datetime,
) -> Any:
    """Add provenance, deterministic identity, partitions, and quarantine reasons."""
    from pyspark.sql import functions as F

    schema = plan.schema
    canonical = F.concat_ws(
        SEP,
        *[
            F.coalesce(_canonical_column(spec), F.lit(NULL_TOKEN))
            for spec in schema.columns
        ],
    )
    pickup = F.col(schema.pickup_column)
    start, end = period_bounds(entry.year, entry.month)
    reason = (
        F.when(pickup.isNull(), F.lit("null_pickup_datetime"))
        .when(pickup < F.lit(start), F.lit("pickup_before_period"))
        .when(pickup >= F.lit(end), F.lit("pickup_at_or_after_next_period"))
    )

    enriched = (
        dataframe.withColumn("service", F.lit(entry.service))
        .withColumn("source_file", F.lit(entry.path))
        .withColumn("source_row_number", F.col("_metadata.row_index").cast("long"))
        .withColumn("ingested_at", F.lit(ingested_at).cast("timestamp"))
        .withColumn("content_hash", F.sha2(canonical, 256))
        .withColumn("pickup_year", F.year(pickup).cast("int"))
        .withColumn("pickup_month", F.month(pickup).cast("int"))
        .withColumn("quarantine_reason", reason)
    )
    enriched = enriched.withColumn(
        "trip_id",
        F.sha2(
            F.concat_ws(
                SEP,
                F.col("service"),
                F.col("source_file"),
                F.col("source_row_number").cast("string"),
                F.col("content_hash"),
            ),
            256,
        ),
    )
    return enriched


def _write_delta(
    dataframe: Any,
    spark: Any,
    path: str,
    *,
    partition_by: tuple[str, ...] = (),
    replace_where: str | None = None,
) -> None:
    from delta.tables import DeltaTable

    exists = DeltaTable.isDeltaTable(spark, path)
    writer = dataframe.write.format("delta").mode("overwrite")
    if exists and replace_where:
        writer = writer.option("replaceWhere", replace_where)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def write_bronze(dataframe: Any, spark: Any, path: str, entry: TlcFile) -> None:
    """Write only valid rows with deterministic month replacement."""
    from pyspark.sql import functions as F

    bronze = (
        dataframe.filter(F.col("quarantine_reason").isNull())
        .drop("quarantine_reason")
        .select(
            *TLC_SCHEMAS[entry.service].column_names,
            *BRONZE_PROVENANCE_COLUMNS,
        )
    )
    _write_delta(
        bronze,
        spark,
        path,
        partition_by=PARTITION_COLUMNS,
        replace_where=build_replace_where(entry),
    )


def write_quarantine(dataframe: Any, spark: Any, path: str, entry: TlcFile) -> None:
    """Write rejected rows and their reason, replacing only this source file."""
    from pyspark.sql import functions as F

    quarantine = (
        dataframe.filter(F.col("quarantine_reason").isNotNull())
        .select(
            F.sha2(
                F.concat_ws(
                    SEP,
                    F.col("service"),
                    F.col("source_file"),
                    F.col("source_row_number").cast("string"),
                    F.col("content_hash"),
                    F.col("quarantine_reason"),
                ),
                256,
            ).alias("quarantine_id"),
            F.col("service"),
            F.col("source_file"),
            F.col("source_row_number"),
            F.col("quarantine_reason"),
            F.col(TLC_SCHEMAS[entry.service].pickup_column).alias("pickup_datetime"),
            F.to_json(
                F.struct(
                    *[F.col(name) for name in TLC_SCHEMAS[entry.service].column_names]
                )
            ).alias("raw_record"),
            F.col("ingested_at"),
        )
    )
    _write_delta(
        quarantine,
        spark,
        path,
        replace_where=f"source_file = '{entry.path}'",
    )


def write_schema_drift_report(
    spark: Any,
    plan: ColumnPlan,
    entry: TlcFile,
    ingested_at: datetime,
    report_root: str,
) -> None:
    """Persist a replay-safe report for missing and unknown source columns."""
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    report_schema = StructType(
        [
            StructField("service", StringType(), False),
            StructField("source_file", StringType(), False),
            StructField("missing_columns", StringType(), False),
            StructField("unknown_columns", StringType(), False),
            StructField("ingested_at", TimestampType(), False),
        ]
    )
    row = (
        entry.service,
        entry.path,
        ",".join(plan.missing),
        ",".join(plan.unknown),
        ingested_at,
    )
    report = spark.createDataFrame([row], schema=report_schema)
    path = f"{report_root}/service={entry.service}/source={Path(entry.path).name}"
    report.write.mode("overwrite").json(path)


def collect_metrics(
    spark: Any,
    table_path: str,
    quarantine_path: str,
    entry: TlcFile,
    plan: ColumnPlan,
    run_id: str,
    ingested_at: datetime,
) -> dict[str, Any]:
    """Read back both Delta tables and return the run's reconciliation metrics."""
    from pyspark.sql import functions as F

    bronze = spark.read.format("delta").load(table_path)
    quarantine = spark.read.format("delta").load(quarantine_path)
    row_count = bronze.count()
    quarantine_count = quarantine.count()
    trip_id_count = bronze.select("trip_id").distinct().count()
    content_hash_count = bronze.select("content_hash").distinct().count()
    sums = bronze.agg(
        F.sum("total_amount").alias("total_amount_sum"),
        F.sum("fare_amount").alias("fare_amount_sum"),
        F.sum("trip_distance").alias("trip_distance_sum"),
    ).first()
    read_rows = entry.rows if entry.rows is not None else row_count + quarantine_count
    reconciled = row_count + quarantine_count == read_rows
    if not reconciled:
        raise BatchError(
            f"row reconciliation failed: read={read_rows}, "
            f"bronze={row_count}, quarantine={quarantine_count}"
        )

    return {
        "run_id": run_id,
        "service": entry.service,
        "period": f"{entry.year}-{entry.month:02d}",
        "read_rows": read_rows,
        "written_rows": row_count,
        "quarantined_rows": quarantine_count,
        "deduplicated_rows": row_count - content_hash_count,
        "trip_id_count": trip_id_count,
        "content_hash_count": content_hash_count,
        "total_amount_sum": float(sums["total_amount_sum"] or 0.0),
        "fare_amount_sum": float(sums["fare_amount_sum"] or 0.0),
        "trip_distance_sum": float(sums["trip_distance_sum"] or 0.0),
        "missing_columns": list(plan.missing),
        "unknown_columns": list(plan.unknown),
        "table_path": table_path,
        "quarantine_path": quarantine_path,
        "ingested_at": ingested_at.isoformat(),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse batch job arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", default="config/tlc_manifest.json")
    parser.add_argument("--root", default=".")
    parser.add_argument("--service", choices=sorted(TLC_SCHEMAS), required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--bucket", default=os.environ.get("S3_BUCKET", "taxiflow"))
    parser.add_argument("--table-path")
    parser.add_argument("--quarantine-path")
    parser.add_argument("--report-path")
    parser.add_argument("--write-partitions", type=int, default=8)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run one TLC month into Delta bronze."""
    args = parse_args(argv)
    root = Path(args.root).resolve()
    files = load_manifest(root / args.manifest)
    entry = select_file(files, args.service, args.year, args.month)
    verify_file(entry, root)

    table_path = args.table_path or f"s3a://{args.bucket}/bronze/tlc_trips"
    quarantine_path = (
        args.quarantine_path or f"s3a://{args.bucket}/bronze/tlc_quarantine"
    )
    report_path = args.report_path or f"s3a://{args.bucket}/bronze/_schema_drift"
    run_id = uuid.uuid4().hex
    ingested_at = datetime.utcnow()

    spark = build_spark()
    try:
        source, plan = read_source(spark, entry, root)
        contracted = apply_contract(source, plan)
        enriched = add_bronze_columns(contracted, plan, entry, ingested_at)
        if args.write_partitions > 0:
            enriched = enriched.repartition(args.write_partitions)

        write_bronze(enriched, spark, table_path, entry)
        write_quarantine(enriched, spark, quarantine_path, entry)
        write_schema_drift_report(spark, plan, entry, ingested_at, report_path)
        metrics = collect_metrics(
            spark,
            table_path,
            quarantine_path,
            entry,
            plan,
            run_id,
            ingested_at,
        )
    finally:
        spark.stop()

    print("TAXIFLOW_METRICS=" + json.dumps(metrics, sort_keys=True))
    if plan.missing:
        print(f"warning: missing columns filled with typed nulls: {plan.missing}", file=sys.stderr)
    if plan.unknown:
        print(f"warning: unknown columns reported: {plan.unknown}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
