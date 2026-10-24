"""Explicit TLC column contracts and schema-drift planning."""

from __future__ import annotations

from dataclasses import dataclass

SPARK_TYPES = {"int", "long", "double", "string", "timestamp"}


class SchemaError(ValueError):
    """Raised when a source schema cannot be mapped safely."""


@dataclass(frozen=True)
class ColumnSpec:
    """One explicitly typed column in a TLC service contract."""

    name: str
    spark_type: str
    nullable: bool = True


@dataclass(frozen=True)
class TlcSchema:
    """The versioned column contract for one TLC service."""

    service: str
    pickup_column: str
    dropoff_column: str
    columns: tuple[ColumnSpec, ...]

    @property
    def column_names(self) -> tuple[str, ...]:
        """Return source column names in contract order."""
        return tuple(spec.name for spec in self.columns)

    def spark_struct(self) -> object:
        """Build the explicit Spark StructType lazily."""
        from pyspark.sql.types import (
            DoubleType,
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        type_map = {
            "int": IntegerType(),
            "long": LongType(),
            "double": DoubleType(),
            "string": StringType(),
            "timestamp": TimestampType(),
        }
        fields = []
        for spec in self.columns:
            if spec.spark_type not in type_map:
                raise SchemaError(f"unsupported spark type: {spec.spark_type}")
            fields.append(StructField(spec.name, type_map[spec.spark_type], spec.nullable))
        return StructType(fields)


@dataclass(frozen=True)
class ColumnPlan:
    """A safe mapping from contract columns to actual source columns."""

    schema: TlcSchema
    mapping: tuple[tuple[str, str | None], ...]
    missing: tuple[str, ...]
    unknown: tuple[str, ...]

    @property
    def clean(self) -> bool:
        """Return true when the source exactly matches the contract."""
        return not self.missing and not self.unknown


def normalize_column_name(name: str) -> str:
    """Normalize a column name for case-insensitive contract matching."""
    return name.strip().lower()


def plan_columns(schema: TlcSchema, actual_columns: list[str]) -> ColumnPlan:
    """Map a source schema to a contract and report drift explicitly."""
    actual_by_normalized: dict[str, str] = {}
    for column in actual_columns:
        normalized = normalize_column_name(column)
        if not normalized:
            raise SchemaError("source contains an empty column name")
        if normalized in actual_by_normalized:
            raise SchemaError(f"source contains duplicate normalized column: {column!r}")
        actual_by_normalized[normalized] = column

    expected_normalized = {
        normalize_column_name(spec.name): spec.name for spec in schema.columns
    }
    mapping = tuple(
        (
            spec.name,
            actual_by_normalized.get(normalize_column_name(spec.name)),
        )
        for spec in schema.columns
    )
    missing = tuple(name for name, actual in mapping if actual is None)
    unknown = tuple(
        sorted(
            actual
            for normalized, actual in actual_by_normalized.items()
            if normalized not in expected_normalized
        )
    )
    return ColumnPlan(schema=schema, mapping=mapping, missing=missing, unknown=unknown)


YELLOW_SCHEMA = TlcSchema(
    service="yellow",
    pickup_column="tpep_pickup_datetime",
    dropoff_column="tpep_dropoff_datetime",
    columns=(
        ColumnSpec("VendorID", "int"),
        ColumnSpec("tpep_pickup_datetime", "timestamp"),
        ColumnSpec("tpep_dropoff_datetime", "timestamp"),
        ColumnSpec("passenger_count", "long"),
        ColumnSpec("trip_distance", "double"),
        ColumnSpec("RatecodeID", "long"),
        ColumnSpec("store_and_fwd_flag", "string"),
        ColumnSpec("PULocationID", "int"),
        ColumnSpec("DOLocationID", "int"),
        ColumnSpec("payment_type", "long"),
        ColumnSpec("fare_amount", "double"),
        ColumnSpec("extra", "double"),
        ColumnSpec("mta_tax", "double"),
        ColumnSpec("tip_amount", "double"),
        ColumnSpec("tolls_amount", "double"),
        ColumnSpec("improvement_surcharge", "double"),
        ColumnSpec("total_amount", "double"),
        ColumnSpec("congestion_surcharge", "double"),
        ColumnSpec("Airport_fee", "double"),
    ),
)

GREEN_SCHEMA = TlcSchema(
    service="green",
    pickup_column="lpep_pickup_datetime",
    dropoff_column="lpep_dropoff_datetime",
    columns=(
        ColumnSpec("VendorID", "int"),
        ColumnSpec("lpep_pickup_datetime", "timestamp"),
        ColumnSpec("lpep_dropoff_datetime", "timestamp"),
        ColumnSpec("store_and_fwd_flag", "string"),
        ColumnSpec("RatecodeID", "long"),
        ColumnSpec("PULocationID", "int"),
        ColumnSpec("DOLocationID", "int"),
        ColumnSpec("passenger_count", "long"),
        ColumnSpec("trip_distance", "double"),
        ColumnSpec("fare_amount", "double"),
        ColumnSpec("extra", "double"),
        ColumnSpec("mta_tax", "double"),
        ColumnSpec("tip_amount", "double"),
        ColumnSpec("tolls_amount", "double"),
        ColumnSpec("ehail_fee", "double"),
        ColumnSpec("improvement_surcharge", "double"),
        ColumnSpec("total_amount", "double"),
        ColumnSpec("payment_type", "long"),
        ColumnSpec("trip_type", "long"),
        ColumnSpec("congestion_surcharge", "double"),
    ),
)

TLC_SCHEMAS = {"yellow": YELLOW_SCHEMA, "green": GREEN_SCHEMA}
