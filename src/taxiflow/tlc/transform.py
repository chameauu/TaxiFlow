"""Pure, deterministic helpers shared by unit tests and the Spark batch job."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from datetime import datetime

SEP = "\x1f"
NULL_TOKEN = "\x00"


def canonical_value(value: object) -> str:
    """Render one value in the canonical form used by Spark expressions."""
    if value is None:
        return NULL_TOKEN
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S")
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def canonical_record(values: Sequence[object]) -> str:
    """Join one source row into a stable, separator-delimited record."""
    return SEP.join(canonical_value(value) for value in values)


def content_hash(values: Sequence[object]) -> str:
    """Return the SHA-256 of a canonical source row."""
    return hashlib.sha256(canonical_record(values).encode("utf-8")).hexdigest()


def trip_id(
    service: str,
    source_file: str,
    row_hash: str,
    source_row_number: int,
) -> str:
    """Derive a stable trip identity from source provenance and row position."""
    material = SEP.join(
        [service, source_file, row_hash, str(source_row_number)]
    ).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def period_bounds(year: int, month: int) -> tuple[datetime, datetime]:
    """Return the half-open pickup-time bounds for a source month."""
    start = datetime(year, month, 1)
    end = datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)
    return start, end


def is_in_period(value: datetime | None, year: int, month: int) -> bool:
    """Return true when a pickup timestamp belongs to the declared source month."""
    if value is None:
        return False
    start, end = period_bounds(year, month)
    return start <= value < end
