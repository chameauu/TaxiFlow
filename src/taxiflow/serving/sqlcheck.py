"""Serving-layer helpers for the Superset dashboard."""

from __future__ import annotations

import re

# Each rule returns a message when the SQL violates the serving contract.
_FORBIDDEN_RULES: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bselect\s+\*", re.IGNORECASE), "SELECT * is forbidden"),
    (
        re.compile(r"\bsum\s*\(\s*passenger_count\b", re.IGNORECASE),
        "sum(passenger_count) is forbidden across grouped trips",
    ),
    (
        re.compile(r"\bsum\s*\(\s*trip_distance\b", re.IGNORECASE),
        "sum(trip_distance) is forbidden across grouped trips",
    ),
)


def validate_query_sql(sql: str) -> list[str]:
    """Return the serving-contract violations found in a SQL query."""
    return [message for pattern, message in _FORBIDDEN_RULES if pattern.search(sql)]
