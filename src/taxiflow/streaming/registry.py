"""Minimal Kafka Connect and Schema Registry REST client for TaxiFlow CDC."""

from __future__ import annotations

import json
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from taxiflow.streaming.contract import (
    COMPATIBILITY,
    KEY_SUBJECT,
    VALUE_SUBJECT,
    CdcError,
)


def _request_json(
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
    timeout: float = 10.0,
) -> Any:
    body = None if payload is None else json.dumps(payload).encode()
    request = Request(
        url,
        data=body,
        method=method,
        headers={"Accept": "application/json", "Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            raw = response.read()
    except HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise CdcError(f"{method} {url} failed: HTTP {exc.code}: {detail}") from exc
    except URLError as exc:
        raise CdcError(f"{method} {url} failed: {exc.reason}") from exc
    if not raw:
        return None
    return json.loads(raw)


def wait_for_url(url: str, timeout: float = 60.0, interval: float = 1.0) -> None:
    """Wait for a control-plane endpoint to answer."""
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            _request_json("GET", url, timeout=3.0)
            return
        except CdcError as exc:
            last_error = exc
            time.sleep(interval)
    raise CdcError(f"timed out waiting for {url}: {last_error}")


def register_connector(
    base_url: str,
    payload: dict[str, Any],
    timeout: float = 120.0,
    interval: float = 2.0,
) -> dict[str, Any]:
    """Create or replace a connector and wait until its task is RUNNING."""
    name = payload.get("name")
    config = payload.get("config")
    if not isinstance(name, str) or not isinstance(config, dict):
        raise CdcError("connector payload must contain name and config")
    wait_for_url(f"{base_url.rstrip('/')}/connectors", timeout=min(timeout, 60.0))
    _request_json(
        "PUT",
        f"{base_url.rstrip('/')}/connectors/{quote(name)}/config",
        config,
    )

    deadline = time.monotonic() + timeout
    last_status: dict[str, Any] = {}
    while time.monotonic() < deadline:
        try:
            last_status = _request_json(
                "GET", f"{base_url.rstrip('/')}/connectors/{quote(name)}/status"
            )
        except CdcError as exc:
            # A freshly created connector can briefly return 404 while the
            # distributed herder finishes persisting its config.
            if "HTTP 404" in str(exc):
                time.sleep(interval)
                continue
            raise
        connector_state = last_status.get("connector", {}).get("state")
        task_states = [task.get("state") for task in last_status.get("tasks", [])]
        if connector_state == "RUNNING" and task_states and all(
            state == "RUNNING" for state in task_states
        ):
            return last_status
        if connector_state == "FAILED" or "FAILED" in task_states:
            raise CdcError(f"connector {name} failed: {last_status}")
        time.sleep(interval)
    raise CdcError(f"connector {name} did not become RUNNING: {last_status}")


def set_compatibility(
    base_url: str,
    subject: str,
    compatibility: str = COMPATIBILITY,
) -> str:
    """Set and return the compatibility policy for one subject."""
    _request_json(
        "PUT",
        f"{base_url.rstrip('/')}/config/{quote(subject, safe='')}",
        {"compatibility": compatibility},
    )
    return compatibility


def configure_subjects(
    base_url: str,
    compatibility: str = COMPATIBILITY,
) -> dict[str, str]:
    """Set the policy on key and value subjects and return both policies."""
    return {
        subject: set_compatibility(base_url, subject, compatibility)
        for subject in (KEY_SUBJECT, VALUE_SUBJECT)
    }


def latest_schema(base_url: str, subject: str = VALUE_SUBJECT) -> dict[str, Any]:
    """Fetch the latest registered schema for a subject."""
    data = _request_json(
        "GET", f"{base_url.rstrip('/')}/subjects/{quote(subject, safe='')}/versions/latest"
    )
    if not isinstance(data, dict) or "schema" not in data:
        raise CdcError(f"no latest schema for subject {subject}")
    return data


def wait_for_latest_schema(
    base_url: str,
    subject: str = VALUE_SUBJECT,
    timeout: float = 120.0,
    interval: float = 2.0,
) -> dict[str, Any]:
    """Wait for Debezium to register the first Avro value schema."""
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return latest_schema(base_url, subject)
        except CdcError as exc:
            last_error = exc
            time.sleep(interval)
    raise CdcError(f"timed out waiting for schema {subject}: {last_error}")


def register_cdc(
    debezium_url: str,
    schema_registry_url: str,
    payload: dict[str, Any],
    compatibility: str = COMPATIBILITY,
    timeout: float = 120.0,
) -> dict[str, Any]:
    """Register the connector, then pin and return the subject policies."""
    status = register_connector(debezium_url, payload, timeout=timeout)
    policies = configure_subjects(schema_registry_url, compatibility)
    return {
        "connector": payload["name"],
        "state": status.get("connector", {}).get("state"),
        "policies": policies,
        "value_subject": VALUE_SUBJECT,
    }
