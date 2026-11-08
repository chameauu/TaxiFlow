"""Runtime contract for the Phase 6 Superset serving layer."""

from __future__ import annotations

import http.cookiejar
import json
import os
import subprocess
from pathlib import Path
from urllib.request import HTTPCookieProcessor, Request, build_opener

import pytest

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROFILES = ("--profile", "core", "--profile", "bi")
BASE_URL = "http://localhost:8088"
COOKIE_JAR = http.cookiejar.CookieJar()
OPENER = build_opener(HTTPCookieProcessor(COOKIE_JAR))
EXPECTED_TABLES = {
    "agg_daily_trips",
    "agg_zone_daily",
    "agg_hourly_demand",
    "agg_tip_rate",
    "quality_summary",
}


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", *PROFILES, *args],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=check,
    )


def require_running(service: str) -> None:
    result = compose("ps", "--status", "running", "--services", check=False)
    if service in result.stdout.split():
        return
    if os.environ.get("TAXIFLOW_REQUIRE_STACK") == "1":
        pytest.fail(f"{service} is not running; start the stack with make up-all")
    pytest.skip(f"{service} is not running")


def api(
    path: str,
    *,
    token: str,
    method: str = "GET",
    payload: dict[str, object] | None = None,
    csrf: str | None = None,
) -> dict[str, object]:
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if csrf:
        headers["X-CSRFToken"] = csrf
    request = Request(f"{BASE_URL}{path}", data=data, headers=headers, method=method)
    with OPENER.open(request, timeout=60) as response:
        body = json.loads(response.read().decode())
    assert isinstance(body, dict)
    return body


def login() -> tuple[str, str]:
    username = os.environ["SUPERSET_ADMIN_USER"]
    password = os.environ["SUPERSET_ADMIN_PASSWORD"]
    body = api(
        "/api/v1/security/login",
        token="",
        method="POST",
        payload={
            "username": username,
            "password": password,
            "provider": "db",
            "refresh": True,
        },
    )
    token = body.get("access_token")
    assert isinstance(token, str) and token
    csrf = api("/api/v1/security/csrf_token/", token=token).get("result")
    assert isinstance(csrf, str) and csrf
    return token, csrf


def test_superset_exposes_the_trino_database_and_dashboard() -> None:
    require_running("superset")
    token, _csrf = login()

    databases = api("/api/v1/database/", token=token)["result"]
    database = next(
        item for item in databases if item["database_name"] == "TaxiFlow Trino"
    )
    assert database["backend"] == "trino"

    datasets = api("/api/v1/dataset/", token=token)["result"]
    tables = {item["table_name"] for item in datasets}
    assert tables >= EXPECTED_TABLES

    dashboards = api("/api/v1/dashboard/", token=token)["result"]
    titles = {item["dashboard_title"] for item in dashboards}
    assert "TaxiFlow NYC TLC Overview" in titles


def test_superset_can_query_the_gold_fact_table() -> None:
    require_running("superset")
    token, csrf = login()
    databases = api("/api/v1/database/", token=token)["result"]
    database = next(
        item for item in databases if item["database_name"] == "TaxiFlow Trino"
    )

    body = api(
        "/api/v1/sqllab/execute/",
        token=token,
        method="POST",
        csrf=csrf,
        payload={
            "database_id": database["id"],
            "schema": "gold",
            "sql": "select count(*) as trip_count from gold.fct_trips",
            "json": True,
            "runAsync": False,
            "expand_data": True,
        },
    )

    rows = body.get("data")
    assert isinstance(rows, list) and rows
    assert int(rows[0]["trip_count"]) > 0


def test_documented_queries_run_against_gold() -> None:
    require_running("superset")
    token, csrf = login()
    databases = api("/api/v1/database/", token=token)["result"]
    database = next(
        item for item in databases if item["database_name"] == "TaxiFlow Trino"
    )

    for path in sorted((PROJECT_ROOT / "config" / "superset" / "queries").glob("*.sql")):
        body = api(
            "/api/v1/sqllab/execute/",
            token=token,
            method="POST",
            csrf=csrf,
            payload={
                "database_id": database["id"],
                "schema": "gold",
                "sql": path.read_text(),
                "json": True,
                "runAsync": False,
                "expand_data": True,
            },
        )
        assert body.get("status") == "success", path.name
