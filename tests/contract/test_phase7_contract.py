"""Static contract for the Phase 7 `make gate` entrypoint.

These tests read the Makefile only. They never run the gate; running it from a
clean volume is the Phase 7 exit gate. The contract asserts that one command
chains every data step with one exit code, and that `verify` stays a fast
static gate instead of absorbing the data path.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.contract

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MAKEFILE_PATH = PROJECT_ROOT / "Makefile"

# The ordered data path the goal sentence promises: platform up, data in,
# bronze verified, dbt built, quality gated, dashboard served.
GATE_STEPS = (
    "up-all",
    "seed",
    "batch",
    "cdc-seed",
    "cdc-report",
    "dbt-build",
    "quality",
    "superset-bootstrap",
    "serving-verify",
)

STATIC_VERIFY_DEPS = ("lint", "test", "test-contract", "compose-check")


def makefile_text() -> str:
    assert MAKEFILE_PATH.exists(), "Makefile is missing"
    return MAKEFILE_PATH.read_text(encoding="utf-8")


def recipe_lines(text: str, target: str) -> list[str]:
    """Return the tab-indented recipe lines of a top-level target."""
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if not line.startswith("\t") and line.split("#", 1)[0].rstrip().startswith(f"{target}:"):
            recipe: list[str] = []
            for candidate in lines[index + 1 :]:
                if candidate.startswith("\t"):
                    recipe.append(candidate)
                elif candidate.strip() == "":
                    continue
                else:
                    break
            return recipe
    raise AssertionError(f"target '{target}:' not found in Makefile")


def test_gate_target_is_declared() -> None:
    text = makefile_text()
    assert re_phony_gate(text)


def re_phony_gate(text: str) -> bool:
    for line in text.splitlines():
        if line.startswith(".PHONY:"):
            names = line[len(".PHONY:") :].split()
            if "gate" in names:
                return True
    return False


def test_gate_runs_every_step_in_order() -> None:
    recipe = recipe_lines(makefile_text(), "gate")
    joined = "\n".join(recipe)
    position = 0
    for step in GATE_STEPS:
        marker = f"$(MAKE) --no-print-directory {step}"
        found = joined.find(marker, position)
        assert found >= 0, f"gate must invoke '{step}' after the previous steps"
        position = found + 1


def test_gate_excludes_destructive_targets() -> None:
    joined = "\n".join(recipe_lines(makefile_text(), "gate"))
    for forbidden in ("reset", "down", "down -v"):
        assert f"{forbidden}" not in joined, f"gate must not run '{forbidden}' itself"


def test_verify_stays_static() -> None:
    deps = None
    for line in makefile_text().splitlines():
        if line.startswith("verify:"):
            deps = line[len("verify:") :].split()
            break
    assert deps == list(STATIC_VERIFY_DEPS), "verify must keep its static dependency set"


def test_gate_is_listed_in_help() -> None:
    help_recipe = "\n".join(recipe_lines(makefile_text(), "help"))
    assert "'make gate" in help_recipe, "help must document the gate entrypoint"


def test_seed_reports_cache_status_before_download() -> None:
    recipe = "\n".join(recipe_lines(makefile_text(), "seed"))
    status = recipe.find("taxiflow tlc status")
    download = recipe.find("taxiflow tlc download")
    assert status >= 0, "seed must print which manifest files are cached"
    assert download >= 0, "seed must still download missing files"
    assert status < download, "cache status must be reported before downloading"
