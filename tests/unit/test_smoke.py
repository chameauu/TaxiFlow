"""Phase 0 smoke tests."""

import pytest

from taxiflow import __version__
from taxiflow.__main__ import main


def test_version_is_defined() -> None:
    assert __version__ == "0.1.0"


def test_cli_returns_zero() -> None:
    assert main([]) == 0


def test_cli_version_exits_zero(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["--version"])

    assert exc.value.code == 0
    assert "taxiflow 0.1.0" in capsys.readouterr().out
