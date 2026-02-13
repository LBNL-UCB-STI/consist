"""Documentation-oriented tests for search/validate CLI guard behavior."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from consist.cli import app


runner = CliRunner()


def test_search_rejects_empty_query(cli_runner) -> None:
    """`search` should reject empty queries with a clear boundary message."""
    result = cli_runner.invoke(app, ["search", ""])

    assert result.exit_code == 2


def test_search_rejects_overlong_query(cli_runner) -> None:
    """`search` should enforce the documented max query length."""
    too_long = "x" * 257
    result = cli_runner.invoke(app, ["search", too_long])

    assert result.exit_code == 2


def test_search_reports_no_results(cli_runner) -> None:
    """`search` should print a user-facing empty-state message when no runs match."""
    result = cli_runner.invoke(app, ["search", "no-match"])

    assert result.exit_code == 0
    assert "No runs found matching 'no-match'" in result.stdout


def test_validate_invalid_env_batch_size_falls_back_to_default(
    monkeypatch: pytest.MonkeyPatch,
    cli_runner,
) -> None:
    """
    `validate` should ignore invalid CONSIST_VALIDATE_BATCH_SIZE and use 1000.

    This test acts as regression protection for environment-driven CLI behavior.
    """
    called: dict[str, Any] = {}

    def fake_iter_artifact_rows(session: Any, batch_size: int):
        called["batch_size"] = batch_size
        return iter(())

    monkeypatch.setenv("CONSIST_VALIDATE_BATCH_SIZE", "not-an-int")
    monkeypatch.setattr("consist.cli._iter_artifact_rows", fake_iter_artifact_rows)

    result = cli_runner.invoke(app, ["validate"])

    assert result.exit_code == 0
    assert called["batch_size"] == 1000
    assert "All artifacts validated successfully" in result.stdout


def test_schema_apply_fks_errors_when_tracker_db_missing() -> None:
    """`schema apply-fks` should fail fast when tracker DB manager is unavailable."""
    tracker = MagicMock()
    tracker.db = None

    with patch("consist.cli.get_tracker", return_value=tracker):
        result = runner.invoke(app, ["schema", "apply-fks"])

    assert result.exit_code == 1
    assert "Tracker database not initialized" in result.stdout
