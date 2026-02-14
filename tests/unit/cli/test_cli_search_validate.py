"""Documentation-oriented tests for search/validate CLI guard behavior."""

from __future__ import annotations

from datetime import datetime
import uuid
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from sqlmodel import Session
from typer.testing import CliRunner

from consist.cli import app
from consist.models.artifact import Artifact
from consist.models.run import Run


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


def test_search_escaped_wildcard_chars_reports_no_results(cli_runner) -> None:
    """`search` should safely handle `%` and `_` patterns and still return no-results."""
    query = "mix%_literal"
    result = cli_runner.invoke(app, ["search", query])

    assert result.exit_code == 0
    assert f"No runs found matching '{query}'" in result.stdout


def test_search_matches_tags(cli_runner, tracker) -> None:
    """`search` should match runs by tags, not just id/model/scenario."""
    run = Run(
        id="run_tagged",
        model_name="model_alpha",
        config_hash=None,
        git_hash=None,
        status="completed",
        tags=["tag_only_term"],
        created_at=datetime(2025, 1, 1, 12, 0),
        started_at=datetime(2025, 1, 1, 12, 0),
    )
    with Session(tracker.engine) as session:
        session.add(run)
        session.commit()

    result = cli_runner.invoke(app, ["search", "tag_only_term"])

    assert result.exit_code == 0
    assert "run_tagged" in result.stdout


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


def test_validate_fix_persists_missing_marker(cli_runner, tracker, tmp_path) -> None:
    """`validate --fix` should persist a missing marker on missing artifacts."""
    missing_path = tmp_path / "missing.txt"
    artifact_id = uuid.uuid4()
    artifact = Artifact(
        id=artifact_id,
        key="missing_artifact",
        container_uri=str(missing_path),
        driver="txt",
        run_id="run_missing",
        hash="hash_missing",
    )
    with Session(tracker.engine) as session:
        session.add(artifact)
        session.commit()

    result = cli_runner.invoke(app, ["validate", "--fix"])

    assert result.exit_code == 0
    with Session(tracker.engine) as session:
        refreshed = session.get(Artifact, artifact_id)
        assert refreshed is not None
        assert refreshed.meta.get("missing_on_disk") is True


def test_validate_fix_noop_when_no_missing(cli_runner, tracker, tmp_path) -> None:
    """`validate --fix` should be a no-op when all artifacts exist."""
    present_path = tmp_path / "present.txt"
    present_path.write_text("ok", encoding="utf-8")
    artifact_id = uuid.uuid4()
    artifact = Artifact(
        id=artifact_id,
        key="present_artifact",
        container_uri=str(present_path),
        driver="txt",
        run_id="run_present",
        hash="hash_present",
        meta={"existing": "value"},
    )
    with Session(tracker.engine) as session:
        session.add(artifact)
        session.commit()

    result = cli_runner.invoke(app, ["validate", "--fix"])

    assert result.exit_code == 0
    assert "All artifacts validated successfully" in result.stdout
    with Session(tracker.engine) as session:
        refreshed = session.get(Artifact, artifact_id)
        assert refreshed is not None
        assert refreshed.meta.get("existing") == "value"
        assert "missing_on_disk" not in refreshed.meta


def test_schema_apply_fks_errors_when_tracker_db_missing() -> None:
    """`schema apply-fks` should fail fast when tracker DB manager is unavailable."""
    tracker = MagicMock()
    tracker.db = None

    with patch("consist.cli.get_tracker", return_value=tracker):
        result = runner.invoke(app, ["schema", "apply-fks"])

    assert result.exit_code == 1
    assert "Tracker database not initialized" in result.stdout
