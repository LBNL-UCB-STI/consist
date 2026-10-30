"""Documentation-oriented tests for search/validate CLI guard behavior."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
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
    assert "╭" in result.stdout
    assert "Tracker database not initialized" in result.stdout


def test_validate_scoped_by_run(cli_runner, tracker, run_dir: Path) -> None:
    """`validate --run` should validate only artifacts for the specified run."""
    missing_file = run_dir / "old_missing.csv"
    missing_file.write_text("a,b\n1,2\n", encoding="utf-8")
    with tracker.start_run("run_old", "model_a"):
        tracker.log_output(missing_file, key="old_out")
    missing_file.unlink()

    present_file = run_dir / "new_present.csv"
    present_file.write_text("a,b\n1,2\n", encoding="utf-8")
    with tracker.start_run("run_new", "model_b"):
        tracker.log_output(present_file, key="new_out")

    ok_result = cli_runner.invoke(app, ["validate", "--run", "run_new"])
    assert ok_result.exit_code == 0
    assert "All artifacts validated successfully" in ok_result.stdout

    missing_run_result = cli_runner.invoke(app, ["validate", "--run", "missing_run"])
    assert missing_run_result.exit_code == 1
    assert "Run with ID 'missing_run' not found." in missing_run_result.stdout


def test_validate_scoped_by_scenario_deduplicates_shared_artifacts(
    cli_runner, tracker, run_dir: Path
) -> None:
    """`validate --scenario` should validate scenario artifacts once and ignore other scenarios."""
    stale_file = run_dir / "stale.csv"
    stale_file.write_text("a\n1\n", encoding="utf-8")
    with tracker.start_run("stale_step", "model_stale", parent_run_id="sc_old"):
        tracker.log_output(stale_file, key="stale_out")
    stale_file.unlink()

    shared_file = run_dir / "shared.csv"
    shared_file.write_text("a\n1\n", encoding="utf-8")
    final_file = run_dir / "final.csv"
    final_file.write_text("a\n2\n", encoding="utf-8")

    with tracker.start_run("step_1", "model_1", parent_run_id="sc_1"):
        shared_art = tracker.log_output(shared_file, key="shared_art")
    with tracker.start_run("step_2", "model_2", parent_run_id="step_1"):
        tracker.log_input(shared_art, key="shared_art")
        tracker.log_output(final_file, key="final_art")

    result = cli_runner.invoke(app, ["validate", "--scenario", "sc_1"])
    assert result.exit_code == 0
    assert "All artifacts validated successfully" in result.stdout

    shared_file.unlink()
    missing_result = cli_runner.invoke(app, ["validate", "--scenario", "sc_1"])
    assert missing_result.exit_code == 0
    assert "Found 1 missing artifacts:" in missing_result.stdout


def test_validate_latest(cli_runner, tracker, run_dir: Path) -> None:
    """`validate --latest` should validate the most recent scenario/root run."""
    stale_file = run_dir / "stale_latest.csv"
    stale_file.write_text("a\n1\n", encoding="utf-8")
    with tracker.start_run("old_step", "model_old", parent_run_id="sc_older"):
        tracker.log_output(stale_file, key="stale_latest_out")
    stale_file.unlink()

    fresh_file = run_dir / "fresh_latest.csv"
    fresh_file.write_text("a\n2\n", encoding="utf-8")
    with tracker.start_run("new_step", "model_new", parent_run_id="sc_newer"):
        tracker.log_output(fresh_file, key="fresh_latest_out")

    result = cli_runner.invoke(app, ["validate", "--latest"])
    assert result.exit_code == 0
    assert "All artifacts validated successfully" in result.stdout


def test_validate_outputs_only(cli_runner, tracker, run_dir: Path) -> None:
    """`validate --outputs-only` should skip input-only artifacts."""
    input_file = run_dir / "missing_input.csv"
    input_file.write_text("a\n1\n", encoding="utf-8")
    output_file = run_dir / "present_output.csv"
    output_file.write_text("a\n2\n", encoding="utf-8")

    with tracker.start_run("run_io", "model_io"):
        tracker.log_input(input_file, key="raw_in")
        tracker.log_output(output_file, key="proc_out")
    input_file.unlink()

    all_result = cli_runner.invoke(app, ["validate", "--run", "run_io"])
    assert all_result.exit_code == 0
    assert "Found 1 missing artifacts:" in all_result.stdout
    assert "raw_in" in all_result.stdout

    outputs_result = cli_runner.invoke(
        app, ["validate", "--run", "run_io", "--outputs-only"]
    )
    assert outputs_result.exit_code == 0
    assert "All artifacts validated successfully" in outputs_result.stdout


def test_validate_rejects_mutually_exclusive_scopes(cli_runner) -> None:
    """`validate` should exit 2 when multiple scope options are combined."""
    result_one = cli_runner.invoke(
        app, ["validate", "--scenario", "sc_1", "--run", "run_1"]
    )
    assert result_one.exit_code == 2
    assert (
        "Options --scenario, --run, and --latest are mutually exclusive."
        in result_one.stdout
    )

    result_two = cli_runner.invoke(app, ["validate", "--latest", "--run", "run_1"])
    assert result_two.exit_code == 2
    assert (
        "Options --scenario, --run, and --latest are mutually exclusive."
        in result_two.stdout
    )
