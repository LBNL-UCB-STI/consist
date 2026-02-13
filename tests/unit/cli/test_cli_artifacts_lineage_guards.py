"""Guard-rail tests for artifacts/lineage command behavior."""

from __future__ import annotations

from unittest.mock import patch

from typer.testing import CliRunner

from consist.cli import app


runner = CliRunner()


def test_artifacts_rejects_mixing_run_id_with_query_flags(cli_runner) -> None:
    """
    `artifacts` should enforce mutually exclusive modes:
    positional run_id mode vs query flag mode.
    """
    result = cli_runner.invoke(app, ["artifacts", "run-1", "--param", "beam.year=2020"])

    assert result.exit_code == 1
    assert "run_id cannot be combined with --param" in result.stdout


def test_artifacts_query_mode_reports_no_matches(cli_runner) -> None:
    """Query mode should emit a no-match message instead of rendering an empty table."""
    with patch("consist.cli.queries.find_artifacts_by_params", return_value=[]):
        result = cli_runner.invoke(app, ["artifacts", "--param", "beam.year=2020"])

    assert result.exit_code == 0
    assert "No artifacts matched the provided filters" in result.stdout


def test_lineage_errors_when_artifact_is_missing(cli_runner) -> None:
    """`lineage` should return a clear error for unknown artifact keys."""
    result = cli_runner.invoke(app, ["lineage", "missing_artifact_key"])

    assert result.exit_code == 1
    assert (
        "Could not find artifact with key or ID 'missing_artifact_key'" in result.stdout
    )


def test_schema_export_rejects_non_uuid_artifact_id(cli_runner) -> None:
    """`schema export --artifact-id` must validate UUID format before tracker calls."""
    result = cli_runner.invoke(
        app,
        ["schema", "export", "--artifact-id", "not-a-uuid"],
    )

    assert result.exit_code == 2
    assert "--artifact-id must be a UUID" in result.stdout
