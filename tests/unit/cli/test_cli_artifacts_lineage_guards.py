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
    normalized = " ".join(result.stdout.split())
    assert "run_id cannot be combined with" in normalized
    assert "--param/--namespace/--key-prefix/--family-prefix." in normalized


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


def test_schema_export_reports_missing_artifact_key() -> None:
    """`schema export --artifact-key` should fail when the artifact is unknown."""
    with patch("consist.cli.get_tracker") as mock_get_tracker:
        tracker = mock_get_tracker.return_value
        tracker.get_artifact.return_value = None

        result = runner.invoke(
            app,
            ["schema", "export", "--artifact-key", "missing_artifact_key"],
        )

    assert result.exit_code == 1
    assert "Artifact 'missing_artifact_key' not found." in result.stdout


def test_schema_capture_file_requires_selector(cli_runner) -> None:
    """`schema capture-file` should require exactly one artifact selector."""
    result = cli_runner.invoke(app, ["schema", "capture-file"])

    assert result.exit_code == 2
    assert "Provide exactly one of --artifact-key or --artifact-id" in result.stdout


def test_schema_capture_file_rejects_run_id_with_artifact_id(cli_runner) -> None:
    """`schema capture-file` should disallow --run-id with --artifact-id."""
    result = cli_runner.invoke(
        app,
        [
            "schema",
            "capture-file",
            "--artifact-id",
            "00000000-0000-0000-0000-000000000000",
            "--run-id",
            "run-123",
        ],
    )

    assert result.exit_code == 2
    assert "--run-id can only be used with --artifact-key selection." in result.stdout


def test_artifacts_query_mode_surfaces_value_errors(cli_runner) -> None:
    """`artifacts` query mode should exit 1 and print parser/query validation errors."""
    with patch(
        "consist.cli.queries.find_artifacts_by_params",
        side_effect=ValueError("invalid --param expression"),
    ):
        result = cli_runner.invoke(app, ["artifacts", "--param", "beam.year>="])

    assert result.exit_code == 1
    assert "invalid --param expression" in result.stdout
