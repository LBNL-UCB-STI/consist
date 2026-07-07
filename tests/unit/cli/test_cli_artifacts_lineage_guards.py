"""Guard-rail tests for artifacts/lineage command behavior."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from typer.testing import CliRunner

from consist import Tracker
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


def test_schema_capture_file_passes_mount_overrides_to_tracker(tmp_path) -> None:
    """`schema capture-file` should pass explicit mount overrides to tracker setup."""
    workspace_root = tmp_path / "archive_workspace"
    workspace_root.mkdir(parents=True)

    with patch("consist.cli.get_tracker") as mock_get_tracker:
        tracker = mock_get_tracker.return_value
        tracker.get_artifact.return_value = None

        result = runner.invoke(
            app,
            [
                "schema",
                "capture-file",
                "--artifact-key",
                "missing_artifact",
                "--db-path",
                "mock.db",
                "--mount",
                f"workspace={workspace_root}",
            ],
        )

    assert result.exit_code == 1
    mock_get_tracker.assert_called_once_with(
        "mock.db",
        run_dir=None,
        mounts={"workspace": str(workspace_root.resolve())},
    )


def test_schema_capture_file_rejects_invalid_mount_override_syntax() -> None:
    """`schema capture-file` should fail fast on malformed --mount values."""
    with patch("consist.cli.get_tracker") as mock_get_tracker:
        result = runner.invoke(
            app,
            [
                "schema",
                "capture-file",
                "--artifact-key",
                "missing_artifact",
                "--db-path",
                "mock.db",
                "--mount",
                "workspace",
            ],
        )

    assert result.exit_code == 2
    assert "Invalid --mount value" in result.stdout
    mock_get_tracker.assert_not_called()


def test_schema_capture_file_explains_exogenous_input_artifacts() -> None:
    """`schema capture-file` should explain how to profile input artifacts."""
    with patch("consist.cli.get_tracker") as mock_get_tracker:
        tracker = mock_get_tracker.return_value
        tracker.get_artifact.return_value = SimpleNamespace(
            id="00000000-0000-0000-0000-000000000123",
            key="raw_firms",
            driver="csv",
            run_id=None,
        )

        result = runner.invoke(
            app,
            [
                "schema",
                "capture-file",
                "--artifact-key",
                "raw_firms",
                "--db-path",
                "mock.db",
            ],
        )

    assert result.exit_code == 1
    normalized = " ".join(result.stdout.split())
    assert "Artifact has no producing run_id" in normalized
    assert "Tracker.run(..., profile_file_schema=True)" in normalized
    assert "post-hoc capture needs a producing run context" in normalized


def test_schema_export_missing_schema_suggests_capture_file() -> None:
    """`schema export` should suggest capture-file when no schema is persisted."""
    with patch("consist.cli.get_tracker") as mock_get_tracker:
        tracker = mock_get_tracker.return_value
        tracker.get_artifact.return_value = SimpleNamespace(
            id="00000000-0000-0000-0000-000000000999"
        )
        tracker.export_schema_sqlmodel.side_effect = KeyError("schema missing")

        result = runner.invoke(
            app,
            ["schema", "export", "--artifact-key", "missing_schema_artifact"],
        )

    assert result.exit_code == 1
    normalized = " ".join(result.stdout.split())
    assert "Captured schema not found for the provided selector." in result.stdout
    assert (
        "consist schema capture-file --artifact-id "
        "00000000-0000-0000-0000-000000000999" in normalized
    )


def test_artifacts_query_mode_surfaces_value_errors(cli_runner) -> None:
    """`artifacts` query mode should exit 1 and print parser/query validation errors."""
    with patch(
        "consist.cli.queries.find_artifacts_by_params",
        side_effect=ValueError("invalid --param expression"),
    ):
        result = cli_runner.invoke(app, ["artifacts", "--param", "beam.year>="])

    assert result.exit_code == 1
    assert "invalid --param expression" in result.stdout


def test_artifacts_run_mode_shows_accessibility_status(
    cli_runner, tracker, run_dir: Path
) -> None:
    """`artifacts` run mode should display a per-artifact accessibility indicator."""
    primary_path = run_dir / "outputs" / "results.csv"
    primary_path.parent.mkdir(parents=True, exist_ok=True)
    primary_path.write_text("a,b\n1,2\n", encoding="utf-8")

    archive_root = run_dir.parent / "archive"
    archive_root.mkdir(parents=True, exist_ok=True)

    with tracker.start_run("accessibility_run", "accessibility_model"):
        tracker.log_output(primary_path, key="results")

    artifact = tracker.get_artifact("results")
    assert artifact is not None

    relative_path = tracker.fs.get_remappable_relative_path(artifact.container_uri)
    assert relative_path is not None
    archived_path = archive_root / relative_path
    archived_path.parent.mkdir(parents=True, exist_ok=True)
    archived_path.write_text("a,b\n1,2\n", encoding="utf-8")
    tracker.set_artifact_recovery_roots(artifact, [archive_root])
    primary_path.unlink()

    result = cli_runner.invoke(app, ["artifacts", "accessibility_run"])

    assert result.exit_code == 0
    assert "Access" in result.stdout
    assert "recovery" in result.stdout


def test_artifacts_accepts_run_dir_for_moved_run_roots(
    tmp_path: Path, monkeypatch
) -> None:
    """`artifacts --run-dir` should resolve relative output URIs from that root."""
    repo_root = tmp_path / "repo"
    archive_root = tmp_path / "archive" / "cli-demo"
    db_dir = repo_root / "db"
    db_dir.mkdir(parents=True)
    archive_root.mkdir(parents=True)
    db_path = db_dir / "provenance.duckdb"

    tracker = Tracker(run_dir=archive_root, db_path=str(db_path))
    with tracker.start_run("moved_run", "demo"):
        output_path = tracker.run_artifact_dir() / "results.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("a,b\n1,2\n", encoding="utf-8")
        tracker.log_output(output_path, key="results")
    tracker.engine.dispose()

    monkeypatch.chdir(repo_root)
    result = runner.invoke(
        app,
        [
            "artifacts",
            "moved_run",
            "--db-path",
            "db/provenance.duckdb",
            "--run-dir",
            str(archive_root),
        ],
    )

    assert result.exit_code == 0
    assert "results" in result.stdout
    assert "primary" in result.stdout
