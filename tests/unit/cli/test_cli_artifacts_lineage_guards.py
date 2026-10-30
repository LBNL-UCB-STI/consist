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


def test_lineage_max_depth_flag(cli_runner, tracker, run_dir: Path) -> None:
    """`lineage --max-depth 1` should truncate deeper runs with a depth marker."""
    f1 = run_dir / "step1.csv"
    f1.write_text("a\n1\n", encoding="utf-8")
    f2 = run_dir / "step2.csv"
    f2.write_text("a\n2\n", encoding="utf-8")

    with tracker.start_run("depth_run_1", "model_1"):
        a1 = tracker.log_output(f1, key="depth_art_1")
    with tracker.start_run("depth_run_2", "model_2"):
        tracker.log_input(a1, key="depth_art_1")
        tracker.log_output(f2, key="depth_art_2")

    result = cli_runner.invoke(app, ["lineage", "depth_art_2", "--max-depth", "1"])
    assert result.exit_code == 0
    assert "depth_run_2" in result.stdout
    assert "depth_art_1" in result.stdout
    assert "depth_run_1" not in result.stdout
    assert "... (max depth reached)" in result.stdout
    assert "No producing run found" not in result.stdout


def test_lineage_collapses_config_inputs_by_default_and_expands_with_flag(
    cli_runner, tracker, run_dir: Path
) -> None:
    """`lineage` should collapse >=2 leaf config:* inputs by default and expand with --expand-configs."""
    data_in = run_dir / "data_in.csv"
    data_in.write_text("a\n1\n", encoding="utf-8")
    out_file = run_dir / "out.csv"
    out_file.write_text("a\n2\n", encoding="utf-8")

    with tracker.start_run("cfg_run", "model"):
        tracker.log_input(data_in, key="primary_input")
        for idx in range(5):
            cfg_path = run_dir / f"cfg_{idx}.yaml"
            cfg_path.write_text(f"k: {idx}\n", encoding="utf-8")
            tracker.log_input(cfg_path, key=f"config:cfg_{idx}.yaml")
        tracker.log_output(out_file, key="cfg_run_output")

    default_res = cli_runner.invoke(app, ["lineage", "cfg_run_output"])
    assert default_res.exit_code == 0
    assert "config:* (5 files)" in default_res.stdout
    assert "config:cfg_0.yaml" not in default_res.stdout
    assert "primary_input" in default_res.stdout
    assert "No producing run found" not in default_res.stdout

    expanded_res = cli_runner.invoke(
        app, ["lineage", "cfg_run_output", "--expand-configs"]
    )
    assert expanded_res.exit_code == 0
    for idx in range(5):
        assert f"config:cfg_{idx}.yaml" in expanded_res.stdout


def test_lineage_collapses_output_set_members_by_default_and_expands_with_flag(
    cli_runner, tracker, run_dir: Path
) -> None:
    """`lineage` should hide output-set manifest and member leaves by default and expand with --expand-sets."""
    parent_path = run_dir / "set_parent.csv"
    parent_path.write_text("v\n1\n", encoding="utf-8")
    manifest_path = run_dir / "set_manifest.json"
    manifest_path.write_text("{}\n", encoding="utf-8")
    final_path = run_dir / "final_set_consumer.csv"
    final_path.write_text("v\n9\n", encoding="utf-8")

    with tracker.start_run("set_consumer_run", "model"):
        manifest = tracker.log_artifact(
            manifest_path,
            key="set_manifest_key",
            driver="json",
            direction="input",
            output_set_manifest=True,
        )
        parent = tracker.log_artifact(
            parent_path,
            key="set_parent_key",
            driver="artifact_set",
            direction="input",
            artifact_set=True,
            manifest_artifact_id=str(manifest.id),
        )
        for idx in range(3):
            m_path = run_dir / f"member_{idx}.csv"
            m_path.write_text(f"v\n{idx}\n", encoding="utf-8")
            tracker.log_artifact(
                m_path,
                key=f"set_member_{idx}",
                driver="csv",
                direction="input",
                parent_artifact_id=parent.id,
            )
        tracker.log_output(final_path, key="set_consumer_out")

    default_res = cli_runner.invoke(app, ["lineage", "set_consumer_out"])
    assert default_res.exit_code == 0
    assert "set_parent_key" in default_res.stdout
    assert "set_manifest_key" not in default_res.stdout
    assert "set_member_0" not in default_res.stdout

    expanded_res = cli_runner.invoke(
        app, ["lineage", "set_consumer_out", "--expand-sets"]
    )
    assert expanded_res.exit_code == 0
    assert "set_parent_key" in expanded_res.stdout
    assert "set_manifest_key" in expanded_res.stdout
    assert "set_member_0" in expanded_res.stdout
