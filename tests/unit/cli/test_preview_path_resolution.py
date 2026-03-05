from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from consist import Tracker
from consist.cli import ConsistShell, app, get_tracker


def _create_relative_csv_artifact(run_dir: Path, db_path: Path) -> None:
    tracker = Tracker(run_dir=run_dir, db_path=str(db_path))
    with tracker.start_run("producer_run", "preview_model"):
        artifact_dir = tracker.run_artifact_dir()
        artifact_dir.mkdir(parents=True, exist_ok=True)
        csv_path = artifact_dir / "trip_table.csv"
        csv_path.write_text("origin_zone,dest_zone\n1,2\n", encoding="utf-8")
        tracker.log_artifact(
            str(csv_path), key="trip_table", driver="csv", direction="output"
        )


def _create_workspace_csv_artifact(run_dir: Path, db_path: Path) -> None:
    tracker = Tracker(
        run_dir=run_dir,
        db_path=str(db_path),
        mounts={"workspace": str(run_dir)},
    )
    with tracker.start_run("producer_run", "preview_model"):
        artifact_dir = tracker.run_artifact_dir()
        artifact_dir.mkdir(parents=True, exist_ok=True)
        csv_path = artifact_dir / "trip_table.csv"
        csv_path.write_text("origin_zone,dest_zone\n1,2\n", encoding="utf-8")
        tracker.log_artifact(
            str(csv_path), key="trip_table", driver="csv", direction="output"
        )

    artifact = tracker.get_artifact("trip_table")
    assert artifact is not None
    assert artifact.container_uri.startswith("workspace://")


def test_preview_resolves_relative_paths_from_current_working_directory(
    tmp_path: Path, monkeypatch
) -> None:
    repo_root = tmp_path / "repo"
    run_dir = repo_root / "examples" / "runs" / "beam_core_demo"
    run_dir.mkdir(parents=True)
    db_path = run_dir / "beam_core_demo.duckdb"
    _create_relative_csv_artifact(run_dir=run_dir, db_path=db_path)

    monkeypatch.chdir(run_dir)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "preview",
            "trip_table",
            "--db-path",
            "beam_core_demo.duckdb",
            "--rows",
            "1",
        ],
    )

    assert result.exit_code == 0
    assert "Preview: trip_table" in result.stdout
    assert "origin_zone" in result.stdout


def test_preview_accepts_run_dir_option_for_moved_run_roots(
    tmp_path: Path, monkeypatch
) -> None:
    repo_root = tmp_path / "repo"
    archive_root = tmp_path / "archive" / "beam_core_demo"
    db_dir = repo_root / "db"
    db_dir.mkdir(parents=True)
    archive_root.mkdir(parents=True)
    db_path = db_dir / "beam_core_demo.duckdb"
    _create_relative_csv_artifact(run_dir=archive_root, db_path=db_path)

    monkeypatch.chdir(repo_root)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "preview",
            "trip_table",
            "--db-path",
            "db/beam_core_demo.duckdb",
            "--run-dir",
            str(archive_root),
            "--rows",
            "1",
        ],
    )

    assert result.exit_code == 0
    assert "Preview: trip_table" in result.stdout


def test_preview_does_not_use_physical_run_dir_without_trust_db(
    tmp_path: Path, monkeypatch
) -> None:
    repo_root = tmp_path / "repo"
    archive_root = tmp_path / "archive" / "beam_core_demo"
    db_dir = repo_root / "db"
    db_dir.mkdir(parents=True)
    archive_root.mkdir(parents=True)
    db_path = db_dir / "beam_core_demo.duckdb"
    _create_workspace_csv_artifact(run_dir=archive_root, db_path=db_path)

    monkeypatch.chdir(repo_root)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "preview",
            "trip_table",
            "--db-path",
            "db/beam_core_demo.duckdb",
            "--rows",
            "1",
        ],
    )

    assert result.exit_code == 1
    assert "Artifact file not found at:" in result.stdout
    assert (
        "workspace://outputs/preview_model/producer_run/trip_table.csv" in result.stdout
    )
    assert "Mount 'workspace://' is not configured" in result.stdout


def test_preview_uses_physical_run_dir_when_trust_db_enabled(
    tmp_path: Path, monkeypatch
) -> None:
    repo_root = tmp_path / "repo"
    archive_root = tmp_path / "archive" / "beam_core_demo"
    db_dir = repo_root / "db"
    db_dir.mkdir(parents=True)
    archive_root.mkdir(parents=True)
    db_path = db_dir / "beam_core_demo.duckdb"
    _create_workspace_csv_artifact(run_dir=archive_root, db_path=db_path)

    monkeypatch.chdir(repo_root)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "preview",
            "trip_table",
            "--db-path",
            "db/beam_core_demo.duckdb",
            "--trust-db",
            "--rows",
            "1",
        ],
    )

    assert result.exit_code == 0
    assert "Preview: trip_table" in result.stdout


def test_validate_auto_resolves_relative_paths_from_db_parent(
    tmp_path: Path, monkeypatch
) -> None:
    repo_root = tmp_path / "repo"
    run_dir = repo_root / "examples" / "runs" / "beam_core_demo"
    run_dir.mkdir(parents=True)
    db_path = run_dir / "beam_core_demo.duckdb"
    _create_relative_csv_artifact(run_dir=run_dir, db_path=db_path)

    monkeypatch.chdir(repo_root)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "validate",
            "--db-path",
            "examples/runs/beam_core_demo/beam_core_demo.duckdb",
        ],
    )

    assert result.exit_code == 0
    assert "All artifacts validated successfully" in result.stdout


def test_validate_accepts_run_dir_option_for_moved_run_roots(
    tmp_path: Path, monkeypatch
) -> None:
    repo_root = tmp_path / "repo"
    archive_root = tmp_path / "archive" / "beam_core_demo"
    db_dir = repo_root / "db"
    db_dir.mkdir(parents=True)
    archive_root.mkdir(parents=True)
    db_path = db_dir / "beam_core_demo.duckdb"
    _create_relative_csv_artifact(run_dir=archive_root, db_path=db_path)

    monkeypatch.chdir(repo_root)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "validate",
            "--db-path",
            "db/beam_core_demo.duckdb",
            "--run-dir",
            str(archive_root),
        ],
    )

    assert result.exit_code == 0
    assert "All artifacts validated successfully" in result.stdout


def test_validate_accepts_trust_db_option_for_moved_run_roots(
    tmp_path: Path, monkeypatch
) -> None:
    repo_root = tmp_path / "repo"
    archive_root = tmp_path / "archive" / "beam_core_demo"
    db_dir = repo_root / "db"
    db_dir.mkdir(parents=True)
    archive_root.mkdir(parents=True)
    db_path = db_dir / "beam_core_demo.duckdb"
    _create_relative_csv_artifact(run_dir=archive_root, db_path=db_path)

    monkeypatch.chdir(repo_root)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "validate",
            "--db-path",
            "db/beam_core_demo.duckdb",
            "--trust-db",
        ],
    )

    assert result.exit_code == 0
    assert "All artifacts validated successfully" in result.stdout


def test_shell_preview_uses_trust_db_mount_inference_for_workspace_uris(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    repo_root = tmp_path / "repo"
    archive_root = tmp_path / "archive" / "beam_core_demo"
    db_dir = repo_root / "db"
    db_dir.mkdir(parents=True)
    archive_root.mkdir(parents=True)
    db_path = db_dir / "beam_core_demo.duckdb"
    _create_workspace_csv_artifact(run_dir=archive_root, db_path=db_path)

    monkeypatch.chdir(repo_root)
    tracker = get_tracker("db/beam_core_demo.duckdb")
    shell = ConsistShell(tracker, trust_db=True)
    shell.do_preview("trip_table --rows 1")

    out = capsys.readouterr().out
    assert "Preview: trip_table" in out
    assert "origin_zone" in out
