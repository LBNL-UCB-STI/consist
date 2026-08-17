from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from rich.console import Console

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


def _create_scenario_csv_artifact(run_dir: Path, db_path: Path) -> None:
    data_root = run_dir / "SynthFirm"
    outputs_root = data_root / "outputs"
    outputs_root.mkdir(parents=True, exist_ok=True)
    csv_path = outputs_root / "synthetic_firms.csv"
    csv_path.write_text("city,value\nAustin,1\n", encoding="utf-8")

    tracker = Tracker(
        run_dir=run_dir,
        db_path=str(db_path),
        mounts={"data": str(data_root)},
    )
    with tracker.scenario("scenario") as sc:
        with sc.trace("step") as t:
            t.log_artifact(
                csv_path, key="synthetic_firms", driver="csv", direction="output"
            )


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


def test_preview_rejects_unbounded_rows_before_loading_artifact() -> None:
    runner = CliRunner()

    result = runner.invoke(app, ["preview", "trip_table", "--rows", "0"])

    assert result.exit_code == 2


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


def test_preview_run_dir_overrides_trusted_stale_workspace_mount(
    tmp_path: Path, monkeypatch
) -> None:
    repo_root = tmp_path / "repo"
    stale_run_dir = tmp_path / "job_workspace" / "beam_core_demo"
    archive_run_dir = tmp_path / "archive" / "beam_core_demo"
    db_dir = repo_root / "db"
    db_dir.mkdir(parents=True)
    stale_run_dir.mkdir(parents=True)
    archive_run_dir.mkdir(parents=True)
    db_path = db_dir / "beam_core_demo.duckdb"

    producer = Tracker(
        run_dir=stale_run_dir,
        db_path=str(db_path),
        mounts={"workspace": str(stale_run_dir)},
    )
    with producer.start_run("producer_run", "preview_model"):
        artifact_dir = producer.run_artifact_dir()
        artifact_dir.mkdir(parents=True, exist_ok=True)
        stale_csv_path = artifact_dir / "network.csv"
        stale_csv_path.write_text("source,value\nstale,0\n", encoding="utf-8")
        logged = producer.log_artifact(
            stale_csv_path, key="network", driver="csv", direction="output"
        )

    artifact = producer.get_artifact(logged.id)
    assert artifact is not None
    assert artifact.container_uri.startswith("workspace://")
    archive_csv_path = archive_run_dir / artifact.container_uri.removeprefix(
        "workspace://"
    )
    archive_csv_path.parent.mkdir(parents=True, exist_ok=True)
    archive_csv_path.write_text("source,value\narchive,1\n", encoding="utf-8")
    stale_csv_path.unlink()

    monkeypatch.chdir(repo_root)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "preview",
            "network",
            "--db-path",
            "db/beam_core_demo.duckdb",
            "--run-dir",
            str(archive_run_dir),
            "--trust-db",
            "--rows",
            "1",
        ],
    )

    assert result.exit_code == 0
    assert "Preview: network" in result.stdout
    assert "archive" in result.stdout
    assert "Artifact file not found" not in result.stdout


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


def test_schema_capture_file_requires_run_dir_or_trust_for_moved_relative_paths(
    tmp_path: Path, monkeypatch
) -> None:
    repo_root = tmp_path / "repo"
    run_dir = tmp_path / "archive" / "beam_core_demo"
    db_dir = repo_root / "db"
    db_dir.mkdir(parents=True)
    run_dir.mkdir(parents=True)
    db_path = db_dir / "beam_core_demo.duckdb"
    _create_relative_csv_artifact(run_dir=run_dir, db_path=db_path)

    monkeypatch.chdir(repo_root)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "schema",
            "capture-file",
            "--artifact-key",
            "trip_table",
            "--db-path",
            "db/beam_core_demo.duckdb",
        ],
    )

    assert result.exit_code == 1
    assert "Could not resolve an existing artifact file path" in result.stdout


def test_schema_capture_file_accepts_run_dir_for_moved_relative_paths(
    tmp_path: Path, monkeypatch
) -> None:
    repo_root = tmp_path / "repo"
    run_dir = tmp_path / "archive" / "beam_core_demo"
    db_dir = repo_root / "db"
    db_dir.mkdir(parents=True)
    run_dir.mkdir(parents=True)
    db_path = db_dir / "beam_core_demo.duckdb"
    _create_relative_csv_artifact(run_dir=run_dir, db_path=db_path)

    monkeypatch.chdir(repo_root)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "schema",
            "capture-file",
            "--artifact-key",
            "trip_table",
            "--db-path",
            "db/beam_core_demo.duckdb",
            "--run-dir",
            str(run_dir),
        ],
    )

    assert result.exit_code == 0
    assert "Captured file schema for artifact 'trip_table'" in result.stdout


def test_schema_capture_file_accepts_trust_db_for_moved_relative_paths(
    tmp_path: Path, monkeypatch
) -> None:
    repo_root = tmp_path / "repo"
    run_dir = tmp_path / "archive" / "beam_core_demo"
    db_dir = repo_root / "db"
    db_dir.mkdir(parents=True)
    run_dir.mkdir(parents=True)
    db_path = db_dir / "beam_core_demo.duckdb"
    _create_relative_csv_artifact(run_dir=run_dir, db_path=db_path)

    monkeypatch.chdir(repo_root)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "schema",
            "capture-file",
            "--artifact-key",
            "trip_table",
            "--db-path",
            "db/beam_core_demo.duckdb",
            "--trust-db",
        ],
    )

    assert result.exit_code == 0
    assert "Captured file schema for artifact 'trip_table'" in result.stdout


def test_preview_requires_trust_db_for_recovery_roots(
    tmp_path: Path, monkeypatch
) -> None:
    repo_root = tmp_path / "repo"
    run_dir = tmp_path / "historical" / "beam_core_demo"
    db_dir = repo_root / "db"
    archive_root = tmp_path / "archive" / "beam_core_demo"
    db_dir.mkdir(parents=True)
    archive_root.mkdir(parents=True)
    run_dir.mkdir(parents=True)
    db_path = db_dir / "beam_core_demo.duckdb"

    _create_relative_csv_artifact(run_dir=run_dir, db_path=db_path)
    producer = Tracker(run_dir=run_dir, db_path=str(db_path))
    artifact = producer.get_artifact("trip_table")
    assert artifact is not None
    relative_path = producer.fs.get_remappable_relative_path(artifact.container_uri)
    assert relative_path is not None
    archived_path = archive_root / relative_path
    archived_path.parent.mkdir(parents=True, exist_ok=True)
    archived_path.write_text("origin_zone,dest_zone\n1,2\n", encoding="utf-8")
    producer.set_artifact_recovery_roots(artifact, [archive_root])
    (run_dir / relative_path).unlink()

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
    assert "Artifact file not found" in result.stdout

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
    assert "origin_zone" in result.stdout


def test_schema_capture_file_requires_trust_db_for_recovery_roots(
    tmp_path: Path, monkeypatch
) -> None:
    repo_root = tmp_path / "repo"
    run_dir = tmp_path / "historical" / "beam_core_demo"
    db_dir = repo_root / "db"
    archive_root = tmp_path / "archive" / "beam_core_demo"
    db_dir.mkdir(parents=True)
    archive_root.mkdir(parents=True)
    run_dir.mkdir(parents=True)
    db_path = db_dir / "beam_core_demo.duckdb"

    _create_relative_csv_artifact(run_dir=run_dir, db_path=db_path)
    producer = Tracker(run_dir=run_dir, db_path=str(db_path))
    artifact = producer.get_artifact("trip_table")
    assert artifact is not None
    relative_path = producer.fs.get_remappable_relative_path(artifact.container_uri)
    assert relative_path is not None
    archived_path = archive_root / relative_path
    archived_path.parent.mkdir(parents=True, exist_ok=True)
    archived_path.write_text("origin_zone,dest_zone\n1,2\n", encoding="utf-8")
    producer.set_artifact_recovery_roots(artifact, [archive_root])
    (run_dir / relative_path).unlink()

    monkeypatch.chdir(repo_root)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "schema",
            "capture-file",
            "--artifact-key",
            "trip_table",
            "--db-path",
            "db/beam_core_demo.duckdb",
        ],
    )

    assert result.exit_code == 1
    assert "Could not resolve an existing artifact file path" in result.stdout

    result = runner.invoke(
        app,
        [
            "schema",
            "capture-file",
            "--artifact-key",
            "trip_table",
            "--db-path",
            "db/beam_core_demo.duckdb",
            "--trust-db",
        ],
    )

    assert result.exit_code == 0
    assert "Captured file schema for artifact 'trip_table'" in result.stdout


def test_validate_requires_trust_db_for_recovery_roots(
    tmp_path: Path, monkeypatch
) -> None:
    repo_root = tmp_path / "repo"
    run_dir = tmp_path / "historical" / "beam_core_demo"
    db_dir = repo_root / "db"
    archive_root = tmp_path / "archive" / "beam_core_demo"
    db_dir.mkdir(parents=True)
    archive_root.mkdir(parents=True)
    run_dir.mkdir(parents=True)
    db_path = db_dir / "beam_core_demo.duckdb"

    _create_relative_csv_artifact(run_dir=run_dir, db_path=db_path)
    producer = Tracker(run_dir=run_dir, db_path=str(db_path))
    artifact = producer.get_artifact("trip_table")
    assert artifact is not None
    relative_path = producer.fs.get_remappable_relative_path(artifact.container_uri)
    assert relative_path is not None
    archived_path = archive_root / relative_path
    archived_path.parent.mkdir(parents=True, exist_ok=True)
    archived_path.write_text("origin_zone,dest_zone\n1,2\n", encoding="utf-8")
    producer.set_artifact_recovery_roots(artifact, [archive_root])
    (run_dir / relative_path).unlink()

    monkeypatch.chdir(repo_root)
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["validate", "--db-path", "db/beam_core_demo.duckdb"],
    )

    assert result.exit_code == 0
    assert "missing artifacts" in result.stdout

    result = runner.invoke(
        app,
        ["validate", "--db-path", "db/beam_core_demo.duckdb", "--trust-db"],
    )

    assert result.exit_code == 0
    assert "All artifacts validated successfully" in result.stdout


def test_validate_mount_uri_fails_without_mount_or_trust_db(
    tmp_path: Path, monkeypatch
) -> None:
    repo_root = tmp_path / "repo"
    db_dir = repo_root / "db"
    inputs_root = tmp_path / "inputs"
    db_dir.mkdir(parents=True)
    inputs_root.mkdir(parents=True)
    db_path = db_dir / "beam_core_demo.duckdb"
    data_path = inputs_root / "trip_table.csv"
    data_path.write_text("origin_zone,dest_zone\n1,2\n", encoding="utf-8")

    producer = Tracker(
        run_dir=tmp_path / "producer",
        db_path=str(db_path),
        mounts={"inputs": str(inputs_root)},
    )
    with producer.start_run("producer_run", "preview_model"):
        producer.log_artifact(data_path, key="trip_table", direction="output")

    monkeypatch.chdir(repo_root)
    runner = CliRunner()
    result = runner.invoke(app, ["validate", "--db-path", "db/beam_core_demo.duckdb"])

    assert result.exit_code == 0
    assert "Found 1 missing artifacts" in result.stdout
    assert "trip_table" in result.stdout


def test_validate_accepts_mount_overrides_for_mount_uris(
    tmp_path: Path, monkeypatch
) -> None:
    repo_root = tmp_path / "repo"
    db_dir = repo_root / "db"
    inputs_root = tmp_path / "inputs"
    db_dir.mkdir(parents=True)
    inputs_root.mkdir(parents=True)
    db_path = db_dir / "beam_core_demo.duckdb"
    data_path = inputs_root / "trip_table.csv"
    data_path.write_text("origin_zone,dest_zone\n1,2\n", encoding="utf-8")

    producer = Tracker(
        run_dir=tmp_path / "producer",
        db_path=str(db_path),
        mounts={"inputs": str(inputs_root)},
    )
    with producer.start_run("producer_run", "preview_model"):
        producer.log_artifact(data_path, key="trip_table", direction="output")

    monkeypatch.chdir(repo_root)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "validate",
            "--db-path",
            "db/beam_core_demo.duckdb",
            "--mount",
            f"inputs={inputs_root}",
        ],
    )

    assert result.exit_code == 0
    assert "All artifacts validated successfully" in result.stdout


def test_schema_capture_file_accepts_mount_overrides_for_mount_uris(
    tmp_path: Path, monkeypatch
) -> None:
    repo_root = tmp_path / "repo"
    db_dir = repo_root / "db"
    inputs_root = tmp_path / "inputs"
    db_dir.mkdir(parents=True)
    inputs_root.mkdir(parents=True)
    db_path = db_dir / "beam_core_demo.duckdb"
    data_path = inputs_root / "trip_table.csv"
    data_path.write_text("origin_zone,dest_zone\n1,2\n", encoding="utf-8")

    producer = Tracker(
        run_dir=tmp_path / "producer",
        db_path=str(db_path),
        mounts={"inputs": str(inputs_root)},
    )
    with producer.start_run("producer_run", "preview_model"):
        producer.log_artifact(data_path, key="trip_table", direction="output")

    monkeypatch.chdir(repo_root)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "schema",
            "capture-file",
            "--artifact-key",
            "trip_table",
            "--db-path",
            "db/beam_core_demo.duckdb",
            "--mount",
            f"inputs={inputs_root}",
        ],
    )

    assert result.exit_code == 0
    assert "Captured file schema for artifact 'trip_table'" in result.stdout


def test_validate_infers_mounts_from_db_when_trusted(
    tmp_path: Path, monkeypatch
) -> None:
    repo_root = tmp_path / "repo"
    db_dir = repo_root / "db"
    inputs_root = tmp_path / "inputs"
    db_dir.mkdir(parents=True)
    inputs_root.mkdir(parents=True)
    db_path = db_dir / "beam_core_demo.duckdb"
    data_path = inputs_root / "trip_table.csv"
    data_path.write_text("origin_zone,dest_zone\n1,2\n", encoding="utf-8")

    producer = Tracker(
        run_dir=tmp_path / "producer",
        db_path=str(db_path),
        mounts={"inputs": str(inputs_root)},
    )
    with producer.start_run("producer_run", "preview_model"):
        producer.log_artifact(data_path, key="trip_table", direction="input")

    monkeypatch.chdir(repo_root)
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["validate", "--db-path", "db/beam_core_demo.duckdb", "--trust-db"],
    )

    assert result.exit_code == 0
    assert "All artifacts validated successfully" in result.stdout


def test_validate_trusted_mount_inference_is_per_artifact(
    tmp_path: Path, monkeypatch
) -> None:
    repo_root = tmp_path / "repo"
    db_dir = repo_root / "db"
    inputs_a = tmp_path / "inputs_a"
    inputs_b = tmp_path / "inputs_b"
    db_dir.mkdir(parents=True)
    inputs_a.mkdir(parents=True)
    inputs_b.mkdir(parents=True)
    db_path = db_dir / "beam_core_demo.duckdb"
    data_a = inputs_a / "data" / "a.csv"
    data_b = inputs_b / "data" / "b.csv"
    data_a.parent.mkdir(parents=True)
    data_b.parent.mkdir(parents=True)
    data_a.write_text("value\n1\n", encoding="utf-8")
    data_b.write_text("value\n2\n", encoding="utf-8")

    producer_a = Tracker(
        run_dir=tmp_path / "producer_a",
        db_path=str(db_path),
        mounts={"inputs": str(inputs_a)},
    )
    with producer_a.start_run("producer_a", "preview_model"):
        producer_a.log_artifact(data_a, key="data_a", direction="input")

    producer_b = Tracker(
        run_dir=tmp_path / "producer_b",
        db_path=str(db_path),
        mounts={"inputs": str(inputs_b)},
    )
    with producer_b.start_run("producer_b", "preview_model"):
        producer_b.log_artifact(data_b, key="data_b", direction="input")

    monkeypatch.chdir(repo_root)
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["validate", "--db-path", "db/beam_core_demo.duckdb", "--trust-db"],
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
    assert shell.tracker.mounts == {}

    shell.do_schema_profile("trip_table")

    out = capsys.readouterr().out
    assert "Schema: trip_table" in out
    assert "origin_zone" in out
    assert shell.tracker.mounts == {}


def test_shell_preview_and_schema_profile_use_recorded_mount_root_for_ambiguous_scenario_artifacts(
    tmp_path: Path, capsys
) -> None:
    run_dir = tmp_path / "inspector"
    db_path = tmp_path / "provenance.duckdb"
    _create_scenario_csv_artifact(run_dir=run_dir, db_path=db_path)

    tracker = Tracker(run_dir=run_dir, db_path=str(db_path))
    shell = ConsistShell(tracker, trust_db=True)

    shell.do_preview("synthetic_firms --rows 1")
    out = capsys.readouterr().out
    assert "Preview: synthetic_firms" in out
    assert "Austin" in out
    assert "Artifact file not found" not in out

    shell.do_schema_profile("synthetic_firms")
    out = capsys.readouterr().out
    assert "Schema: synthetic_firms" in out
    assert "city" in out
    assert "Artifact file not found" not in out


def test_shell_artifacts_uses_trust_db_mount_inference_for_named_mount_uris(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "SynthFirm"
    outputs_root = data_root / "outputs"
    outputs_root.mkdir(parents=True, exist_ok=True)
    data_file = outputs_root / "out.csv"
    data_file.write_text("value\n1\n", encoding="utf-8")
    db_path = tmp_path / "provenance.duckdb"

    producer = Tracker(
        run_dir=tmp_path / "producer",
        db_path=str(db_path),
        mounts={"data": str(data_root)},
    )
    with producer.start_run("producer_run", "synthfirm"):
        producer.log_artifact(
            data_file,
            key="out",
            driver="csv",
            direction="output",
        )

    tracker = get_tracker(str(db_path))
    shell = ConsistShell(tracker, trust_db=True)

    record_console = Console(record=True, width=200)
    with patch("consist.cli.console", record_console):
        shell.do_artifacts("producer_run")

    out = record_console.export_text()
    assert "Artifacts for Run" in out
    assert "out.csv" in out
    assert "primary" in out
    assert "missing" not in out


def test_shell_artifacts_explicit_mount_overrides_trusted_db_metadata(
    tmp_path: Path,
) -> None:
    stale_data_root = tmp_path / "SynthFirm"
    explicit_data_root = tmp_path / "explicit"
    stale_outputs_root = stale_data_root / "outputs"
    explicit_outputs_root = explicit_data_root / "outputs"
    stale_outputs_root.mkdir(parents=True, exist_ok=True)
    explicit_outputs_root.mkdir(parents=True, exist_ok=True)
    data_file = stale_outputs_root / "out.csv"
    data_file.write_text("value\n1\n", encoding="utf-8")
    db_path = tmp_path / "provenance.duckdb"

    producer = Tracker(
        run_dir=tmp_path / "producer",
        db_path=str(db_path),
        mounts={"data": str(stale_data_root)},
    )
    with producer.start_run("producer_run", "synthfirm"):
        producer.log_artifact(
            data_file,
            key="out",
            driver="csv",
            direction="output",
        )

    data_file.unlink()
    explicit_file = explicit_outputs_root / "out.csv"
    explicit_file.write_text("value\n2\n", encoding="utf-8")

    tracker = get_tracker(str(db_path), mounts={"data": str(explicit_data_root)})
    shell = ConsistShell(tracker, trust_db=True)

    record_console = Console(record=True, width=200)
    with patch("consist.cli.console", record_console):
        shell.do_artifacts("producer_run")

    out = record_console.export_text()
    assert "Artifacts for Run" in out
    assert "primary" in out
    assert "missing" not in out
