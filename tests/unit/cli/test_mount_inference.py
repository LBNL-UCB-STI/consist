from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from consist import Tracker
from consist.cli import ConsistShell, _ensure_tracker_mounts_for_artifact, get_tracker


def test_cli_infers_mounts_from_run_metadata(tmp_path: Path) -> None:
    inputs_root = tmp_path / "inputs_root"
    inputs_root.mkdir(parents=True)
    data_path = inputs_root / "data.csv"
    data_path.write_text("a,b\n1,2\n", encoding="utf-8")

    db_path = tmp_path / "provenance.duckdb"

    tracker = Tracker(
        run_dir=tmp_path / "producer",
        db_path=str(db_path),
        mounts={"inputs": str(inputs_root)},
    )
    with tracker.start_run(run_id="producer", model="test"):
        logged = tracker.log_artifact(data_path, key="data", direction="input")

    inspector = Tracker(run_dir=tmp_path / "inspector", db_path=str(db_path))
    artifact = inspector.get_artifact(logged.id)
    assert artifact is not None

    _ensure_tracker_mounts_for_artifact(inspector, artifact, trust_db=True)

    assert inspector.mounts["inputs"] == str(inputs_root.resolve())
    assert inspector.resolve_uri(artifact.container_uri) == str(data_path.resolve())


def test_shell_preview_run_dir_overrides_trusted_stale_workspace_mount(
    tmp_path: Path, capsys
) -> None:
    stale_run_dir = tmp_path / "job_workspace" / "pilates-run"
    archive_run_dir = tmp_path / "archive" / "pilates-run"
    relative_path = Path("beam/input/seattle/network.csv")

    stale_file = stale_run_dir / relative_path
    archive_file = archive_run_dir / relative_path
    stale_file.parent.mkdir(parents=True)
    archive_file.parent.mkdir(parents=True)
    stale_file.write_text("node,value\nstale,0\n", encoding="utf-8")
    archive_file.write_text("node,value\narchive,1\n", encoding="utf-8")

    db_path = tmp_path / "provenance.duckdb"
    producer = Tracker(
        run_dir=stale_run_dir,
        db_path=str(db_path),
        mounts={"workspace": str(stale_run_dir)},
    )
    with producer.start_run(run_id="producer", model="pilates"):
        logged = producer.log_artifact(stale_file, key="network", direction="output")

    stale_file.unlink()

    inspector = get_tracker(str(db_path), run_dir=str(archive_run_dir))
    artifact = inspector.get_artifact(logged.id)
    assert artifact is not None

    _ensure_tracker_mounts_for_artifact(inspector, artifact, trust_db=True)
    assert inspector.mounts["workspace"] == str(archive_run_dir.resolve())
    assert inspector.resolve_uri(artifact.container_uri) == str(archive_file.resolve())

    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(
            inspector,
            trust_db=True,
            db_path=str(db_path),
            run_dir=str(archive_run_dir),
        )

    shell.do_preview("network --rows 1")

    out = capsys.readouterr().out
    assert "Preview: network" in out
    assert "archive" in out
    assert "Artifact file not found" not in out


def test_explicit_workspace_mount_overrides_run_dir_workspace_mount(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "provenance.duckdb"
    Tracker(run_dir=tmp_path / "producer", db_path=str(db_path))

    explicit_workspace = tmp_path / "explicit_workspace"
    cli_tracker = get_tracker(
        str(db_path),
        run_dir=str(tmp_path / "archive_run"),
        mounts={"workspace": str(explicit_workspace)},
    )

    assert cli_tracker.mounts["workspace"] == str(explicit_workspace)
