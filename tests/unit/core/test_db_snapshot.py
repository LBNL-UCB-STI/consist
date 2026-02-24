from __future__ import annotations

import json
from pathlib import Path

import pytest

from consist.core.persistence import DatabaseManager
from consist.core.tracker import Tracker


def test_snapshot_creates_copy_and_sidecar(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs"
    db_path = tmp_path / "provenance.duckdb"
    tracker = Tracker(run_dir=run_dir, db_path=db_path)

    tracker.begin_run("snapshot_run", "snapshot_model")
    artifact_path = tmp_path / "out.txt"
    artifact_path.write_text("ok\n", encoding="utf-8")
    tracker.log_artifact(artifact_path, key="out", direction="output")
    completed = tracker.end_run(status="completed")

    destination = tmp_path / "snapshots" / "latest" / "provenance.duckdb"
    snapshot_path = tracker.snapshot_db(destination)

    assert snapshot_path == destination
    assert snapshot_path.exists()

    sidecar_path = destination.parent / "provenance.snapshot_meta.json"
    assert sidecar_path.exists()

    payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    assert payload["run_id"] is None
    assert payload["last_completed_run_id"] == completed.id
    assert payload["cache_epoch"] == 1
    assert payload["snapshot_ts_utc"]
    assert payload["source_db_path"] == str(db_path)

    snapshot_db = DatabaseManager(str(snapshot_path))
    try:
        assert len(snapshot_db.find_runs(limit=1)) >= 1
    finally:
        snapshot_db.engine.dispose()
        if tracker.engine is not None:
            tracker.engine.dispose()


def test_snapshot_without_checkpoint_copies_wal_when_present(tmp_path: Path) -> None:
    source_db_path = tmp_path / "source.duckdb"
    db = DatabaseManager(str(source_db_path))
    try:
        source_wal_path = Path(f"{source_db_path}.wal")
        source_wal_path.write_text("fake wal\n", encoding="utf-8")

        destination = tmp_path / "snapshots" / "no_checkpoint" / "snapshot.duckdb"
        db.snapshot_to(destination, checkpoint=False)

        destination_wal_path = Path(f"{destination}.wal")
        assert destination_wal_path.exists()
    finally:
        db.engine.dispose()


def test_snapshot_rejects_in_memory_db(tmp_path: Path) -> None:
    db = DatabaseManager(":memory:")
    try:
        with pytest.raises(ValueError, match="in-memory"):
            db.snapshot_to(tmp_path / "snapshot.duckdb")
    finally:
        db.engine.dispose()
