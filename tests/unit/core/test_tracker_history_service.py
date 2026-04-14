from __future__ import annotations

from pathlib import Path

import pytest

from consist.models.artifact import get_tracker_ref
from consist.models.run import ConsistRecord, Run


def test_history_service_caches_non_current_run_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    tracker,
) -> None:
    service = tracker._history_service
    output_path = tracker.run_dir / "history_output.csv"
    output_path.write_text("value\n1\n", encoding="utf-8")

    with tracker.start_run("history_run", "demo"):
        tracker.log_artifact(output_path, key="result", direction="output")

    if tracker.db is None:
        pytest.skip("Tracker fixture should provide a DB-backed tracker.")

    call_count = 0
    original = tracker.db.get_artifacts_for_run

    def wrapped(run_id: str):
        nonlocal call_count
        call_count += 1
        return original(run_id)

    monkeypatch.setattr(tracker.db, "get_artifacts_for_run", wrapped)

    first = service.get_artifacts_for_run("history_run")
    second = service.get_artifacts_for_run("history_run")

    assert call_count == 1
    assert second is first
    tracker_ref = get_tracker_ref(first.outputs["result"])
    assert tracker_ref is not None
    assert tracker_ref() is tracker


def test_history_service_snapshot_db_uses_active_and_last_completed_metadata(
    monkeypatch: pytest.MonkeyPatch,
    tracker,
    tmp_path: Path,
) -> None:
    service = tracker._history_service
    active_run = Run(
        id="active-run",
        model_name="demo",
        config_hash=None,
        git_hash=None,
    )
    completed_run = Run(
        id="completed-run",
        model_name="demo",
        config_hash=None,
        git_hash=None,
        status="completed",
    )
    tracker.current_consist = ConsistRecord(run=active_run, config={})
    tracker._last_consist = ConsistRecord(run=completed_run, config={})
    captured: dict[str, object] = {}

    if tracker.db is None:
        pytest.skip("Tracker fixture should provide a DB-backed tracker.")

    def fake_snapshot_to(*, dest_path, checkpoint, metadata):
        captured.update(
            dest_path=dest_path,
            checkpoint=checkpoint,
            metadata=metadata,
        )
        return Path(dest_path)

    monkeypatch.setattr(tracker.db, "snapshot_to", fake_snapshot_to)

    destination = tmp_path / "snapshot.duckdb"
    result = service.snapshot_db(destination, checkpoint=False)

    assert result == destination
    assert captured["dest_path"] == destination
    assert captured["checkpoint"] is False
    assert captured["metadata"] == {
        "run_id": "active-run",
        "last_completed_run_id": "completed-run",
        "cache_epoch": tracker._cache_epoch,
    }
