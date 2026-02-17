from pathlib import Path

from sqlmodel import Session, select

from consist.models.run import RunArtifactLink


def test_log_artifacts_batches_flush_and_db_sync(tracker, run_dir: Path, monkeypatch):
    first = run_dir / "first.csv"
    second = run_dir / "second.csv"
    first.write_text("a,b\n1,2\n", encoding="utf-8")
    second.write_text("c,d\n3,4\n", encoding="utf-8")

    flush_now_calls = 0
    sync_batch_calls = 0
    sync_single_calls = 0

    original_flush_now = tracker.persistence._flush_json_now
    original_sync_artifacts = tracker.db.sync_artifacts
    original_sync_artifact = tracker.db.sync_artifact

    def counting_flush_now() -> None:
        nonlocal flush_now_calls
        flush_now_calls += 1
        original_flush_now()

    def counting_sync_artifacts(*, artifacts, run_id, direction) -> None:
        nonlocal sync_batch_calls
        sync_batch_calls += 1
        original_sync_artifacts(
            artifacts=artifacts,
            run_id=run_id,
            direction=direction,
        )

    def counting_sync_artifact(artifact, run_id, direction) -> None:
        nonlocal sync_single_calls
        sync_single_calls += 1
        original_sync_artifact(artifact, run_id, direction)

    with tracker.start_run("run_batch_log_artifacts", "test_model"):
        monkeypatch.setattr(tracker.persistence, "_flush_json_now", counting_flush_now)
        monkeypatch.setattr(tracker.db, "sync_artifacts", counting_sync_artifacts)
        monkeypatch.setattr(tracker.db, "sync_artifact", counting_sync_artifact)

        logged = tracker.log_artifacts(
            {"first": first, "second": second},
            batch="demo",
        )

        assert set(logged) == {"first", "second"}
        assert logged["first"].meta.get("batch") == "demo"
        assert logged["second"].meta.get("batch") == "demo"
        assert flush_now_calls == 1
        assert sync_batch_calls == 1
        assert sync_single_calls == 0

    assert [art.key for art in tracker.last_run.outputs] == ["first", "second"]

    with Session(tracker.engine) as session:
        links = session.exec(
            select(RunArtifactLink).where(
                RunArtifactLink.run_id == "run_batch_log_artifacts"
            )
        ).all()
    assert len(links) == 2


def test_log_artifact_single_call_keeps_immediate_sync(tracker, run_dir: Path, monkeypatch):
    output = run_dir / "single.csv"
    output.write_text("x,y\n1,2\n", encoding="utf-8")

    flush_now_calls = 0
    sync_batch_calls = 0
    sync_single_calls = 0

    original_flush_now = tracker.persistence._flush_json_now
    original_sync_artifacts = tracker.db.sync_artifacts
    original_sync_artifact = tracker.db.sync_artifact

    def counting_flush_now() -> None:
        nonlocal flush_now_calls
        flush_now_calls += 1
        original_flush_now()

    def counting_sync_artifacts(*, artifacts, run_id, direction) -> None:
        nonlocal sync_batch_calls
        sync_batch_calls += 1
        original_sync_artifacts(
            artifacts=artifacts,
            run_id=run_id,
            direction=direction,
        )

    def counting_sync_artifact(artifact, run_id, direction) -> None:
        nonlocal sync_single_calls
        sync_single_calls += 1
        original_sync_artifact(artifact, run_id, direction)

    with tracker.start_run("run_single_log_artifact", "test_model"):
        monkeypatch.setattr(tracker.persistence, "_flush_json_now", counting_flush_now)
        monkeypatch.setattr(tracker.db, "sync_artifacts", counting_sync_artifacts)
        monkeypatch.setattr(tracker.db, "sync_artifact", counting_sync_artifact)

        artifact = tracker.log_artifact(output, key="single", direction="output")

        assert artifact.key == "single"
        assert flush_now_calls == 1
        assert sync_batch_calls == 0
        assert sync_single_calls == 1
