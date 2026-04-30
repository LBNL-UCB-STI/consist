from pathlib import Path

import pytest
from sqlmodel import Session, select

from consist.core.config_canonicalization import (
    ArtifactSpec,
    CanonicalConfigIdentity,
    ConfigContribution,
)
from consist.models.run import RunArtifactLink
from consist.types import ExecutionOptions


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

    def counting_sync_artifact(
        artifact, run_id, direction, *, profile_label=None
    ) -> None:
        nonlocal sync_single_calls
        sync_single_calls += 1
        original_sync_artifact(artifact, run_id, direction, profile_label=profile_label)

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


def test_log_artifact_single_call_keeps_immediate_sync(
    tracker, run_dir: Path, monkeypatch
):
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

    seen_profile_labels: list[str | None] = []

    def counting_sync_artifact(
        artifact, run_id, direction, *, profile_label=None
    ) -> None:
        nonlocal sync_single_calls
        sync_single_calls += 1
        seen_profile_labels.append(profile_label)
        original_sync_artifact(artifact, run_id, direction, profile_label=profile_label)

    with tracker.start_run("run_single_log_artifact", "test_model"):
        monkeypatch.setattr(tracker.persistence, "_flush_json_now", counting_flush_now)
        monkeypatch.setattr(tracker.db, "sync_artifacts", counting_sync_artifacts)
        monkeypatch.setattr(tracker.db, "sync_artifact", counting_sync_artifact)

        artifact = tracker.log_artifact(output, key="single", direction="output")

        assert artifact.key == "single"
        assert flush_now_calls == 1
        assert sync_batch_calls == 0
        assert sync_single_calls == 1
        assert seen_profile_labels == ["log_artifact:output"]


def test_begin_run_inputs_batch_flush_and_db_sync(tracker, run_dir: Path, monkeypatch):
    first = run_dir / "begin_input_first.csv"
    second = run_dir / "begin_input_second.csv"
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

    def counting_sync_artifact(
        artifact, run_id, direction, *, profile_label=None
    ) -> None:
        nonlocal sync_single_calls
        sync_single_calls += 1
        original_sync_artifact(artifact, run_id, direction, profile_label=profile_label)

    monkeypatch.setattr(tracker.persistence, "_flush_json_now", counting_flush_now)
    monkeypatch.setattr(tracker.db, "sync_artifacts", counting_sync_artifacts)
    monkeypatch.setattr(tracker.db, "sync_artifact", counting_sync_artifact)

    tracker.begin_run(
        "run_batch_begin_inputs",
        "test_model",
        inputs=[first, second],
        cache_mode="overwrite",
    )

    assert [artifact.key for artifact in tracker.current_consist.inputs] == [
        "begin_input_first",
        "begin_input_second",
    ]
    # begin_run() still performs its normal post-initialization flush after the
    # batched input logging flush. The optimization here is avoiding one flush
    # per input artifact.
    assert flush_now_calls == 2
    assert sync_batch_calls == 1
    assert sync_single_calls == 0

    tracker.end_run()

    with Session(tracker.engine) as session:
        links = session.exec(
            select(RunArtifactLink).where(
                RunArtifactLink.run_id == "run_batch_begin_inputs"
            )
        ).all()
    assert len(links) == 2


def test_tracker_run_output_paths_batch_db_sync(tracker, monkeypatch):
    sync_batch_calls = 0
    sync_single_calls = 0

    original_sync_artifacts = tracker.db.sync_artifacts
    original_sync_artifact = tracker.db.sync_artifact

    def counting_sync_artifacts(*, artifacts, run_id, direction) -> None:
        nonlocal sync_batch_calls
        sync_batch_calls += 1
        original_sync_artifacts(
            artifacts=artifacts,
            run_id=run_id,
            direction=direction,
        )

    def counting_sync_artifact(
        artifact, run_id, direction, *, profile_label=None
    ) -> None:
        nonlocal sync_single_calls
        sync_single_calls += 1
        original_sync_artifact(artifact, run_id, direction, profile_label=profile_label)

    monkeypatch.setattr(tracker.db, "sync_artifacts", counting_sync_artifacts)
    monkeypatch.setattr(tracker.db, "sync_artifact", counting_sync_artifact)

    def produce(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "left.txt").write_text("left", encoding="utf-8")
        (ctx.run_dir / "right.txt").write_text("right", encoding="utf-8")

    result = tracker.run(
        fn=produce,
        output_paths={"left": "left.txt", "right": "right.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    assert set(result.outputs) == {"left", "right"}
    assert sync_batch_calls == 1
    assert sync_single_calls == 0


def test_tracker_run_inferred_dict_outputs_batch_db_sync(tracker, monkeypatch):
    sync_batch_calls = 0
    sync_single_calls = 0

    original_sync_artifacts = tracker.db.sync_artifacts
    original_sync_artifact = tracker.db.sync_artifact

    def counting_sync_artifacts(*, artifacts, run_id, direction) -> None:
        nonlocal sync_batch_calls
        sync_batch_calls += 1
        original_sync_artifacts(
            artifacts=artifacts,
            run_id=run_id,
            direction=direction,
        )

    def counting_sync_artifact(
        artifact, run_id, direction, *, profile_label=None
    ) -> None:
        nonlocal sync_single_calls
        sync_single_calls += 1
        original_sync_artifact(artifact, run_id, direction, profile_label=profile_label)

    monkeypatch.setattr(tracker.db, "sync_artifacts", counting_sync_artifacts)
    monkeypatch.setattr(tracker.db, "sync_artifact", counting_sync_artifact)

    def produce(ctx):
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        left = ctx.run_dir / "left.txt"
        right = ctx.run_dir / "right.txt"
        left.write_text("left", encoding="utf-8")
        right.write_text("right", encoding="utf-8")
        return {"left": left, "right": right}

    result = tracker.run(
        fn=produce,
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    assert set(result.outputs) == {"left", "right"}
    assert sync_batch_calls == 1
    assert sync_single_calls == 0


def test_log_h5_container_batches_table_sync(tracker, run_dir: Path, monkeypatch):
    h5py = pytest.importorskip("h5py")

    h5_file = run_dir / "batched_tables.h5"
    with h5py.File(h5_file, "w") as handle:
        handle.create_dataset("a", data=[1, 2, 3])
        handle.create_dataset("b", data=[4, 5, 6])

    sync_batch_calls = 0
    sync_single_calls = 0

    original_sync_artifacts = tracker.db.sync_artifacts
    original_sync_artifact = tracker.db.sync_artifact

    def counting_sync_artifacts(*, artifacts, run_id, direction) -> None:
        nonlocal sync_batch_calls
        sync_batch_calls += 1
        original_sync_artifacts(
            artifacts=artifacts,
            run_id=run_id,
            direction=direction,
        )

    def counting_sync_artifact(
        artifact, run_id, direction, *, profile_label=None
    ) -> None:
        nonlocal sync_single_calls
        sync_single_calls += 1
        original_sync_artifact(artifact, run_id, direction, profile_label=profile_label)

    monkeypatch.setattr(tracker.db, "sync_artifacts", counting_sync_artifacts)
    monkeypatch.setattr(tracker.db, "sync_artifact", counting_sync_artifact)

    with tracker.start_run("run_h5_batched_tables", "test_model"):
        container, tables = tracker.log_h5_container(
            h5_file,
            key="batched_tables",
            discover_tables=True,
        )

    assert container.key == "batched_tables"
    assert len(tables) == 2
    assert sync_batch_calls == 1
    assert sync_single_calls == 0


def test_log_artifacts_with_facets_keeps_batched_artifact_sync(
    tracker, run_dir: Path, monkeypatch
):
    first = run_dir / "facet_first.csv"
    second = run_dir / "facet_second.csv"
    first.write_text("a,b\n1,2\n", encoding="utf-8")
    second.write_text("c,d\n3,4\n", encoding="utf-8")

    sync_batch_calls = 0
    sync_single_calls = 0
    combined_calls = 0
    facet_bundle_calls = 0

    original_sync_artifacts = tracker.db.sync_artifacts
    original_sync_artifact = tracker.db.sync_artifact
    original_sync_with_facet = tracker.db.sync_artifact_with_facet_bundle
    original_facet_bundle = tracker.db.persist_artifact_facet_bundle

    def counting_sync_artifacts(*, artifacts, run_id, direction) -> None:
        nonlocal sync_batch_calls
        sync_batch_calls += 1
        original_sync_artifacts(
            artifacts=artifacts,
            run_id=run_id,
            direction=direction,
        )

    def counting_sync_artifact(
        artifact, run_id, direction, *, profile_label=None
    ) -> None:
        nonlocal sync_single_calls
        sync_single_calls += 1
        original_sync_artifact(artifact, run_id, direction, profile_label=profile_label)

    def counting_sync_with_facet(*args, **kwargs) -> None:
        nonlocal combined_calls
        combined_calls += 1
        original_sync_with_facet(*args, **kwargs)

    def counting_facet_bundle(*, artifact, facet, meta_updates, kv_rows=None) -> None:
        nonlocal facet_bundle_calls
        facet_bundle_calls += 1
        original_facet_bundle(
            artifact=artifact,
            facet=facet,
            meta_updates=meta_updates,
            kv_rows=kv_rows,
        )

    monkeypatch.setattr(tracker.db, "sync_artifacts", counting_sync_artifacts)
    monkeypatch.setattr(tracker.db, "sync_artifact", counting_sync_artifact)
    monkeypatch.setattr(
        tracker.db, "sync_artifact_with_facet_bundle", counting_sync_with_facet
    )
    monkeypatch.setattr(
        tracker.db, "persist_artifact_facet_bundle", counting_facet_bundle
    )

    with tracker.start_run("run_batch_log_artifacts_facets", "test_model"):
        tracker.log_artifacts(
            {"first": first, "second": second},
            facets_by_key={
                "first": {"artifact_family": "demo", "ordinal": 1},
                "second": {"artifact_family": "demo", "ordinal": 2},
            },
            facet_index=True,
        )

    assert sync_batch_calls == 1
    assert sync_single_calls == 0
    assert combined_calls == 0
    assert facet_bundle_calls == 2


def test_apply_config_contribution_batches_artifact_sync(
    tracker, run_dir: Path, monkeypatch
):
    first = run_dir / "config_a.txt"
    second = run_dir / "config_b.txt"
    first.write_text("a=1\n", encoding="utf-8")
    second.write_text("b=2\n", encoding="utf-8")

    sync_batch_calls = 0
    sync_single_calls = 0

    original_sync_artifacts = tracker.db.sync_artifacts
    original_sync_artifact = tracker.db.sync_artifact

    def counting_sync_artifacts(*, artifacts, run_id, direction) -> None:
        nonlocal sync_batch_calls
        sync_batch_calls += 1
        original_sync_artifacts(
            artifacts=artifacts,
            run_id=run_id,
            direction=direction,
        )

    def counting_sync_artifact(
        artifact, run_id, direction, *, profile_label=None
    ) -> None:
        nonlocal sync_single_calls
        sync_single_calls += 1
        original_sync_artifact(artifact, run_id, direction, profile_label=profile_label)

    monkeypatch.setattr(tracker.db, "sync_artifacts", counting_sync_artifacts)
    monkeypatch.setattr(tracker.db, "sync_artifact", counting_sync_artifact)

    contribution = ConfigContribution(
        identity=CanonicalConfigIdentity(
            adapter_name="test",
            adapter_version="test-adapter",
            primary_config=None,
            identity_hash="config-batch",
        ),
        adapter_version="test-adapter",
        artifacts=[
            ArtifactSpec(first, "config_a", "input", {"kind": "config"}),
            ArtifactSpec(second, "config_b", "input", {"kind": "config"}),
        ],
        ingestables=[],
    )

    with tracker.start_run("run_config_contribution_batch", "test_model"):
        tracker._apply_config_contribution(
            contribution,
            run=tracker.current_consist.run,
            ingest=False,
            profile_schema=False,
        )

    assert sync_batch_calls == 1
    assert sync_single_calls == 0
