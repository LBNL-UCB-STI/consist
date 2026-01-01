import logging
from pathlib import Path

import pandas as pd
import pytest
from sqlmodel import SQLModel

from consist.core.cache import ActiveRunCacheOptions, hydrate_cache_hit_outputs
from consist.core.materialize import (
    build_materialize_items_for_keys,
    materialize_artifacts,
    materialize_artifacts_from_sources,
)
from consist.core.tracker import Tracker
from consist.models.artifact import Artifact
from consist.models.artifact_schema import (
    ArtifactSchema,
    ArtifactSchemaField,
    ArtifactSchemaObservation,
)
from consist.models.config_facet import ConfigFacet
from consist.models.run import ConsistRecord, Run, RunArtifactLink, RunArtifacts
from consist.models.run_config_kv import RunConfigKV


def _init_core_tables(tracker: Tracker) -> None:
    core_tables = [
        getattr(Run, "__table__"),
        getattr(Artifact, "__table__"),
        getattr(RunArtifactLink, "__table__"),
        getattr(ConfigFacet, "__table__"),
        getattr(RunConfigKV, "__table__"),
        getattr(ArtifactSchema, "__table__"),
        getattr(ArtifactSchemaField, "__table__"),
        getattr(ArtifactSchemaObservation, "__table__"),
    ]
    if tracker.engine:
        with tracker.engine.connect() as connection:
            with connection.begin():
                SQLModel.metadata.create_all(connection, tables=core_tables)
                if tracker.db:
                    tracker.db._relax_run_parent_fk()


def test_cache_hydration_policies_end_to_end(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Consolidated cache hydration coverage (behavior + failure modes).

    We deliberately simulate the two-run-dir scenario that motivated the feature:
    - run_dir_a produces cached outputs (the original producer run_dir).
    - run_dir_b consumes the cached outputs (a new run_dir).

    The test is both a regression suite and narrative documentation of behavior:
    1) metadata: cache hits hydrate Artifact objects only (no bytes copied).
    2) outputs-requested: cache hits materialize only selected outputs to caller paths.
    3) outputs-all: cache hits materialize all outputs to a caller-provided directory.
    4) inputs-missing: cache misses copy missing inputs into the current run_dir
       before execution, so downstream code can read bytes without rerunning cached steps.
    5) invalid combinations of cache_hydration + args raise ValueError early.
    """
    caplog.set_level(logging.WARNING)
    db_path = str(tmp_path / "provenance.db")
    run_dir_a = tmp_path / "runs_a"
    run_dir_b = tmp_path / "runs_b"

    tracker_a = Tracker(run_dir=run_dir_a, db_path=db_path)
    _init_core_tables(tracker_a)

    # Seed a producer run with two CSV outputs that will be reused as cached artifacts.
    with tracker_a.start_run("producer", model="producer", cache_mode="overwrite"):
        out_dir = tracker_a.run_dir / "outputs"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "a.csv").write_text("value\n1\n")
        (out_dir / "b.csv").write_text("value\n2\n")
        tracker_a.log_artifact(out_dir / "a.csv", key="a", direction="output")
        tracker_a.log_artifact(out_dir / "b.csv", key="b", direction="output")

    cached_outputs = tracker_a.get_artifacts_for_run("producer").outputs
    bundle_outputs = tracker_a.load_input_bundle("producer")
    assert set(bundle_outputs.keys()) == set(cached_outputs.keys())
    with pytest.raises(ValueError):
        tracker_a.load_input_bundle("missing_bundle")
    cached_a = cached_outputs["a"]
    cached_b = cached_outputs["b"]

    # Cache-hydration = metadata: cache hits hydrate artifacts only (no bytes copied).
    tracker_b = Tracker(run_dir=run_dir_b, db_path=db_path)
    _init_core_tables(tracker_b)
    with tracker_b.start_run("meta_hit", model="producer", cache_mode="reuse"):
        pass
    assert not (tracker_b.run_dir / "a.csv").exists()
    assert not (tracker_b.run_dir / "b.csv").exists()

    # Cache-hydration = outputs-requested: copy only selected outputs on cache hits.
    # Missing keys should log a warning, not raise.
    caplog.clear()
    requested_dest = tmp_path / "requested_a.csv"
    if requested_dest.exists():
        requested_dest.unlink()
    requested_meta = None
    with tracker_b.start_run(
        "requested_hit",
        model="producer",
        cache_mode="reuse",
        cache_hydration="outputs-requested",
        materialize_cached_output_paths={
            "a": requested_dest,
            "missing_key": tmp_path / "missing.txt",
        },
    ) as t:
        assert t.is_cached
        requested_meta = dict(
            (t.current_consist.run.meta or {}).get("materialized_outputs", {})
        )
    assert requested_dest.exists()
    assert requested_dest.read_text() == "value\n1\n"
    assert not (tmp_path / "requested_b.txt").exists()
    assert any("missing keys" in record.message for record in caplog.records)
    assert requested_meta == {"a": str(requested_dest.resolve())}

    # Cache-hydration = outputs-all: copy all outputs to a requested directory.
    all_dir = tmp_path / "materialized_all"
    if all_dir.exists():
        for path in all_dir.iterdir():
            if path.is_file():
                path.unlink()
    else:
        all_dir.mkdir(parents=True, exist_ok=True)
    all_meta = None
    with tracker_b.start_run(
        "all_hit",
        model="producer",
        cache_mode="reuse",
        cache_hydration="outputs-all",
        materialize_cached_outputs_dir=all_dir,
    ) as t:
        assert t.is_cached
        all_meta = dict(
            (t.current_consist.run.meta or {}).get("materialized_outputs", {})
        )
    assert (all_dir / "a.csv").read_text() == "value\n1\n"
    assert (all_dir / "b.csv").read_text() == "value\n2\n"
    assert all_meta == {
        "a": str((all_dir / "a.csv").resolve()),
        "b": str((all_dir / "b.csv").resolve()),
    }

    # Cache-hydration = inputs-missing: copy missing inputs before executing a cache miss.
    # We simulate a cache miss by changing the model name, while keeping inputs the same.
    expected_input_a = tracker_b.run_dir / "outputs" / "a.csv"
    expected_input_b = tracker_b.run_dir / "outputs" / "b.csv"
    if expected_input_a.exists():
        expected_input_a.unlink()
    if expected_input_b.exists():
        expected_input_b.unlink()

    with tracker_b.start_run(
        "consumer_miss",
        model="consumer",
        inputs=[cached_a, cached_b],
        cache_mode="reuse",
        cache_hydration="inputs-missing",
    ):
        # Missing inputs should be copied into the new run_dir before execution.
        assert expected_input_a.exists()
        assert expected_input_b.exists()
        assert expected_input_a.read_text() == "value\n1\n"
        assert expected_input_b.read_text() == "value\n2\n"

    # Multi-model usage from a single bundle: each run selects its own subset of inputs.
    expected_input_b.unlink()
    with tracker_b.start_run(
        "model_a",
        model="model_a",
        inputs=[bundle_outputs["a"]],
        cache_mode="reuse",
        cache_hydration="inputs-missing",
    ):
        assert (tracker_b.run_dir / "outputs" / "a.csv").exists()
        assert not (tracker_b.run_dir / "outputs" / "b.csv").exists()

    expected_input_a.unlink()
    with tracker_b.start_run(
        "model_b",
        model="model_b",
        inputs=[bundle_outputs["b"]],
        cache_mode="reuse",
        cache_hydration="inputs-missing",
    ):
        assert (tracker_b.run_dir / "outputs" / "b.csv").exists()

    # Sanity check: core materialize helper still works for direct copies.
    dest_manual = tmp_path / "manual_copy.csv"
    result = materialize_artifacts(
        tracker=tracker_a, items=[(cached_a, dest_manual)], on_missing="raise"
    )
    assert dest_manual.exists()
    assert result["a"] == str(dest_manual.resolve())

    # DB-backed inputs-missing: ingested CSVs should be reconstructed from DuckDB
    # when the original run_dir files are gone.
    tracker_a.ingest(cached_a)
    tracker_a.ingest(cached_b)
    cached_outputs_ingested = tracker_a.get_artifacts_for_run("producer").outputs
    cached_a_ingested = cached_outputs_ingested["a"]
    cached_b_ingested = cached_outputs_ingested["b"]

    (run_dir_a / "outputs" / "a.csv").unlink()
    (run_dir_a / "outputs" / "b.csv").unlink()

    if expected_input_a.exists():
        expected_input_a.unlink()
    if expected_input_b.exists():
        expected_input_b.unlink()

    with tracker_b.start_run(
        "consumer_miss_db",
        model="consumer_db",
        inputs=[cached_a_ingested, cached_b_ingested],
        cache_mode="reuse",
        cache_hydration="inputs-missing",
    ):
        assert expected_input_a.exists()
        assert expected_input_b.exists()
        df_a = pd.read_csv(expected_input_a)
        df_b = pd.read_csv(expected_input_b)
        assert "value" in df_a.columns
        assert "value" in df_b.columns

    # Outputs-all should fail loudly when a cached source is missing.
    with pytest.raises(FileNotFoundError):
        try:
            with tracker_b.start_run(
                "all_hit_missing_source",
                model="producer",
                cache_mode="reuse",
                cache_hydration="outputs-all",
                materialize_cached_outputs_dir=tmp_path / "materialized_all_missing",
            ):
                pass
        finally:
            # begin_run can raise before the context manager yields; ensure cleanup
            # so subsequent runs are not blocked by a dangling active run.
            if tracker_b.current_consist:
                tracker_b.end_run(status="failed")

    # Inputs-missing should raise for ingested artifacts with unsupported drivers.
    bad_artifact = Artifact(
        key="unsupported",
        uri="./outputs/unsupported.txt",
        driver="txt",
        run_id=cached_a.run_id,
        meta={"is_ingested": True},
    )
    with pytest.raises(ValueError):
        try:
            with tracker_b.start_run(
                "consumer_miss_unsupported",
                model="consumer_bad",
                inputs=[bad_artifact],
                cache_mode="reuse",
                cache_hydration="inputs-missing",
            ):
                pass
        finally:
            if tracker_b.current_consist:
                tracker_b.end_run(status="failed")

    # Invalid option combinations should raise ValueError early.
    with pytest.raises(ValueError):
        with tracker_b.start_run(
            "invalid_missing_paths",
            model="producer",
            cache_mode="reuse",
            cache_hydration="outputs-requested",
        ):
            pass

    with pytest.raises(ValueError):
        with tracker_b.start_run(
            "invalid_missing_dir",
            model="producer",
            cache_mode="reuse",
            cache_hydration="outputs-all",
        ):
            pass

    with pytest.raises(ValueError):
        with tracker_b.start_run(
            "invalid_metadata_with_paths",
            model="producer",
            cache_mode="reuse",
            cache_hydration="metadata",
            materialize_cached_output_paths={"a": tmp_path / "unused.txt"},
        ):
            pass

    with pytest.raises(ValueError):
        with tracker_b.start_run(
            "invalid_outputs_requested_with_dir",
            model="producer",
            cache_mode="reuse",
            cache_hydration="outputs-requested",
            materialize_cached_output_paths={"a": tmp_path / "unused.txt"},
            materialize_cached_outputs_dir=tmp_path / "unused_dir",
        ):
            pass

    with pytest.raises(ValueError):
        with tracker_b.start_run(
            "invalid_outputs_all_with_paths",
            model="producer",
            cache_mode="reuse",
            cache_hydration="outputs-all",
            materialize_cached_outputs_dir=tmp_path / "unused_dir",
            materialize_cached_output_paths={"a": tmp_path / "unused.txt"},
        ):
            pass

    with pytest.raises(ValueError):
        with tracker_b.start_run(
            "invalid_cache_hydration_value",
            model="producer",
            cache_mode="reuse",
            cache_hydration="not-a-policy",
        ):
            pass

    if tracker_a.engine:
        tracker_a.engine.dispose()
    if tracker_b.engine:
        tracker_b.engine.dispose()


def test_build_materialize_items_and_materialize_from_sources(tmp_path: Path) -> None:
    """
    Build materialization items by key and copy from explicit sources.

    This covers the helper that maps keys to artifacts and the copy routine
    used for historical run recovery.
    """
    source_dir = tmp_path / "sources"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_file = source_dir / "source.txt"
    source_file.write_text("payload", encoding="utf-8")

    artifact = Artifact(
        key="alpha",
        uri="./source.txt",
        driver="txt",
        meta={},
    )
    missing_artifact = Artifact(
        key="missing",
        uri="./missing.txt",
        driver="txt",
        meta={},
    )

    items = build_materialize_items_for_keys(
        [artifact, missing_artifact],
        destinations_by_key={"alpha": tmp_path / "dest.txt", "unknown": tmp_path / "x"},
    )
    assert items == [(artifact, tmp_path / "dest.txt")]

    materialized = materialize_artifacts_from_sources(
        [(artifact, source_file, tmp_path / "dest.txt")],
        on_missing="raise",
    )
    assert materialized == {"alpha": str((tmp_path / "dest.txt").resolve())}
    assert (tmp_path / "dest.txt").read_text(encoding="utf-8") == "payload"


def test_outputs_requested_permission_denied_warns_and_continues(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Permission errors during output materialization should warn and continue.
    """
    caplog.set_level(logging.WARNING)
    db_path = str(tmp_path / "provenance.db")
    run_dir = tmp_path / "runs"
    tracker = Tracker(run_dir=run_dir, db_path=db_path)
    _init_core_tables(tracker)

    with tracker.start_run("seed", model="model", cache_mode="overwrite"):
        out_dir = tracker.run_dir / "outputs"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "out.csv"
        out_path.write_text("value\n1\n", encoding="utf-8")
        tracker.log_artifact(out_path, key="out", direction="output")

    def _raise_permission(*_args, **_kwargs):
        raise PermissionError("denied")

    monkeypatch.setattr("consist.core.materialize.shutil.copy2", _raise_permission)

    dest_path = tmp_path / "dest.csv"
    with tracker.start_run(
        "hit",
        model="model",
        cache_mode="reuse",
        cache_hydration="outputs-requested",
        materialize_cached_output_paths={"out": dest_path},
    ):
        pass

    assert not dest_path.exists()
    run = tracker.get_run("hit")
    assert run is not None
    assert "materialized_outputs" not in (run.meta or {})
    assert any(
        "Failed to materialize cached input" in record.message
        for record in caplog.records
    )

    if tracker.engine:
        tracker.engine.dispose()


def test_inputs_missing_permission_denied_warns_and_continues(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Permission errors during inputs-missing materialization should warn and continue.
    """
    caplog.set_level(logging.WARNING)
    db_path = str(tmp_path / "provenance.db")
    run_dir_a = tmp_path / "runs_a"
    run_dir_b = tmp_path / "runs_b"

    tracker_a = Tracker(run_dir=run_dir_a, db_path=db_path)
    _init_core_tables(tracker_a)
    tracker_b = Tracker(run_dir=run_dir_b, db_path=db_path)
    _init_core_tables(tracker_b)

    with tracker_a.start_run("seed", model="model", cache_mode="overwrite"):
        out_dir = tracker_a.run_dir / "outputs"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "out.csv"
        out_path.write_text("value\n1\n", encoding="utf-8")
        artifact = tracker_a.log_artifact(out_path, key="out", direction="output")

    def _raise_permission(*_args, **_kwargs):
        raise PermissionError("denied")

    monkeypatch.setattr("consist.core.materialize.shutil.copy2", _raise_permission)

    with tracker_b.start_run(
        "consumer",
        model="consumer",
        inputs=[artifact],
        cache_mode="reuse",
        cache_hydration="inputs-missing",
    ):
        pass

    dest_path = tracker_b.run_dir / "outputs" / "out.csv"
    assert not dest_path.exists()
    run = tracker_b.get_run("consumer")
    assert run is not None
    assert "materialized_inputs" not in (run.meta or {})
    assert any(
        "Failed to materialize cached input" in record.message
        for record in caplog.records
    )

    if tracker_a.engine:
        tracker_a.engine.dispose()
    if tracker_b.engine:
        tracker_b.engine.dispose()


def test_outputs_requested_warns_on_stale_mount_source(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Stale mounts should warn and skip materialization in outputs-requested mode.
    """
    caplog.set_level(logging.WARNING)
    db_path = str(tmp_path / "provenance.db")
    run_dir_a = tmp_path / "runs_a"
    run_dir_b = tmp_path / "runs_b"
    mount_a = tmp_path / "mount_a"
    mount_b = tmp_path / "mount_b"
    mount_a.mkdir(parents=True, exist_ok=True)
    mount_b.mkdir(parents=True, exist_ok=True)

    source_path = mount_a / "a.csv"
    source_path.write_text("value\n1\n", encoding="utf-8")

    tracker_a = Tracker(
        run_dir=run_dir_a, db_path=db_path, mounts={"outputs": str(mount_a)}
    )
    _init_core_tables(tracker_a)
    with tracker_a.start_run("producer", model="producer", cache_mode="overwrite"):
        tracker_a.log_artifact(source_path, key="a", direction="output")

    tracker_b = Tracker(
        run_dir=run_dir_b, db_path=db_path, mounts={"outputs": str(mount_b)}
    )
    _init_core_tables(tracker_b)

    dest = tmp_path / "dest.csv"
    with tracker_b.start_run(
        "requested_hit",
        model="producer",
        cache_mode="reuse",
        cache_hydration="outputs-requested",
        materialize_cached_output_paths={"a": dest},
    ) as t:
        assert t.is_cached

    assert not dest.exists()
    assert any(
        "Cannot materialize cached input" in record.message
        for record in caplog.records
    )


def test_outputs_requested_warns_on_moved_run_dir(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Moved run directories should warn and skip materialization in outputs-requested mode.
    """
    caplog.set_level(logging.WARNING)
    db_path = str(tmp_path / "provenance.db")
    run_dir_a = tmp_path / "runs_a"
    run_dir_b = tmp_path / "runs_b"

    tracker_a = Tracker(run_dir=run_dir_a, db_path=db_path)
    _init_core_tables(tracker_a)
    with tracker_a.start_run("producer", model="producer", cache_mode="overwrite"):
        out_dir = tracker_a.run_dir / "outputs"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "out.csv"
        out_path.write_text("value\n1\n", encoding="utf-8")
        tracker_a.log_artifact(out_path, key="out", direction="output")

    moved_dir = tmp_path / "runs_moved"
    run_dir_a.rename(moved_dir)

    tracker_b = Tracker(run_dir=run_dir_b, db_path=db_path)
    _init_core_tables(tracker_b)

    dest = tmp_path / "dest.csv"
    with tracker_b.start_run(
        "requested_hit",
        model="producer",
        cache_mode="reuse",
        cache_hydration="outputs-requested",
        materialize_cached_output_paths={"out": dest},
    ) as t:
        assert t.is_cached

    assert not dest.exists()
    assert any(
        "Cannot materialize cached input" in record.message
        for record in caplog.records
    )
    run = tracker_b.get_run("requested_hit")
    assert run is not None
    assert "materialized_outputs" not in (run.meta or {})

    if tracker_a.engine:
        tracker_a.engine.dispose()
    if tracker_b.engine:
        tracker_b.engine.dispose()

    if tracker_a.engine:
        tracker_a.engine.dispose()
    if tracker_b.engine:
        tracker_b.engine.dispose()


def test_hydrate_cache_hit_outputs_records_materialized_outputs_meta(
    tmp_path: Path,
) -> None:
    """
    hydrate_cache_hit_outputs should record materialized output destinations in run.meta.
    """
    tracker = Tracker(run_dir=tmp_path)

    cached_run_dir = tmp_path / "cached_run"
    cached_outputs = cached_run_dir / "outputs"
    cached_outputs.mkdir(parents=True, exist_ok=True)
    source_path = cached_outputs / "a.csv"
    source_path.write_text("value\n1\n", encoding="utf-8")

    artifact = Artifact(
        key="a",
        uri="./outputs/a.csv",
        driver="csv",
        run_id="cached",
        meta={},
    )
    run = Run(id="active", model_name="model", meta={})
    cached_run = Run(
        id="cached",
        model_name="model",
        meta={"_physical_run_dir": str(cached_run_dir)},
    )

    tracker.current_consist = ConsistRecord(run=run, config={})
    tracker.get_artifacts_for_run = lambda _run_id: RunArtifacts(
        inputs={}, outputs={"a": artifact}
    )

    dest = tmp_path / "requested_a.csv"
    options = ActiveRunCacheOptions(
        cache_hydration="outputs-requested",
        materialize_cached_output_paths={"a": dest},
    )

    hydrate_cache_hit_outputs(
        tracker=tracker,
        run=run,
        cached_run=cached_run,
        options=options,
        link_outputs=False,
    )

    assert dest.exists()
    assert run.meta["materialized_outputs"] == {"a": str(dest.resolve())}
