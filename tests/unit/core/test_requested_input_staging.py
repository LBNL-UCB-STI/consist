from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from consist.core.cache import ActiveRunCacheOptions, materialize_requested_inputs
from consist.core.directory_artifacts import build_directory_manifest
from consist.core.tracker_orchestration import RunTraceCoordinator, RunTraceHelpers
from consist.digest import Digest
from consist.models.artifact import Artifact
from consist.models.run import ConsistRecord, Run
from consist.types import BindingResult, ExecutionOptions


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _make_run(run_id: str = "run_001") -> Run:
    now = datetime.now(timezone.utc)
    return Run(
        id=run_id,
        model_name="model",
        status="running",
        config_hash="config",
        git_hash="git",
        started_at=now,
        created_at=now,
        updated_at=now,
        meta={},
    )


class _FakeTracker:
    def __init__(
        self,
        *,
        run_dir: Path,
        current_consist: ConsistRecord | None = None,
        compute_file_checksum: Callable[[Path], str] = _sha256,
    ):
        self.current_consist = current_consist
        self.db = None
        self.fs = SimpleNamespace(
            resolve_historical_path=lambda uri, original_run_dir, **kwargs: uri,
            normalize_recovery_roots=lambda roots, **kwargs: [],
            get_remappable_relative_path=lambda uri, **kwargs: None,
            get_historical_root=lambda *args, **kwargs: None,
        )
        self.identity = SimpleNamespace(
            compute_file_checksum=compute_file_checksum,
            hashing_strategy="full",
        )
        self.run_dir = run_dir
        self.mounts = {}
        self.allow_external_paths = False

    def resolve_uri(self, uri: str) -> str:
        return uri

    def run_artifact_dir(self) -> Path:
        return self.run_dir


def test_materialize_requested_inputs_stages_requested_key(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("value\n1\n", encoding="utf-8")
    destination = tmp_path / "staged" / "raw.csv"

    artifact = Artifact(
        key="raw",
        uri=str(source),
        driver="csv",
        digest=Digest("sha256:file", _sha256(source)),
        meta={},
    )
    run = _make_run()

    tracker = _FakeTracker(
        run_dir=tmp_path,
        current_consist=ConsistRecord(run=run, inputs=[artifact]),
    )

    staged = materialize_requested_inputs(
        tracker=tracker,
        options=ActiveRunCacheOptions(
            requested_input_paths={"raw": destination},
            requested_input_materialization="requested",
        ),
    )

    assert staged == {"raw": str(destination.resolve())}
    assert destination.read_text(encoding="utf-8") == "value\n1\n"
    assert tracker.current_consist.inputs[0].abs_path == str(destination.resolve())


def test_materialize_requested_inputs_rejects_distinct_artifacts_for_one_key(
    tmp_path: Path,
) -> None:
    first_source = tmp_path / "first.csv"
    first_source.write_text("value\n1\n", encoding="utf-8")
    second_source = tmp_path / "second.csv"
    second_source.write_text("value\n2\n", encoding="utf-8")
    destination = tmp_path / "staged" / "raw.csv"
    first = Artifact(
        key="raw",
        uri=str(first_source),
        driver="csv",
        digest=Digest("sha256:file", _sha256(first_source)),
        meta={},
    )
    second = Artifact(
        key="raw",
        uri=str(second_source),
        driver="csv",
        digest=Digest("sha256:file", _sha256(second_source)),
        meta={},
    )
    tracker = _FakeTracker(
        run_dir=tmp_path,
        current_consist=ConsistRecord(run=_make_run(), inputs=[first, second]),
    )

    with pytest.raises(ValueError, match="is ambiguous"):
        materialize_requested_inputs(
            tracker=tracker,
            options=ActiveRunCacheOptions(
                requested_input_paths={"raw": destination},
                requested_input_materialization="requested",
            ),
        )


def test_materialize_requested_inputs_stages_manifest_backed_directory(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.zarr"
    source.mkdir()
    (source / "0.0").write_bytes(b"skim")
    destination = tmp_path / "staged" / "skims.zarr"
    manifest = build_directory_manifest(source)

    artifact = Artifact(
        key="skims",
        uri=str(source),
        driver="zarr",
        digest=Digest("dir_manifest_v1:sha256", manifest["tree_hash"]),
        meta={"directory_artifact": True, "directory_manifest": manifest},
    )
    tracker = _FakeTracker(
        run_dir=tmp_path,
        current_consist=ConsistRecord(run=_make_run(), inputs=[artifact]),
        compute_file_checksum=lambda _path: "legacy-directory-checksum",
    )

    staged = materialize_requested_inputs(
        tracker=tracker,
        options=ActiveRunCacheOptions(
            requested_input_paths={"skims": destination},
            requested_input_materialization="requested",
        ),
    )

    assert staged == {"skims": str(destination.resolve())}
    assert build_directory_manifest(destination)["tree_hash"] == artifact.digest.hex


def test_materialize_requested_inputs_rejects_mutated_manifest_directory(
    tmp_path: Path,
) -> None:
    destination = tmp_path / "staged" / "skims.zarr"
    destination.mkdir(parents=True)
    member = destination / "0.0"
    member.write_bytes(b"original")
    manifest = build_directory_manifest(destination)
    member.write_bytes(b"mutated")

    artifact = Artifact(
        key="skims",
        uri=str(destination),
        driver="zarr",
        digest=Digest("dir_manifest_v1:sha256", manifest["tree_hash"]),
        meta={"directory_artifact": True, "directory_manifest": manifest},
    )
    tracker = _FakeTracker(
        run_dir=tmp_path,
        current_consist=ConsistRecord(run=_make_run(), inputs=[artifact]),
        compute_file_checksum=lambda _path: "legacy-directory-checksum",
    )

    with pytest.raises(ValueError, match="Hash mismatch"):
        materialize_requested_inputs(
            tracker=tracker,
            options=ActiveRunCacheOptions(
                requested_input_paths={"skims": destination},
                requested_input_materialization="requested",
            ),
        )


def test_materialize_requested_inputs_preserves_legacy_directory_checksum(
    tmp_path: Path,
) -> None:
    destination = tmp_path / "staged" / "legacy.zarr"
    destination.mkdir(parents=True)
    (destination / "0.0").write_bytes(b"legacy")
    from consist.digest import build_manifest_v1

    legacy_manifest = build_manifest_v1(destination)
    artifact = Artifact(
        key="legacy",
        uri=str(tmp_path / "missing.zarr"),
        driver="zarr",
        digest=Digest("dir_manifest_v1:sha256", legacy_manifest["tree_hash"]),
        meta={},
    )
    tracker = _FakeTracker(
        run_dir=tmp_path,
        current_consist=ConsistRecord(run=_make_run(), inputs=[artifact]),
        compute_file_checksum=lambda _path: "legacy-directory-checksum",
    )

    staged = materialize_requested_inputs(
        tracker=tracker,
        options=ActiveRunCacheOptions(
            requested_input_paths={"legacy": destination},
            requested_input_materialization="requested",
        ),
    )

    assert staged == {"legacy": str(destination.resolve())}


def test_execute_python_run_uses_staged_input_path(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("source\n", encoding="utf-8")
    staged = tmp_path / "staged" / "raw.csv"
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.write_text("staged\n", encoding="utf-8")

    artifact = Artifact(
        key="raw",
        uri=str(source),
        driver="csv",
        digest=Digest("sha256:file", _sha256(staged)),
        meta={},
    ).with_abs_path(staged)

    tracker = _FakeTracker(run_dir=tmp_path)

    coordinator = RunTraceCoordinator(
        tracker=tracker,
        helpers=RunTraceHelpers(
            resolve_input_refs=lambda *args, **kwargs: ([], {}),
            preview_run_artifact_dir=lambda *args, **kwargs: tmp_path,
            resolve_output_path=lambda *args, **kwargs: tmp_path / "out",
            is_xarray_dataset=lambda value: False,
            write_xarray_dataset=lambda *args, **kwargs: None,
        ),
    )

    def fn(raw: Path) -> None:
        assert raw == staged
        assert raw.read_text(encoding="utf-8") == "staged\n"

    result, captured = coordinator._execute_python_run(
        tracker=tracker,
        active_tracker=tracker,
        fn=fn,
        resolved_name="step",
        config=None,
        inputs={"raw": artifact},
        runtime_kwargs_dict=None,
        inject_context=None,
        input_binding="paths",
        input_artifacts_by_key={"raw": artifact},
        requested_input_paths={"raw": staged},
        capture_dir=None,
        capture_pattern="*",
    )

    assert result is None
    assert captured == {}


def test_tracker_run_stages_requested_input_paths(tracker, tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("value\n42\n", encoding="utf-8")
    destination = tracker.run_dir / "staged" / "raw.csv"

    def fn(raw: Path) -> None:
        assert raw == destination.resolve()
        assert raw.read_text(encoding="utf-8") == "value\n42\n"

    result = tracker.run(
        fn=fn,
        name="requested_input_staging",
        inputs={"raw": source},
        execution_options=ExecutionOptions(
            input_binding="paths",
            input_materialization="requested",
            input_paths={"raw": destination},
        ),
    )

    assert result.cache_hit is False
    assert result.run.meta["staged_inputs"] == {"raw": str(destination.resolve())}
    assert destination.read_text(encoding="utf-8") == "value\n42\n"


def test_scenario_binding_stages_requested_input_by_binding_key(
    tracker, tmp_path: Path
) -> None:
    source = tmp_path / "bootstrap.h5"
    source.write_bytes(b"UrbanSim bootstrap")
    destination = tracker.run_dir / "staged" / "usim_datastore.h5"

    tracker.begin_run("bootstrap", "model")
    artifact = tracker.log_artifact(
        source,
        key="usim_datastore_base_h5",
        direction="output",
    )
    tracker.end_run(status="completed")

    received: list[Path] = []

    def consume(usim_datastore_h5: Path) -> None:
        received.append(usim_datastore_h5)
        assert usim_datastore_h5.read_bytes() == b"UrbanSim bootstrap"

    with tracker.scenario("requested_input_binding_alias") as scenario:
        result = scenario.run(
            fn=consume,
            binding=BindingResult(inputs={"usim_datastore_h5": artifact}),
            execution_options=ExecutionOptions(
                input_binding="paths",
                input_materialization="requested",
                input_paths={"usim_datastore_h5": destination},
            ),
        )

    assert result.cache_hit is False
    assert received == [destination.resolve()]
    assert destination.read_bytes() == b"UrbanSim bootstrap"


def test_requested_input_staging_rejects_unknown_binding_key(
    tracker, tmp_path: Path
) -> None:
    source = tmp_path / "bootstrap.h5"
    source.write_bytes(b"UrbanSim bootstrap")

    tracker.begin_run("bootstrap", "model")
    artifact = tracker.log_artifact(
        source,
        key="usim_datastore_base_h5",
        direction="output",
    )
    tracker.end_run(status="completed")

    called = False

    def consume(usim_datastore_h5: Path) -> None:
        nonlocal called
        called = True

    with tracker.scenario("requested_input_unknown_key") as scenario:
        with pytest.raises(ValueError, match="not present in the resolved inputs"):
            scenario.run(
                fn=consume,
                binding=BindingResult(inputs={"usim_datastore_h5": artifact}),
                execution_options=ExecutionOptions(
                    input_binding="paths",
                    input_materialization="requested",
                    input_paths={"usim_datastore_base_h5": tmp_path / "staged.h5"},
                ),
            )

    assert called is False


def test_requested_input_staging_recovers_cold_scratch_from_recovery_roots(
    tracker, tmp_path: Path
) -> None:
    import shutil
    from consist.core.materialize import find_existing_recovery_source_path

    archive_dir = tmp_path / "archive"
    events_path = tracker.run_dir / "beam" / "output" / "0.events.parquet"
    events_path.parent.mkdir(parents=True, exist_ok=True)
    events_path.write_bytes(b"parquet-events-bytes")

    skims_dir = tracker.run_dir / "beam" / "output" / "0.skims.zarr"
    (skims_dir / "chunk").mkdir(parents=True, exist_ok=True)
    (skims_dir / "chunk" / "0.0").write_bytes(b"zarr-chunk-bytes")

    tracker.begin_run("beam_producer", "beam")
    events_artifact = tracker.log_artifact(
        events_path,
        key="events",
        direction="output",
    )
    skims_artifact = tracker.log_output(
        skims_dir,
        key="skims",
        artifact_kind="directory",
        driver="zarr",
    )
    tracker.end_run(status="completed")

    tracker.archive_artifact(events_artifact, archive_dir, mode="copy")
    tracker.archive_artifact(skims_artifact, archive_dir, mode="copy")

    # Simulate cold-scratch wipe of producer outputs
    events_path.unlink()
    shutil.rmtree(skims_dir)

    # Verify find_existing_recovery_source_path also works when run=None
    _, recovered_null_run, _ = find_existing_recovery_source_path(
        tracker,
        artifact=events_artifact,
        run=None,
        source_root=None,
    )
    assert recovered_null_run is not None
    assert recovered_null_run.read_bytes() == b"parquet-events-bytes"

    staged_events = tracker.run_dir / "staged" / "events.parquet"
    staged_skims = tracker.run_dir / "staged" / "skims.zarr"
    received: list[tuple[Path, Path]] = []

    def consume(events: Path, skims: Path) -> None:
        received.append((events, skims))
        assert events.read_bytes() == b"parquet-events-bytes"
        assert (skims / "chunk" / "0.0").read_bytes() == b"zarr-chunk-bytes"

    with tracker.scenario("cold_scratch_requested_inputs") as scenario:
        result = scenario.run(
            fn=consume,
            inputs={"events": events_artifact, "skims": skims_artifact},
            execution_options=ExecutionOptions(
                input_binding="paths",
                input_materialization="requested",
                input_paths={
                    "events": staged_events,
                    "skims": staged_skims,
                },
            ),
        )

    assert result.cache_hit is False
    assert received == [(staged_events.resolve(), staged_skims.resolve())]
    assert staged_events.read_bytes() == b"parquet-events-bytes"
    assert (staged_skims / "chunk" / "0.0").read_bytes() == b"zarr-chunk-bytes"


def test_requested_input_staging_skips_corrupted_first_recovery_root(
    tracker, tmp_path: Path
) -> None:
    import shutil

    bad_archive = tmp_path / "archive_bad"
    good_archive = tmp_path / "archive_good"

    events_path = tracker.run_dir / "beam" / "output" / "0.events.parquet"
    events_path.parent.mkdir(parents=True, exist_ok=True)
    events_path.write_bytes(b"valid-events-payload")

    skims_dir = tracker.run_dir / "beam" / "output" / "0.skims.zarr"
    (skims_dir / "chunk").mkdir(parents=True, exist_ok=True)
    (skims_dir / "chunk" / "0.0").write_bytes(b"valid-zarr-chunk")

    tracker.begin_run("beam_multi_root", "beam")
    events_artifact = tracker.log_artifact(
        events_path,
        key="events",
        direction="output",
    )
    skims_artifact = tracker.log_output(
        skims_dir,
        key="skims",
        artifact_kind="directory",
        driver="zarr",
    )
    tracker.end_run(status="completed")

    tracker.archive_artifact(events_artifact, bad_archive, mode="copy")
    tracker.archive_artifact(events_artifact, good_archive, mode="copy")
    tracker.archive_artifact(skims_artifact, bad_archive, mode="copy")
    tracker.archive_artifact(skims_artifact, good_archive, mode="copy")

    # Corrupt the copies in the first recovery root (bad_archive)
    (bad_archive / "beam" / "output" / "0.events.parquet").write_bytes(
        b"corrupted-events"
    )
    (bad_archive / "beam" / "output" / "0.skims.zarr" / "chunk" / "0.0").write_bytes(
        b"corrupted-zarr-chunk"
    )

    # Wipe scratch copies
    events_path.unlink()
    shutil.rmtree(skims_dir)

    staged_events = tracker.run_dir / "staged_multi" / "events.parquet"
    staged_skims = tracker.run_dir / "staged_multi" / "skims.zarr"

    def consume(events: Path, skims: Path) -> None:
        assert events.read_bytes() == b"valid-events-payload"
        assert (skims / "chunk" / "0.0").read_bytes() == b"valid-zarr-chunk"

    with tracker.scenario("multi_root_requested_inputs") as scenario:
        result = scenario.run(
            fn=consume,
            inputs={"events": events_artifact, "skims": skims_artifact},
            execution_options=ExecutionOptions(
                input_binding="paths",
                input_materialization="requested",
                input_paths={
                    "events": staged_events,
                    "skims": staged_skims,
                },
            ),
        )

    assert result.cache_hit is False
    assert staged_events.read_bytes() == b"valid-events-payload"
    assert (staged_skims / "chunk" / "0.0").read_bytes() == b"valid-zarr-chunk"
