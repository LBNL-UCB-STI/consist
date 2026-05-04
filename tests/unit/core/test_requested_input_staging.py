from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from consist.core.cache import ActiveRunCacheOptions, materialize_requested_inputs
from consist.core.tracker_orchestration import RunTraceCoordinator, RunTraceHelpers
from consist.models.artifact import Artifact
from consist.models.run import ConsistRecord, Run
from consist.types import ExecutionOptions


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
    def __init__(self, *, run_dir: Path, current_consist: ConsistRecord | None = None):
        self.current_consist = current_consist
        self.db = None
        self.fs = SimpleNamespace(
            resolve_historical_path=lambda uri, original_run_dir: uri,
            normalize_recovery_roots=lambda roots: [],
            get_remappable_relative_path=lambda uri: None,
            get_historical_root=lambda **kwargs: None,
        )
        self.identity = SimpleNamespace(compute_file_checksum=_sha256)
        self.run_dir = run_dir
        self.mounts = {}

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
        container_uri=str(source),
        driver="csv",
        hash=_sha256(source),
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
    assert artifact.abs_path == str(destination.resolve())


def test_execute_python_run_uses_staged_input_path(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("source\n", encoding="utf-8")
    staged = tmp_path / "staged" / "raw.csv"
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.write_text("staged\n", encoding="utf-8")

    artifact = Artifact(
        key="raw",
        container_uri=str(source),
        driver="csv",
        hash=_sha256(staged),
        meta={},
    )
    artifact.abs_path = str(staged)

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
