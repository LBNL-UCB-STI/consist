from __future__ import annotations

from pathlib import Path
import uuid

import pytest

import consist
import consist.core.materialize as consist_materialize
from consist.api import hydrate_run_outputs as hydrate_run_outputs_api
from consist.api import materialize_run_outputs as materialize_run_outputs_api
from consist.api import archive_artifact as archive_artifact_api
from consist.api import archive_current_run_outputs as archive_current_run_outputs_api
from consist.api import archive_run_outputs as archive_run_outputs_api
from consist.api import set_artifact_recovery_roots as set_artifact_recovery_roots_api
from consist.core.tracker import Tracker
from consist.models.artifact import Artifact
from consist.models.run import Run


def _hydrated_result(
    outputs: dict[str, tuple[str | None, str, bool, str | None]],
) -> consist_materialize.HydratedRunOutputsResult:
    hydrated_outputs: dict[str, consist_materialize.HydratedRunOutput] = {}
    for key, (path, status, resolvable, message) in outputs.items():
        hydrated_outputs[key] = consist_materialize.HydratedRunOutput(
            key=key,
            artifact=Artifact(
                id=uuid.uuid4(),
                key=key,
                container_uri=f"./{key}.csv",
                driver="csv",
                meta={},
            ),
            path=Path(path) if path is not None else None,
            status=status,
            message=message,
            resolvable=resolvable,
        )
    return consist_materialize.HydratedRunOutputsResult(outputs=hydrated_outputs)


def test_tracker_materialize_run_outputs_requires_db(tmp_path: Path) -> None:
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=None)

    with pytest.raises(RuntimeError, match="tracker has no database configured"):
        tracker.materialize_run_outputs("missing", target_root=tracker.run_dir)


def test_tracker_materialize_run_outputs_requires_existing_run(
    tracker: Tracker,
) -> None:
    with pytest.raises(KeyError, match="Run 'missing' was not found"):
        tracker.materialize_run_outputs("missing", target_root=tracker.run_dir)


def test_tracker_materialize_run_outputs_rejects_external_target_root_when_disallowed(
    tracker: Tracker, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )
    monkeypatch.setattr(tracker, "get_run", lambda _run_id: run)
    outside = tracker.run_dir.parent / "outside"

    with pytest.raises(
        ValueError,
        match="outside allowed base|configured mount root|allow_external_paths",
    ):
        tracker.materialize_run_outputs("run_1", target_root=outside)


def test_tracker_materialize_run_outputs_folds_hydrated_results(
    tracker: Tracker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hydrated_result = _hydrated_result(
        {
            "from_fs": (
                str((tracker.run_dir / "copied.csv").resolve()),
                "materialized_from_filesystem",
                True,
                None,
            ),
            "from_db": (
                str((tracker.run_dir / "exported.csv").resolve()),
                "materialized_from_db",
                True,
                None,
            ),
            "keep-me": (
                str((tracker.run_dir / "existing.csv").resolve()),
                "preserved_existing",
                True,
                None,
            ),
            "already-unmapped": (None, "skipped_unmapped", False, None),
            "missing-source": (
                str((tracker.run_dir / "missing.csv").resolve()),
                "missing_source",
                False,
                "source path missing",
            ),
            "bad": (
                str((tracker.run_dir / "bad.csv").resolve()),
                "failed",
                False,
                "copy failed",
            ),
        }
    )
    calls: dict[str, object] = {}

    def _fake_hydrate(self, run_id: str, **kwargs):
        calls["run_id"] = run_id
        calls["kwargs"] = kwargs
        return hydrated_result

    monkeypatch.setattr(Tracker, "hydrate_run_outputs", _fake_hydrate)

    result = tracker.materialize_run_outputs(
        "run_1",
        target_root=tracker.run_dir / "restored",
        source_root=tracker.run_dir / "archive",
        keys=("a", "b"),
        preserve_existing=False,
        on_missing="raise",
        db_fallback="never",
    )

    assert calls == {
        "run_id": "run_1",
        "kwargs": {
            "target_root": tracker.run_dir / "restored",
            "source_root": tracker.run_dir / "archive",
            "keys": ("a", "b"),
            "preserve_existing": False,
            "on_missing": "raise",
            "db_fallback": "never",
        },
    }
    assert result.materialized_from_filesystem == {
        "from_fs": str((tracker.run_dir / "copied.csv").resolve()),
    }
    assert result.materialized_from_db == {
        "from_db": str((tracker.run_dir / "exported.csv").resolve())
    }
    assert result.skipped_existing == ["keep-me"]
    assert result.skipped_unmapped == ["already-unmapped"]
    assert result.skipped_missing_source == ["missing-source"]
    assert result.failed == [("bad", "copy failed")]


def test_tracker_hydrate_run_outputs_delegates_to_core_helper(
    tracker: Tracker,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )
    calls: dict[str, object] = {}
    hydrated_result = _hydrated_result({})

    monkeypatch.setattr(tracker, "get_run", lambda _run_id: run)

    def _fake_hydrate_core(
        *,
        tracker,
        run,
        target_root,
        source_root,
        keys,
        allowed_base,
        preserve_existing,
        on_missing,
        db_fallback,
    ):
        calls.update(
            tracker=tracker,
            run=run,
            target_root=target_root,
            source_root=source_root,
            keys=keys,
            allowed_base=allowed_base,
            preserve_existing=preserve_existing,
            on_missing=on_missing,
            db_fallback=db_fallback,
        )
        return hydrated_result

    monkeypatch.setattr(
        "consist.core.tracker.hydrate_run_outputs_core",
        _fake_hydrate_core,
    )

    result = tracker.hydrate_run_outputs(
        "run_1",
        target_root=tracker.run_dir / "restored",
        source_root=tmp_path / "archive",
        keys=("a", "b"),
        preserve_existing=False,
        on_missing="raise",
        db_fallback="never",
    )

    assert result is hydrated_result
    assert calls == {
        "tracker": tracker,
        "run": run,
        "target_root": (tracker.run_dir / "restored").resolve(),
        "source_root": (tmp_path / "archive").resolve(),
        "keys": ("a", "b"),
        "allowed_base": (tracker.run_dir.resolve(),),
        "preserve_existing": False,
        "on_missing": "raise",
        "db_fallback": "never",
    }


def test_set_artifact_recovery_roots_persists_and_invalidates_cache(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "table.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n1\n", encoding="utf-8")

    with tracker.start_run("producer_recovery_roots", model="producer"):
        tracker.log_artifact(output_path, key="table", direction="output")

    cached = tracker.get_run_outputs("producer_recovery_roots")["table"]
    archive_a = tmp_path / "archive_a"
    archive_b = tmp_path / "archive_b"

    updated = set_artifact_recovery_roots_api(
        cached,
        [archive_a, archive_a],
        tracker=tracker,
    )
    assert updated.meta["recovery_roots"] == [str(archive_a.resolve())]

    refreshed = tracker.get_run_outputs("producer_recovery_roots")["table"]
    assert refreshed.meta["recovery_roots"] == [str(archive_a.resolve())]

    tracker.set_artifact_recovery_roots(refreshed, [archive_b], append=True)
    appended = tracker.get_run_outputs("producer_recovery_roots")["table"]
    assert appended.meta["recovery_roots"] == [
        str(archive_a.resolve()),
        str(archive_b.resolve()),
    ]

    tracker.set_artifact_recovery_roots(appended, [], append=False)
    cleared = tracker.get_run_outputs("producer_recovery_roots")["table"]
    assert "recovery_roots" not in cleared.meta


def test_log_artifact_normalizes_recovery_roots_metadata(tracker: Tracker) -> None:
    output_path = tracker.run_dir / "outputs" / "normalized.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n1\n", encoding="utf-8")
    archive = tracker.run_dir.parent / "archive_root"

    with tracker.start_run("producer_log_recovery_roots", model="producer"):
        artifact = tracker.log_artifact(
            output_path,
            key="normalized",
            direction="output",
            recovery_roots=[archive, archive],
        )

    assert artifact.meta["recovery_roots"] == [str(archive.resolve())]
    assert artifact.recovery_roots == [str(archive.resolve())]


def test_archive_artifact_copies_bytes_and_records_recovery_root(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "archived.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n1\n", encoding="utf-8")
    archive_root = tmp_path / "archive_copy"

    with tracker.start_run("producer_archive_copy", model="producer"):
        artifact = tracker.log_artifact(output_path, key="archived", direction="output")

    archived_path = archive_artifact_api(artifact, archive_root, tracker=tracker)

    assert archived_path == (archive_root / "outputs" / "archived.csv").resolve()
    assert archived_path.read_text(encoding="utf-8") == "value\n1\n"
    assert output_path.exists()
    refreshed = tracker.get_run_outputs("producer_archive_copy")["archived"]
    assert refreshed.meta["recovery_roots"] == [str(archive_root.resolve())]


def test_archive_artifact_unmappable_layout_has_actionable_error(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    artifact = Artifact(
        id=uuid.uuid4(),
        key="absolute",
        container_uri=str((tmp_path / "absolute.csv").resolve()),
        driver="csv",
        meta={},
    )

    with pytest.raises(
        ValueError,
        match="Use managed output paths|cannot be recovered from root-only recovery metadata",
    ):
        tracker.archive_artifact(artifact, tmp_path / "archive")


def test_archive_artifact_move_updates_runtime_path(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "moved.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n2\n", encoding="utf-8")
    archive_root = tmp_path / "archive_move"

    with tracker.start_run("producer_archive_move", model="producer"):
        artifact = tracker.log_artifact(output_path, key="moved", direction="output")

    archived_path = tracker.archive_artifact(artifact, archive_root, mode="move")

    assert archived_path.read_text(encoding="utf-8") == "value\n2\n"
    assert not output_path.exists()
    assert artifact.abs_path == str(archived_path.resolve())
    assert artifact.path == archived_path


def test_archive_current_run_outputs_uses_active_run(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "current.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n4\n", encoding="utf-8")
    archive_root = tmp_path / "archive_current"

    with tracker.start_run("producer_archive_current", model="producer"):
        tracker.log_artifact(output_path, key="current", direction="output")
        archived = tracker.archive_current_run_outputs(archive_root, mode="copy")

    archived_path = (archive_root / "outputs" / "current.csv").resolve()
    assert archived == {"current": archived_path}
    assert archived_path.read_text(encoding="utf-8") == "value\n4\n"
    refreshed = tracker.get_run_outputs("producer_archive_current")["current"]
    assert refreshed.recovery_roots == [str(archive_root.resolve())]


def test_archive_current_run_outputs_requires_active_run(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    with pytest.raises(RuntimeError, match="requires an active run context"):
        tracker.archive_current_run_outputs(tmp_path / "archive_current")


def test_archive_artifact_prefers_historical_source_over_current_workspace(
    tmp_path: Path,
) -> None:
    db_path = str(tmp_path / "provenance.db")
    tracker_a = Tracker(run_dir=tmp_path / "runs_a", db_path=db_path)
    tracker_b = Tracker(run_dir=tmp_path / "runs_b", db_path=db_path)

    source_path = tracker_a.run_dir / "outputs" / "shared.csv"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text("historical\n", encoding="utf-8")

    with tracker_a.start_run("producer_archive_history", model="producer"):
        tracker_a.log_artifact(source_path, key="shared", direction="output")

    current_path = tracker_b.run_dir / "outputs" / "shared.csv"
    current_path.parent.mkdir(parents=True, exist_ok=True)
    current_path.write_text("current\n", encoding="utf-8")

    historical_artifact = tracker_b.get_run_outputs("producer_archive_history")[
        "shared"
    ]
    archive_root = tmp_path / "archive_history"
    archived_path = tracker_b.archive_artifact(historical_artifact, archive_root)

    assert archived_path.read_text(encoding="utf-8") == "historical\n"

    if tracker_a.engine:
        tracker_a.engine.dispose()
    if tracker_b.engine:
        tracker_b.engine.dispose()


def test_archive_artifact_move_rolls_back_on_metadata_failure(
    tracker: Tracker,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_path = tracker.run_dir / "outputs" / "rollback.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n3\n", encoding="utf-8")
    archive_root = tmp_path / "archive_rollback"

    with tracker.start_run("producer_archive_rollback", model="producer"):
        artifact = tracker.log_artifact(output_path, key="rollback", direction="output")

    def _raise_update(*_args, **_kwargs):
        raise RuntimeError("db down")

    monkeypatch.setattr(tracker.db, "update_artifact_meta", _raise_update)

    with pytest.raises(RuntimeError, match="db down"):
        tracker.archive_artifact(artifact, archive_root, mode="move")

    assert output_path.read_text(encoding="utf-8") == "value\n3\n"
    assert not (archive_root / "outputs" / "rollback.csv").exists()


def test_archive_artifact_copy_is_idempotent_for_matching_file(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "retry.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n4\n", encoding="utf-8")
    archive_root = tmp_path / "archive_retry_copy"

    with tracker.start_run("producer_archive_retry_copy", model="producer"):
        artifact = tracker.log_artifact(output_path, key="retry", direction="output")

    first = tracker.archive_artifact(artifact, archive_root, mode="copy")
    second = tracker.archive_artifact(artifact, archive_root, mode="copy")

    assert first == second
    assert second.read_text(encoding="utf-8") == "value\n4\n"


def test_archive_artifact_move_is_idempotent_for_matching_file(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "retry_move.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n5\n", encoding="utf-8")
    archive_root = tmp_path / "archive_retry_move"

    with tracker.start_run("producer_archive_retry_move", model="producer"):
        artifact = tracker.log_artifact(
            output_path, key="retry_move", direction="output"
        )

    first = tracker.archive_artifact(artifact, archive_root, mode="move")
    second = tracker.archive_artifact(artifact, archive_root, mode="move")

    assert first == second
    assert second.read_text(encoding="utf-8") == "value\n5\n"
    assert not output_path.exists()


def test_archive_run_outputs_archives_selected_keys(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    a_path = tracker.run_dir / "outputs" / "a.csv"
    b_path = tracker.run_dir / "outputs" / "b.csv"
    a_path.parent.mkdir(parents=True, exist_ok=True)
    a_path.write_text("value\n1\n", encoding="utf-8")
    b_path.write_text("value\n2\n", encoding="utf-8")
    archive_root = tmp_path / "archive_bulk"

    with tracker.start_run("producer_archive_bulk", model="producer"):
        tracker.log_artifact(a_path, key="a", direction="output")
        tracker.log_artifact(b_path, key="b", direction="output")

    archived = archive_run_outputs_api(
        "producer_archive_bulk",
        archive_root,
        keys=["a"],
        tracker=tracker,
    )

    assert archived == {"a": (archive_root / "outputs" / "a.csv").resolve()}
    outputs = tracker.get_run_outputs("producer_archive_bulk")
    assert outputs["a"].meta["recovery_roots"] == [str(archive_root.resolve())]
    assert "recovery_roots" not in outputs["b"].meta


def test_api_archive_current_run_outputs_delegates_with_default_tracker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    class _FakeTracker:
        def archive_current_run_outputs(self, archive_root, **kwargs):
            calls["archive_root"] = archive_root
            calls["kwargs"] = kwargs
            return {"x": Path("/tmp/archive/x.csv")}

    fake_tracker = _FakeTracker()
    monkeypatch.setattr(
        "consist.api._resolve_tracker", lambda tracker=None: fake_tracker
    )

    result = archive_current_run_outputs_api(
        "/tmp/archive",
        keys=("x", "y"),
        mode="move",
        append=False,
    )

    assert result == {"x": Path("/tmp/archive/x.csv")}
    assert calls == {
        "archive_root": "/tmp/archive",
        "kwargs": {
            "keys": ("x", "y"),
            "mode": "move",
            "append": False,
        },
    }
    assert consist.archive_current_run_outputs is archive_current_run_outputs_api


def test_tracker_hydrate_run_outputs_allows_external_target_root_when_configured(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = Tracker(
        run_dir=tmp_path / "runs",
        db_path=str(tmp_path / "provenance.db"),
        allow_external_paths=True,
    )
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )
    calls: dict[str, object] = {}

    monkeypatch.setattr(tracker, "get_run", lambda _run_id: run)

    def _fake_hydrate_core(**kwargs):
        calls["allowed_base"] = kwargs["allowed_base"]
        return _hydrated_result({})

    monkeypatch.setattr(
        "consist.core.tracker.hydrate_run_outputs_core",
        _fake_hydrate_core,
    )

    outside = tmp_path / "external"
    tracker.hydrate_run_outputs("run_1", target_root=outside)

    assert calls["allowed_base"] is None

    if tracker.engine:
        tracker.engine.dispose()


def test_run_materialize_outputs_delegates_to_tracker() -> None:
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )
    calls: dict[str, object] = {}

    class _FakeTracker:
        def materialize_run_outputs(self, run_id: str, **kwargs):
            calls["run_id"] = run_id
            calls["kwargs"] = kwargs
            return "result"

    result = run.materialize_outputs(
        _FakeTracker(),
        target_root="/tmp/restored",
        source_root="/tmp/archive",
        keys=("a",),
        preserve_existing=False,
        on_missing="raise",
        db_fallback="never",
    )

    assert result == "result"
    assert calls == {
        "run_id": "run_1",
        "kwargs": {
            "target_root": "/tmp/restored",
            "source_root": "/tmp/archive",
            "keys": ("a",),
            "preserve_existing": False,
            "on_missing": "raise",
            "db_fallback": "never",
        },
    }


def test_run_hydrate_outputs_delegates_to_tracker() -> None:
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )
    calls: dict[str, object] = {}

    class _FakeTracker:
        def hydrate_run_outputs(self, run_id: str, **kwargs):
            calls["run_id"] = run_id
            calls["kwargs"] = kwargs
            return "hydrated"

    result = run.hydrate_outputs(
        _FakeTracker(),
        target_root="/tmp/restored",
        source_root="/tmp/archive",
        keys=("a",),
        preserve_existing=False,
        on_missing="raise",
        db_fallback="never",
    )

    assert result == "hydrated"
    assert calls == {
        "run_id": "run_1",
        "kwargs": {
            "target_root": "/tmp/restored",
            "source_root": "/tmp/archive",
            "keys": ("a",),
            "preserve_existing": False,
            "on_missing": "raise",
            "db_fallback": "never",
        },
    }


def test_api_materialize_run_outputs_delegates_with_default_tracker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    class _FakeTracker:
        def materialize_run_outputs(self, run_id: str, **kwargs):
            calls["run_id"] = run_id
            calls["kwargs"] = kwargs
            return "api-result"

    fake_tracker = _FakeTracker()
    monkeypatch.setattr(
        "consist.api._resolve_tracker", lambda tracker=None: fake_tracker
    )

    result = materialize_run_outputs_api(
        "run_9",
        target_root="/tmp/restored",
        source_root="/tmp/archive",
        keys=("x", "y"),
        preserve_existing=False,
        on_missing="raise",
        db_fallback="never",
    )

    assert result == "api-result"
    assert calls == {
        "run_id": "run_9",
        "kwargs": {
            "target_root": "/tmp/restored",
            "source_root": "/tmp/archive",
            "keys": ("x", "y"),
            "preserve_existing": False,
            "on_missing": "raise",
            "db_fallback": "never",
        },
    }
    assert consist.materialize_run_outputs is materialize_run_outputs_api
    assert consist.MaterializationResult is consist_materialize.MaterializationResult


def test_api_hydrate_run_outputs_delegates_with_default_tracker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    class _FakeTracker:
        def hydrate_run_outputs(self, run_id: str, **kwargs):
            calls["run_id"] = run_id
            calls["kwargs"] = kwargs
            return "hydrated-api-result"

    fake_tracker = _FakeTracker()
    monkeypatch.setattr(
        "consist.api._resolve_tracker", lambda tracker=None: fake_tracker
    )

    result = hydrate_run_outputs_api(
        "run_9",
        target_root="/tmp/restored",
        source_root="/tmp/archive",
        keys=("x", "y"),
        preserve_existing=False,
        on_missing="raise",
        db_fallback="never",
    )

    assert result == "hydrated-api-result"
    assert calls == {
        "run_id": "run_9",
        "kwargs": {
            "target_root": "/tmp/restored",
            "source_root": "/tmp/archive",
            "keys": ("x", "y"),
            "preserve_existing": False,
            "on_missing": "raise",
            "db_fallback": "never",
        },
    }
    assert consist.hydrate_run_outputs is hydrate_run_outputs_api


def test_tracker_hydrate_run_outputs_allows_configured_mount_roots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace_root = tmp_path / "workspace"
    tracker = Tracker(
        run_dir=tmp_path / "runs",
        db_path=str(tmp_path / "provenance.db"),
        mounts={"workspace": str(workspace_root)},
    )
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )
    calls: dict[str, object] = {}

    monkeypatch.setattr(tracker, "get_run", lambda _run_id: run)

    def _fake_hydrate_core(**kwargs):
        calls["allowed_base"] = kwargs["allowed_base"]
        return _hydrated_result({})

    monkeypatch.setattr(
        "consist.core.tracker.hydrate_run_outputs_core",
        _fake_hydrate_core,
    )

    tracker.hydrate_run_outputs(
        "run_1",
        target_root=workspace_root / "restored",
    )

    assert calls["allowed_base"] == (
        tracker.run_dir.resolve(),
        workspace_root.resolve(),
    )


@pytest.mark.parametrize("bad_keys", ["out", b"out"])
def test_tracker_materialize_run_outputs_rejects_scalar_string_keys(
    tracker: Tracker,
    monkeypatch: pytest.MonkeyPatch,
    bad_keys: str | bytes,
) -> None:
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )
    monkeypatch.setattr(tracker, "get_run", lambda _run_id: run)

    with pytest.raises(TypeError, match="keys must be a sequence"):
        tracker.materialize_run_outputs(
            "run_1", target_root=tracker.run_dir, keys=bad_keys
        )


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("on_missing", "ignore"),
        ("db_fallback", "always"),
    ],
)
def test_tracker_materialize_run_outputs_rejects_invalid_runtime_options(
    tracker: Tracker,
    monkeypatch: pytest.MonkeyPatch,
    field_name: str,
    field_value: str,
) -> None:
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )
    monkeypatch.setattr(tracker, "get_run", lambda _run_id: run)
    kwargs = {"target_root": tracker.run_dir, field_name: field_value}

    with pytest.raises(ValueError, match=field_name):
        tracker.materialize_run_outputs("run_1", **kwargs)


@pytest.mark.parametrize("bad_keys", ["out", b"out"])
def test_run_materialize_outputs_rejects_scalar_string_keys_before_delegation(
    bad_keys: str | bytes,
) -> None:
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )

    class _FakeTracker:
        def materialize_run_outputs(self, run_id: str, **kwargs):
            raise AssertionError("delegation should not happen for invalid keys")

    with pytest.raises(TypeError, match="keys must be a sequence"):
        run.materialize_outputs(
            _FakeTracker(),
            target_root="/tmp/restored",
            keys=bad_keys,
        )


@pytest.mark.parametrize("bad_keys", ["out", b"out"])
def test_run_hydrate_outputs_rejects_scalar_string_keys_before_delegation(
    bad_keys: str | bytes,
) -> None:
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )

    class _FakeTracker:
        def hydrate_run_outputs(self, run_id: str, **kwargs):
            raise AssertionError("delegation should not happen for invalid keys")

    with pytest.raises(TypeError, match="keys must be a sequence"):
        run.hydrate_outputs(
            _FakeTracker(),
            target_root="/tmp/restored",
            keys=bad_keys,
        )


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("on_missing", "ignore"),
        ("db_fallback", "always"),
    ],
)
def test_run_materialize_outputs_rejects_invalid_runtime_options_before_delegation(
    field_name: str,
    field_value: str,
) -> None:
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )

    class _FakeTracker:
        def materialize_run_outputs(self, run_id: str, **kwargs):
            raise AssertionError("delegation should not happen for invalid options")

    kwargs = {"target_root": "/tmp/restored", field_name: field_value}

    with pytest.raises(ValueError, match=field_name):
        run.materialize_outputs(_FakeTracker(), **kwargs)


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("on_missing", "ignore"),
        ("db_fallback", "always"),
    ],
)
def test_run_hydrate_outputs_rejects_invalid_runtime_options_before_delegation(
    field_name: str,
    field_value: str,
) -> None:
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )

    class _FakeTracker:
        def hydrate_run_outputs(self, run_id: str, **kwargs):
            raise AssertionError("delegation should not happen for invalid options")

    kwargs = {"target_root": "/tmp/restored", field_name: field_value}

    with pytest.raises(ValueError, match=field_name):
        run.hydrate_outputs(_FakeTracker(), **kwargs)


@pytest.mark.parametrize("bad_keys", ["out", b"out"])
def test_api_materialize_run_outputs_passes_scalar_string_keys_to_tracker(
    monkeypatch: pytest.MonkeyPatch,
    bad_keys: str | bytes,
) -> None:
    calls: dict[str, object] = {}

    class _FakeTracker:
        def materialize_run_outputs(self, run_id: str, **kwargs):
            calls["run_id"] = run_id
            calls["kwargs"] = kwargs
            return "api-result"

    monkeypatch.setattr(
        "consist.api._resolve_tracker", lambda tracker=None: _FakeTracker()
    )

    result = materialize_run_outputs_api(
        "run_1",
        target_root="/tmp/restored",
        keys=bad_keys,
    )

    assert result == "api-result"
    assert calls == {
        "run_id": "run_1",
        "kwargs": {
            "target_root": "/tmp/restored",
            "source_root": None,
            "keys": bad_keys,
            "preserve_existing": True,
            "on_missing": "warn",
            "db_fallback": "if_ingested",
        },
    }


@pytest.mark.parametrize("bad_keys", ["out", b"out"])
def test_api_hydrate_run_outputs_passes_scalar_string_keys_to_tracker(
    monkeypatch: pytest.MonkeyPatch,
    bad_keys: str | bytes,
) -> None:
    calls: dict[str, object] = {}

    class _FakeTracker:
        def hydrate_run_outputs(self, run_id: str, **kwargs):
            calls["run_id"] = run_id
            calls["kwargs"] = kwargs
            return "hydrated-api-result"

    monkeypatch.setattr(
        "consist.api._resolve_tracker", lambda tracker=None: _FakeTracker()
    )

    result = hydrate_run_outputs_api(
        "run_1",
        target_root="/tmp/restored",
        keys=bad_keys,
    )

    assert result == "hydrated-api-result"
    assert calls == {
        "run_id": "run_1",
        "kwargs": {
            "target_root": "/tmp/restored",
            "source_root": None,
            "keys": bad_keys,
            "preserve_existing": True,
            "on_missing": "warn",
            "db_fallback": "if_ingested",
        },
    }


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("on_missing", "ignore"),
        ("db_fallback", "always"),
    ],
)
def test_api_materialize_run_outputs_passes_invalid_runtime_options_to_tracker(
    monkeypatch: pytest.MonkeyPatch,
    field_name: str,
    field_value: str,
) -> None:
    calls: dict[str, object] = {}

    class _FakeTracker:
        def materialize_run_outputs(self, run_id: str, **kwargs):
            calls["run_id"] = run_id
            calls["kwargs"] = kwargs
            return "api-result"

    monkeypatch.setattr(
        "consist.api._resolve_tracker", lambda tracker=None: _FakeTracker()
    )
    kwargs = {"target_root": "/tmp/restored", field_name: field_value}

    result = materialize_run_outputs_api("run_1", **kwargs)

    assert result == "api-result"
    assert calls == {
        "run_id": "run_1",
        "kwargs": {
            "target_root": "/tmp/restored",
            "source_root": None,
            "keys": None,
            "preserve_existing": True,
            "on_missing": "warn" if field_name != "on_missing" else field_value,
            "db_fallback": (
                "if_ingested" if field_name != "db_fallback" else field_value
            ),
        },
    }


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("on_missing", "ignore"),
        ("db_fallback", "always"),
    ],
)
def test_api_hydrate_run_outputs_passes_invalid_runtime_options_to_tracker(
    monkeypatch: pytest.MonkeyPatch,
    field_name: str,
    field_value: str,
) -> None:
    calls: dict[str, object] = {}

    class _FakeTracker:
        def hydrate_run_outputs(self, run_id: str, **kwargs):
            calls["run_id"] = run_id
            calls["kwargs"] = kwargs
            return "hydrated-api-result"

    monkeypatch.setattr(
        "consist.api._resolve_tracker", lambda tracker=None: _FakeTracker()
    )
    kwargs = {"target_root": "/tmp/restored", field_name: field_value}

    result = hydrate_run_outputs_api("run_1", **kwargs)

    assert result == "hydrated-api-result"
    assert calls == {
        "run_id": "run_1",
        "kwargs": {
            "target_root": "/tmp/restored",
            "source_root": None,
            "keys": None,
            "preserve_existing": True,
            "on_missing": "warn" if field_name != "on_missing" else field_value,
            "db_fallback": (
                "if_ingested" if field_name != "db_fallback" else field_value
            ),
        },
    }
