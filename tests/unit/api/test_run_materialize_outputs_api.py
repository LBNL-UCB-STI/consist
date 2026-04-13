from __future__ import annotations

from pathlib import Path
import uuid

import pytest

import consist
import consist.core.materialize as consist_materialize
from consist.api import hydrate_run_outputs as hydrate_run_outputs_api
from consist.api import materialize_run_outputs as materialize_run_outputs_api
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

    with pytest.raises(ValueError, match="outside allowed base"):
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
def test_api_materialize_run_outputs_rejects_scalar_string_keys_before_delegation(
    monkeypatch: pytest.MonkeyPatch,
    bad_keys: str | bytes,
) -> None:
    class _FakeTracker:
        def materialize_run_outputs(self, run_id: str, **kwargs):
            raise AssertionError("delegation should not happen for invalid keys")

    monkeypatch.setattr(
        "consist.api._resolve_tracker", lambda tracker=None: _FakeTracker()
    )

    with pytest.raises(TypeError, match="keys must be a sequence"):
        materialize_run_outputs_api("run_1", target_root="/tmp/restored", keys=bad_keys)


@pytest.mark.parametrize("bad_keys", ["out", b"out"])
def test_api_hydrate_run_outputs_rejects_scalar_string_keys_before_delegation(
    monkeypatch: pytest.MonkeyPatch,
    bad_keys: str | bytes,
) -> None:
    class _FakeTracker:
        def hydrate_run_outputs(self, run_id: str, **kwargs):
            raise AssertionError("delegation should not happen for invalid keys")

    monkeypatch.setattr(
        "consist.api._resolve_tracker", lambda tracker=None: _FakeTracker()
    )

    with pytest.raises(TypeError, match="keys must be a sequence"):
        hydrate_run_outputs_api("run_1", target_root="/tmp/restored", keys=bad_keys)


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("on_missing", "ignore"),
        ("db_fallback", "always"),
    ],
)
def test_api_materialize_run_outputs_rejects_invalid_runtime_options_before_delegation(
    monkeypatch: pytest.MonkeyPatch,
    field_name: str,
    field_value: str,
) -> None:
    class _FakeTracker:
        def materialize_run_outputs(self, run_id: str, **kwargs):
            raise AssertionError("delegation should not happen for invalid options")

    monkeypatch.setattr(
        "consist.api._resolve_tracker", lambda tracker=None: _FakeTracker()
    )
    kwargs = {"target_root": "/tmp/restored", field_name: field_value}

    with pytest.raises(ValueError, match=field_name):
        materialize_run_outputs_api("run_1", **kwargs)


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("on_missing", "ignore"),
        ("db_fallback", "always"),
    ],
)
def test_api_hydrate_run_outputs_rejects_invalid_runtime_options_before_delegation(
    monkeypatch: pytest.MonkeyPatch,
    field_name: str,
    field_value: str,
) -> None:
    class _FakeTracker:
        def hydrate_run_outputs(self, run_id: str, **kwargs):
            raise AssertionError("delegation should not happen for invalid options")

    monkeypatch.setattr(
        "consist.api._resolve_tracker", lambda tracker=None: _FakeTracker()
    )
    kwargs = {"target_root": "/tmp/restored", field_name: field_value}

    with pytest.raises(ValueError, match=field_name):
        hydrate_run_outputs_api("run_1", **kwargs)
