from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import consist
import consist.core as consist_core
from consist.api import materialize_run_outputs as materialize_run_outputs_api
from consist.core.tracker import Tracker
from consist.models.run import Run


def _result(
    *,
    fs: dict[str, str] | None = None,
    db: dict[str, str] | None = None,
    skipped_existing: list[str] | None = None,
    skipped_unmapped: list[str] | None = None,
    skipped_missing_source: list[str] | None = None,
    failed: list[tuple[str, str]] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        materialized_from_filesystem=fs or {},
        materialized_from_db=db or {},
        skipped_existing=skipped_existing or [],
        skipped_unmapped=skipped_unmapped or [],
        skipped_missing_source=skipped_missing_source or [],
        failed=failed or [],
    )


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


def test_tracker_materialize_run_outputs_delegates_to_core_helpers(
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
    plan = [object()]
    planner_result = _result(
        fs={"planned": "planner-only"},
        skipped_unmapped=["already-unmapped"],
    )
    execution_result = _result(
        fs={"from_fs": str((tracker.run_dir / "copied.csv").resolve())},
        db={"from_db": str((tracker.run_dir / "exported.csv").resolve())},
        skipped_existing=["keep-me"],
        skipped_missing_source=["missing-source"],
        failed=[("bad", "copy failed")],
    )
    planner_calls: dict[str, object] = {}
    execution_calls: dict[str, object] = {}

    monkeypatch.setattr(
        tracker, "get_run", lambda run_id: run if run_id == "run_1" else None
    )

    def _fake_plan(
        tracker_arg,
        run_arg,
        *,
        target_root,
        source_root,
        keys,
        preserve_existing,
        db_fallback,
    ):
        planner_calls.update(
            tracker=tracker_arg,
            run=run_arg,
            target_root=target_root,
            source_root=source_root,
            keys=keys,
            preserve_existing=preserve_existing,
            db_fallback=db_fallback,
        )
        return plan, planner_result

    def _fake_execute(
        plan_arg,
        *,
        tracker,
        allowed_base,
        on_missing,
        preserve_existing,
    ):
        execution_calls.update(
            plan=plan_arg,
            tracker=tracker,
            allowed_base=allowed_base,
            on_missing=on_missing,
            preserve_existing=preserve_existing,
        )
        return execution_result

    fake_materialize_module = SimpleNamespace(
        build_run_output_materialize_plan=_fake_plan,
        materialize_planned_outputs=_fake_execute,
        build_allowed_materialization_roots=(
            consist_core.materialize.build_allowed_materialization_roots
        ),
        validate_allowed_materialization_destination=(
            consist_core.materialize.validate_allowed_materialization_destination
        ),
    )
    monkeypatch.setattr(consist_core, "materialize", fake_materialize_module)

    result = tracker.materialize_run_outputs(
        "run_1",
        target_root=tracker.run_dir / "restored",
        source_root=tmp_path / "archive",
        keys=("a", "b"),
        preserve_existing=False,
        on_missing="raise",
        db_fallback="never",
    )

    assert planner_calls == {
        "tracker": tracker,
        "run": run,
        "target_root": (tracker.run_dir / "restored").resolve(),
        "source_root": (tmp_path / "archive").resolve(),
        "keys": ("a", "b"),
        "preserve_existing": False,
        "db_fallback": "never",
    }
    assert execution_calls == {
        "plan": plan,
        "tracker": tracker,
        "allowed_base": (tracker.run_dir.resolve(),),
        "on_missing": "raise",
        "preserve_existing": False,
    }
    assert result.materialized_from_filesystem == {
        "planned": "planner-only",
        "from_fs": str((tracker.run_dir / "copied.csv").resolve()),
    }
    assert result.materialized_from_db == {
        "from_db": str((tracker.run_dir / "exported.csv").resolve())
    }
    assert result.skipped_existing == ["keep-me"]
    assert result.skipped_unmapped == ["already-unmapped"]
    assert result.skipped_missing_source == ["missing-source"]
    assert result.failed == [("bad", "copy failed")]


def test_tracker_materialize_run_outputs_allows_external_target_root_when_configured(
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
    planner_result = _result()
    execution_result = _result()
    execution_calls: dict[str, object] = {}

    monkeypatch.setattr(tracker, "get_run", lambda _run_id: run)
    fake_materialize_module = SimpleNamespace(
        build_run_output_materialize_plan=lambda *_args, **_kwargs: (
            [],
            planner_result,
        ),
        build_allowed_materialization_roots=(
            consist_core.materialize.build_allowed_materialization_roots
        ),
        validate_allowed_materialization_destination=(
            consist_core.materialize.validate_allowed_materialization_destination
        ),
    )

    def _fake_execute(
        plan_arg,
        *,
        tracker,
        allowed_base,
        on_missing,
        preserve_existing,
    ):
        execution_calls["allowed_base"] = allowed_base
        return execution_result

    fake_materialize_module.materialize_planned_outputs = _fake_execute
    monkeypatch.setattr(consist_core, "materialize", fake_materialize_module)

    outside = tmp_path / "external"
    tracker.materialize_run_outputs("run_1", target_root=outside)

    assert execution_calls["allowed_base"] is None

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
    assert (
        consist.MaterializationResult is consist_core.materialize.MaterializationResult
    )


def test_tracker_materialize_run_outputs_allows_configured_mount_roots(
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
    planner_result = _result()
    execution_result = _result()
    execution_calls: dict[str, object] = {}

    monkeypatch.setattr(tracker, "get_run", lambda _run_id: run)
    fake_materialize_module = SimpleNamespace(
        build_run_output_materialize_plan=lambda *_args, **_kwargs: (
            [],
            planner_result,
        ),
    )

    def _fake_execute(
        plan_arg,
        *,
        tracker,
        allowed_base,
        on_missing,
        preserve_existing,
    ):
        execution_calls["allowed_base"] = allowed_base
        return execution_result

    fake_materialize_module.materialize_planned_outputs = _fake_execute
    fake_materialize_module.build_allowed_materialization_roots = (
        consist_core.materialize.build_allowed_materialization_roots
    )
    fake_materialize_module.validate_allowed_materialization_destination = (
        consist_core.materialize.validate_allowed_materialization_destination
    )
    monkeypatch.setattr(consist_core, "materialize", fake_materialize_module)

    tracker.materialize_run_outputs(
        "run_1",
        target_root=workspace_root / "restored",
    )

    assert execution_calls["allowed_base"] == (
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
