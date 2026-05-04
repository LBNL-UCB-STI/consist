from __future__ import annotations

from pathlib import Path

import pytest

from consist.core.config_canonicalization import (
    CanonicalConfig,
    ConfigPlan,
    canonical_identity_from_config,
)
from consist.types import CacheOptions


def _assert_problem_cause_fix(message: str) -> None:
    assert "Problem:" in message
    assert "Cause:" in message
    assert "Fix:" in message


def test_pre_pilates_adapter_and_identity_inputs_surface(
    tracker, tmp_path, monkeypatch
):
    dep_file = tmp_path / "identity_dep.yaml"
    dep_file.write_text("threshold: 0.5\n")

    config_root = tmp_path / "adapter_config"
    config_root.mkdir(parents=True, exist_ok=True)

    canonical = CanonicalConfig(
        root_dirs=[config_root],
        primary_config=None,
        config_files=[],
        external_files=[],
        content_hash="adapter_identity_hash",
    )
    adapter_plan = ConfigPlan(
        adapter_name="dummy_adapter",
        adapter_version="1.0",
        canonical=canonical,
        artifacts=[],
        ingestables=[],
        identity=canonical_identity_from_config(
            adapter_name="dummy_adapter",
            adapter_version="1.0",
            config=canonical,
        ),
    )

    class DummyAdapter:
        model_name = "dummy_adapter"
        root_dirs = [config_root]

    dummy_adapter = DummyAdapter()
    calls: list[list[Path]] = []

    def fake_prepare_config(adapter, config_dirs, **kwargs):
        del kwargs
        assert adapter is dummy_adapter
        resolved_dirs = [Path(p) for p in config_dirs]
        calls.append(resolved_dirs)
        return adapter_plan

    monkeypatch.setattr(tracker, "prepare_config", fake_prepare_config)

    result = tracker.run(
        fn=lambda: None,
        name="pilates_preflight_identity",
        adapter=dummy_adapter,
        identity_inputs=[("dep_cfg", dep_file)],
        cache_options=CacheOptions(cache_mode="overwrite"),
    )

    summary = result.run.identity_summary
    assert calls == [[config_root]]
    assert summary["adapter"]["name"] == "dummy_adapter"
    assert summary["adapter"]["hash"] == "adapter_identity_hash"
    assert summary["identity_inputs_count"] == 1
    assert "dep_cfg" in summary["identity_inputs"]


def test_pre_pilates_rejects_removed_hash_inputs_kwarg(tracker) -> None:
    with pytest.raises(TypeError) as excinfo:
        tracker.run(
            fn=lambda: None,
            name="pilates_mixed_identity_kwargs",
            identity_inputs=[],
            hash_inputs=[],
        )

    message = str(excinfo.value)
    assert "unexpected keyword argument 'hash_inputs'" in message


def test_pre_pilates_missing_identity_path_error_shape(tracker, tmp_path: Path) -> None:
    missing = tmp_path / "missing_pre_pilates_identity.yaml"
    with pytest.raises(ValueError) as excinfo:
        tracker.run(
            fn=lambda: None,
            name="pilates_missing_identity_path",
            identity_inputs=[missing],
            cache_options=CacheOptions(cache_mode="overwrite"),
        )

    message = str(excinfo.value)
    _assert_problem_cause_fix(message)
    assert "Failed to compute identity input digests" in message
