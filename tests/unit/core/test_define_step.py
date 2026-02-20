from pathlib import Path

import pytest

from consist.core.config_canonicalization import CanonicalConfig, ConfigPlan
from consist.core.decorators import define_step
from consist.core.tracker import Tracker
from consist.types import CacheOptions, ExecutionOptions


def test_define_step_metadata_applied(tracker: Tracker) -> None:
    @tracker.define_step(outputs=["out"], tags=["demo"], description="demo step")
    def step(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        out_path = ctx.run_dir / "out.txt"
        out_path.write_text("ok")

    result = tracker.run(
        fn=step,
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    assert "out" in result.outputs
    assert result.outputs["out"].path.exists()
    assert "demo" in tracker.last_run.run.tags
    assert tracker.last_run.run.description == "demo step"


def test_define_step_name_template_and_callable_metadata(tracker: Tracker) -> None:
    @tracker.define_step(
        name_template="{func_name}__y{year}__phase_{phase}",
        tags=lambda ctx: [f"phase:{ctx.phase}"],
        description=lambda ctx: f"year:{ctx.year}",
    )
    def step(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        out_path = ctx.run_dir / "out.txt"
        out_path.write_text("ok")

    tracker.run(
        fn=step,
        year=2030,
        phase="demand",
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    run = tracker.last_run.run
    assert run.model_name == "step__y2030__phase_demand"
    assert run.tags == ["phase:demand"]
    assert run.description == "year:2030"


def test_define_step_adapter_default_applied(
    tracker: Tracker, tmp_path, monkeypatch
) -> None:
    config_root = tmp_path / "adapter_cfg"
    config_root.mkdir(parents=True, exist_ok=True)

    class DummyAdapter:
        root_dirs = [config_root]

    adapter = DummyAdapter()
    captured: list[Path] = []

    def fake_prepare_config(*, adapter, config_dirs, **kwargs):
        del kwargs
        assert adapter is not None
        captured.extend(Path(p) for p in config_dirs)
        return ConfigPlan(
            adapter_name="dummy",
            adapter_version="1.0",
            canonical=CanonicalConfig(
                root_dirs=[Path(p) for p in config_dirs],
                primary_config=None,
                config_files=[],
                external_files=[],
                content_hash="hash_a",
            ),
            artifacts=[],
            ingestables=[],
        )

    # Keep behavior realistic while still asserting adapter resolution path.
    monkeypatch.setattr(tracker, "prepare_config", fake_prepare_config)

    @tracker.define_step(adapter=adapter)
    def step() -> None:
        return None

    tracker.run(
        fn=step,
        year=2035,
        cache_options=CacheOptions(cache_mode="overwrite"),
    )

    record = tracker.last_run
    assert record is not None
    assert record.config["__consist_config_plan__"]["adapter"]
    assert captured == [config_root]


def test_define_step_adapter_explicit_override(
    tracker: Tracker, tmp_path, monkeypatch
) -> None:
    root_a = tmp_path / "cfg_a"
    root_b = tmp_path / "cfg_b"
    root_a.mkdir(parents=True, exist_ok=True)
    root_b.mkdir(parents=True, exist_ok=True)

    class AdapterA:
        root_dirs = [root_a]

    class AdapterB:
        root_dirs = [root_b]

    decorator_adapter = AdapterA()
    explicit_adapter = AdapterB()

    called: list[Path] = []

    def fake_prepare_config(*, adapter, config_dirs, **kwargs):
        del adapter, kwargs
        called.extend(Path(p) for p in config_dirs)
        return ConfigPlan(
            adapter_name="dummy",
            adapter_version="1.0",
            canonical=CanonicalConfig(
                root_dirs=[Path(p) for p in config_dirs],
                primary_config=None,
                config_files=[],
                external_files=[],
                content_hash="hash_b",
            ),
            artifacts=[],
            ingestables=[],
        )

    monkeypatch.setattr(tracker, "prepare_config", fake_prepare_config)

    @tracker.define_step(adapter=decorator_adapter)
    def step() -> None:
        return None

    tracker.run(
        fn=step,
        adapter=explicit_adapter,
        cache_options=CacheOptions(cache_mode="overwrite"),
    )

    assert called == [root_b]


def test_define_step_adapter_identity_inputs_metadata_stored() -> None:
    adapter = object()

    @define_step(adapter=adapter, identity_inputs=["dep.yaml"])
    def step() -> None:
        return None

    metadata = getattr(step, "__consist_step__")
    assert metadata.adapter is adapter
    assert metadata.identity_inputs == ["dep.yaml"]


def test_define_step_identity_inputs_default_applied(
    tracker: Tracker, tmp_path
) -> None:
    dep = tmp_path / "identity_dep.yaml"
    dep.write_text("threshold: 0.5\n")

    @tracker.define_step(identity_inputs=[dep])
    def step() -> None:
        return None

    tracker.run(
        fn=step,
        name="identity_metadata_step",
        cache_options=CacheOptions(cache_mode="overwrite"),
    )

    record = tracker.last_run
    assert record is not None
    digest_map = record.run.meta.get("consist_hash_inputs")
    assert isinstance(digest_map, dict)
    assert len(digest_map) == 1


def test_define_step_legacy_metadata_removed() -> None:
    with pytest.raises(TypeError, match="unexpected keyword argument 'config_plan'"):

        @define_step(config_plan=object())
        def step_config_plan() -> None:
            return None

    with pytest.raises(TypeError, match="unexpected keyword argument 'hash_inputs'"):

        @define_step(hash_inputs=["dep.yaml"])
        def step_hash_inputs() -> None:
            return None

    assert True
