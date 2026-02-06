from typing import Any, cast

from consist.core.config_canonicalization import CanonicalConfig, ConfigPlan
from consist.core.tracker import Tracker


def _dummy_config_plan(*, adapter_version: str, content_hash: str) -> ConfigPlan:
    canonical = CanonicalConfig(
        root_dirs=[],
        primary_config=None,
        config_files=[],
        external_files=[],
        content_hash=content_hash,
    )
    return ConfigPlan(
        adapter_name="dummy",
        adapter_version=adapter_version,
        canonical=canonical,
        artifacts=[],
        ingestables=[],
    )


def test_define_step_metadata_applied(tracker: Tracker) -> None:
    @tracker.define_step(outputs=["out"], tags=["demo"], description="demo step")
    def step(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        out_path = ctx.run_dir / "out.txt"
        out_path.write_text("ok")

    result = tracker.run(
        fn=step,
        output_paths={"out": "out.txt"},
        inject_context="ctx",
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
        inject_context="ctx",
    )

    run = tracker.last_run.run
    assert run.model_name == "step__y2030__phase_demand"
    assert run.tags == ["phase:demand"]
    assert run.description == "year:2030"


def test_define_step_config_plan_default_applied(tracker: Tracker) -> None:
    @tracker.define_step(
        config_plan=lambda ctx: _dummy_config_plan(
            adapter_version=str(ctx.year),
            content_hash=f"hash_{ctx.year}",
        )
    )
    def step() -> None:
        return None

    tracker.run(fn=step, year=2035, cache_mode="overwrite")

    record = tracker.last_run
    assert record is not None
    assert record.config["__consist_config_plan__"]["adapter_version"] == "2035"
    assert record.config["__consist_config_plan__"]["hash"] == "hash_2035"


def test_define_step_config_plan_explicit_override(tracker: Tracker) -> None:
    decorator_plan = _dummy_config_plan(
        adapter_version="decorator", content_hash="h_dec"
    )
    explicit_plan = _dummy_config_plan(adapter_version="explicit", content_hash="h_exp")

    @tracker.define_step(config_plan=decorator_plan)
    def step() -> None:
        return None

    tracker.run(fn=step, config_plan=explicit_plan, cache_mode="overwrite")

    record = tracker.last_run
    assert record is not None
    assert record.config["__consist_config_plan__"]["adapter_version"] == "explicit"
    assert record.config["__consist_config_plan__"]["hash"] == "h_exp"


def test_define_step_config_plan_prepare_config_resolver(
    monkeypatch, tracker: Tracker
) -> None:
    resolved_plan = _dummy_config_plan(adapter_version="resolver", content_hash="h_res")
    captured: dict[str, object] = {}

    def fake_prepare_config(**kwargs):
        captured.update(kwargs)
        return resolved_plan

    monkeypatch.setattr(tracker, "prepare_config", fake_prepare_config)

    @tracker.define_step(
        config_plan=tracker.prepare_config_resolver(
            adapter=cast(Any, "activitysim"),
            config_dirs_from="settings.config_dirs",
        )
    )
    def step(settings) -> None:
        assert settings is not None
        return None

    tracker.run(
        fn=step,
        runtime_kwargs={
            "settings": {"config_dirs": ["configs/base", "configs/overlay"]}
        },
        cache_mode="overwrite",
    )

    record = tracker.last_run
    assert record is not None
    assert record.config["__consist_config_plan__"]["adapter_version"] == "resolver"
    assert record.config["__consist_config_plan__"]["hash"] == "h_res"
    assert list(captured["config_dirs"]) == ["configs/base", "configs/overlay"]
