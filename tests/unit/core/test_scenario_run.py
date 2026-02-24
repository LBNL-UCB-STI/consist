from __future__ import annotations

import pandas as pd
import pytest

import consist
from consist.core.config_canonicalization import CanonicalConfig, ConfigPlan
from consist.types import CacheOptions, ExecutionOptions, OutputPolicyOptions


def test_scenario_run_updates_coupler_and_cache_hit(tracker):
    calls: list[str] = []

    def step(ctx) -> None:
        calls.append("called")
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        out_path = ctx.run_dir / "out.txt"
        out_path.write_text(f"calls={len(calls)}\n")

    with tracker.scenario("scen_run_cache_A") as sc:
        result = sc.run(
            fn=step,
            output_paths={"out": "out.txt"},
            execution_options=ExecutionOptions(inject_context="ctx"),
        )
        assert "out" in sc.coupler
        assert result.cache_hit is False

    with tracker.scenario("scen_run_cache_B") as sc:
        result = sc.run(
            fn=step,
            output_paths={"out": "out.txt"},
            execution_options=ExecutionOptions(inject_context="ctx"),
        )
        assert "out" in sc.coupler
        assert result.cache_hit is True

    assert calls == ["called"]


def test_scenario_run_resolves_coupler_inputs(tracker):
    def produce(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        out_path = ctx.run_dir / "data.csv"
        pd.DataFrame({"value": [1, 2]}).to_csv(out_path, index=False)

    def consume(data: pd.DataFrame) -> None:
        assert list(data["value"]) == [1, 2]

    with tracker.scenario("scen_run_inputs") as sc:
        sc.run(
            fn=produce,
            output_paths={"data": "data.csv"},
            execution_options=ExecutionOptions(inject_context="ctx"),
        )
        sc.run(
            fn=consume,
            inputs={"data": "data"},
            execution_options=ExecutionOptions(load_inputs=True),
        )


def test_scenario_trace_updates_coupler(tracker):
    with tracker.scenario("scen_trace") as sc:
        with sc.trace(name="plot") as t:
            out_path = t.run_dir / "plot.txt"
            out_path.write_text("ok")
            t.log_artifact(out_path, key="plot", direction="output")

        assert "plot" in sc.coupler


def test_scenario_trace_updates_coupler_after_exit(tracker):
    with tracker.scenario("scen_trace_order") as sc:
        with sc.trace(name="first") as t:
            t.run_dir.mkdir(parents=True, exist_ok=True)
            out_path = t.run_dir / "alpha.txt"
            out_path.write_text("alpha")
            t.log_artifact(out_path, key="alpha", direction="output")
            first_run_id = t.current_consist.run.id

        alpha = sc.coupler.require("alpha")
        assert alpha.run_id == first_run_id

        with sc.trace(name="second", inputs=[alpha]) as t:
            assert sc.coupler.require("alpha").id == alpha.id


def test_scenario_run_supports_options_objects(tracker):
    calls: list[str] = []

    def step(ctx) -> None:
        calls.append("called")
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        out_path = ctx.run_dir / "out.txt"
        out_path.write_text(f"calls={len(calls)}\n")

    with tracker.scenario("scen_opts_A") as sc:
        first = sc.run(
            fn=step,
            output_paths={"out": "out.txt"},
            cache_options=CacheOptions(cache_mode="reuse"),
            execution_options=ExecutionOptions(inject_context="ctx"),
        )
        assert first.cache_hit is False

    with tracker.scenario("scen_opts_B") as sc:
        second = sc.run(
            fn=step,
            output_paths={"out": "out.txt"},
            cache_options=CacheOptions(cache_mode="reuse"),
            execution_options=ExecutionOptions(inject_context="ctx"),
        )
        assert second.cache_hit is True

    assert calls == ["called"]


def test_scenario_run_rejects_legacy_policy_kwargs(tracker):
    def step() -> None:
        return None

    with tracker.scenario("scen_opts_conflict") as sc:
        with pytest.raises(
            TypeError,
            match="unexpected keyword argument 'output_missing'",
        ):
            sc.run(
                fn=step,
                name="produce",
                outputs=["out"],
                output_missing="error",
            )


def test_scenario_run_accepts_adapter_identity_flow(tracker, tmp_path, monkeypatch):
    config_root = tmp_path / "sc_adapter"
    config_root.mkdir(parents=True, exist_ok=True)

    adapter_plan = ConfigPlan(
        adapter_name="scenario_adapter",
        adapter_version="1.0",
        canonical=CanonicalConfig(
            root_dirs=[config_root],
            primary_config=None,
            config_files=[],
            external_files=[],
            content_hash="scenario_adapter_hash",
        ),
        artifacts=[],
        ingestables=[],
    )

    class DummyAdapter:
        model_name = "scenario_adapter"
        root_dirs = [config_root]

    dummy_adapter = DummyAdapter()
    calls: list[list[str]] = []

    def fake_prepare_config(adapter, config_dirs, **kwargs):
        del kwargs
        assert adapter is dummy_adapter
        calls.append([str(p) for p in config_dirs])
        return adapter_plan

    monkeypatch.setattr(tracker, "prepare_config", fake_prepare_config)

    with tracker.scenario("scen_adapter_flow") as sc:
        result = sc.run(
            fn=lambda: None,
            name="produce",
            adapter=dummy_adapter,
            cache_options=CacheOptions(cache_mode="overwrite"),
        )

    assert result.cache_hit is False
    assert calls == [[str(config_root)]]
    assert result.run.meta["config_adapter"] == "scenario_adapter"


def test_scenario_run_accepts_ref_and_single_output_run_result(tracker):
    def produce_multi(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "left.txt").write_text("left")
        (ctx.run_dir / "right.txt").write_text("right")

    def produce_single(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "single.txt").write_text("single")

    with tracker.scenario("scen_run_refs") as sc:
        multi = sc.run(
            fn=produce_multi,
            output_paths={"left": "left.txt", "right": "right.txt"},
            execution_options=ExecutionOptions(inject_context="ctx"),
        )
        sc.run(
            fn=lambda: None,
            inputs={"picked": consist.ref(multi, key="right")},
            execution_options=ExecutionOptions(load_inputs=False),
        )

        single = sc.run(
            fn=produce_single,
            output_paths={"single": "single.txt"},
            execution_options=ExecutionOptions(inject_context="ctx"),
        )
        sc.run(
            fn=lambda: None,
            inputs={"direct": single},
            execution_options=ExecutionOptions(load_inputs=False),
        )


def test_scenario_run_rejects_ambiguous_run_result_input(tracker):
    def produce_multi(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "a.txt").write_text("a")
        (ctx.run_dir / "b.txt").write_text("b")

    with tracker.scenario("scen_run_ambiguous_result") as sc:
        produced = sc.run(
            fn=produce_multi,
            output_paths={"a": "a.txt", "b": "b.txt"},
            execution_options=ExecutionOptions(inject_context="ctx"),
        )

        with pytest.raises(
            ValueError, match="consist.ref\\(\\.\\.\\., key='\\.\\.\\.'\\)"
        ):
            sc.run(
                fn=lambda: None,
                inputs={"upstream": produced},
                execution_options=ExecutionOptions(load_inputs=False),
            )


def test_scenario_run_cache_options_match_tracker_run(tracker):
    def step(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        out_path = ctx.run_dir / "out.txt"
        out_path.write_text("ok\n")

    options = CacheOptions(
        cache_mode="overwrite",
        cache_epoch=11,
        cache_version=3,
        validate_cached_outputs="eager",
        code_identity="callable_module",
    )

    tracker_result = tracker.run(
        fn=step,
        year=2035,
        output_paths={"out": "out.txt"},
        cache_options=options,
        execution_options=ExecutionOptions(inject_context="ctx"),
    )
    tracker_run = tracker.last_run.run

    with tracker.scenario("scen_cache_parity") as sc:
        scenario_result = sc.run(
            fn=step,
            year=2035,
            output_paths={"out": "out.txt"},
            cache_options=options,
            execution_options=ExecutionOptions(inject_context="ctx"),
        )
        scenario_run = tracker.last_run.run

    assert tracker_result.cache_hit is False
    assert scenario_result.cache_hit is False
    assert tracker_run.config_hash == scenario_run.config_hash
    assert tracker_run.git_hash == scenario_run.git_hash
    assert tracker_run.meta["cache_epoch"] == scenario_run.meta["cache_epoch"] == 11
    assert tracker_run.meta["cache_version"] == scenario_run.meta["cache_version"] == 3
    assert (
        tracker_run.meta["code_identity"]
        == scenario_run.meta["code_identity"]
        == "callable_module"
    )


def test_scenario_run_output_policy_matches_tracker_run(tracker):
    def missing_step() -> None:
        return None

    def mismatch_step():
        return ["only-one"]

    with pytest.raises(RuntimeError, match="missing outputs"):
        tracker.run(
            fn=missing_step,
            name="tracker_missing",
            outputs=["out"],
            output_policy=OutputPolicyOptions(output_missing="error"),
        )

    with tracker.scenario("scen_output_policy_missing") as sc:
        with pytest.raises(RuntimeError, match="missing outputs"):
            sc.run(
                fn=missing_step,
                name="scenario_missing",
                outputs=["out"],
                output_policy=OutputPolicyOptions(output_missing="error"),
            )

    with pytest.raises(RuntimeError, match="Output list length does not match"):
        tracker.run(
            fn=mismatch_step,
            name="tracker_mismatch",
            outputs=["a", "b"],
            output_policy=OutputPolicyOptions(
                output_mismatch="error",
                output_missing="ignore",
            ),
        )

    with tracker.scenario("scen_output_policy_mismatch") as sc:
        with pytest.raises(RuntimeError, match="Output list length does not match"):
            sc.run(
                fn=mismatch_step,
                name="scenario_mismatch",
                outputs=["a", "b"],
                output_policy=OutputPolicyOptions(
                    output_mismatch="error",
                    output_missing="ignore",
                ),
            )


def test_scenario_run_execution_options_match_tracker_run(tracker, tmp_path):
    input_path = tmp_path / "data.csv"
    pd.DataFrame({"value": [0, 1, 2, 3]}).to_csv(input_path, index=False)

    def step(data: pd.DataFrame, threshold: int, ctx) -> dict:
        filtered = data[data["value"] >= threshold]
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        out_path = ctx.run_dir / "filtered.csv"
        filtered.to_csv(out_path, index=False)
        return {"filtered": out_path}

    options = ExecutionOptions(
        load_inputs=True,
        inject_context="ctx",
        runtime_kwargs={"threshold": 2},
    )

    tracker_result = tracker.run(
        fn=step,
        inputs={"data": input_path},
        outputs=["filtered"],
        execution_options=options,
    )

    with tracker.scenario("scen_execution_parity") as sc:
        scenario_result = sc.run(
            fn=step,
            inputs={"data": input_path},
            outputs=["filtered"],
            execution_options=options,
        )

    assert set(tracker_result.outputs.keys()) == {"filtered"}
    assert set(scenario_result.outputs.keys()) == {"filtered"}
    tracker_values = pd.read_csv(tracker_result.outputs["filtered"].path)[
        "value"
    ].tolist()
    scenario_values = pd.read_csv(scenario_result.outputs["filtered"].path)[
        "value"
    ].tolist()
    assert tracker_values == scenario_values == [2, 3]


def test_scenario_run_metadata_resolution_matches_tracker_run(tracker):
    @tracker.define_step(
        name_template="{func_name}__y{year}",
        description=lambda ctx: f"phase:{ctx.phase}",
        tags=lambda ctx: [f"stage:{ctx.stage}"],
    )
    def step(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "out.txt").write_text("ok\n")

    tracker.run(
        fn=step,
        year=2040,
        phase="calibration",
        stage="model",
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )
    tracker_run = tracker.last_run.run

    with tracker.scenario("scen_metadata_parity") as sc:
        sc.run(
            fn=step,
            year=2040,
            phase="calibration",
            stage="model",
            output_paths={"out": "out.txt"},
            execution_options=ExecutionOptions(inject_context="ctx"),
        )
        scenario_run = tracker.last_run.run

    assert tracker_run.model_name == scenario_run.model_name == "step__y2040"
    assert tracker_run.description == scenario_run.description == "phase:calibration"
    assert tracker_run.tags == scenario_run.tags == ["stage:model"]
    assert "step__y2040" in tracker_run.id
    assert "step__y2040" in scenario_run.id
