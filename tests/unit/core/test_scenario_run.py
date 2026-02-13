from __future__ import annotations

import pandas as pd
import pytest

import consist
from consist.types import CacheOptions, ExecutionOptions


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
