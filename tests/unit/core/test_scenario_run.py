from __future__ import annotations

import pandas as pd


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
            inject_context="ctx",
        )
        assert "out" in sc.coupler
        assert result.cache_hit is False

    with tracker.scenario("scen_run_cache_B") as sc:
        result = sc.run(
            fn=step,
            output_paths={"out": "out.txt"},
            inject_context="ctx",
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
            inject_context="ctx",
        )
        sc.run(
            fn=consume,
            inputs={"data": "data"},
            load_inputs=True,
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
