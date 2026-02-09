from pathlib import Path

import consist


def test_noop_scenario_run_outputs(tmp_path: Path) -> None:
    out_path = tmp_path / "output.txt"

    with consist.scenario("noop", enabled=False) as sc:
        result = sc.run(
            name="step",
            outputs=["out"],
            output_paths={"out": out_path},
        )

        assert result.cache_hit is False
        assert "out" in result.outputs
        assert result.outputs["out"].path == out_path

        sc.collect_by_keys(result.outputs, "out")
        assert sc.coupler.path("out") == out_path


def test_noop_scenario_trace_context() -> None:
    with consist.noop_scenario("noop_trace") as sc:
        with sc.trace("step") as ctx:
            artifact = ctx.log_output("file.txt", key="out")
            assert artifact.key == "out"
            assert artifact.get_path().name == "file.txt"


def test_noop_scenario_coupler_view_namespaces_keys() -> None:
    with consist.scenario("noop_namespace", enabled=False) as sc:
        beam = sc.coupler.view("beam")
        artifact = beam.set("plans", "plans.csv")

        assert artifact == "plans.csv"
        assert beam.require("plans") == "plans.csv"
        assert sc.coupler.require("beam/plans") == "plans.csv"
