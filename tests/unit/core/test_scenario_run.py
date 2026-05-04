from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

import consist
from consist.core.config_canonicalization import (
    CanonicalConfig,
    ConfigPlan,
    canonical_identity_from_config,
)
from consist.types import CacheOptions, ExecutionOptions, OutputPolicyOptions


def _assert_problem_cause_fix(message: str) -> None:
    assert "Problem:" in message
    assert "Cause:" in message
    assert "Fix:" in message


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


def test_scenario_run_promotes_coupler_inputs_for_path_binding(tracker):
    def produce(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        out_path = ctx.run_dir / "data.csv"
        pd.DataFrame({"value": [1, 2]}).to_csv(out_path, index=False)

    def consume(data: Path) -> None:
        frame = pd.read_csv(data)
        assert list(frame["value"]) == [1, 2]

    with tracker.scenario("scen_run_path_inputs") as sc:
        sc.run(
            fn=produce,
            output_paths={"data": "data.csv"},
            execution_options=ExecutionOptions(inject_context="ctx"),
        )
        sc.run(
            fn=consume,
            inputs=["data"],
            execution_options=ExecutionOptions(input_binding="paths"),
        )


def test_scenario_run_accepts_binding_result(tracker, tmp_path):
    explicit_input = tmp_path / "raw.csv"
    pd.DataFrame({"value": [10, 20]}).to_csv(explicit_input, index=False)

    def produce(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        out_path = ctx.run_dir / "data.csv"
        pd.DataFrame({"value": [1, 2]}).to_csv(out_path, index=False)

    def consume(raw: pd.DataFrame, data: pd.DataFrame) -> None:
        assert list(raw["value"]) == [10, 20]
        assert list(data["value"]) == [1, 2]

    with tracker.scenario("scen_binding_result") as sc:
        sc.run(
            fn=produce,
            output_paths={"data": "data.csv"},
            execution_options=ExecutionOptions(inject_context="ctx"),
        )
        result = sc.run(
            fn=consume,
            binding=consist.BindingResult(
                inputs={"raw": explicit_input},
                input_keys=["data"],
                optional_input_keys=["missing"],
                metadata={"source": "binding-plan"},
            ),
            execution_options=ExecutionOptions(load_inputs=True),
        )

    assert result.cache_hit is False


def test_scenario_run_binding_result_supports_path_binding(tracker, tmp_path):
    raw_path = tmp_path / "raw.csv"
    pd.DataFrame({"value": [10, 20]}).to_csv(raw_path, index=False)

    def produce(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        out_path = ctx.run_dir / "data.csv"
        pd.DataFrame({"value": [1, 2]}).to_csv(out_path, index=False)

    def consume(raw: Path, data: Path) -> None:
        assert isinstance(raw, Path)
        assert isinstance(data, Path)
        assert list(pd.read_csv(raw)["value"]) == [10, 20]
        assert list(pd.read_csv(data)["value"]) == [1, 2]

    with tracker.scenario("scen_binding_paths") as sc:
        sc.run(
            fn=produce,
            output_paths={"data": "data.csv"},
            execution_options=ExecutionOptions(inject_context="ctx"),
        )
        result = sc.run(
            fn=consume,
            binding=consist.BindingResult(
                inputs={"raw": raw_path},
                input_keys=["data"],
            ),
            execution_options=ExecutionOptions(input_binding="paths"),
        )

    assert result.cache_hit is False


def test_scenario_run_binding_result_metadata_is_inert(tracker, tmp_path):
    raw_path = tmp_path / "raw.csv"
    pd.DataFrame({"value": [7, 8]}).to_csv(raw_path, index=False)
    calls: list[str] = []

    def consume(raw: pd.DataFrame) -> None:
        calls.append("called")
        assert list(raw["value"]) == [7, 8]

    with tracker.scenario("scen_binding_metadata") as sc:
        first = sc.run(
            fn=consume,
            name="consume",
            binding=consist.BindingResult(
                inputs={"raw": raw_path},
                metadata={"source": "first-plan"},
            ),
            execution_options=ExecutionOptions(load_inputs=True),
        )
        second = sc.run(
            fn=consume,
            name="consume",
            binding=consist.BindingResult(
                inputs={"raw": raw_path},
                metadata={"source": "second-plan"},
            ),
            execution_options=ExecutionOptions(load_inputs=True),
        )

    assert calls == ["called"]
    assert first.cache_hit is False
    assert second.cache_hit is True
    assert (first.run.meta or {}).get("source") is None
    assert (second.run.meta or {}).get("source") is None
    assert (
        first.run.identity_summary["signature"]
        == second.run.identity_summary["signature"]
    )
    assert (
        first.run.identity_summary["input_hash"]
        == second.run.identity_summary["input_hash"]
    )
    assert first.run.identity_summary["inputs"] == second.run.identity_summary["inputs"]
    assert tracker.get_run_config_kv(first.run.id, prefix="source") == []
    assert set(tracker.get_artifacts_for_run(first.run.id).inputs) == {"raw"}


def test_scenario_run_binding_result_matches_primitive_cache_behavior(
    tracker, tmp_path
):
    raw_path = tmp_path / "raw.csv"
    pd.DataFrame({"value": [1, 2, 3]}).to_csv(raw_path, index=False)
    calls: list[str] = []

    def consume(raw: pd.DataFrame) -> None:
        calls.append("called")
        assert list(raw["value"]) == [1, 2, 3]

    with tracker.scenario("scen_binding_cache_parity_primitive") as primitive_sc:
        primitive = primitive_sc.run(
            fn=consume,
            name="consume",
            inputs={"raw": raw_path},
            execution_options=ExecutionOptions(load_inputs=True),
        )

    with tracker.scenario("scen_binding_cache_parity_binding") as binding_sc:
        binding = binding_sc.run(
            fn=consume,
            name="consume",
            binding=consist.BindingResult(inputs={"raw": raw_path}),
            execution_options=ExecutionOptions(load_inputs=True),
        )

    assert calls == ["called"]
    assert primitive.cache_hit is False
    assert binding.cache_hit is True
    assert (
        primitive.run.identity_summary["signature"]
        == binding.run.identity_summary["signature"]
    )
    assert (
        primitive.run.identity_summary["input_hash"]
        == binding.run.identity_summary["input_hash"]
    )


@pytest.mark.parametrize(
    ("primitive_kwargs", "expected_fragment"),
    [
        ({"inputs": {"raw": "raw.csv"}}, "inputs"),
        ({"input_keys": ["data"]}, "input_keys"),
        ({"optional_input_keys": ["data"]}, "optional_input_keys"),
    ],
)
def test_scenario_run_rejects_mixed_binding_and_primitive_input_kwargs(
    tracker, tmp_path, primitive_kwargs: dict[str, Any], expected_fragment: str
):
    binding = consist.BindingResult(inputs={"raw": tmp_path / "raw.csv"})

    with tracker.scenario("scen_binding_conflict") as sc:
        with pytest.raises(ValueError) as exc_info:
            sc.run(
                fn=lambda: None,
                name="noop",
                binding=binding,
                execution_options=ExecutionOptions(load_inputs=True),
                **primitive_kwargs,
            )

    message = str(exc_info.value)
    _assert_problem_cause_fix(message)
    assert "binding" in message
    assert expected_fragment in message


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


def test_scenario_trace_batches_parent_link_inserts(tracker, monkeypatch):
    persistence_bulk_calls: list[tuple[list[object], str]] = []
    bulk_calls: list[tuple[list[object], str]] = []
    single_calls = 0

    original_persistence_bulk = tracker.persistence.sync_run_with_links
    original_bulk = tracker.db.link_artifacts_to_run_bulk
    original_single = tracker.db.link_artifact_to_run

    def counting_persistence_bulk(run, *, artifact_ids, direction) -> None:
        persistence_bulk_calls.append((list(artifact_ids), direction))
        original_persistence_bulk(run, artifact_ids=artifact_ids, direction=direction)

    def counting_bulk(*, artifact_ids, run_id, direction) -> None:
        bulk_calls.append((list(artifact_ids), direction))
        original_bulk(artifact_ids=artifact_ids, run_id=run_id, direction=direction)

    def counting_single(artifact_id, run_id, direction) -> None:
        nonlocal single_calls
        single_calls += 1
        original_single(artifact_id, run_id, direction)

    with tracker.scenario("scen_bulk_links") as sc:
        with sc.trace(name="stage") as t:
            out_path = t.run_dir / "out.txt"
            out_path.write_text("out")
            t.log_artifact(out_path, key="out", direction="output")

            in_path = t.run_dir / "in.txt"
            in_path.write_text("in")
            t.log_artifact(in_path, key="in", direction="input")

            monkeypatch.setattr(
                tracker.persistence, "sync_run_with_links", counting_persistence_bulk
            )
            monkeypatch.setattr(tracker.db, "link_artifacts_to_run_bulk", counting_bulk)
            monkeypatch.setattr(tracker.db, "link_artifact_to_run", counting_single)

    assert persistence_bulk_calls
    assert {direction for _, direction in persistence_bulk_calls} == {"output"}
    assert bulk_calls
    assert {direction for _, direction in bulk_calls} == {"input"}
    assert single_calls == 0


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

    canonical = CanonicalConfig(
        root_dirs=[config_root],
        primary_config=None,
        config_files=[],
        external_files=[],
        content_hash="scenario_adapter_hash",
    )
    adapter_plan = ConfigPlan(
        adapter_name="scenario_adapter",
        adapter_version="1.0",
        canonical=canonical,
        artifacts=[],
        ingestables=[],
        identity=canonical_identity_from_config(
            adapter_name="scenario_adapter",
            adapter_version="1.0",
            config=canonical,
        ),
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


def test_scenario_run_forwards_define_step_adapter_identity_metadata(
    tracker, tmp_path, monkeypatch
):
    dep_file = tmp_path / "identity_dep.yaml"
    dep_file.write_text("mode: test\n")

    config_root = tmp_path / "sc_adapter_metadata"
    config_root.mkdir(parents=True, exist_ok=True)

    canonical = CanonicalConfig(
        root_dirs=[config_root],
        primary_config=None,
        config_files=[],
        external_files=[],
        content_hash="scenario_adapter_metadata_hash",
    )
    adapter_plan = ConfigPlan(
        adapter_name="scenario_adapter_metadata",
        adapter_version="1.1",
        canonical=canonical,
        artifacts=[],
        ingestables=[],
        identity=canonical_identity_from_config(
            adapter_name="scenario_adapter_metadata",
            adapter_version="1.1",
            config=canonical,
        ),
    )

    class DummyAdapter:
        model_name = "scenario_adapter_metadata"
        root_dirs = [config_root]

    dummy_adapter = DummyAdapter()
    calls: list[list[str]] = []

    def fake_prepare_config(adapter, config_dirs, **kwargs):
        del kwargs
        assert adapter is dummy_adapter
        calls.append([str(p) for p in config_dirs])
        return adapter_plan

    monkeypatch.setattr(tracker, "prepare_config", fake_prepare_config)

    @tracker.define_step(adapter=dummy_adapter, identity_inputs=[dep_file])
    def step() -> None:
        return None

    with tracker.scenario("scen_adapter_metadata_flow") as sc:
        result = sc.run(
            fn=step,
            cache_options=CacheOptions(cache_mode="overwrite"),
        )

    assert result.cache_hit is False
    assert calls == [[str(config_root)]]
    assert result.run.meta["config_adapter"] == "scenario_adapter_metadata"
    digest_map = result.run.meta.get("consist_hash_inputs")
    assert isinstance(digest_map, dict)
    assert len(digest_map) == 1


def test_scenario_run_rejects_removed_hash_inputs_kwarg(tracker):
    with tracker.scenario("scen_mixed_identity_kwargs") as sc:
        with pytest.raises(
            TypeError, match="unexpected keyword argument 'hash_inputs'"
        ):
            sc.run(
                fn=lambda: None,
                name="step",
                identity_inputs=[],
                hash_inputs=[],
            )


def test_scenario_run_rejects_removed_config_plan_kwarg(tracker):
    with tracker.scenario("scen_mixed_adapter_kwargs") as sc:
        with pytest.raises(
            TypeError, match="unexpected keyword argument 'config_plan'"
        ):
            sc.run(
                fn=lambda: None,
                name="step",
                adapter=object(),
                config_plan=object(),
            )


def test_scenario_run_rejects_removed_config_plan_kwarg_with_decorator_metadata(
    tracker,
):
    @tracker.define_step(adapter=object())
    def step() -> None:
        return None

    with tracker.scenario("scen_mixed_adapter_metadata_kwargs") as sc:
        with pytest.raises(
            TypeError, match="unexpected keyword argument 'config_plan'"
        ):
            sc.run(fn=step, config_plan=object())


def test_scenario_run_rejects_removed_hash_inputs_kwarg_with_decorator_metadata(
    tracker, tmp_path
):
    dep_file = tmp_path / "identity_dep.yaml"
    dep_file.write_text("mode: test\n")

    @tracker.define_step(identity_inputs=[dep_file])
    def step() -> None:
        return None

    with tracker.scenario("scen_mixed_identity_metadata_kwargs") as sc:
        with pytest.raises(
            TypeError, match="unexpected keyword argument 'hash_inputs'"
        ):
            sc.run(fn=step, hash_inputs=[dep_file])


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


def test_scenario_run_infers_artifact_outputs_when_outputs_omitted(tracker):
    def step(ctx):
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        out_path = ctx.run_dir / "out.txt"
        out_path.write_text("ok\n")
        return out_path

    tracker_result = tracker.run(
        fn=step,
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    with tracker.scenario("scen_infer_outputs") as sc:
        scenario_result = sc.run(
            fn=step,
            execution_options=ExecutionOptions(inject_context="ctx"),
        )

    assert set(tracker_result.outputs.keys()) == {"step"}
    assert set(scenario_result.outputs.keys()) == {"step"}
    assert tracker_result.outputs["step"].path.exists()
    assert scenario_result.outputs["step"].path.exists()


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
