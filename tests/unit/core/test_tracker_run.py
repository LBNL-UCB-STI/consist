from __future__ import annotations

import logging

import pandas as pd
import pytest

import consist
from consist.core.tracker import Tracker
from consist.types import CacheOptions, ExecutionOptions, OutputPolicyOptions


def test_tracker_run_load_inputs_requires_mapping(tracker, sample_csv):
    input_path = sample_csv("inputs.csv", rows=2)

    def step(data: pd.DataFrame) -> None:
        assert not data.empty

    with pytest.raises(
        ValueError, match="load_inputs=True requires inputs to be a dict"
    ):
        tracker.run(
            fn=step,
            inputs=[input_path],
            execution_options=ExecutionOptions(load_inputs=True),
        )


def test_tracker_run_loads_inputs_and_injects_context(tracker, sample_csv):
    input_path = sample_csv("data.csv", rows=4)

    def step(data: pd.DataFrame, threshold: int, ctx) -> None:
        assert len(data) == 4
        assert threshold == 2
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        out_path = ctx.run_dir / "filtered.csv"
        data[data["value"] >= threshold].to_csv(out_path, index=False)

    result = tracker.run(
        fn=step,
        inputs={"data": input_path},
        config={"threshold": 2},
        output_paths={"filtered": "filtered.csv"},
        execution_options=ExecutionOptions(load_inputs=True, inject_context="ctx"),
    )

    assert "filtered" in result.outputs
    assert result.outputs["filtered"].path.exists()


def test_tracker_run_cache_hit_skips_callable(tracker):
    calls: list[str] = []

    def step(ctx) -> None:
        calls.append("called")
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        out_path = ctx.run_dir / "out.txt"
        out_path.write_text(f"calls={len(calls)}\n")

    first = tracker.run(
        fn=step,
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )
    second = tracker.run(
        fn=step,
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    assert calls == ["called"]
    assert first.outputs["out"].path.read_text() == "calls=1\n"
    assert second.cache_hit is True


def test_run_context_run_dir_is_created(tracker):
    def step(ctx):
        out_path = ctx.run_dir / "out.txt"
        out_path.write_text("ok\n")
        return out_path

    result = tracker.run(
        fn=step,
        outputs=["out"],
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    assert result.outputs["out"].path.exists()


def test_start_run_overwrite_updates_cache_index(tracker, tmp_path):
    input_path = tmp_path / "input.txt"
    input_path.write_text("payload")
    config = {"param": 1}

    with tracker.start_run(
        "run_a_overwrite", "overwrite_model", config=config, inputs=[str(input_path)]
    ):
        pass

    with tracker.start_run(
        "run_b_overwrite",
        "overwrite_model",
        config=config,
        inputs=[str(input_path)],
        cache_mode="overwrite",
    ) as t:
        assert not t.is_cached

    with tracker.start_run(
        "run_c_overwrite",
        "overwrite_model",
        config=config,
        inputs=[str(input_path)],
        cache_mode="reuse",
    ) as t:
        assert t.is_cached
        assert t.current_consist.cached_run.id == "run_b_overwrite"


def test_tracker_run_output_mismatch_warns(tracker, caplog):
    def step():
        return ["only-one"]

    with caplog.at_level(logging.WARNING):
        result = tracker.run(
            fn=step,
            outputs=["a", "b"],
            output_policy=OutputPolicyOptions(
                output_mismatch="warn",
                output_missing="ignore",
            ),
        )

    assert any(
        "Output list length does not match declared outputs" in record.message
        for record in caplog.records
    )
    assert result.outputs == {}


def test_tracker_run_output_missing_error(tracker):
    def step() -> None:
        return None

    with pytest.raises(RuntimeError, match="missing outputs"):
        tracker.run(
            fn=step,
            outputs=["out"],
            output_policy=OutputPolicyOptions(output_missing="error"),
        )


def test_tracker_run_inject_context_requires_param(tracker):
    def step() -> None:
        return None

    with pytest.raises(ValueError, match="inject_context requested"):
        tracker.run(
            fn=step,
            execution_options=ExecutionOptions(inject_context=True),
        )


def test_consist_ref_selects_outputs_and_validates_keys(tracker):
    def multi_output_step(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "left.txt").write_text("left")
        (ctx.run_dir / "right.txt").write_text("right")

    result = tracker.run(
        fn=multi_output_step,
        output_paths={"left": "left.txt", "right": "right.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    selected = consist.ref(result, key="right")
    assert selected.key == "right"

    with pytest.raises(KeyError, match="Available keys: 'left', 'right'"):
        consist.ref(result, key="missing")

    with pytest.raises(ValueError, match="consist.ref\\(\\.\\.\\., key='\\.\\.\\.'\\)"):
        consist.ref(result)

    empty = tracker.run(fn=lambda: None)
    with pytest.raises(ValueError, match="consist.ref\\(\\.\\.\\., key='\\.\\.\\.'\\)"):
        consist.ref(empty)


def test_consist_refs_builds_and_combines_input_mappings(tracker):
    def produce_people(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "households.txt").write_text("households")
        (ctx.run_dir / "persons.txt").write_text("persons")

    def produce_network(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "skims.txt").write_text("skims")

    people = tracker.run(
        fn=produce_people,
        output_paths={"households": "households.txt", "persons": "persons.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )
    network = tracker.run(
        fn=produce_network,
        output_paths={"skims": "skims.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    single_result_mapping = consist.refs(people, "households", "persons")
    assert set(single_result_mapping.keys()) == {"households", "persons"}

    combined = consist.refs((people, "households"), (network, "skims"))
    assert set(combined.keys()) == {"households", "skims"}

    aliased = consist.refs(hh=(people, "households"), travel_skims=(network, "skims"))
    assert aliased["hh"].key == "households"
    assert aliased["travel_skims"].key == "skims"

    alias_map = consist.refs(
        people, {"hh": "households", "travel_persons": "persons"}
    )
    assert alias_map["hh"].key == "households"
    assert alias_map["travel_persons"].key == "persons"

    tracker.run(
        fn=lambda: None,
        inputs=combined,
        execution_options=ExecutionOptions(load_inputs=False),
    )
    assert any(a.key == "households" for a in tracker.last_run.inputs)
    assert any(a.key == "skims" for a in tracker.last_run.inputs)


def test_consist_refs_validates_duplicates_and_empty_selectors(tracker):
    def produce(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "single.txt").write_text("single")

    result = tracker.run(
        fn=produce,
        output_paths={"single": "single.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    with pytest.raises(ValueError, match="requires at least one selector"):
        consist.refs()

    with pytest.raises(ValueError, match="Duplicate input key"):
        consist.refs((result, "single"), single=(result, "single"))

    with pytest.raises(ValueError, match="alias mapping cannot be empty"):
        consist.refs(result, {})

    with pytest.raises(TypeError, match="alias mapping keys must be strings"):
        consist.refs(result, {1: "single"})

    with pytest.raises(TypeError, match="alias mapping values must be strings"):
        consist.refs(result, {"x": 1})


def test_tracker_run_accepts_direct_single_output_run_result_input(tracker):
    def produce(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "single.txt").write_text("single")

    produced = tracker.run(
        fn=produce,
        output_paths={"single": "single.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    tracker.run(
        fn=lambda: None,
        inputs={"upstream": produced},
        execution_options=ExecutionOptions(load_inputs=False),
    )

    assert any(a.key == "single" for a in tracker.last_run.inputs)


def test_tracker_run_rejects_ambiguous_run_result_input(tracker):
    def produce(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "a.txt").write_text("a")
        (ctx.run_dir / "b.txt").write_text("b")

    produced = tracker.run(
        fn=produce,
        output_paths={"a": "a.txt", "b": "b.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    with pytest.raises(ValueError, match="consist.ref\\(\\.\\.\\., key='\\.\\.\\.'\\)"):
        tracker.run(
            fn=lambda: None,
            inputs={"upstream": produced},
            execution_options=ExecutionOptions(load_inputs=False),
        )


def test_tracker_get_run_result_returns_selected_outputs(tracker):
    def step(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "a.txt").write_text("a")
        (ctx.run_dir / "b.txt").write_text("b")

    produced = tracker.run(
        fn=step,
        output_paths={"a": "a.txt", "b": "b.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    historical = tracker.get_run_result(produced.run.id)
    assert historical.run.id == produced.run.id
    assert set(historical.outputs.keys()) == {"a", "b"}

    subset = tracker.get_run_result(produced.run.id, keys=["b"])
    assert list(subset.outputs.keys()) == ["b"]


def test_tracker_get_run_result_validates_keys_and_run_id(tracker):
    def step(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "out.txt").write_text("ok")

    produced = tracker.run(
        fn=step,
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    with pytest.raises(KeyError, match="missing requested output keys"):
        tracker.get_run_result(produced.run.id, keys=["missing"])

    with pytest.raises(KeyError, match="was not found"):
        tracker.get_run_result("missing-run-id")

    with pytest.raises(ValueError, match="validate must be one of"):
        tracker.get_run_result(produced.run.id, validate="bogus")


def test_tracker_get_run_result_strict_validation_checks_paths(tracker):
    def step(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        out_path = ctx.run_dir / "out.txt"
        out_path.write_text("ok")

    produced = tracker.run(
        fn=step,
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )
    produced.outputs["out"].path.unlink()

    with pytest.raises(FileNotFoundError, match="missing output files"):
        tracker.get_run_result(produced.run.id, validate="strict")


def test_cache_epoch_affects_config_hash(tmp_path):
    def step(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "out.txt").write_text("ok")

    tracker_a = Tracker(run_dir=tmp_path / "epoch_a", cache_epoch=1)
    tracker_a.run(
        fn=step,
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
        cache_options=CacheOptions(cache_mode="overwrite"),
    )
    hash_a = tracker_a.last_run.run.config_hash

    tracker_b = Tracker(run_dir=tmp_path / "epoch_b", cache_epoch=2)
    tracker_b.run(
        fn=step,
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
        cache_options=CacheOptions(cache_mode="overwrite"),
    )
    hash_b = tracker_b.last_run.run.config_hash

    assert hash_a != hash_b


def test_cache_version_affects_config_hash(tracker):
    def step(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "out.txt").write_text("ok")

    tracker.run(
        fn=step,
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
        cache_options=CacheOptions(cache_version=1, cache_mode="overwrite"),
    )
    hash_v1 = tracker.last_run.run.config_hash

    tracker.run(
        fn=step,
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
        cache_options=CacheOptions(cache_version=2, cache_mode="overwrite"),
    )
    hash_v2 = tracker.last_run.run.config_hash

    assert hash_v1 != hash_v2


def test_tracker_run_rejects_legacy_policy_kwargs(tracker):
    def step() -> None:
        return None

    with pytest.raises(
        TypeError,
        match="unexpected keyword argument 'cache_mode'",
    ):
        tracker.run(fn=step, cache_mode="reuse")


def test_tracker_run_output_policy_options_enforced(tracker):
    def step() -> None:
        return None

    with pytest.raises(RuntimeError, match="missing outputs"):
        tracker.run(
            fn=step,
            outputs=["out"],
            output_policy=OutputPolicyOptions(output_missing="error"),
        )
