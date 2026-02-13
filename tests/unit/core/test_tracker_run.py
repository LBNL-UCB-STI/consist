from __future__ import annotations

import logging

import pandas as pd
import pytest

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
