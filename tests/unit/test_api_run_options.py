from __future__ import annotations

import consist
import pytest
from consist import use_tracker
from consist.types import CacheOptions, ExecutionOptions


def test_consist_run_forwards_options_objects(tracker):
    calls: list[str] = []

    def step(ctx) -> None:
        calls.append("called")
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "out.txt").write_text("ok")

    with use_tracker(tracker):
        first = consist.run(
            fn=step,
            output_paths={"out": "out.txt"},
            cache_options=CacheOptions(cache_mode="reuse"),
            execution_options=ExecutionOptions(inject_context="ctx"),
        )
        second = consist.run(
            fn=step,
            output_paths={"out": "out.txt"},
            cache_options=CacheOptions(cache_mode="reuse"),
            execution_options=ExecutionOptions(inject_context="ctx"),
        )

    assert first.cache_hit is False
    assert second.cache_hit is True
    assert calls == ["called"]


def test_consist_run_rejects_legacy_policy_kwargs(tracker):
    def step() -> None:
        return None

    with use_tracker(tracker):
        with pytest.raises(
            TypeError,
            match="consist.run no longer accepts legacy policy kwarg: `cache_mode`",
        ):
            consist.run(fn=step, cache_mode="reuse")


def test_consist_get_run_result_wrapper(tracker):
    def step(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "out.txt").write_text("ok")

    with use_tracker(tracker):
        produced = consist.run(
            fn=step,
            output_paths={"out": "out.txt"},
            execution_options=ExecutionOptions(inject_context="ctx"),
        )
        historical = consist.get_run_result(produced.run.id, keys=["out"])

    assert historical.run.id == produced.run.id
    assert list(historical.outputs.keys()) == ["out"]
