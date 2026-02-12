from __future__ import annotations

import consist
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
