from pathlib import Path

import pytest

import consist
from consist.types import ExecutionOptions


def test_noop_and_real_run_parity(tracker, tmp_path: Path) -> None:
    out_path = tmp_path / "out.txt"

    def step(*, output_path: Path) -> dict:
        output_path.write_text("ok")
        return {"out": output_path}

    with tracker.scenario("real") as sc:
        real_result = sc.run(
            fn=step,
            name="step",
            execution_options=ExecutionOptions(
                runtime_kwargs={"output_path": out_path}
            ),
            outputs=["out"],
        )

    noop_tracker = consist.NoopTracker()
    with noop_tracker.scenario("noop") as sc:
        noop_result = sc.run(
            fn=step,
            name="step",
            execution_options=ExecutionOptions(
                runtime_kwargs={"output_path": out_path}
            ),
            outputs=["out"],
        )

    assert set(real_result.outputs.keys()) == set(noop_result.outputs.keys())
    assert real_result.outputs["out"].path == out_path
    assert noop_result.outputs["out"].path == out_path


def test_noop_and_real_missing_runtime_kwargs(tracker) -> None:
    def step(*, required: int) -> None:
        return None

    with pytest.raises(TypeError):
        tracker.run(
            fn=step,
            name="real_step",
            execution_options=ExecutionOptions(runtime_kwargs={}),
        )

    noop_tracker = consist.NoopTracker()
    with pytest.raises(TypeError):
        noop_tracker.run(
            fn=step,
            name="noop_step",
            execution_options=ExecutionOptions(runtime_kwargs={}),
        )
