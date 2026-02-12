import pytest

import consist
from consist.types import ExecutionOptions


def test_noop_tracker_run_validates_signature() -> None:
    def step(*, required: int) -> None:
        return None

    tracker = consist.NoopTracker()

    with pytest.raises(TypeError, match="missing 1 required keyword-only argument"):
        tracker.run(
            fn=step,
            name="noop_step",
            execution_options=ExecutionOptions(runtime_kwargs={}),
        )


def test_noop_tracker_scenario_context() -> None:
    tracker = consist.NoopTracker()

    with tracker.scenario("noop") as sc:
        result = sc.run(name="step")
        assert result.run.model_name == "step"
