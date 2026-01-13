import pytest

import consist


def test_noop_tracker_run_validates_signature() -> None:
    def step(*, required: int) -> None:
        return None

    tracker = consist.NoopTracker()

    with pytest.raises(TypeError, match="missing 1 required keyword-only argument"):
        tracker.run(fn=step, name="noop_step", runtime_kwargs={})


def test_noop_tracker_scenario_context() -> None:
    tracker = consist.NoopTracker()

    with tracker.scenario("noop") as sc:
        result = sc.run(name="step")
        assert result.run.model_name == "step"
