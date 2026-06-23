import pytest

import consist
from consist.types import ExecutionOptions, OutputSet


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


def test_noop_tracker_accepts_output_sets(tmp_path) -> None:
    annual_dir = tmp_path / "annual"
    annual_dir.mkdir()
    (annual_dir / "annual_2030.csv").write_text("year\n2030\n")

    tracker = consist.NoopTracker(output_base_dir=tmp_path)
    result = tracker.run(
        name="noop_output_sets",
        output_sets={
            "annual_outputs": OutputSet(root="annual", include="annual_*.csv")
        },
    )

    parent = result.outputs["annual_outputs"]
    assert parent.key == "annual_outputs"
    assert parent.meta["artifact_set"] is True
    assert parent.meta["output_set_manifest"]["members"][0]["relative_path"] == (
        "annual_2030.csv"
    )
