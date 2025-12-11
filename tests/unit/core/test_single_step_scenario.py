import pandas as pd

import consist
from consist.core.tracker import Tracker


def test_single_step_scenario_simple(tmp_path):
    """
    Demonstrates single_step_scenario for a one-off run with a header + single step.
    """
    run_dir = tmp_path / "runs_single_step"
    db_path = str(tmp_path / "single_step.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)
    tracker.identity.hashing_strategy = "fast"

    df = pd.DataFrame({"x": [1, 2, 3]})

    # First scenario run
    with consist.single_step_scenario("scenario_single", tracker=tracker, model="demo"):
        art = consist.log_dataframe(df, key="table1")
        assert art.key == "table1"

    # Header and step runs should be persisted and completed
    header = tracker.get_run("scenario_single")
    step_run = tracker.get_run("scenario_single_scenario_single")
    assert header is not None and header.status == "completed"
    assert step_run is not None and step_run.status == "completed"

    # Artifact retrievable via tracker helper
    arts = tracker.get_artifacts_for_run(step_run.id)
    assert "table1" in arts.outputs
