import time

import pytest


def test_find_latest_run_by_iteration(tracker):
    with tracker.start_run(
        "run_0", "demo_model", parent_run_id="scenario_iter", iteration=0
    ):
        pass
    with tracker.start_run(
        "run_1", "demo_model", parent_run_id="scenario_iter", iteration=1
    ):
        pass

    latest = tracker.find_latest_run(
        parent_id="scenario_iter", model="demo_model", status="completed"
    )
    assert latest.id == "run_1"


def test_find_latest_run_by_created_at(tracker):
    with tracker.start_run("run_a", "demo_no_iter", parent_run_id="scenario_time"):
        pass
    time.sleep(0.01)
    with tracker.start_run("run_b", "demo_no_iter", parent_run_id="scenario_time"):
        pass

    latest = tracker.find_latest_run(
        parent_id="scenario_time", model="demo_no_iter", status="completed"
    )
    assert latest.id == "run_b"


def test_get_latest_run_id(tracker):
    with tracker.start_run(
        "run_latest", "demo_latest", parent_run_id="scenario_latest", iteration=3
    ):
        pass

    latest_id = tracker.get_latest_run_id(
        parent_id="scenario_latest", model="demo_latest"
    )
    assert latest_id == "run_latest"


def test_find_latest_run_raises_when_missing(tracker):
    with pytest.raises(ValueError, match="No runs found"):
        tracker.find_latest_run(parent_id="missing", model="nope")
