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


def test_find_latest_run_accepts_iteration_filter(tracker):
    with tracker.start_run(
        "run_iter0_old",
        "demo_iter_filter",
        parent_run_id="scenario_iter_filter",
        iteration=0,
    ):
        pass
    with tracker.start_run(
        "run_iter1_latest",
        "demo_iter_filter",
        parent_run_id="scenario_iter_filter",
        iteration=1,
    ):
        pass
    with tracker.start_run(
        "run_iter0_latest",
        "demo_iter_filter",
        parent_run_id="scenario_iter_filter",
        iteration=0,
    ):
        pass

    latest = tracker.find_latest_run(
        parent_id="scenario_iter_filter",
        model="demo_iter_filter",
        iteration=0,
        status="completed",
    )
    assert latest.id == "run_iter0_latest"


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


def test_find_latest_run_filters_stage_and_phase(tracker):
    with tracker.start_run(
        "run_stage_old",
        "demo_stage_phase",
        parent_run_id="scenario_stage_phase",
        iteration=1,
        stage="supply_demand_loop",
        phase="warm_start",
    ):
        pass
    with tracker.start_run(
        "run_stage_latest",
        "demo_stage_phase",
        parent_run_id="scenario_stage_phase",
        iteration=2,
        stage="supply_demand_loop",
        phase="traffic_assignment",
    ):
        pass
    with tracker.start_run(
        "run_stage_other",
        "demo_stage_phase",
        parent_run_id="scenario_stage_phase",
        iteration=3,
        stage="supply_demand_loop",
        phase="sketch",
    ):
        pass

    latest = tracker.find_latest_run(
        parent_id="scenario_stage_phase",
        model="demo_stage_phase",
        stage="supply_demand_loop",
        phase="traffic_assignment",
        status="completed",
    )
    assert latest.id == "run_stage_latest"


def test_find_latest_run_filters_iteration_and_facet_before_latest_selection(tracker):
    base_facet = {"scenario_id": "base"}
    alt_facet = {"scenario_id": "alt"}

    with tracker.start_run(
        "facet_iter0_old",
        "demo_iter_facet",
        parent_run_id="scenario_iter_facet",
        year=2017,
        iteration=0,
        stage="activity_demand_run",
        phase="run",
        facet=base_facet,
    ):
        pass
    with tracker.start_run(
        "facet_iter1_newer",
        "demo_iter_facet",
        parent_run_id="scenario_iter_facet",
        year=2017,
        iteration=1,
        stage="activity_demand_run",
        phase="run",
        facet=base_facet,
    ):
        pass
    with tracker.start_run(
        "facet_iter0_latest",
        "demo_iter_facet",
        parent_run_id="scenario_iter_facet",
        year=2017,
        iteration=0,
        stage="activity_demand_run",
        phase="run",
        facet=base_facet,
    ):
        pass
    with tracker.start_run(
        "facet_iter0_other_facet",
        "demo_iter_facet",
        parent_run_id="scenario_iter_facet",
        year=2017,
        iteration=0,
        stage="activity_demand_run",
        phase="run",
        facet=alt_facet,
    ):
        pass

    latest = tracker.find_latest_run(
        parent_id="scenario_iter_facet",
        model="demo_iter_facet",
        year=2017,
        iteration=0,
        stage="activity_demand_run",
        phase="run",
        status="completed",
        facet={"scenario_id": "base"},
    )
    assert latest.id == "facet_iter0_latest"


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
