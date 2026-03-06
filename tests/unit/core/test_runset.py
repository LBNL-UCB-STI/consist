import pandas as pd
import pytest

import consist
from consist import RunSet, Tracker


def _create_run(
    tracker: Tracker,
    *,
    run_id: str,
    model: str,
    year: int | None = None,
    facet: dict | None = None,
    config: dict | None = None,
) -> None:
    tracker.begin_run(
        run_id=run_id,
        model=model,
        year=year,
        facet=facet,
        config=config,
    )
    tracker.end_run()


def test_runset_split_filter_and_latest(tracker: Tracker) -> None:
    _create_run(
        tracker,
        run_id="base_2025_a",
        model="sim",
        year=2025,
        facet={"scenario_id": "base", "seed": 41},
        config={"alpha": 1},
    )
    _create_run(
        tracker,
        run_id="base_2025_b",
        model="sim",
        year=2025,
        facet={"scenario_id": "base", "seed": 42},
        config={"alpha": 2},
    )
    _create_run(
        tracker,
        run_id="policy_2030",
        model="sim",
        year=2030,
        facet={"scenario_id": "policy", "seed": 99},
        config={"alpha": 3},
    )

    runs = RunSet.from_query(tracker, label="all", model="sim", limit=20)

    by_year = runs.split_by("year")
    assert list(by_year.keys()) == [2025, 2030]
    assert len(by_year[2025]) == 2
    assert len(by_year[2030]) == 1

    by_scenario = runs.split_by("scenario_id")
    assert list(by_scenario.keys()) == ["base", "policy"]
    assert len(by_scenario["base"]) == 2

    base_completed = runs.filter(status="completed", scenario_id="base")
    assert len(base_completed) == 2

    latest_grouped = base_completed.latest(group_by=["year", "scenario_id"])
    assert len(latest_grouped) == 1
    assert latest_grouped[0].id == "base_2025_b"

    latest_overall = base_completed.latest()
    assert len(latest_overall) == 1
    assert latest_overall[0].id == "base_2025_b"


def test_runset_align_apply_and_config_diffs(tracker: Tracker) -> None:
    _create_run(
        tracker,
        run_id="baseline_2025",
        model="compare_model",
        year=2025,
        facet={"scenario": "baseline"},
        config={"alpha": 1, "beta": 2025},
    )
    _create_run(
        tracker,
        run_id="baseline_2030",
        model="compare_model",
        year=2030,
        facet={"scenario": "baseline"},
        config={"alpha": 1, "beta": 2030},
    )
    _create_run(
        tracker,
        run_id="policy_2025",
        model="compare_model",
        year=2025,
        facet={"scenario": "policy"},
        config={"alpha": 2, "beta": 2025},
    )
    _create_run(
        tracker,
        run_id="policy_2030",
        model="compare_model",
        year=2030,
        facet={"scenario": "policy"},
        config={"alpha": 3, "beta": 2030},
    )

    runs = RunSet.from_query(tracker, model="compare_model", limit=20)
    baseline = runs.filter(scenario="baseline")
    policy = runs.filter(scenario="policy")
    aligned = baseline.align(policy, on="year")

    assert aligned.keys == [2025, 2030]
    assert [(left.id, right.id) for left, right in aligned.pairs()] == [
        ("baseline_2025", "policy_2025"),
        ("baseline_2030", "policy_2030"),
    ]

    summary = aligned.to_frame()
    assert summary["left_run_id"].tolist() == ["baseline_2025", "baseline_2030"]
    assert summary["right_run_id"].tolist() == ["policy_2025", "policy_2030"]

    applied = aligned.apply(
        lambda left, right, year: pd.DataFrame(
            {"left": [left.id], "right": [right.id], "year": [year]}
        )
    )
    assert applied["_align_key"].tolist() == [2025, 2030]
    assert applied["year"].tolist() == [2025, 2030]

    diffs = aligned.config_diffs()
    assert {
        "on_value",
        "key",
        "namespace",
        "status",
        "left_value",
        "right_value",
    }.issubset(diffs.columns)
    assert (diffs["status"] == "changed").any()


def test_runset_align_raises_on_duplicate_keys(tracker: Tracker) -> None:
    _create_run(
        tracker,
        run_id="left_2025_a",
        model="dup_model",
        year=2025,
        facet={"side": "left"},
        config={"alpha": 1},
    )
    _create_run(
        tracker,
        run_id="left_2025_b",
        model="dup_model",
        year=2025,
        facet={"side": "left"},
        config={"alpha": 2},
    )
    _create_run(
        tracker,
        run_id="right_2025",
        model="dup_model",
        year=2025,
        facet={"side": "right"},
        config={"alpha": 3},
    )

    runs = RunSet.from_query(tracker, model="dup_model", limit=20)
    left = runs.filter(side="left")
    right = runs.filter(side="right")

    with pytest.raises(
        ValueError, match="Use \\.latest\\(\\) or \\.filter\\(\\) first"
    ):
        left.align(right, on="year")


def test_runset_to_frame_includes_union_of_facet_columns(tracker: Tracker) -> None:
    _create_run(
        tracker,
        run_id="frame_1",
        model="frame_model",
        year=2025,
        facet={"scenario": "base", "seed": 42},
        config={"alpha": 1},
    )
    _create_run(
        tracker,
        run_id="frame_2",
        model="frame_model",
        year=2030,
        facet={"scenario": "policy", "policy_name": "transit"},
        config={"alpha": 2},
    )

    runs = RunSet.from_query(tracker, label="frames", model="frame_model", limit=20)
    frame = runs.to_frame()

    assert frame["label"].tolist() == ["frames", "frames"]
    assert {"scenario", "seed", "policy_name"}.issubset(frame.columns)
    second = frame.loc[frame["run_id"] == "frame_2"].iloc[0]
    assert pd.isna(second["seed"])


def test_runset_empty_behavior_and_helpers(tracker: Tracker) -> None:
    empty = RunSet.from_runs([], label="empty")
    assert empty.split_by("year") == {}
    assert len(empty.filter(status="completed")) == 0
    assert len(empty.latest()) == 0

    _create_run(
        tracker,
        run_id="helper_run",
        model="helper_model",
        year=2025,
        facet={"scenario": "base"},
        config={"alpha": 1},
    )
    tracker_runs = tracker.run_set(label="helpers", model="helper_model")
    assert isinstance(tracker_runs, RunSet)
    assert tracker_runs.label == "helpers"
    assert len(tracker_runs) == 1

    api_runs = consist.run_set(tracker=tracker, model="helper_model")
    assert len(api_runs) == 1

    empty_query = RunSet.from_query(tracker, model="does_not_exist")
    aligned_empty = empty_query.align(empty_query, on="year")
    assert aligned_empty.keys == []
    assert aligned_empty.to_frame().empty
    assert aligned_empty.config_diffs().empty


def test_runset_from_runs_raises_for_facet_filters(tracker: Tracker) -> None:
    _create_run(
        tracker,
        run_id="facet_run",
        model="facet_model",
        year=2025,
        facet={"scenario": "base"},
        config={"alpha": 1},
    )
    run = tracker.find_run(model="facet_model")
    assert run is not None

    runs = RunSet.from_runs([run], label="facetless")

    with pytest.raises(RuntimeError, match="tracker-backed RunSet"):
        runs.filter(scenario="base")


def test_runset_latest_orders_grouped_numeric_keys_numerically(
    tracker: Tracker,
) -> None:
    _create_run(
        tracker,
        run_id="year_10",
        model="order_model",
        year=10,
        facet={"scenario": "base"},
        config={"alpha": 10},
    )
    _create_run(
        tracker,
        run_id="year_2",
        model="order_model",
        year=2,
        facet={"scenario": "base"},
        config={"alpha": 2},
    )

    runs = RunSet.from_query(tracker, model="order_model", limit=20)
    latest_grouped = runs.latest(group_by=["year"])

    assert [run.year for run in latest_grouped] == [2, 10]


def test_runset_config_diffs_requires_same_tracker_database(tmp_path) -> None:
    left_root = tmp_path / "left"
    right_root = tmp_path / "right"
    left_root.mkdir()
    right_root.mkdir()
    left_tracker = Tracker(run_dir=left_root, db_path=left_root / "prov.duckdb")
    right_tracker = Tracker(run_dir=right_root, db_path=right_root / "prov.duckdb")

    _create_run(
        left_tracker,
        run_id="left_run",
        model="compare_model",
        year=2025,
        facet={"scenario": "base"},
        config={"alpha": 1},
    )
    _create_run(
        right_tracker,
        run_id="right_run",
        model="compare_model",
        year=2025,
        facet={"scenario": "policy"},
        config={"alpha": 2},
    )

    left = RunSet.from_query(left_tracker, model="compare_model")
    right = RunSet.from_query(right_tracker, model="compare_model")
    aligned = left.align(right, on="year")

    with pytest.raises(RuntimeError, match="same tracker/database"):
        aligned.config_diffs()
