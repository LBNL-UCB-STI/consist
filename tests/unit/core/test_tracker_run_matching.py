import time

import consist
import pytest


def _record_run(
    tracker,
    run_id,
    *,
    model="traffic_assignment",
    status="completed",
    year=2030,
    iteration=2,
    stage="assignment",
    phase="run",
    cache_epoch=4,
    description=None,
):
    tracker.begin_run(
        run_id,
        model,
        year=year,
        iteration=iteration,
        stage=stage,
        phase=phase,
        cache_epoch=cache_epoch,
        description=description,
    )
    tracker.end_run(status=status)


def test_find_matching_run_returns_latest_canonical_match(tracker):
    _record_run(tracker, "scenario_a__old")
    time.sleep(0.01)
    _record_run(tracker, "scenario_a__new")
    time.sleep(0.01)
    _record_run(tracker, "scenario_a__failed", status="failed")

    match = tracker.find_matching_run(
        model="traffic_assignment",
        stage="assignment",
        phase="run",
        status="completed",
        year=2030,
        iteration=2,
        cache_epoch=4,
    )

    assert match is not None
    assert match.id == "scenario_a__new"


def test_find_matching_run_returns_none_when_no_match(tracker):
    _record_run(tracker, "scenario_a__run")

    assert (
        tracker.find_matching_run(
            model="traffic_assignment",
            status="completed",
            year=2035,
        )
        is None
    )


def test_run_scope_prefix_excludes_other_scopes(tracker):
    _record_run(tracker, "scenario_a__old")
    time.sleep(0.01)
    _record_run(tracker, "scenario_b__new")

    match = tracker.find_matching_run(
        model="traffic_assignment",
        stage="assignment",
        phase="run",
        status="completed",
        year=2030,
        iteration=2,
        cache_epoch=4,
        run_scope="scenario_a",
    )

    assert match is not None
    assert match.id == "scenario_a__old"


def test_run_scope_is_applied_before_limit(tracker):
    _record_run(tracker, "scenario_a__old")
    time.sleep(0.01)
    _record_run(tracker, "scenario_b__new")

    match = tracker.find_matching_run(
        model="traffic_assignment",
        status="completed",
        year=2030,
        iteration=2,
        cache_epoch=4,
        run_scope="scenario_a",
        limit=1,
    )

    assert match is not None
    assert match.id == "scenario_a__old"


def test_run_scope_sql_prefix_treats_underscores_literally(tracker):
    _record_run(tracker, "scenario_a__old")
    time.sleep(0.01)
    _record_run(tracker, "scenarioXa__new")

    match = tracker.find_matching_run(
        model="traffic_assignment",
        status="completed",
        year=2030,
        iteration=2,
        cache_epoch=4,
        run_scope="scenario_a",
        limit=1,
    )

    assert match is not None
    assert match.id == "scenario_a__old"


def test_run_scope_matches_description_prefix(tracker):
    _record_run(
        tracker,
        "generated_run_id",
        description="scenario_a__assignment_iteration_2",
    )

    match = tracker.find_matching_run(
        model="traffic_assignment",
        status="completed",
        year=2030,
        iteration=2,
        cache_epoch=4,
        run_scope="scenario_a",
    )

    assert match is not None
    assert match.id == "generated_run_id"


def test_cache_epoch_matches_numerically_across_string_and_int(tracker):
    _record_run(tracker, "epoch_numeric")

    match = tracker.find_matching_run(
        model="traffic_assignment",
        status="completed",
        year="2030",
        iteration="2",
        cache_epoch="4",
    )

    assert match is not None
    assert match.id == "epoch_numeric"


def test_missing_cache_epoch_fails_by_default(tracker):
    _record_run(tracker, "missing_epoch", cache_epoch=None)

    match = tracker.find_matching_run(
        model="traffic_assignment",
        status="completed",
        year=2030,
        iteration=2,
        cache_epoch=4,
    )

    assert match is None


def test_missing_cache_epoch_can_match_when_explicitly_allowed(tracker):
    _record_run(tracker, "missing_epoch_allowed", cache_epoch=None)

    match = tracker.find_matching_run(
        model="traffic_assignment",
        status="completed",
        year=2030,
        iteration=2,
        cache_epoch=4,
        allow_missing_cache_epoch=True,
    )

    assert match is not None
    assert match.id == "missing_epoch_allowed"


def test_find_matching_runs_returns_deterministic_recency_order(tracker):
    _record_run(tracker, "scenario_a__first")
    time.sleep(0.01)
    _record_run(tracker, "scenario_a__second")
    time.sleep(0.01)
    _record_run(tracker, "scenario_a__third")

    matches = tracker.find_matching_runs(
        model="traffic_assignment",
        status="completed",
        year=2030,
        iteration=2,
        cache_epoch=4,
        run_scope="scenario_a",
    )

    assert [run.id for run in matches] == [
        "scenario_a__third",
        "scenario_a__second",
        "scenario_a__first",
    ]


def test_unexpected_query_errors_propagate(tracker, monkeypatch):
    def fail_execute_with_retry(*_args, **_kwargs):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(tracker.db, "execute_with_retry", fail_execute_with_retry)

    with pytest.raises(RuntimeError, match="database unavailable"):
        tracker.find_matching_run(model="traffic_assignment")


def test_signature_without_hash_tuple_is_rejected(tracker):
    with pytest.raises(TypeError, match="signature matching requires"):
        tracker.find_matching_run(signature="signature_only")


def test_top_level_find_matching_run_helper(tracker):
    _record_run(tracker, "top_level_match")

    match = consist.find_matching_run(
        tracker=tracker,
        model="traffic_assignment",
        status="completed",
        year=2030,
        iteration=2,
        cache_epoch=4,
    )

    assert match is not None
    assert match.id == "top_level_match"
