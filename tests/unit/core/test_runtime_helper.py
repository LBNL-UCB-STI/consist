from pathlib import Path

import pytest

import consist
from consist.core.tracker import Tracker


def test_create_tracker_emits_deprecation_warning_and_calls_factory(
    tmp_path: Path,
) -> None:
    def factory() -> Tracker:
        return Tracker(run_dir=tmp_path, db_path=None)

    with pytest.deprecated_call(match="consist.create_tracker is deprecated"):
        tracker = consist.create_tracker(enabled=True, tracker_factory=factory)

    assert isinstance(tracker, Tracker)
    assert tracker.db is None


def test_create_tracker_requires_factory_when_enabled() -> None:
    with pytest.deprecated_call(match="consist.create_tracker is deprecated"):
        with pytest.raises(ValueError, match="requires tracker_factory"):
            consist.create_tracker(enabled=True)


def test_create_tracker_propagates_factory_exceptions() -> None:
    def factory() -> Tracker:
        raise RuntimeError("boom")

    with pytest.deprecated_call(match="consist.create_tracker is deprecated"):
        with pytest.raises(RuntimeError, match="boom"):
            consist.create_tracker(enabled=True, tracker_factory=factory)


@pytest.mark.parametrize(
    "call_fn",
    [
        lambda: consist.create_tracker(enabled=False),
        lambda: consist.scenario("disabled_scen", enabled=False),
        lambda: consist.log_input("in.csv", key="in", enabled=False),
        lambda: consist.log_output("out.csv", key="out", enabled=False),
        lambda: consist.log_artifact("art.csv", key="art", enabled=False),
        lambda: consist.log_artifacts({"art": "art.csv"}, enabled=False),
    ],
)
def test_disabled_flag_raises_value_error(call_fn) -> None:
    with pytest.raises(ValueError, match="enabled=False is no longer supported"):
        call_fn()
