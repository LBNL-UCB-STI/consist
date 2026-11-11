import pytest
import consist
from consist import Tracker
from consist.api import (
    cached_artifacts,
    cached_output,
    capture_outputs,
    current_consist,
    current_run,
    current_tracker,
    set_current_tracker,
)


def test_global_context_logging(tracker):
    """
    Verifies that `consist.log_artifact()` can be used globally without
    explicitly passing the `Tracker` object, relying on the active global context.
    """
    with tracker.start_run(run_id="global_run_1", model="test_global"):
        art = consist.log_artifact(
            path="/tmp/fake_global.csv",
            key="global_artifact",
            direction="output",
            rows=500,
        )

        assert art.key == "global_artifact"
        assert art.meta["rows"] == 500
        assert len(tracker.current_consist.outputs) == 1

    with pytest.raises(RuntimeError, match="No active Consist run"):
        consist.log_artifact("fail.csv", key="fail")


def test_global_capture_outputs(tracker, run_dir):
    """
    Verifies that `capture_outputs()` correctly monitors a directory
    and logs newly created files as artifacts, using the global active `Tracker` context.
    """
    out_dir = run_dir / "global_out"
    out_dir.mkdir()

    with tracker.start_run("global_cap", "test_model"):
        with capture_outputs(out_dir, pattern="*.txt") as capture:
            (out_dir / "test.txt").write_text("content")

        assert len(capture.artifacts) == 1
        assert capture.artifacts[0].key == "test"
        assert len(tracker.current_consist.outputs) == 1


def test_global_introspection(tracker):
    """
    Verifies the behavior of `current_tracker()` for retrieving the
    active `Tracker` instance from the global context.
    """
    with tracker.start_run("intro", "test_model"):
        t = current_tracker()
        assert t is tracker
        assert t.current_consist.run.id == "intro"

    with pytest.raises(
        RuntimeError, match="No active Consist run and no default tracker configured"
    ):
        current_tracker()


def test_default_tracker_and_introspection_helpers(tracker):
    def noop():
        return None

    with pytest.raises(
        RuntimeError, match="No default tracker configured for consist.run"
    ):
        with pytest.warns(DeprecationWarning, match="consist.run"):
            consist.run(fn=noop)

    with consist.use_tracker(tracker):
        with pytest.warns(DeprecationWarning, match="consist.run"):
            result = consist.run(fn=noop, name="noop")
        assert result.run.model_name == "noop"

    previous = set_current_tracker(tracker)
    assert previous is None
    assert current_tracker() is tracker
    set_current_tracker(None)

    assert current_run() is None
    assert current_consist() is None
    assert cached_artifacts() == {}
    assert cached_output() is None


def test_use_tracker_nested_and_override_precedence(tracker, tmp_path):
    def noop():
        return None

    other_tracker = Tracker(
        run_dir=tmp_path / "runs_other",
        db_path=tmp_path / "provenance_other.db",
    )
    try:
        with consist.use_tracker(tracker):
            with pytest.warns(DeprecationWarning, match="consist.run"):
                outer = consist.run(fn=noop, name="outer_default")
            assert tracker.last_run.run.id == outer.run.id

            with consist.use_tracker(other_tracker):
                with pytest.warns(DeprecationWarning, match="consist.run"):
                    inner = consist.run(fn=noop, name="inner_default")
                assert other_tracker.last_run.run.id == inner.run.id

                with pytest.warns(DeprecationWarning, match="consist.run"):
                    overridden = consist.run(
                        fn=noop,
                        name="override",
                        tracker=tracker,
                        run_id="explicit_override",
                    )
                assert tracker.last_run.run.id == overridden.run.id

            with pytest.warns(DeprecationWarning, match="consist.run"):
                after_inner = consist.run(fn=noop, name="after_inner")
            assert tracker.last_run.run.id == after_inner.run.id

        with pytest.raises(
            RuntimeError, match="No default tracker configured for consist.run"
        ):
            with pytest.warns(DeprecationWarning, match="consist.run"):
                consist.run(fn=noop)
    finally:
        other_tracker.close()


def test_active_run_context_beats_default_tracker_for_logging(tracker, tmp_path):
    other_tracker = Tracker(
        run_dir=tmp_path / "runs_other",
        db_path=tmp_path / "provenance_other.db",
    )
    try:
        with consist.use_tracker(tracker):
            with other_tracker.start_run("active_run", "test_model"):
                art = consist.log_artifact(
                    path="/tmp/fake_active.csv",
                    key="active_artifact",
                    direction="output",
                )
                assert art.run_id == other_tracker.current_consist.run.id
    finally:
        other_tracker.close()


def test_nested_start_run_raises(tracker):
    with tracker.start_run("outer_run", "test_model"):
        with pytest.raises(RuntimeError, match="A run is already active"):
            with tracker.start_run("inner_run", "test_model"):
                pass
