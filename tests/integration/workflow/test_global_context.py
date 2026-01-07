import pytest
import consist
from consist import Tracker

# Note: 'tracker', 'run_dir' fixtures are provided by conftest.py


def test_global_context_logging(tracker):
    """
    Verifies that `consist.log_artifact()` can be used globally without
    explicitly passing the `Tracker` object, relying on the active global context.

    This test confirms the convenience of Consist's top-level API for logging
    artifacts within an active run, and also ensures that attempts to log
    outside an active run correctly raise an error.

    What happens:
    1. A `tracker.start_run` context is initiated.
    2. Inside the run, `consist.log_artifact` is called with a fake path and metadata.
    3. An attempt is made to call `consist.log_artifact` outside any active run context.

    What's checked:
    - The `Artifact` returned by `consist.log_artifact` has the correct key and metadata.
    - The `tracker.current_consist.outputs` list contains the logged artifact.
    - Calling `consist.log_artifact` outside an active run correctly raises a `RuntimeError`.
    """
    with tracker.start_run(run_id="global_run_1", model="test_global"):
        # Log via Top-Level API
        art = consist.log_artifact(
            path="/tmp/fake_global.csv",
            key="global_artifact",
            direction="output",
            rows=500,  # Extra meta
        )

        assert art.key == "global_artifact"
        assert art.meta["rows"] == 500
        assert len(tracker.current_consist.outputs) == 1

    # Verify failure outside of run context
    with pytest.raises(RuntimeError, match="No active Consist run"):
        consist.log_artifact("fail.csv", key="fail")


def test_global_capture_outputs(tracker, run_dir):
    """
    Verifies that `consist.capture_outputs()` correctly monitors a directory
    and logs newly created files as artifacts, using the global active `Tracker` context.

    This test confirms the functionality of the global context manager for
    automatically tracking side-effect outputs from external tools or legacy code.

    What happens:
    1. A new output directory `global_out` is created.
    2. A `tracker.start_run` context is initiated.
    3. Inside the run, `consist.capture_outputs` is used as a context manager
       to monitor `global_out` for `.txt` files.
    4. A `test.txt` file is created within `global_out` during the `with` block.

    What's checked:
    - The `capture` object returned by the context manager contains exactly one artifact.
    - The `key` of the captured artifact is "test".
    - The `tracker.current_consist.outputs` list in the active tracker contains the captured artifact.
    """
    out_dir = run_dir / "global_out"
    out_dir.mkdir()

    with tracker.start_run("global_cap", "test_model"):
        # Use Global Context Manager
        with consist.capture_outputs(out_dir, pattern="*.txt") as capture:
            (out_dir / "test.txt").write_text("content")

        assert len(capture.artifacts) == 1
        assert capture.artifacts[0].key == "test"

        # Verify it hit the tracker
        assert len(tracker.current_consist.outputs) == 1


def test_global_introspection(tracker):
    """
    Verifies the behavior of `consist.current_tracker()` for retrieving the
    active `Tracker` instance from the global context.

    This test ensures that `current_tracker()` correctly returns the active
    instance when inside a run context and raises an error when called
    outside any active run, preventing operations on a non-existent context.

    What happens:
    1. A `tracker.start_run` context is initiated.
    2. Inside the run, `consist.current_tracker()` is called.
    3. `consist.current_tracker()` is called again outside any run context.

    What's checked:
    - Inside the run, `consist.current_tracker()` returns the same `Tracker`
      instance that initiated the run.
    - The `id` of the run returned by `current_tracker()` matches the expected `run_id`.
    - Outside the run context, `consist.current_tracker()` correctly raises a `RuntimeError`.
    """
    with tracker.start_run("intro", "test_model"):
        # Should return the active tracker instance
        t = consist.current_tracker()
        assert t is tracker
        assert t.current_consist.run.id == "intro"

    # Outside context, should raise RuntimeError
    with pytest.raises(RuntimeError, match="No active Consist run"):
        consist.current_tracker()


def test_default_tracker_and_introspection_helpers(tracker):
    def noop():
        return None

    # No default tracker configured
    with pytest.raises(
        RuntimeError, match="No default tracker configured for consist.run"
    ):
        consist.run(fn=noop)

    # Default tracker resolves consist.run
    with consist.use_tracker(tracker):
        result = consist.run(fn=noop, name="noop")
        assert result.run.model_name == "noop"

    # Outside any run: helpers should be graceful
    assert consist.current_run() is None
    assert consist.current_consist() is None
    assert consist.cached_artifacts() == {}
    assert consist.cached_output() is None


def test_use_tracker_nested_and_override_precedence(tracker, tmp_path):
    def noop():
        return None

    other_tracker = Tracker(
        run_dir=tmp_path / "runs_other",
        db_path=tmp_path / "provenance_other.db",
    )

    with consist.use_tracker(tracker):
        outer = consist.run(fn=noop, name="outer_default")
        assert tracker.last_run.run.id == outer.run.id

        with consist.use_tracker(other_tracker):
            inner = consist.run(fn=noop, name="inner_default")
            assert other_tracker.last_run.run.id == inner.run.id

            overridden = consist.run(
                fn=noop, name="override", tracker=tracker, run_id="explicit_override"
            )
            assert tracker.last_run.run.id == overridden.run.id

        after_inner = consist.run(fn=noop, name="after_inner")
        assert tracker.last_run.run.id == after_inner.run.id

    with pytest.raises(
        RuntimeError, match="No default tracker configured for consist.run"
    ):
        consist.run(fn=noop)


def test_active_run_context_beats_default_tracker_for_logging(tracker, tmp_path):
    other_tracker = Tracker(
        run_dir=tmp_path / "runs_other",
        db_path=tmp_path / "provenance_other.db",
    )

    with consist.use_tracker(tracker):
        with other_tracker.start_run("active_run", "test_model"):
            art = consist.log_artifact(
                path="/tmp/fake_active.csv",
                key="active_artifact",
                direction="output",
            )
            assert art.run_id == other_tracker.current_consist.run.id


def test_nested_start_run_raises(tracker):
    with tracker.start_run("outer_run", "test_model"):
        with pytest.raises(RuntimeError, match="A run is already active"):
            with tracker.start_run("inner_run", "test_model"):
                pass
