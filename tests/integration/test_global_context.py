import pytest
import consist

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
