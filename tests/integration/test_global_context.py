import pytest
import consist

# Note: 'tracker', 'run_dir' fixtures are provided by conftest.py


def test_global_context_logging(tracker):
    """
    Verifies that we can use consist.log_artifact() directly
    without passing the tracker object around.
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
    Verifies consist.capture_outputs().
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
    Verifies consist.current_tracker().
    """
    with tracker.start_run("intro", "test_model"):
        # Should return the active tracker instance
        t = consist.current_tracker()
        assert t is tracker
        assert t.current_consist.run.id == "intro"

    # Outside context, should raise RuntimeError
    with pytest.raises(RuntimeError, match="No active Consist run"):
        consist.current_tracker()
