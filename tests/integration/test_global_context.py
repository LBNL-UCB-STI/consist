import pytest
import consist

# Note: 'tracker', 'run_dir' fixtures are provided by conftest.py


def test_global_context_logging(tracker):
    """
    Verifies that we can use consist.log_artifact() directly
    without passing the tracker object around.
    """

    # 1. Verify failure outside of run context
    with pytest.raises(RuntimeError, match="No active Consist run"):
        consist.log_artifact("fail.csv", key="fail")

    # 2. Verify success inside run context
    with tracker.start_run(run_id="global_run_1", model="test_global"):
        # Log via Top-Level API
        art = consist.log_artifact(
            path="/tmp/fake_global.csv",
            key="global_artifact",
            direction="output",
            rows=500,  # Extra meta
        )

        # Verify return object
        assert art.key == "global_artifact"
        assert art.meta["rows"] == 500

        # Verify state inside the tracker
        assert len(tracker.current_consist.outputs) == 1
        assert tracker.current_consist.outputs[0].key == "global_artifact"

    # 3. Verify cleanup (stack should be empty)
    with pytest.raises(RuntimeError, match="No active Consist run"):
        consist.log_artifact("fail_again.csv", key="fail")
