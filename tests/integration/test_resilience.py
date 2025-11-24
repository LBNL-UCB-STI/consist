from unittest.mock import patch
import json


def test_db_failure_does_not_crash_run(tracker):
    """
    If DuckDB is locked/corrupted, the run should still finish
    and write consist.json.
    """

    # Start a run
    with tracker.start_run("run_fail_test", "model"):
        # Mock the Session object that the method uses internally.
        # This simulates the DB blowing up, while letting the method handle it.
        with patch("consist.core.tracker.Session", side_effect=Exception("DB EXPLODED")):
            tracker.log_artifact("test.csv", "test")

    # Assert JSON still exists (The Source of Truth survived)
    assert (tracker.run_dir / "consist.json").exists()

    # Verify content
    with open(tracker.run_dir / "consist.json") as f:
        data = json.load(f)
        assert len(data["outputs"]) == 1  # The artifact was still tracked in JSON