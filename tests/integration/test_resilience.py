from unittest.mock import patch
import json


def test_db_failure_does_not_crash_run(tracker):
    """
    Tests the Tracker's resilience to database failures during a run.

    This test ensures that if the database connection or a database operation fails
    for any reason (e.g., lock contention, corruption), the core tracking functionality
    does not crash. The run should complete, and the primary "source of truth"
    JSON log (`consist.json`) should still be written correctly.

    What happens:
    1. A run is started.
    2. The `sqlmodel.Session` object is mocked to raise an exception whenever it's used,
       simulating a catastrophic database failure.
    3. An attempt is made to log an artifact, which would normally trigger a database write.

    What's checked:
    - The run does not crash and completes its context block.
    - The `consist.json` file is successfully created in the run directory.
    - The content of the JSON file is correct and includes the artifact that was logged,
      proving that the in-memory state was not compromised by the DB failure.
    """

    # Start a run
    with tracker.start_run("run_fail_test", "model"):
        # Mock the Session object that the method uses internally.
        # This simulates the DB blowing up, while letting the method handle it.
        with patch(
            "consist.core.tracker.Session", side_effect=Exception("DB EXPLODED")
        ):
            tracker.log_artifact("test.csv", "test")

    # Assert JSON still exists (The Source of Truth survived)
    assert (tracker.run_dir / "consist.json").exists()

    # Verify content
    with open(tracker.run_dir / "consist.json") as f:
        data = json.load(f)
        assert len(data["outputs"]) == 1  # The artifact was still tracked in JSON
