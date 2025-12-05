"""
Consist Resilience and Fault Tolerance Integration Tests

This module contains integration tests focused on the resilience and fault tolerance
of the Consist system. It specifically verifies that core operations, such as run
tracking and artifact logging, behave gracefully and do not crash the application
when underlying external dependencies (e.g., the database) encounter failures.

These tests ensure that Consist prioritizes the completion of the user's workflow
while logging warnings about database issues, adhering to the "Dual-Write Safety"
principle where the JSON log serves as the primary source of truth in adverse conditions.
"""

from unittest.mock import patch
import json


def test_db_failure_does_not_crash_run(tracker):
    """
    Tests the `Tracker`'s resilience to database failures during a run by ensuring
    the main application logic does not crash.

    This test verifies that if the underlying database connection or an operation
    fails (e.g., due to lock contention or an unexpected error), Consist's
    `_sync_run_to_db` and `_sync_artifact_to_db` methods gracefully handle
    these exceptions by logging warnings instead of crashing the user's run.
    The primary "source of truth" JSON log (`consist.json`) should still be
    written correctly, preserving the run's provenance.

    What happens:
    1. A `tracker.start_run` context is initiated.
    2. The `consist.core.tracker.Session` object is mocked to raise an exception
       whenever it's instantiated, simulating a catastrophic database failure
       during any database interaction.
    3. An attempt is made to log an artifact inside the run context, which would
       normally trigger both JSON and database writes.

    What's checked:
    - The run's context manager exits successfully without raising an uncaught exception.
    - The `consist.json` file is successfully created in the run directory.
    - The content of the `consist.json` file is validated to include the logged artifact,
      proving that the in-memory state of the run was correctly persisted to JSON
      despite the database failure.
    """

    # Start a run
    with tracker.start_run("run_fail_test", "model"):
        # Mock the Session object that the method uses internally.
        # This simulates the DB blowing up, while letting the method handle it.
        with patch(
            "consist.core.persistence.Session", side_effect=Exception("DB EXPLODED")
        ):
            tracker.log_artifact("test.csv", "test")

    # Assert JSON still exists (The Source of Truth survived)
    assert (tracker.run_dir / "consist.json").exists()

    # Verify content
    with open(tracker.run_dir / "consist.json") as f:
        data = json.load(f)
        assert len(data["outputs"]) == 1  # The artifact was still tracked in JSON
