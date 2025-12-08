# tests/integration/test_tracker_behavior.py
import pytest
import json
from sqlmodel import Session, select
from consist.models.run import Run
from consist.models.artifact import Artifact
import sqlmodel
from unittest.mock import MagicMock
import logging


def test_start_run_with_exception(tracker):
    """
    Tests that if an exception is raised within a run context, the run's status is
    correctly marked as 'failed' in both the JSON log and the database.

    This ensures that Consist reliably captures failures in workflows and records
    them in its provenance tracking system, providing accurate historical data
    about execution outcomes.

    What happens:
    1. A `tracker.start_run` context is initiated.
    2. Inside the run, an input artifact is logged.
    3. A `ValueError` is intentionally raised, simulating a failure in the user's code.

    What's checked:
    - The `ValueError` is caught and re-raised by the context manager.
    - The `consist.json` log file exists.
    - The run's `status` in the JSON log is "failed".
    - The `run.meta` in the JSON log contains an "error" entry with the exception message.
    - In the DuckDB database, the corresponding `Run` record also has `status="failed"`
      and the error message in its `meta` field.
    """
    with pytest.raises(ValueError, match="Something went wrong"):
        with tracker.start_run("run_exception", "error_model"):
            # This code will execute, but then an exception is raised
            tracker.log_artifact("input.csv", "input", direction="input")
            raise ValueError("Something went wrong")

    # 1. Check JSON log
    json_log = tracker.run_dir / "consist.json"
    assert json_log.exists()
    with open(json_log) as f:
        data = json.load(f)
    assert data["run"]["status"] == "failed"
    assert "error" in data["run"]["meta"]
    assert data["run"]["meta"]["error"] == "Something went wrong"

    # 2. Check Database
    with Session(tracker.engine) as session:
        run = session.exec(select(Run).where(Run.id == "run_exception")).one()
        assert run.status == "failed"
        assert "error" in run.meta
        assert run.meta["error"] == "Something went wrong"


def test_log_artifact_outside_of_run_context(tracker):
    """
    Tests that calling `tracker.log_artifact()` outside of an active run context
    raises a `RuntimeError`.

    This prevents improper use of `log_artifact` and ensures that all artifacts
    are always associated with a specific `Run` for correct provenance tracking.

    What happens:
    1. `tracker.log_artifact()` is called directly, without being enclosed
       in a `with tracker.start_run():` block.

    What's checked:
    - The call raises a `RuntimeError` with a message indicating that `log_artifact`
      cannot be called outside a run context.
    """
    with pytest.raises(RuntimeError, match=r"^(Cannot log artifact)"):
        tracker.log_artifact("some_file.csv", "some_key")


def test_ingest_outside_of_run_context(tracker):
    """
    Tests that calling `tracker.ingest()` outside of an active run context
    raises a `RuntimeError`.

    This ensures that all data ingestion operations are properly attributed
    to a specific `Run`, maintaining the integrity of the provenance graph.

    What happens:
    1. A dummy `Artifact` is created (without logging it in a run).
    2. `tracker.ingest()` is called with this dummy artifact, without being
       enclosed in a `with tracker.start_run():` block.

    What's checked:
    - The call raises a `RuntimeError` with a message indicating that `ingest`
      cannot be called outside an active run context.
    """
    dummy_artifact = Artifact(key="dummy", uri="dummy.csv", driver="csv")

    with pytest.raises(
        RuntimeError,
        match=r"^(Cannot ingest)",
    ):
        tracker.ingest(artifact=dummy_artifact, data=[])


def test_tracker_db_write_failure_tolerated(tracker, mocker, caplog, run_dir):
    """
    Tests that Consist's `Tracker` tolerates database write failures, logs a warning,
    and still successfully writes the run and artifact data to the `consist.json` file,
    upholding the 'JSON for Truth' principle.

    This test verifies the resilience mechanism of Consist's "dual-write" strategy,
    ensuring that the workflow can complete even if the analytical database (`DuckDB`)
    encounters issues, with the `consist.json` serving as the definitive record.

    What happens:
    1. The `sqlmodel.Session.commit` method is mocked to raise an exception, simulating
       a database commit failure.
    2. A `tracker.start_run` context is initiated, and an artifact is logged within it.
       These operations would normally trigger database writes.

    What's checked:
    - The `tracker.start_run` context manager completes without an unhandled exception.
    - Warning messages are logged indicating that both run and artifact synchronization
      to the database failed.
    - The `consist.json` file is successfully created and contains the complete run
      and artifact details, proving the in-memory state was correctly persisted to JSON.
    - Crucially, the DuckDB database does *not* contain records for the run or artifact,
      confirming that the simulated database commit failure prevented persistence.
    """
    caplog.set_level(logging.WARNING)

    # Mock Session.commit to raise an exception, simulating a DB write failure
    mock_commit = MagicMock(side_effect=Exception("Simulated DB commit failure"))
    mocker.patch.object(sqlmodel.Session, "commit", new=mock_commit)

    test_run_id = "run_with_db_fail"
    test_artifact_key = "output_data"
    test_artifact_path = run_dir / "output.parquet"
    test_artifact_path.touch()  # Create dummy file for hashing

    # Execute a run block where DB writes will fail
    with tracker.start_run(test_run_id, "test_model", config={"param": 1}):
        tracker.log_artifact(str(test_artifact_path), test_artifact_key)

    # Assertions
    # 1. Ensure no unexpected exception was raised by the tracker's context manager
    #    (this is implicit if the test reaches here without pytest.raises)

    # 2. Check that warning messages were logged
    warning_messages = [
        r.message for r in caplog.records if r.levelno == logging.WARNING
    ]
    assert any("Database sync failed" in msg for msg in warning_messages)
    assert any("Artifact sync failed" in msg for msg in warning_messages)

    # 3. Check JSON log - it should contain the completed run and artifact
    json_log_path = run_dir / "consist.json"
    assert json_log_path.exists()
    with open(json_log_path, "r") as f:
        json_data = json.load(f)

    # Verify run details in JSON
    assert json_data["run"]["id"] == test_run_id
    assert json_data["run"]["status"] == "completed"
    assert "param" in json_data["config"] and json_data["config"]["param"] == 1

    # Verify artifact details in JSON
    assert len(json_data["outputs"]) == 1
    logged_artifact = json_data["outputs"][0]
    assert logged_artifact["key"] == test_artifact_key
    assert logged_artifact["uri"].endswith(
        "output.parquet"
    )  # Check for virtualized path

    # 4. Check Database (Optional but good to show DB is NOT updated)
    # The DB should NOT have this run or artifact because commit was mocked to fail
    with Session(tracker.engine) as session:
        db_run = session.exec(select(Run).where(Run.id == test_run_id)).first()
        assert db_run is None

        db_artifact = session.exec(
            select(Artifact).where(Artifact.key == test_artifact_key)
        ).first()
        assert db_artifact is None


def test_tracker_robustness_to_missing_input_file(tracker, caplog):
    """
    Tests that the `Tracker` gracefully handles a missing physical input file
    when attempting to compute the run's input hash.

    This test ensures that if a user logs an input artifact that points to a
    non-existent file, Consist does not crash. Instead, it logs a warning
    and marks the run's `input_hash` and `signature` as "error" to prevent
    unreliable cache hits based on invalid input provenance.

    What happens:
    1. A `tracker.start_run` context is initiated.
    2. An input artifact is logged, pointing to a file path (`missing_file_path`)
       that does not exist on the filesystem.
    3. The tracker proceeds with the run. When it attempts to compute the
       `input_hash` during the `start_run` setup, the hashing of the missing
       file will fail.

    What's checked:
    - The run completes its execution without crashing.
    - A warning message is logged by Consist, indicating that the input hash
      computation failed for the missing file.
    - In both the `consist.json` log and the DuckDB database, the `input_hash`
      and `signature` fields for the run are set to the string "error".
    - The run's `status` remains "completed" if no other errors occur.
    """
    run_id = "run_missing_input"
    missing_file_path = "path/to/non_existent_input.csv"
    caplog.set_level(logging.WARNING)

    # Run should complete without raising an error
    with tracker.start_run(run_id, "model_robust", inputs=[missing_file_path]):
        pass  # No operations needed inside

    # 1. Check for warning log
    assert any(
        "Failed to compute inputs hash" in record.message for record in caplog.records
    ), "A warning about failed input hashing should be logged."

    # 2. Check JSON log
    json_log = tracker.run_dir / "consist.json"
    assert json_log.exists()
    with open(json_log) as f:
        data = json.load(f)

    assert data["run"]["id"] == run_id
    assert data["run"]["input_hash"] == "error"
    assert data["run"]["signature"] == "error"
    assert data["run"]["status"] == "completed"

    # 3. Check Database
    with Session(tracker.engine) as session:
        run = session.exec(select(Run).where(Run.id == run_id)).one()
        assert run.input_hash == "error"
        assert run.signature == "error"
        assert run.status == "completed"
