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
    marked as 'failed' in both the JSON log and the database.
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
    Tests that calling `log_artifact` outside of an active run context
    raises a RuntimeError, preventing improper use.
    """
    with pytest.raises(
        RuntimeError, match="Cannot log artifact outside of a run context"
    ):
        tracker.log_artifact("some_file.csv", "some_key")


def test_ingest_outside_of_run_context(tracker):
    """
    Tests that calling `ingest` outside of an active run context
    raises a RuntimeError, preventing improper use.
    """

    # We need a dummy artifact to pass to the method.
    # Crucially, we create it directly, not via tracker.log_artifact.
    dummy_artifact = Artifact(key="dummy", uri="dummy.csv", driver="csv")

    with pytest.raises(
        RuntimeError,
        match="Cannot ingest data: No active run context.",
    ):
        tracker.ingest(artifact=dummy_artifact, data=[])


def test_tracker_db_write_failure_tolerated(tracker, mocker, caplog, run_dir):
    """
    Tests that if the DuckDB write fails, the Consist Tracker tolerates the failure,
    logs a warning, and still successfully writes the run and artifact data to the
    consist.json file, upholding the 'JSON for Truth' principle.
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
    Tests that the tracker gracefully handles a missing input file during hashing.

    What happens:
    1. A run is started, and an input file is logged that does not exist on the filesystem.
    2. The tracker proceeds with the run. When it attempts to compute the `input_hash`
       at the end of the `start_run` setup, the hashing will fail.

    What's checked:
    - The run does not crash.
    - A warning is logged indicating that the input hash computation failed.
    - The `input_hash` and `signature` fields for the run are set to "error" in both
      the JSON log and the database, preventing false cache hits in the future.
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
