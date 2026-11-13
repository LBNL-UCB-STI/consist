# tests/integration/test_tracker_behavior.py
import json
import logging
from unittest.mock import MagicMock

import pytest

from consist.models.artifact import Artifact


def test_start_run_with_exception(tracker):
    """
    Tests that if an exception is raised within a run context, the run's status is
    correctly marked as 'failed' in both the JSON log and the database.
    """
    with pytest.raises(ValueError, match="Something went wrong"):
        with tracker.start_run("run_exception", "error_model"):
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
    run = tracker.get_run("run_exception")
    assert run is not None
    assert run.status == "failed"
    assert "error" in run.meta
    assert run.meta["error"] == "Something went wrong"


def test_log_artifact_outside_of_run_context(tracker):
    """
    Tests that calling `tracker.log_artifact()` outside of an active run context
    raises a `RuntimeError`.
    """
    with pytest.raises(RuntimeError, match=r"^(Cannot log artifact)"):
        tracker.log_artifact("some_file.csv", "some_key")


def test_ingest_outside_of_run_context(tracker):
    """
    Tests that calling `tracker.ingest()` outside of an active run context
    raises a `RuntimeError`.
    """
    dummy_artifact = Artifact(key="dummy", uri="dummy.csv", driver="csv")

    with pytest.raises(
        RuntimeError,
        match=r"^(Cannot ingest)",
    ):
        tracker.ingest(artifact=dummy_artifact, data=[])


def test_tracker_db_write_failure_tolerated(tracker, monkeypatch, caplog, run_dir):
    """
    Tests that Consist's `Tracker` tolerates database write failures, logs a warning,
    and still successfully writes the run and artifact data to the `consist.json` file,
    upholding the 'JSON for Truth' principle.
    """
    caplog.set_level(logging.WARNING)

    mock_tx = MagicMock(side_effect=RuntimeError("Simulated DB commit failure"))
    monkeypatch.setattr(tracker.db, "transaction", mock_tx)

    test_run_id = "run_with_db_fail"
    test_artifact_key = "output_data"
    test_artifact_path = run_dir / "output.parquet"
    test_artifact_path.touch()

    with tracker.start_run(test_run_id, "test_model", config={"param": 1}):
        tracker.log_artifact(str(test_artifact_path), test_artifact_key)

    logged_messages = [
        r.message for r in caplog.records if r.levelno >= logging.WARNING
    ]
    assert len(logged_messages) >= 2
    assert any("Database sync failed" in msg for msg in logged_messages)

    json_log_path = run_dir / "consist.json"
    assert json_log_path.exists()
    with open(json_log_path, "r") as f:
        json_data = json.load(f)

    assert json_data["run"]["id"] == test_run_id
    assert json_data["run"]["status"] == "completed"
    assert "param" in json_data["config"] and json_data["config"]["param"] == 1

    assert len(json_data["outputs"]) == 1
    logged_artifact = json_data["outputs"][0]
    assert logged_artifact["key"] == test_artifact_key
    assert logged_artifact["uri"].endswith("output.parquet")

    assert tracker.get_run(test_run_id) is None
    assert tracker.get_artifact(test_artifact_key) is None


def test_tracker_robustness_to_missing_input_file(tracker, caplog):
    """
    Tests that the `Tracker` gracefully handles a missing physical input file
    when attempting to compute the run's input hash.
    """
    run_id = "run_missing_input"
    missing_file_path = "path/to/non_existent_input.csv"
    caplog.set_level(logging.WARNING)

    with tracker.start_run(run_id, "model_robust", inputs=[missing_file_path]):
        pass

    assert any(
        "Failed to compute inputs hash" in record.message for record in caplog.records
    ), "A warning about failed input hashing should be logged."

    json_log = tracker.run_dir / "consist.json"
    assert json_log.exists()
    with open(json_log) as f:
        data = json.load(f)

    assert data["run"]["id"] == run_id
    assert data["run"]["input_hash"] == "error"
    assert data["run"]["signature"] is None
    assert data["run"]["status"] == "completed"

    run = tracker.get_run(run_id)
    assert run is not None
    assert run.input_hash == "error"
    assert run.signature is None
    assert run.status == "completed"
