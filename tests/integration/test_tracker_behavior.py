# tests/integration/test_tracker_behavior.py

import pytest
import json
from sqlmodel import Session, select
from consist.models.run import Run
from consist.models.artifact import Artifact


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
        match="Cannot ingest data: No active run context and no explicit 'run' argument provided.",
    ):
        tracker.ingest(artifact=dummy_artifact, data=[])
