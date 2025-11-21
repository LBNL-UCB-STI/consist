import pytest
from pathlib import Path
import json
import os
from sqlmodel import Session, select, create_engine

# Note: Because of 'src' layout, this import works if environment is set up right
from consist.core.tracker import Tracker
from consist.models.run import Run
from consist.models.artifact import Artifact


def test_dual_write_workflow(tmp_path):
    """
    Verifies that the Tracker writes to BOTH JSON and DuckDB.
    tmp_path is a pytest fixture that gives a unique temp folder.
    """
    # 1. Setup Paths
    run_dir = tmp_path / "test_run_1"
    db_path = str(tmp_path / "provenance.duckdb")

    # 2. Initialize Tracker
    tracker = Tracker(run_dir=run_dir, db_path=db_path)

    # 3. Run a Fake Workflow
    config = {"random_seed": 42, "scenario": "test"}

    with tracker.start_run(run_id="run_1", model="test_model", config=config):
        # Log an input
        tracker.log_artifact(
            path="/inputs/land_use.csv",
            key="land_use",
            direction="input"
        )

        # Log an output
        tracker.log_artifact(
            path=str(run_dir / "trips.csv"),
            key="trips",
            direction="output",
            meta={"rows": 100}
        )

    # --- ASSERTION TIME ---

    # 4. Check JSON (The Truth)
    json_file = run_dir / "consist.json"
    assert json_file.exists(), "JSON log was not created!"

    with open(json_file) as f:
        data = json.load(f)

    assert data["run"]["id"] == "run_1"
    assert data["run"]["status"] == "completed"
    assert len(data["inputs"]) == 1
    assert len(data["outputs"]) == 1
    assert data["config"]["random_seed"] == 42

    print(f"\nJSON Output content: {json.dumps(data, indent=2)}")

    # 5. Check SQL (The Index)
    engine = create_engine(f"duckdb:///{db_path}")
    with Session(engine) as session:
        # Check Run
        runs = session.exec(select(Run)).all()
        assert len(runs) == 1
        assert runs[0].id == "run_1"

        # Check Artifacts
        artifacts = session.exec(select(Artifact)).all()
        assert len(artifacts) == 2  # 1 input + 1 output

        print(f"\nDB Artifacts found: {[a.key for a in artifacts]}")