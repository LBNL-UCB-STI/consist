import logging
import json
from sqlmodel import Session, select

# Note: Because of 'src' layout, this import works if environment is set up right
from consist.core.tracker import Tracker
from consist.models.run import Run
from consist.models.artifact import Artifact
from consist.core.identity import IdentityManager


def test_dual_write_workflow(tracker: Tracker):
    """
    Tests the end-to-end "dual-write" functionality of the `Tracker`,
    including automatic identity hashing (Config & Git).

    This test simulates a complete Consist workflow, from initializing the `Tracker`
    to logging input/output artifacts, and verifies that provenance information is
    correctly recorded in both the human-readable JSON file (`consist.json`)
    and the analytical DuckDB database. It ensures that the primary record-keeping
    mechanisms of Consist are functioning in harmony and that key identity hashes
    are correctly computed and stored.

    What happens:
    1. A `Tracker` is initialized with temporary paths for the run directory and database.
    2. A configuration dictionary is defined, and its expected hash is manually computed
       using `IdentityManager` for later verification.
    3. A run context ("run_1") is started using `tracker.start_run`, simulating a model execution
       with the defined configuration.
    4. An input artifact (`land_use.csv`) and an output artifact (`trips.csv`) are logged
       within the run.
    5. The run completes successfully.

    What's checked:
    -   **JSON Log Verification**:
        - The `consist.json` file is created in the run directory.
        - Its contents are validated: the run ID, status ("completed"), presence of
          one input and one output artifact, and the configuration.
        - The `config_hash` and `git_hash` stored in the JSON match the expected values
          (the `git_hash` is checked for existence and non-null status, as its exact
          value depends on the Git environment).
    -   **DuckDB Verification**:
        - The DuckDB database is checked to ensure that one `Run` record with the
          correct ID, `config_hash`, and `git_hash` was created.
        - Two `Artifact` records (one input, one output) are found in the database.
    """
    # 3. Run a Fake Workflow
    config = {"random_seed": 42, "scenario": "test"}

    # Calculate what we expect the hash to be manually
    expected_config_hash = IdentityManager().compute_config_hash(config)

    with tracker.start_run(run_id="run_1", model="test_model", config=config):
        # Log an input
        tracker.log_artifact(
            path="/inputs/land_use.csv", key="land_use", direction="input"
        )

        # Log an output
        tracker.log_artifact(
            path=str(tracker.run_dir / "trips.csv"),
            key="trips",
            direction="output",
            meta={"rows": 100},
        )

    # --- ASSERTION TIME ---

    # 4. Check JSON (The Truth)
    json_file = tracker.run_dir / "consist.json"
    assert json_file.exists(), "JSON log was not created!"

    with open(json_file) as f:
        data = json.load(f)

    # Standard checks
    assert data["run"]["id"] == "run_1"
    assert data["run"]["status"] == "completed"
    assert len(data["inputs"]) == 1
    assert len(data["outputs"]) == 1
    assert data["config"]["random_seed"] == 42

    # Hashing checks
    assert (
        data["run"]["config_hash"] == expected_config_hash
    ), "Config hash mismatch in JSON"
    assert "git_hash" in data["run"], "Git hash field missing in JSON"
    # Note: git_hash might be 'no_git_module_found' or 'unknown' depending on environment,
    # but it should not be None/Null.
    assert data["run"]["git_hash"] is not None

    logging.info(f"\nJSON Output content: {json.dumps(data, indent=2)}")

    # 5. Check SQL (The Index)
    with Session(tracker.engine) as session:
        # Check Run
        runs = session.exec(select(Run)).all()
        assert len(runs) == 1
        db_run = runs[0]

        assert db_run.id == "run_1"
        assert db_run.config_hash == expected_config_hash, "Config hash mismatch in DB"
        assert db_run.git_hash is not None, "Git hash missing in DB"

        # Check Artifacts
        artifacts = session.exec(select(Artifact)).all()
        assert len(artifacts) == 2  # 1 input + 1 output

        logging.info(f"\nDB Artifacts found: {[a.key for a in artifacts]}")
