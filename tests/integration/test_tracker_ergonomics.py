import pytest
from consist import Tracker
from consist.models.artifact import Artifact
from consist.models.run import RunArtifacts


def test_find_run_ergonomics(tracker: Tracker):
    """
    Verifies the new singular find_run() and index_by functionality.
    """
    # 1. Setup: Create a few runs
    tracker.begin_run("run_2020", "model_a", year=2020, tags=["baseline"])
    tracker.end_run()

    tracker.begin_run("run_2021", "model_a", year=2021, tags=["baseline"])
    tracker.end_run()

    tracker.begin_run("run_other", "model_b", year=2020, tags=["alternative"])
    tracker.end_run()

    # 2. Test: Singular find_run success
    run = tracker.find_run(year=2021)
    assert run.id == "run_2021"

    # 3. Test: Singular find_run failure (0 matches)
    with pytest.raises(ValueError) as exc:
        tracker.find_run(year=2030)
    assert "No run found" in str(exc.value)

    # 4. Test: Singular find_run failure (>1 matches)
    with pytest.raises(ValueError) as exc:
        tracker.find_run(year=2020)  # Matches run_2020 and run_other
    assert "Multiple runs" in str(exc.value)

    # 5. Test: index_by dictionary return
    # Get all baseline runs, indexed by year
    runs_by_year = tracker.find_runs(tags=["baseline"], index_by="year")

    assert isinstance(runs_by_year, dict)
    assert len(runs_by_year) == 2
    assert runs_by_year[2020].id == "run_2020"
    assert runs_by_year[2021].id == "run_2021"


def test_structured_artifact_access(tracker: Tracker, run_dir):
    """
    Verifies that get_artifacts_for_run returns a structured RunArtifacts object
    allowing dictionary access to inputs and outputs.
    """
    # Setup files
    (run_dir / "in.csv").touch()
    (run_dir / "out.csv").touch()

    # Run
    with tracker.start_run("io_test", "model_io"):
        # FIX: Use absolute paths from run_dir to avoid CWD resolution issues
        tracker.log_artifact(run_dir / "in.csv", key="config_file", direction="input")
        tracker.log_artifact(run_dir / "out.csv", key="results_data", direction="output")

    # Test
    artifacts = tracker.get_artifacts_for_run("io_test")

    # Check Type
    assert isinstance(artifacts, RunArtifacts)

    # Check Output Dictionary Access
    assert "results_data" in artifacts.outputs
    assert isinstance(artifacts.outputs["results_data"], Artifact)
    assert artifacts.outputs["results_data"].key == "results_data"

    # Check Input Dictionary Access
    assert "config_file" in artifacts.inputs
    assert artifacts.inputs["config_file"].key == "config_file"

    # Check Missing Key behavior (standard dict behavior)
    assert "missing" not in artifacts.outputs


def test_access_modes(tracker: Tracker, run_dir):
    """
    Verifies permissions for 'analysis' and 'read_only' modes.
    """
    # Setup: Create a base run in standard mode so we have something to ingest/query later
    csv_path = run_dir / "data.csv"
    csv_path.write_text("a,b\n1,2")

    with tracker.start_run("base_run", "setup"):
        # FIX: Use absolute path
        art = tracker.log_artifact(csv_path, key="my_data")

    # --- TEST: Analysis Mode ---
    # Create a NEW tracker instance pointing to same DB
    analysis_tracker = Tracker(
        run_dir=run_dir,
        db_path=tracker.db_path,
        access_mode="analysis"
    )

    # 1. Allowed: Ingest (Backfill)
    # This should work by attributing data to 'base_run' found in artifact metadata
    analysis_tracker.ingest(art)

    # 2. Blocked: Start Run
    with pytest.raises(RuntimeError) as exc:
        analysis_tracker.begin_run("new_run", "test")
    assert "forbidden in 'analysis' mode" in str(exc.value)

    # --- TEST: Read-Only Mode ---
    ro_tracker = Tracker(
        run_dir=run_dir,
        db_path=tracker.db_path,
        access_mode="read_only"
    )

    # 1. Allowed: Find Run
    run = ro_tracker.find_run(run_id="base_run")
    assert run is not None

    # 2. Blocked: Ingest
    with pytest.raises(RuntimeError) as exc:
        ro_tracker.ingest(art)
    assert "forbidden in 'read_only' mode" in str(exc.value)

    # 3. Blocked: Log Artifact
    with pytest.raises(RuntimeError) as exc:
        # FIX: Use absolute path
        ro_tracker.log_artifact(csv_path, key="rogue")
    assert "forbidden in 'read_only' mode" in str(exc.value)