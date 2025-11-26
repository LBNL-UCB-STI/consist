import pytest
import pandas as pd
from consist.core.tracker import Tracker


@pytest.fixture
def tracker_setup(tmp_path):
    """Sets up a tracker with a temporary run directory and database."""
    run_dir = tmp_path / "runs"
    db_path = str(tmp_path / "provenance.duckdb")

    # Create a dummy data file for inputs
    input_file = tmp_path / "source_data.csv"
    input_file.write_text("id,val\n1,100")

    tracker = Tracker(run_dir=run_dir, db_path=db_path)
    return tracker, input_file


def test_caching_and_forking(tracker_setup):
    tracker, input_file = tracker_setup

    config = {"param": 42}
    input_path = str(input_file)

    # ---------------------------------------------------------
    # 1. Run A: The Base Run
    # ---------------------------------------------------------
    print("\n--- Starting Run A ---")
    with tracker.start_run(
        "run_A", model="test_model", config=config, inputs=[input_path]
    ) as t:
        # Verify hashes calculated
        assert t.current_consist.run.config_hash is not None
        assert t.current_consist.run.input_hash is not None

        # Create an output
        df = pd.DataFrame({"a": [1, 2]})
        out_path = t.run_dir / "run_A_out.parquet"
        df.to_parquet(out_path)
        t.log_artifact(out_path, key="intermediate", direction="output")

    # ---------------------------------------------------------
    # 2. Run B: The Identical Twin (Cache Test)
    # ---------------------------------------------------------
    print("\n--- Starting Run B (Expect Cache Hit) ---")
    with tracker.start_run(
        "run_B", model="test_model", config=config, inputs=[input_path]
    ) as t:
        # ASSERTION: Cache Lookup
        cached = t.current_consist.cached_run
        assert cached is not None, "Run B should have found Run A in cache"
        assert cached.id == "run_A"
        assert cached.status == "completed"

    # ---------------------------------------------------------
    # 3. Run C: The Child (Lineage/Forking Test)
    # ---------------------------------------------------------
    print("\n--- Starting Run C (Expect Lineage Link) ---")
    # Run C takes Run A's output as its input
    prev_output = str(tracker.run_dir / "run_A_out.parquet")

    with tracker.start_run(
        "run_C", model="downstream_model", inputs=[prev_output]
    ) as t:
        # ASSERTION: Parent Linking
        current_run = t.current_consist.run
        assert (
            current_run.parent_run_id == "run_A"
        ), f"Run C should automatically link to Run A. Got {current_run.parent_run_id}"

        # ASSERTION: Input Artifact Lineage
        # The input artifact object should have run_id="run_A" populated from DB
        inp_artifact = t.current_consist.inputs[0]
        assert inp_artifact.run_id == "run_A"
