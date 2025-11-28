import pytest
import pandas as pd
from consist.core.tracker import Tracker
import logging


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
    logging.info("\n--- Starting Run A ---")
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
    logging.info("\n--- Starting Run B (Expect Cache Hit) ---")
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
    logging.info("\n--- Starting Run C (Expect Lineage Link) ---")
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


def test_cache_overwrite_mode(tracker_setup):
    """
    Tests the `cache_mode="overwrite"` functionality.

    What happens:
    1. Run A is executed and completes, populating the cache.
    2. Run B, identical to A, is executed with `cache_mode="overwrite"`. It should
       execute without using the cache.
    3. Run C, identical to A and B, is executed with the default `reuse` mode. It should
       find a cache hit, and the hit should be Run B, not Run A.

    What's checked:
    - Run B is not cached (`is_cached` is False).
    - Run C is cached (`is_cached` is True).
    - Run C's cache hit correctly points to Run B, proving the cache was updated.
    """
    tracker, input_file = tracker_setup
    config = {"param": 100}
    input_path = str(input_file)

    # 1. Run A: The Base Run
    with tracker.start_run(
        "run_A_overwrite", "overwrite_model", config=config, inputs=[input_path]
    ):
        pass  # Just run to populate cache

    # 2. Run B: Overwrite the cache
    logging.info("--- Starting Run B (Overwrite Mode) ---")
    with tracker.start_run(
        "run_B_overwrite",
        "overwrite_model",
        config=config,
        inputs=[input_path],
        cache_mode="overwrite",
    ) as t:
        assert not t.is_cached, "Run B in overwrite mode should not be cached."

    # 3. Run C: Check which run is now cached
    logging.info("--- Starting Run C (Expect Cache Hit on Run B) ---")
    with tracker.start_run(
        "run_C_overwrite",
        "overwrite_model",
        config=config,
        inputs=[input_path],
        cache_mode="reuse",  # Default behavior
    ) as t:
        assert t.is_cached, "Run C should have a cache hit."
        cached_run = t.current_consist.cached_run
        assert cached_run is not None
        assert (
            cached_run.id == "run_B_overwrite"
        ), "Cache hit should be Run B, not Run A."
