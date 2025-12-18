# tests/stress/test_post_hoc_ingestion.py

"""
Consist Post-Hoc (Offline) Ingestion Stress Tests

This module contains stress tests for Consist's "post-hoc" or "offline" ingestion
capabilities, which are particularly relevant for High-Performance Computing (HPC)
or distributed computing workflows.

It verifies that Consist can track runs and artifacts by writing only minimal
JSON metadata during the primary computation phase. Later, the associated data
can be ingested into the DuckDB database for analytical querying, correctly
attributing provenance back to the original computation run. This decouples
computation from data materialization.
"""

import logging
import pytest
import pandas as pd
import numpy as np
import json
import consist
from consist.core.tracker import Tracker
from consist.models.run import Run
from consist.models.artifact import Artifact
from sqlmodel import text

N_ROWS = 1_000_000


@pytest.mark.heavy
def test_offline_ingestion(tmp_path):
    """
    Tests Consist's "post-hoc" or "offline" ingestion workflow, simulating an HPC scenario.

    This test demonstrates a common High-Performance Computing (HPC) pattern:
    1.  **Simulation Phase**: Computations run on a cluster, efficiently generating
        large data files and minimal JSON metadata without taxing a shared database.
    2.  **Ingestion Phase**: A separate "head node" process later collects the JSON
        metadata and ingests the associated data files into a central DuckDB database
        for analysis.

    This verifies that Consist can correctly reconstruct run and artifact provenance
    from JSON logs and attribute ingested data to historical runs.

    What happens:
    1. A `Tracker` is initialized with `hashing_strategy="fast"`.
    2. **Phase 1 (HPC Simulation)**:
       - A large Pandas DataFrame (`df_sim`) is created.
       - A Consist run ("run_hpc_01") is started.
       - The DataFrame is saved to a Parquet file.
       - The Parquet file is logged as a "weather_data" artifact using `consist.log_artifact`.
       - *Crucially, `tracker.ingest()` is NOT called in this phase.*
    3. **Phase 2 (Nightly Ingestor - Post-Hoc)**:
       - The `consist.json` file generated in Phase 1 is loaded.
       - The `Run` and `Artifact` objects are reconstructed from the JSON metadata.
       - The physical data (Parquet file) is loaded.
       - `tracker.ingest()` is called, explicitly passing the `restored_artifact` and `restored_run`
         to ensure the data is linked to the original run.
    4. **Phase 3 (Verification)**: A view (`v_weather`) is created for "weather_data".

    What's checked:
    - After Phase 1, the `consist.json` file exists.
    - After Phase 3, querying `v_weather` reveals that `consist_run_id` for the ingested
      data correctly matches "run_hpc_01".
    - The row count in the `v_weather` view matches the original `N_ROWS` from the simulation.
    """
    run_dir = tmp_path / "hpc_run_01"
    db_path = str(tmp_path / "central_warehouse.duckdb")

    # Setup Tracker (No DB needed for the simulation phase technically,
    # but we pass it just to init the object)
    tracker = Tracker(run_dir=run_dir, db_path=db_path)
    tracker.identity.hashing_strategy = "fast"

    # =========================================================================
    # PHASE 1: The "HPC Simulation" (No DB Writes)
    # =========================================================================
    logging.info("\n[Phase 1] Simulation running (Writing Parquet + JSON only)...")

    df_sim = pd.DataFrame({"id": np.arange(N_ROWS), "val": np.random.rand(N_ROWS)})

    # Run creates artifacts but DOES NOT ingest
    with tracker.start_run("run_hpc_01", model="weather_sim"):
        path = run_dir / "sim_output.parquet"
        df_sim.to_parquet(path)

        # Log it so it appears in consist.json
        consist.log_artifact(str(path), key="weather_data")

    # Verify: JSON exists, DB is empty (conceptually)
    json_path = run_dir / "consist.json"
    assert json_path.exists()

    # =========================================================================
    # PHASE 2: The "Nightly Ingestor" (Post-Hoc)
    # =========================================================================
    logging.info("[Phase 2] Reading JSON and Backfilling Database...")

    # 1. Load the JSON Metadata
    with open(json_path, "r") as f:
        run_log = json.load(f)

    # 2. Reconstruct Objects (Pydantic/SQLModel makes this easy)
    # We parse the 'run' dictionary back into a Run object
    restored_run = Run.model_validate(run_log["run"])

    # We find the artifact we want to ingest
    # (In a real script, you'd loop through outputs)
    target_output_data = run_log["outputs"][0]
    restored_artifact = Artifact.model_validate(target_output_data)

    # 3. Load the physical data
    # We need to resolve the path. Since the JSON has the 'uri',
    # we use the tracker to resolve it back to absolute path.
    abs_path = tracker.resolve_uri(restored_artifact.uri)
    logging.info(f"   -> Loading data from {abs_path}")
    df_to_load = pd.read_parquet(abs_path)

    # 4. PERFORM OFFLINE INGESTION
    # We pass the 'restored_run' explicitly.
    # This tags the data with 'run_hpc_01' even though that run is finished.
    tracker.ingest(
        artifact=restored_artifact,
        data=df_to_load,
        run=restored_run,  # <--- THE MAGIC
    )

    # =========================================================================
    # PHASE 3: Verification
    # =========================================================================
    logging.info("[Phase 3] Verifying Data Provenance...")

    # Create view using the restored artifact key
    tracker.create_view("v_weather", "weather_data")

    with tracker.engine.connect() as conn:
        # Check that 'consist_run_id' matches the original HPC run
        query = text("SELECT consist_run_id, COUNT(*) FROM v_weather GROUP BY 1")
        row = conn.execute(query).fetchone()

        logging.info(f"   -> Found data for Run ID: {row[0]}")
        logging.info(f"   -> Row Count: {row[1]}")

        assert row[0] == "run_hpc_01"
        assert row[1] == N_ROWS
