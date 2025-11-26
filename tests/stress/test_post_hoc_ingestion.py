# tests/stress/test_post_hoc_ingestion.py

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
    Demonstrates the "HPC Workflow":
    1. Simulation Nodes run efficiently (creating Files + JSON only).
    2. A Head Node runs later to ingest data into DuckDB for analysis.
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
    print("\n[Phase 1] Simulation running (Writing Parquet + JSON only)...")

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
    print("[Phase 2] Reading JSON and Backfilling Database...")

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
    print(f"   -> Loading data from {abs_path}")
    df_to_load = pd.read_parquet(abs_path)

    # 4. PERFORM OFFLINE INGESTION
    # We pass the 'restored_run' explicitly.
    # This tags the data with 'run_hpc_01' even though that run is finished.
    tracker.ingest(
        artifact=restored_artifact, data=df_to_load, run=restored_run  # <--- THE MAGIC
    )

    # =========================================================================
    # PHASE 3: Verification
    # =========================================================================
    print("[Phase 3] Verifying Data Provenance...")

    # Create view using the restored artifact key
    tracker.create_view("v_weather", "weather_data")

    with tracker.engine.connect() as conn:
        # Check that 'consist_run_id' matches the original HPC run
        query = text("SELECT consist_run_id, COUNT(*) FROM v_weather GROUP BY 1")
        row = conn.execute(query).fetchone()

        print(f"   -> Found data for Run ID: {row[0]}")
        print(f"   -> Row Count: {row[1]}")

        assert row[0] == "run_hpc_01"
        assert row[1] == N_ROWS
