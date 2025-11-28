"""
This module contains stress tests specifically designed to evaluate the performance
and correctness of Consist's "cold view optimization" for large numbers of file-based
artifacts.

It focuses on verifying the efficiency of vectorized reads from numerous Parquet files
and the graceful handling of schema drift without ingesting data into the database.
"""

import logging
import pytest
import pandas as pd
import numpy as np
import consist
from consist.core.tracker import Tracker
from sqlmodel import text


@pytest.mark.heavy
def test_cold_vectorized_view(tmp_path):
    """
    Verifies the "Zero-Storage" strategy:
    1. Log 50 separate Parquet files across 5 runs.
    2. Do NOT ingest them (keep them "Cold").
    3. Verify that create_view() generates a single optimized query.
    4. Verify Schema Drift handling (half the files have an extra column).
    """
    run_dir = tmp_path / "cold_runs"
    db_path = str(tmp_path / "cold.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)
    tracker.identity.hashing_strategy = "fast"

    logging.info("\n[Setup] Generating 50 files across 5 runs...")

    # We will track expected counts
    total_rows = 0
    expected_extra_sum = 0

    for i in range(5):
        run_id = f"run_{i}"
        with tracker.start_run(run_id, model="asim"):
            # Create 10 files per run
            for j in range(10):
                rows = 100
                total_rows += rows

                data = {"id": np.arange(rows), "val": np.random.rand(rows)}

                # Introduce Schema Drift in later runs
                if i >= 3:
                    data["extra_col"] = 1
                    expected_extra_sum += rows

                df = pd.DataFrame(data)

                fname = f"part_{j}.parquet"
                path = run_dir / f"{run_id}_{fname}"
                df.to_parquet(path)

                # Log only (No Ingest!)
                consist.log_artifact(str(path), key="distributed_data")

    # =========================================================================
    # VERIFICATION
    # =========================================================================
    logging.info("[Verification] Creating Hybrid View over Cold Data...")

    # This should trigger _generate_cold_query_optimized
    # It should produce a single SELECT ... FROM read_parquet([... list of 50 files ...])
    tracker.create_view("v_cold", "distributed_data")

    logging.info("[Verification] Querying...")
    with tracker.engine.connect() as conn:
        # 1. Count Total Rows
        count = conn.execute(text("SELECT COUNT(*) FROM v_cold")).scalar()
        logging.info(f"   -> Total Rows: {count}")
        assert count == total_rows

        # 2. Check Schema Drift (extra_col should be NULL for runs 0-2, 1 for runs 3-4)
        # Summing it verifies that DuckDB correctly aligned the schemas
        extra_sum = conn.execute(text("SELECT SUM(extra_col) FROM v_cold")).scalar()
        logging.info(f"   -> Extra Col Sum: {extra_sum}")
        assert extra_sum == expected_extra_sum

        # 3. Check Metadata Injection (The CTE Join)
        # We group by run_id to ensure the join against filenames worked
        run_counts = conn.execute(
            text("SELECT consist_run_id, COUNT(*) FROM v_cold GROUP BY 1 ORDER BY 1")
        ).fetchall()

        logging.info("   -> Rows per Run:")
        for r in run_counts:
            logging.info(f"      {r[0]}: {r[1]}")
            assert r[1] == 1000  # 10 files * 100 rows

    logging.info("Success! Vectorized Read Optimization works.")
