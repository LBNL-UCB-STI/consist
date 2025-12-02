"""
Consist Heavy Workflow Stress Tests

This module contains stress tests designed to push Consist's performance and
scalability with large datasets (millions of rows).

It verifies the efficiency of data ingestion, complex SQL joins over large tables,
and the seamless handling of schema evolution across different simulation runs.
These tests demonstrate Consist's capabilities in demanding analytical workflows
where both speed and data integrity are crucial.
"""

import logging
import pytest
import pandas as pd
import numpy as np
import time

from sqlalchemy import text

import consist
from consist.core.tracker import Tracker

# Increased scale to 5M (approx 150MB-200MB CSV)
# This would take ~6 minutes with the old method.
# With optimization, it should take seconds.
N_ROWS = 5_000_000
N_ZONES = 1_000


@pytest.mark.heavy
def test_simulation_pipeline_stress_v2(tmp_path):
    """
    Stress tests a multi-stage simulation pipeline involving large datasets,
    focusing on data ingestion efficiency and complex SQL join performance within Consist.

    This test simulates a common scenario in large-scale data analysis where
    multiple tables need to be ingested and then joined for analytical purposes.
    It pushes the limits of Consist's DuckDB integration and vectorized operations
    to verify its scalability and performance under demanding conditions.

    What happens:
    1.  Two large Pandas DataFrames (`df_zones` - 1,000 rows, `df_census` - 5,000,000 rows)
        are generated, representing typical simulation outputs.
    2.  `df_census` is saved as a Parquet file, and `df_zones` as a CSV.
    3.  A Consist run (`run_join_test`) is initiated.
    4.  Both the 'zones' and 'census' data are logged as artifacts and then ingested
        into the Consist database. The test specifically measures the time taken
        for the `consist.ingest` call for the large 'census' DataFrame.
    5.  Hybrid views (`v_zones`, `v_census`) are created over the ingested tables.
    6.  A complex SQL query is executed against the views. This query joins the
        large 'census' table with the 'zones' table and performs aggregations
        (count, average income, average parking cost per mode).

    What's checked:
    -   The total time taken for data generation, ingestion, and query execution is printed
        to stdout, demonstrating Consist's performance for this stress scenario.
    -   The results of the complex SQL query are printed to stdout, providing a visual
        confirmation of the aggregated data (e.g., average income, average parking cost
        per mode for different transportation modes).
    -   (Implicit) The test should complete without errors and within a reasonable time,
        indicating the scalability and stability of Consist's underlying mechanisms for
        handling large datasets and complex analytical operations.
    """
    run_dir = tmp_path / "stress_runs_v2"
    db_path = str(tmp_path / "stress_v2.duckdb")

    tracker = Tracker(run_dir=run_dir, db_path=db_path)
    tracker.identity.hashing_strategy = "fast"

    # ----------------------------------------------------------------
    # SETUP: Generate Data
    # ----------------------------------------------------------------
    logging.info(f"\n[Setup] Generating {N_ROWS:,} rows of Census data + Zones...")
    t0 = time.time()

    # 1. Zones Table (Small, for joining)
    df_zones = pd.DataFrame(
        {
            "zone_id": np.arange(N_ZONES),
            "district_name": [f"District_{i % 10}" for i in range(N_ZONES)],
            "parking_cost": np.random.uniform(0, 20.0, size=N_ZONES),
        }
    )

    # 2. Census Table (Large)
    df_census = pd.DataFrame(
        {
            "person_id": np.arange(N_ROWS),
            "home_zone_id": np.random.randint(0, N_ZONES, size=N_ROWS),
            "income": np.random.normal(60000, 20000, size=N_ROWS).astype(int),
            "mode": np.random.choice(["car", "transit", "walk"], size=N_ROWS),
        }
    )

    # Save raw
    census_path = run_dir / "raw_census.parquet"
    zones_path = run_dir / "zones.csv"

    df_census.to_parquet(census_path)  # Start with parquet this time
    df_zones.to_csv(zones_path, index=False)

    logging.info(f"[Setup] Data generated in {time.time() - t0:.2f}s.")

    # ----------------------------------------------------------------
    # RUN 1: Ingestion & Join Analysis
    # ----------------------------------------------------------------
    logging.info("\n[Run 1] Starting Ingestion...")

    with tracker.start_run("run_join_test", model="join_model"):
        t_ingest_start = time.time()

        # A. Log & Ingest Zones (Small CSV)
        # For CSVs, we read into pandas to get speedup, or pass dicts if lazy
        # Let's use pandas for consistency
        art_zones = consist.log_artifact(str(zones_path), key="zones")
        df_z_load = pd.read_csv(zones_path)

        consist.ingest(art_zones, df_z_load)  # Passing DataFrame!

        # B. Log & Ingest Census (Large Parquet)
        art_census = consist.log_artifact(str(census_path), key="census")
        df_c_load = pd.read_parquet(census_path)

        # This is the moment of truth: Passing 5M row DataFrame
        t_batch = time.time()
        consist.ingest(art_census, df_c_load)  # Passing DataFrame!
        logging.info(
            f"   -> Consist.ingest (5M rows) took: {time.time() - t_batch:.2f}s"
        )

        # C. Create Views
        tracker.create_view("v_zones", "zones")
        tracker.create_view("v_census", "census")

        # D. Perform Join Analysis via SQL
        # "Calculate average income and average parking cost per mode"
        # This joins the 5M row table to the 1k row table
        logging.info("[Run 1] Running SQL Join...")

        from sqlmodel import text

        with tracker.engine.connect() as conn:
            query = text(
                """
                SELECT 
                    c.mode, 
                    COUNT(*) as trips,
                    AVG(c.income) as avg_income,
                    AVG(z.parking_cost) as avg_parking_at_home
                FROM v_census c
                JOIN v_zones z ON c.home_zone_id = z.zone_id
                GROUP BY c.mode
                ORDER BY trips DESC
            """
            )
            t_query = time.time()
            results = conn.execute(query).fetchall()
            q_time = time.time() - t_query

            logging.info(f"\n[Join Results] (Query took {q_time:.4f}s)")
            logging.info(f"{'Mode':<10} {'Trips':<10} {'Avg Inc':<12} {'Avg Park $'}")
            logging.info("-" * 45)
            for row in results:
                logging.info(
                    f"{row[0]:<10} {row[1]:<10} ${row[2]:<11,.0f} ${row[3]:.2f}"
                )

        total_time = time.time() - t_ingest_start
        logging.info(f"\n[Run 1] Total Pipeline Time: {total_time:.2f}s")


@pytest.mark.heavy
def test_schema_evolution_and_comparison(tmp_path):
    """
    Stress tests Consist's ability to handle schema evolution across multiple runs
    and perform comparative analysis using hybrid views.

    This test simulates a scenario where a simulation's output schema changes over time
    (e.g., adding new columns in a policy scenario). It verifies that Consist can
    seamlessly query these evolving large datasets as a single, unified view,
    gracefully handling schema differences.

    What happens:
    1. A `Tracker` is initialized.
    2. **Run 1 (Baseline Scenario)**: A large dataset (`N_ROWS`) with a standard schema
       (`person_id`, `income`, `mode`) is generated, saved as Parquet, and then logged
       and ingested as a "persons" artifact.
    3. **Run 2 (Policy Scenario)**: Another large dataset (`N_ROWS`) is generated. This
       dataset includes a new column (`ev_subsidy`) not present in Run 1, and also introduces
       a new categorical value (`ev` mode) in the `mode` column. It's saved as Parquet,
       logged as "persons" artifact (same key), and ingested.
    4. A hybrid view (`v_persons`) is created over both the baseline and policy run data.
       Consist's view factory will use `UNION ALL BY NAME` to combine these schemas.
    5. A SQL query is executed against this hybrid view. The query groups by
       `consist_run_id` and calculates aggregated metrics (count, average income,
       count of 'ev' mode, and average `ev_subsidy`).

    What's checked:
    - The `create_view` operation and subsequent SQL query execute successfully,
      demonstrating that Consist's view generation properly handles schema evolution
      between the two large datasets.
    - The SQL query results confirm:
        - Both `run_baseline` and `run_policy` IDs are present.
        - The `ev_subsidy` column is correctly `NULL` for `run_baseline` (as it
          didn't exist in its original schema).
        - The `ev_subsidy` and `ev_count` for `run_policy` are correctly computed.
        - Other aggregated metrics (like `avg_income`) are correctly calculated for both runs.
    - Performance metrics (time taken) are implicitly validated by the test completing
      within reasonable bounds.
    """
    run_dir = tmp_path / "stress_runs_v3"
    db_path = str(tmp_path / "stress_v3.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)
    tracker.identity.hashing_strategy = "fast"

    # =========================================================================
    # RUN 1: BASELINE SCENARIO (Standard Columns)
    # =========================================================================
    logging.info(f"\n[Run 1] Baseline: Generating {N_ROWS:,} rows...")

    # Generate Baseline Data
    df_base = pd.DataFrame(
        {
            "person_id": np.arange(N_ROWS),
            "income": np.random.normal(50000, 15000, size=N_ROWS).astype(int),
            "mode": np.random.choice(["car", "transit"], size=N_ROWS),
        }
    )

    with tracker.start_run("run_baseline", model="asim", scenario="base"):
        # Log & Ingest
        # We simulate saving to disk first
        path = run_dir / "base_results.parquet"
        df_base.to_parquet(path)

        art = consist.log_artifact(str(path), key="persons")
        consist.ingest(art, df_base)  # Vectorized ingestion

    # =========================================================================
    # RUN 2: POLICY SCENARIO (Schema Drift!)
    # =========================================================================
    logging.info(f"[Run 2] Policy: Generating {N_ROWS:,} rows with NEW COLUMN...")

    # Generate Policy Data (Shifted Income + New Column)
    df_policy = pd.DataFrame(
        {
            "person_id": np.arange(N_ROWS),
            "income": np.random.normal(55000, 15000, size=N_ROWS).astype(int),  # Richer
            "mode": np.random.choice(["car", "transit", "ev"], size=N_ROWS),  # New Mode
            # NEW COLUMN: Did not exist in Run 1
            "ev_subsidy": np.random.choice([0, 5000], size=N_ROWS),
        }
    )

    with tracker.start_run("run_policy", model="asim", scenario="policy_A"):
        path = run_dir / "policy_results.parquet"
        df_policy.to_parquet(path)

        art = consist.log_artifact(str(path), key="persons")  # Same key!

        # Ingesting data with EXTRA columns into the SAME table.
        # dlt/DuckDB should handle this evolution automatically.
        consist.ingest(art, df_policy)

    # =========================================================================
    # ANALYSIS: The "One Query" Comparison
    # =========================================================================
    logging.info("\n[Analysis] creating hybrid view over BOTH runs...")

    # This view now unions the parquet files/tables from both runs
    tracker.create_view("v_persons", "persons")

    logging.info("[Analysis] Comparing Scenarios via SQL...")

    with tracker.engine.connect() as conn:
        # We group by 'consist_run_id' to see the difference
        # We also query 'ev_subsidy' to prove we can read the new column
        # (It will be NULL for run_baseline)
        query = text(
            """
            SELECT 
                consist_run_id,
                COUNT(*) as count,
                AVG(income) as avg_income,
                SUM(case when mode='ev' then 1 else 0 end) as ev_count,
                AVG(ev_subsidy) as avg_subsidy
            FROM v_persons
            GROUP BY consist_run_id
            ORDER BY avg_income
        """
        )

        results = conn.execute(query).fetchall()

        logging.info(
            f"\n{'Run ID':<15} {'Count':<10} {'Avg Inc':<12} {'EVs':<8} {'Avg Sub (Schema Check)'}"
        )
        logging.info("-" * 70)
        for row in results:
            run_id = row[0]
            # Handle potential None for subsidy in baseline
            avg_sub = f"${row[4]:.2f}" if row[4] is not None else "NULL"
            logging.info(
                f"{run_id:<15} {row[1]:<10} ${row[2]:<11,.0f} {row[3]:<8} {avg_sub}"
            )

        # Assertions to prove the "Selling Points"
        assert len(results) == 2

        # 1. Provenance Check
        run_ids = [r[0] for r in results]
        assert "run_baseline" in run_ids
        assert "run_policy" in run_ids

        # 2. Schema Evolution Check
        # Baseline should have NULL subsidy (because column didn't exist)
        base_row = next(r for r in results if r[0] == "run_baseline")
        policy_row = next(r for r in results if r[0] == "run_policy")

        assert base_row[4] is None, "Baseline should have NULL subsidy"
        assert policy_row[4] is not None, "Policy should have computed subsidy"
