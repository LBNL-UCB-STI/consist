"""
Heavy simulation pipeline stress test: large ingestion + join performance.
"""

import logging
import time

import numpy as np
import pandas as pd
import pytest
from sqlmodel import text

import consist
from consist.core.tracker import Tracker

N_ROWS = 5_000_000
N_ZONES = 1_000


@pytest.mark.heavy
def test_simulation_pipeline_stress_v2(tmp_path):
    run_dir = tmp_path / "stress_runs_v2"
    db_path = str(tmp_path / "stress_v2.duckdb")

    tracker = Tracker(run_dir=run_dir, db_path=db_path)
    tracker.identity.hashing_strategy = "fast"

    logging.info(f"\n[Setup] Generating {N_ROWS:,} rows of Census data + Zones...")
    t0 = time.time()

    df_zones = pd.DataFrame(
        {
            "zone_id": np.arange(N_ZONES),
            "district_name": [f"District_{i % 10}" for i in range(N_ZONES)],
            "parking_cost": np.random.uniform(0, 20.0, size=N_ZONES),
        }
    )

    df_census = pd.DataFrame(
        {
            "person_id": np.arange(N_ROWS),
            "home_zone_id": np.random.randint(0, N_ZONES, size=N_ROWS),
            "income": np.random.normal(60000, 20000, size=N_ROWS).astype(int),
            "mode": np.random.choice(["car", "transit", "walk"], size=N_ROWS),
        }
    )

    census_path = run_dir / "raw_census.parquet"
    zones_path = run_dir / "zones.csv"
    df_census.to_parquet(census_path)
    df_zones.to_csv(zones_path, index=False)

    logging.info(f"[Setup] Data generated in {time.time() - t0:.2f}s.")

    logging.info("\n[Run 1] Starting Ingestion...")

    with tracker.start_run("run_join_test", model="join_model"):
        t_ingest_start = time.time()

        art_zones = consist.log_artifact(str(zones_path), key="zones")
        df_z_load = pd.read_csv(zones_path)
        consist.ingest(art_zones, df_z_load)

        art_census = consist.log_artifact(str(census_path), key="census")
        df_c_load = pd.read_parquet(census_path)
        t_batch = time.time()
        consist.ingest(art_census, df_c_load)
        logging.info(
            f"   -> Consist.ingest (5M rows) took: {time.time() - t_batch:.2f}s"
        )

        tracker.create_view("v_zones", "zones")
        tracker.create_view("v_census", "census")

        logging.info("[Run 1] Running SQL Join...")

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
