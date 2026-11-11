"""
Stress test for schema evolution across large runs and hybrid views.
"""

import logging

import numpy as np
import pandas as pd
import pytest

import consist
from consist.core.tracker import Tracker

N_ROWS = 5_000_000


@pytest.mark.heavy
def test_schema_evolution_and_comparison(tmp_path):
    run_dir = tmp_path / "stress_runs_v3"
    db_path = str(tmp_path / "stress_v3.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)
    try:
        tracker.identity.hashing_strategy = "fast"

        logging.info(f"\n[Run 1] Baseline: Generating {N_ROWS:,} rows...")

        df_base = pd.DataFrame(
            {
                "person_id": np.arange(N_ROWS),
                "income": np.random.normal(50000, 15000, size=N_ROWS).astype(int),
                "mode": np.random.choice(["car", "transit"], size=N_ROWS),
            }
        )

        with tracker.start_run("run_baseline", model="asim", scenario="base"):
            path = run_dir / "base_results.parquet"
            df_base.to_parquet(path)
            art = consist.log_artifact(str(path), key="persons")
            consist.ingest(art, df_base)

        logging.info(f"[Run 2] Policy: Generating {N_ROWS:,} rows with NEW COLUMN...")

        df_policy = pd.DataFrame(
            {
                "person_id": np.arange(N_ROWS),
                "income": np.random.normal(55000, 15000, size=N_ROWS).astype(int),
                "mode": np.random.choice(["car", "transit", "ev"], size=N_ROWS),
                "ev_subsidy": np.random.choice([0, 5000], size=N_ROWS),
            }
        )

        with tracker.start_run("run_policy", model="asim", scenario="policy_A"):
            path = run_dir / "policy_results.parquet"
            df_policy.to_parquet(path)
            art = consist.log_artifact(str(path), key="persons")
            consist.ingest(art, df_policy)

        logging.info("\n[Analysis] creating hybrid view over BOTH runs...")
        tracker.create_view("v_persons", "persons")

        logging.info("[Analysis] Comparing Scenarios via SQL...")

        res_df = tracker.query_df(
            """
            SELECT
                consist_run_id,
                COUNT(*) AS count,
                AVG(income) AS avg_income,
                SUM(CASE WHEN mode = 'ev' THEN 1 ELSE 0 END) AS ev_count,
                AVG(ev_subsidy) AS avg_subsidy
            FROM v_persons
            GROUP BY consist_run_id
            ORDER BY avg_income
            """
        )

        logging.info(
            f"\n{'Run ID':<15} {'Count':<10} {'Avg Inc':<12} {'EVs':<8} {'Avg Sub (Schema Check)'}"
        )
        logging.info("-" * 70)
        for _, row in res_df.iterrows():
            run_id = str(row["consist_run_id"])
            avg_sub = (
                f"${float(row['avg_subsidy']):.2f}"
                if pd.notna(row["avg_subsidy"])
                else "NULL"
            )
            logging.info(
                f"{run_id:<15} {int(row['count']):<10} ${float(row['avg_income']):<11,.0f} {int(row['ev_count']):<8} {avg_sub}"
            )

        assert len(res_df) == 2
        run_ids = set(res_df["consist_run_id"].tolist())
        assert "run_baseline" in run_ids
        assert "run_policy" in run_ids

        base_row = res_df[res_df["consist_run_id"] == "run_baseline"].iloc[0]
        policy_row = res_df[res_df["consist_run_id"] == "run_policy"].iloc[0]

        assert pd.isna(base_row["avg_subsidy"]), "Baseline should have NULL subsidy"
        assert pd.notna(policy_row["avg_subsidy"]), (
            "Policy should have computed subsidy"
        )
    finally:
        tracker.close()
