"""
Stress test for schema evolution across large runs and hybrid views.
"""

import logging

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import text

import consist
from consist.core.tracker import Tracker

N_ROWS = 5_000_000


@pytest.mark.heavy
def test_schema_evolution_and_comparison(tmp_path):
    run_dir = tmp_path / "stress_runs_v3"
    db_path = str(tmp_path / "stress_v3.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)
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

    with tracker.engine.connect() as conn:
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
            avg_sub = f"${row[4]:.2f}" if row[4] is not None else "NULL"
            logging.info(
                f"{run_id:<15} {row[1]:<10} ${row[2]:<11,.0f} {row[3]:<8} {avg_sub}"
            )

        assert len(results) == 2
        run_ids = [r[0] for r in results]
        assert "run_baseline" in run_ids
        assert "run_policy" in run_ids

        base_row = next(r for r in results if r[0] == "run_baseline")
        policy_row = next(r for r in results if r[0] == "run_policy")

        assert base_row[4] is None, "Baseline should have NULL subsidy"
        assert policy_row[4] is not None, "Policy should have computed subsidy"
