# tests/benchmarks/test_benchmark_comparison.py

import pytest
import pandas as pd
import numpy as np
import time
import os
import gc
from sqlmodel import text

import consist
from consist.core.tracker import Tracker

# Try imports for competitors/profiling
try:
    import polars as pl
except ImportError:
    pl = None

try:
    import psutil
except ImportError:
    psutil = None

# Scale: 5M rows per run (10M total analysis)
# Enough to stress Pandas, but fast for Polars/DuckDB
N_ROWS = 5_000_000


class Profiler:
    """Helper to measure Time and Peak RAM."""

    def __init__(self, name):
        self.name = name
        self.start_time = 0
        self.start_mem = 0
        self.peak_mem = 0

    def __enter__(self):
        gc.collect()  # Clean up before starting
        self.start_time = time.time()
        if psutil:
            process = psutil.Process(os.getpid())
            self.start_mem = process.memory_info().rss
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        mem_str = "N/A"
        if psutil:
            process = psutil.Process(os.getpid())
            end_mem = process.memory_info().rss
            # This is a rough proxy, capturing the diff is tricky with
            # external engines (DuckDB) but gives an idea of Python overhead.
            diff_mb = (end_mem - self.start_mem) / 1024 / 1024
            mem_str = f"{diff_mb:,.0f} MB (Delta)"

        print(f"[{self.name:<15}] Time: {duration:>6.3f}s | RAM: {mem_str}")


@pytest.mark.heavy
def test_comparative_analysis_benchmark(tmp_path):
    run_dir = tmp_path / "bench_runs"
    db_path = str(tmp_path / "bench.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)
    tracker.identity.hashing_strategy = "fast"

    # =========================================================================
    # 1. SETUP: GENERATE DATA
    # =========================================================================
    print(f"\n[Setup] Generating 2 runs x {N_ROWS:,} rows (Schema Drift included)...")

    # Run A (Baseline)
    df_a = pd.DataFrame(
        {
            "id": np.arange(N_ROWS),
            "val": np.random.normal(100, 10, size=N_ROWS),
            "category": np.random.choice(["X", "Y"], size=N_ROWS),
        }
    )

    # Run B (Policy - Has extra 'subsidy' column)
    df_b = pd.DataFrame(
        {
            "id": np.arange(N_ROWS),
            "val": np.random.normal(110, 10, size=N_ROWS),
            "category": np.random.choice(["X", "Y"], size=N_ROWS),
            "subsidy": np.random.uniform(0, 5, size=N_ROWS),  # <--- DRIFT
        }
    )

    # We must register these with Consist to make them queryable
    # but we save them to known paths for Pandas/Polars to find easily.
    path_a = run_dir / "run_a.parquet"
    path_b = run_dir / "run_b.parquet"

    with tracker.start_run("run_a", model="a"):
        df_a.to_parquet(path_a)
        consist.log_artifact(str(path_a), key="results")

    with tracker.start_run("run_b", model="a"):
        df_b.to_parquet(path_b)
        art_b = consist.log_artifact(str(path_b), key="results")

    # For Hot Benchmark: Ingest Run B only (simulate partial ingestion)
    # Actually let's ingest BOTH for the Hot test to be fair
    # We'll do ingestion inside the benchmark timer to show the cost if we want,
    # or pre-ingest. Let's pre-ingest for the "Query Speed" benchmark.
    # Note: We can't easily separate Hot/Cold artifacts with the same key in the same view
    # unless we manage `is_ingested` flag carefully.
    # For this test, let's keep them Cold for the main comparison.

    print("[Setup] Data generated.")
    del df_a, df_b
    gc.collect()

    # =========================================================================
    # BENCHMARK 1: PANDAS
    # =========================================================================
    # Workflow: Load A, Load B, Concat, Groupby
    with Profiler("Pandas") as p:
        # 1. Read
        # We must align schemas manually or rely on concat(sort=False)
        pdf_a = pd.read_parquet(path_a)
        pdf_a["run_id"] = "run_a"

        pdf_b = pd.read_parquet(path_b)
        pdf_b["run_id"] = "run_b"

        # 2. Concat (Handles drift by introducing NaNs)
        combined = pd.concat([pdf_a, pdf_b], ignore_index=True, sort=False)

        # 3. Aggregate
        res_pandas = combined.groupby("run_id").agg(
            {"val": "mean", "subsidy": "mean"}  # Will handle NaN for run_a correctly
        )

        # Force computation
        _ = len(res_pandas)

    # =========================================================================
    # BENCHMARK 2: POLARS (If installed)
    # =========================================================================
    if pl:
        with Profiler("Polars (Lazy)") as p:
            # 1. Scan (Lazy)
            lf_a = pl.scan_parquet(path_a).with_columns(pl.lit("run_a").alias("run_id"))
            lf_b = pl.scan_parquet(path_b).with_columns(pl.lit("run_b").alias("run_id"))

            # 2. Concat
            # diagonal=True handles schema drift (missing cols become null)
            combined_pl = pl.concat([lf_a, lf_b], how="diagonal")

            # 3. Aggregate
            res_polars = (
                combined_pl.group_by("run_id")
                .agg([pl.col("val").mean(), pl.col("subsidy").mean()])
                .collect()
            )  # Execute

    else:
        print("[Polars         ] Skipped (not installed)")

    # =========================================================================
    # BENCHMARK 3: CONSIST (Cold View)
    # =========================================================================
    # Workflow: Create View (Metadata scan), SQL Query (Vectorized Read)
    with Profiler("Consist (Cold)") as p:
        # 1. Create View
        # This scans the artifacts DB and generates the 'read_parquet([file1, file2])' SQL
        tracker.create_view("v_bench_cold", "results")

        # 2. Query
        with tracker.engine.connect() as conn:
            query = text(
                """
                SELECT 
                    consist_run_id, 
                    AVG(val), 
                    AVG(subsidy) 
                FROM v_bench_cold 
                GROUP BY 1
            """
            )
            res_consist = conn.execute(query).fetchall()

    # =========================================================================
    # VERIFICATION
    # =========================================================================
    # Ensure they got similar results
    print("\n[Verification]")
    print(f"Pandas Rows: {len(res_pandas)}")
    if pl:
        print(f"Polars Rows: {len(res_polars)}")
    print(f"Consist Rows: {len(res_consist)}")

    assert len(res_consist) == 2
