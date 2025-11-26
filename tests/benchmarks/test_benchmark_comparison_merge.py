import pytest
import pandas as pd
import numpy as np
import time
import os
import gc
from sqlmodel import text

import consist
from consist import Run
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

N_ROWS = 5_000_000


class Profiler:
    """Helper to measure Time and Peak RAM."""

    def __init__(self, name):
        self.name = name
        self.start_time = 0
        self.start_mem = 0

    def __enter__(self):
        gc.collect()
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
            diff_mb = (end_mem - self.start_mem) / 1024 / 1024
            mem_str = f"{diff_mb:,.0f} MB (Delta)"

        print(f"[{self.name:<15}] Time: {duration:>6.3f}s | RAM: {mem_str}")


@pytest.mark.heavy
def test_comparative_join_benchmark(tmp_path):
    run_dir = tmp_path / "bench_join"
    db_path = str(tmp_path / "bench_join.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)
    tracker.identity.hashing_strategy = "fast"

    # =========================================================================
    # 1. SETUP: GENERATE DATA
    # =========================================================================
    print(f"\n[Setup] Generating 2 runs x {N_ROWS:,} rows...")

    ids = np.arange(N_ROWS)

    # Run A
    df_a = pd.DataFrame(
        {
            "id": ids,
            "val": np.random.normal(100, 10, size=N_ROWS),
            "category": np.random.choice(["X", "Y"], size=N_ROWS),
        }
    )

    # Run B
    df_b = pd.DataFrame(
        {
            "id": np.random.permutation(ids),
            "val": np.random.normal(110, 10, size=N_ROWS),
            "category": np.random.choice(["X", "Y"], size=N_ROWS),
            "subsidy": np.random.uniform(0, 5, size=N_ROWS),
        }
    )

    path_a = run_dir / "run_a.parquet"
    path_b = run_dir / "run_b.parquet"

    # Capture artifacts for Hot Ingestion later
    art_a = None
    art_b = None

    with tracker.start_run("run_a", model="a"):
        df_a.to_parquet(path_a)
        art_a = consist.log_artifact(str(path_a), key="results")

    with tracker.start_run("run_b", model="a"):
        df_b.to_parquet(path_b)
        art_b = consist.log_artifact(str(path_b), key="results")

    print("[Setup] Data generated.")
    del df_a, df_b
    gc.collect()

    # =========================================================================
    # BENCHMARK 1: PANDAS (Merge)
    # =========================================================================
    with Profiler("Pandas") as p:
        pdf_a = pd.read_parquet(path_a)
        pdf_b = pd.read_parquet(path_b)

        merged = pd.merge(pdf_a, pdf_b, on="id", suffixes=("_a", "_b"))
        merged["delta"] = merged["val_b"] - merged["val_a"]
        res_pandas = merged.groupby("category_a")["delta"].mean()

    del pdf_a, pdf_b, merged
    gc.collect()

    # =========================================================================
    # BENCHMARK 2: POLARS (Join)
    # =========================================================================
    if pl:
        with Profiler("Polars (Lazy)") as p:
            lf_a = pl.scan_parquet(path_a)
            lf_b = pl.scan_parquet(path_b)
            res_polars = (
                lf_a.join(lf_b, on="id", suffix="_b")
                .group_by("category")
                .agg([(pl.col("val_b") - pl.col("val")).mean().alias("avg_delta")])
                .collect()
            )
    else:
        print("[Polars         ] Skipped")

    # =========================================================================
    # BENCHMARK 3: CONSIST (Cold / Zero-Copy)
    # =========================================================================
    with Profiler("Consist (Cold)") as p:
        # Create View over raw Parquet files
        tracker.create_view("v_bench_cold", "results")

        query = text(
            """
            SELECT a.category, AVG(b.val - a.val) as avg_delta
            FROM v_bench_cold a
            JOIN v_bench_cold b ON a.id = b.id
            WHERE a.consist_run_id = 'run_a' AND b.consist_run_id = 'run_b'
            GROUP BY 1 ORDER BY 1
        """
        )
        with tracker.engine.connect() as conn:
            res_consist_cold = conn.execute(query).fetchall()

    # =========================================================================
    # BENCHMARK 4: CONSIST (Hot / Native)
    # =========================================================================
    # 4a. Measure Ingestion Cost (Setup)
    print("\n[Ingestion Phase] moving data to DuckDB...")
    with Profiler("Ingestion Cost") as p:
        # We pass the paths directly to avoid loading into Python RAM
        # Tracker sees the path string and streams it via Arrow
        tracker.ingest(artifact=art_a, data=str(path_a), run=Run(id="run_a", model="x"))
        tracker.ingest(artifact=art_b, data=str(path_b), run=Run(id="run_b", model="x"))

    # 4b. Measure Query Cost
    with Profiler("Consist (Hot)") as p:
        # Create View over Native Table
        # (Since artifacts are now marked 'is_ingested', this view points to global_tables.results)
        tracker.create_view("v_bench_hot", "results")

        query_hot = text(
            """
            SELECT a.category, AVG(b.val - a.val) as avg_delta
            FROM v_bench_hot a
            JOIN v_bench_hot b ON a.id = b.id
            WHERE a.consist_run_id = 'run_a' AND b.consist_run_id = 'run_b'
            GROUP BY 1 ORDER BY 1
        """
        )
        with tracker.engine.connect() as conn:
            res_consist_hot = conn.execute(query_hot).fetchall()

    # =========================================================================
    # VERIFICATION
    # =========================================================================
    print("\n[Verification (Avg Delta for Cat X)]")
    val_pd = res_pandas.loc["X"]
    val_cold = next(r[1] for r in res_consist_cold if r[0] == "X")
    val_hot = next(r[1] for r in res_consist_hot if r[0] == "X")

    print(f"Pandas:  {val_pd:.4f}")
    print(f"Cold:    {val_cold:.4f}")
    print(f"Hot:     {val_hot:.4f}")

    assert abs(val_pd - val_cold) < 0.01
    assert abs(val_pd - val_hot) < 0.01
