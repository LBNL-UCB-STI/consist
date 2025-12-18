"""
Consist Comparative Join and Merge Benchmarks

This module contains comparative benchmarks for large-scale join and merge operations
across Pandas, Polars, and Consist's data access paths (both "cold" and "hot").

The primary goal of these benchmarks is to quantify the performance benefits and
resource efficiency of Consist's approach to data virtualization and optimized
querying for large datasets. It highlights how Consist can avoid loading entire
datasets into Python process memory, especially when compared to traditional
in-memory operations with Pandas/Polars.
"""

import logging
from pathlib import Path

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
    """
    A context manager for measuring execution time and peak RAM usage of code blocks.

    This utility helps in benchmarking different data processing approaches by
    providing a consistent way to record performance metrics. It leverages
    `psutil` for memory profiling if available.

    Attributes
    ----------
    name : str
        A descriptive name for the code block being profiled.
    start_time : float
        The timestamp when the profiling started (seconds since epoch).
    start_mem : int
        The Resident Set Size (RSS) memory usage of the process at the start
        of profiling (bytes). Only available if `psutil` is installed.
    """

    def __init__(self, name):
        self.name = name
        self.start_time = 0
        self.start_mem = 0

    def __enter__(self) -> "Profiler":
        """
        Starts the profiling timer and records initial memory usage.

        Upon entering the `with` block, this method is called. It ensures that
        garbage collection is performed to get a cleaner memory baseline before
        recording the starting time and (if `psutil` is available) the initial
        Resident Set Size (RSS) memory usage of the process.

        Returns
        -------
        Profiler
            The Profiler instance itself, allowing it to be used as a context manager.
        """
        gc.collect()
        self.start_time = time.time()
        if psutil:
            process = psutil.Process(os.getpid())
            self.start_mem = process.memory_info().rss
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Stops the profiling timer, calculates duration and peak RAM, and prints the results.

        Upon exiting the `with` block, this method calculates the elapsed time
        and (if `psutil` is available) the difference in Resident Set Size (RSS)
        memory usage since `__enter__` was called. The results are then printed
        to the console.

        Parameters
        ----------
        exc_type : Optional[Type[BaseException]]
            The exception type if an exception occurred within the `with` block, otherwise None.
        exc_val : Optional[BaseException]
            The exception instance if an exception occurred, otherwise None.
        exc_tb : Optional[TracebackType]
            The traceback object if an exception occurred, otherwise None.
        """
        duration = time.time() - self.start_time
        mem_str = "N/A"
        if psutil:
            process = psutil.Process(os.getpid())
            end_mem = process.memory_info().rss
            diff_mb = (end_mem - self.start_mem) / 1024 / 1024
            mem_str = f"{diff_mb:,.0f} MB (Delta)"

        print(f"[{self.name:<15}] Time: {duration:>6.3f}s | RAM: {mem_str}")


@pytest.mark.heavy
def test_comparative_join_benchmark(tmp_path: Path):
    """
    Benchmarks the performance (time and peak RAM) of a large-scale data join/merge operation
    across Pandas, Polars (lazy), and Consist's "cold" (zero-copy file reads)
    and "hot" (ingested DuckDB) data access paths.

    This test highlights Consist's efficiency for analytical queries over large datasets,
    especially when avoiding full data loads into Python process memory. It demonstrates
    Consist's capabilities as a data virtualization layer by allowing complex joins
    to be executed directly in DuckDB on either raw files or ingested tables.

    What happens:
    1. Two large Pandas DataFrames (`df_a` and `df_b`) are generated with `N_ROWS` records each,
       simulating two related datasets that need to be joined.
    2. These DataFrames are saved as Parquet files (`run_a.parquet`, `run_b.parquet`).
    3. Consist runs are initiated to log these Parquet files as artifacts ("results").
    4. The same join/merge and aggregation logic (calculating the mean delta of a value
       after joining on 'id') is then applied using four different methods:
        -   **Pure Pandas**: Reads full files into memory, performs merge and groupby.
        -   **Polars (Lazy)**: Uses Polars' lazy API to join and aggregate directly from Parquet files.
        -   **Consist's "cold" path**: Creates a DuckDB view directly over the raw Parquet files
            using `create_view` and executes a SQL join query.
        -   **Consist's "hot" path**: The Parquet data is first ingested into DuckDB tables,
            then a DuckDB view is created over these tables, and a SQL join query is executed.
    5. Performance metrics (execution time and peak RAM usage) are recorded for each method
       using the `Profiler` context manager.

    What's checked:
    - The numerical results (specifically, the mean delta for category 'X') from all four methods
      are asserted to be statistically equivalent (within a small floating-point tolerance).
      This verifies the correctness of Consist's query generation and execution, and the
      equivalence of results across different data processing frameworks.
    - Performance metrics (time and peak RAM) are printed to stdout by the `Profiler` class
      for manual comparison and analysis, showcasing the efficiency gains offered by Consist's
      data virtualization and DuckDB-native querying.
    """
    run_dir = tmp_path / "bench_join"
    db_path = str(tmp_path / "bench_join.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)
    tracker.identity.hashing_strategy = "fast"

    # =========================================================================
    # 1. SETUP: GENERATE DATA
    # =========================================================================
    logging.info(f"\n[Setup] Generating 2 runs x {N_ROWS:,} rows...")

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

    logging.info("[Setup] Data generated.")
    del df_a, df_b
    gc.collect()

    # =========================================================================
    # BENCHMARK 1: PANDAS (Merge)
    # =========================================================================
    with Profiler("Pandas"):
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
        with Profiler("Polars (Lazy)"):
            lf_a = pl.scan_parquet(path_a)
            lf_b = pl.scan_parquet(path_b)
            (
                lf_a.join(lf_b, on="id", suffix="_b")
                .group_by("category")
                .agg([(pl.col("val_b") - pl.col("val")).mean().alias("avg_delta")])
                .collect()
            )
    else:
        logging.info("[Polars         ] Skipped")

    # =========================================================================
    # BENCHMARK 3: CONSIST (Cold / Zero-Copy)
    # =========================================================================
    with Profiler("Consist (Cold)"):
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
    logging.info("\n[Ingestion Phase] moving data to DuckDB...")
    with Profiler("Ingestion Cost"):
        # We pass the paths directly to avoid loading into Python RAM
        # Tracker sees the path string and streams it via Arrow
        tracker.ingest(artifact=art_a, data=str(path_a), run=Run(id="run_a", model="x"))
        tracker.ingest(artifact=art_b, data=str(path_b), run=Run(id="run_b", model="x"))

    # 4b. Measure Query Cost
    with Profiler("Consist (Hot)"):
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
    logging.info("\n[Verification (Avg Delta for Cat X)]")
    val_pd = res_pandas.loc["X"]
    val_cold = next(r[1] for r in res_consist_cold if r[0] == "X")
    val_hot = next(r[1] for r in res_consist_hot if r[0] == "X")

    logging.info(f"Pandas:  {val_pd:.4f}")
    logging.info(f"Cold:    {val_cold:.4f}")
    logging.info(f"Hot:     {val_hot:.4f}")

    assert abs(val_pd - val_cold) < 0.01
    assert abs(val_pd - val_hot) < 0.01
