# Consist

**Consist** is the new provenance, caching, and data virtualization layer for PILATES.

## Core Architecture

1.  **The Tracker:** Manages Run lifecycle and Identity (Config + Code + Inputs).
2.  **The Materializer:** Bridges raw files into DuckDB with automatic Schema Evolution.
3.  **The Virtualizer:** Creates "Hybrid Views" querying both database tables and raw files transparently.

## Usage Example

```python
import consist
import pandas as pd
from consist.core.tracker import Tracker
from pathlib import Path

# 1. Initialize (Once per script)
tracker = Tracker(
    run_dir=Path("./runs"), 
    db_path="./provenance.duckdb",
    hashing_strategy="fast" # Use metadata for speed
)

# 2. Run Context
config = {"scenario": "high_growth", "year": 2030}
with tracker.start_run("run_101", model="asim", config=config):
    
    # A. Log Inputs (Global API)
    consist.log_artifact("inputs/households.csv", key="households", direction="input")
    
    # B. Run Logic
    # ... (generate outputs) ...
    df_out = pd.DataFrame({"id": [1], "val": [100]})
    df_out.to_parquet("runs/output.parquet")
    
    # C. Log Outputs
    # Returns an Artifact object
    art = consist.log_artifact("runs/output.parquet", key="results", direction="output")
    
    # D. Ingest (Optional - For High Performance Querying)
    # Pass the DataFrame directly for vectorized speed (5M+ rows/sec)
    consist.ingest(artifact=art, data=df_out)

# 3. Analysis (Hybrid Views)
# Query the data using SQL, regardless of whether it was ingested or just logged.
tracker.create_view("v_results", "results")

with tracker.engine.connect() as conn:
    print(conn.execute("SELECT avg(val) FROM v_results").fetchone())
```

## Data Strategy: Hot vs. Cold

Consist supports two modes of operation for every artifact, which can be mixed freely.

| Feature | **Cold Data** (Log Only) | **Hot Data** (Log + Ingest) |
| :--- | :--- | :--- |
| **Storage** | Raw Files (Parquet/CSV) | DuckDB File |
| **Ingest Time** | Instant (Zero Copy) | Fast (Vectorized Copy) |
| **Query Speed** | Good (Vectorized Read) | Best (Native Table) |
| **Use Case** | Archival, Huge Files (>10GB) | Dashboards, Heavy Joins |

*   **Hybrid Views** automatically Union both types. If you ingest a file later (Post-Hoc), the view automatically switches to using the Hot copy.


## Benchmarks: The "Zero-Copy" Advantage

Why use Consist instead of just loading files with Pandas?

Because Consist's **Hybrid Views** leverage DuckDB's vectorized reader to query files directly from disk without loading them entirely into RAM. This is particularly powerful for comparative analysis across many simulation runs.

We benchmarked Consist against standard data tools on two common research tasks:
1.  **Aggregate:** GroupBy on 10M rows across 2 files.
2.  **Compare:** Hash Join on 5M rows (Policy vs. Baseline) with Schema Drift.

| Task            | Implementation     | Time      | Peak RAM    | Notes                           |
|:----------------|:-------------------|:----------|:------------|:--------------------------------|
| **Aggregation** | Pandas             | 0.46s     | ~780 MB     | Eagerly loads all columns       |
| (10M Rows)      | Polars (Lazy)      | 0.24s     | ~280 MB     |                                 |
|                 | **Consist (Cold)** | **0.04s** | **~80 MB**  | **Only reads required columns** |
|                 |                    |           |             |                                 |
| **Join**        | Pandas             | 0.76s     | ~620 MB     | Memory spike due to merge       |
| (5M Rows)       | Polars (Lazy)      | 0.20s     | ~440 MB     |                                 |
|                 | **Consist (Cold)** | **0.12s** | **~272 MB** | Zero Setup Time                 |
|                 | **Consist (Hot)**  | **0.06s** | **~3 MB**   | **Fastest Query (Native DB)**   |

*Benchmark run on Apple M3 Max. "Consist (Cold)" refers to querying raw Parquet files via Consist Views without pre-ingestion.*

**Key Takeaways:**
1.  **Smart I/O:** Consist/DuckDB pushes queries down to the scan layer. If you don't ask for a column, Consist doesn't read it.
2.  **Hot vs. Cold:** Use **Cold** views for ad-hoc analysis (zero setup). Use **Hot** (Ingested) tables for dashboards or repeated heavy joins to get sub-0.1s performance (at the cost of a one-time ingestion step).
3.  **Automatic Schema Drift:** The benchmarks included files with mismatched columns. Consist handled this automatically (`UNION BY NAME`), whereas Pandas required manual alignment logic.