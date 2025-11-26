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
