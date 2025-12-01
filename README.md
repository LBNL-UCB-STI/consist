# Consist

**Automatic provenance tracking, intelligent caching, and data virtualization for scientific simulation workflows.**

Consist tracks what you ran, what it produced, and automatically skips re-running unchanged work. It bridges the gap between raw file-based workflows (CSV/Parquet/HDF5) and analytical databases (DuckDB), offering a "Zero-Copy" virtualization layer for high-performance analysis.

---

## The Problem

Research pipelines are often brittle:
- **"Which config produced Figure 3?"** (Provenance Hell)
- **Re-running a 10-step pipeline** because you tweaked one parameter in step 7.
- **Writing one-off scripts** to compare outputs across dozens of simulation runs.
- **Transferring 40GB of CSVs** just to share results with a colleague.

**Consist solves this.**

---

## Key Features

### 🔄 Smart Caching (Merkle DAG)
Change a parameter? Consist detects it.
*   **Hash-Based Identity:** Runs are identified by `SHA256(Code + Config + Inputs)`.
*   **Auto-Forking:** If inputs change, downstream runs automatically fork.
*   **Ghost Mode:** If results exist in the DB, Consist skips execution even if raw files are missing.

### 📊 Hybrid Data Views
Query your simulation outputs with SQL without ETL.
*   **Hot & Cold Data:** Consist creates "Hybrid Views" that UNION raw files (Parquet/CSV) with ingested data (DuckDB) transparently.
*   **Vectorized Reads:** Queries scanning raw files are pushed down to DuckDB's vectorized reader.
*   **Schema Evolution:** Handles changing columns across runs gracefully (`UNION BY NAME`).

### 📦 Container-Native
Treats Docker/Singularity containers as "Pure Functions".
*   Tracks `Image Digest + Command` as configuration.
*   Tracks mounted volumes as Inputs/Outputs.
*   Perfect for reproducible HPC workflows.

---

## Quickstart

### Installation

```bash
git clone https://github.com/your-org/consist.git
cd consist
pip install -e .
```

### Basic Usage

```python
from consist import Tracker
import pandas as pd

# 1. Initialize (Once per project)
tracker = Tracker("./runs", db_path="./provenance.duckdb")

# 2. Run Context
config = {"growth_rate": 0.05, "year": 2030}

with tracker.start_run("run_1", model="demand_model", config=config):
    
    # A. Check Cache (Optional explicit check)
    if tracker.is_cached:
        print("Skipping execution - results hydrated!")
    else:
        # B. Do Work
        df = pd.DataFrame({"id": [1, 2], "val": [10, 20]})
        df.to_parquet("runs/output.parquet")
        
        # C. Log Output
        art = tracker.log_artifact("runs/output.parquet", key="demand_results")
        
        # D. Ingest (Optional: For sub-second query performance)
        tracker.ingest(art, df)
```

### Running Containers

Consist wraps Docker/Singularity execution to provide caching for black-box models.

```python
from consist.integrations.containers import run_container

run_container(
    tracker=tracker,
    run_id="model_step_1",
    image="pilates/asim:v2.1",
    command=["python", "run.py", "-c", "config.yaml"],
    inputs=["./host/inputs/land_use.csv"],   
    outputs=["./host/outputs/results.csv"],
    volumes={
        "/abs/host/inputs": "/data/inputs", 
        "/abs/host/outputs": "/data/outputs"
    },
    backend_type="docker" # or "singularity"
)
# If run again with same inputs/image, Consist skips execution instantly.
```

---

## Data Strategy: Hot vs. Cold

Consist supports two modes of operation for every artifact, which can be mixed freely.

| Feature         | **Cold Data** (Log Only)     | **Hot Data** (Log + Ingest) |
|:----------------|:-----------------------------|:----------------------------|
| **Storage**     | Raw Files (Parquet/CSV/H5)   | DuckDB File                 |
| **Ingest Time** | Instant (Zero Copy)          | Fast (Vectorized Copy)      |
| **Query Speed** | Good (Vectorized Read)       | Best (Native Table)         |
| **Use Case**    | Archival, Huge Files (>10GB) | Dashboards, Heavy Joins     |

**Analysis Example:**
```python
# Create a view that unions ALL runs (Hot and Cold)
tracker.create_view("all_results", "demand_results")

# Query via SQL
with tracker.engine.connect() as conn:
    df = pd.read_sql("""
        SELECT consist_run_id, AVG(val) 
        FROM all_results 
        GROUP BY consist_run_id
    """, conn)
```

### Advanced Formats (HDF5 & Zarr)

Consist supports complex scientific formats.

**HDF5 Tables (Virtual Artifacts):**
HDF5 files often contain multiple datasets. You can log "Virtual Artifacts" pointing to specific tables.

```python
# Log the physical file
tracker.log_artifact("pipeline.h5", key="store", driver="h5")

# Log a virtual table inside it
tracker.log_artifact(
    "pipeline.h5", 
    key="persons", 
    driver="h5_table", 
    meta={"table_path": "/2018/persons"}
)
```

**Zarr Matrices:**
For N-Dimensional arrays, Consist ingests the **structure** (metadata) but keeps pixel data on disk.
```python
from consist.core.matrix import MatrixViewFactory

# Load a virtual xarray dataset spanning multiple runs
ds = MatrixViewFactory(tracker).load_matrix_view("congestion_matrix")
# Slice across runs efficiently
diff = ds.sel(year=2030) - ds.sel(year=2020)
```

---

## Benchmarks

Why use Consist instead of just loading files with Pandas? 
**Smart I/O.** Consist pushes queries down to the scan layer. If you don't ask for a column, Consist doesn't read it.

| Task (5M Rows) | Implementation | Time | Peak RAM |
|:---|:---|:---|:---|
| **Aggregation** | Pandas | 0.46s | ~780 MB |
| | **Consist (Cold)** | **0.04s** | **~80 MB** |
| **Join** | Pandas | 0.76s | ~620 MB |
| | **Consist (Hot)** | **0.06s** | **~3 MB** |

---

## Architecture

1.  **The Tracker:** Manages Run lifecycle, Identity hashing, and Locking.
2.  **The Materializer (`dlt` Bridge):** Bridges raw files into DuckDB with automatic Schema Evolution.
3.  **The Virtualizer (View Factory):** Creates "Hybrid Views" querying both database tables and raw files.
4.  **Identity Manager:** Computes Merkle Hashes of Code (Git SHA), Config (Canonical JSON), and Inputs (Provenance Graph).

## Contributing

Consist is currently in active development for the PILATES project.

**Requirements:**
*   Python 3.13+
*   DuckDB
*   Docker (Optional, for container support)