# Consist

**Automatic provenance tracking, intelligent caching, and data virtualization for scientific simulation workflows.**

Consist tracks what you ran, what it produced, and automatically skips re-running unchanged work. It bridges the gap between raw file-based workflows (CSV/Parquet/HDF5) and analytical databases (DuckDB), offering a "Zero-Copy" virtualization layer for high-performance analysis.

---

### 🚂 Etymology: What is a "Consist"?
In railroad terminology, a **consist** (*noun*, pronounced **CON-sist**) refers to the specific lineup of locomotives and cars that make up a train.

In this library, a **Consist** is the immutable record of exactly what "vehicles" (Code Version, Configuration, Input Artifacts) were coupled together to execute a specific run.

---

## The Problem

Research pipelines are often brittle:
- **"Which config produced Figure 3?"** (Provenance Hell)
- **Re-running a 10-step pipeline** because you tweaked one parameter in step 7.
- **Writing one-off scripts** to compare outputs across dozens of simulation runs.
- **Transferring 40GB of CSVs** just to share results with a colleague.

**Consist solves this.**

---

## Quickstart

### Installation

```bash
git clone https://github.com/your-org/consist.git
cd consist
pip install -e .
```

### 1. Functional Pipelines (The Happy Path)

The easiest way to use Consist is via the `@task` decorator. It handles caching, hashing, and linkage automatically. Consist enforces **Strict Contracts** on return types to prevent ambiguity.

```python
from consist import Tracker
from pydantic import BaseModel
from pathlib import Path

# Initialize
tracker = Tracker("./runs", db_path="./provenance.duckdb")

class CleanConfig(BaseModel):
    threshold: float = 0.5

# --- Pattern 1: The Pipe (Return a single Path) ---
@tracker.task(cache_mode="reuse")
def clean_data(raw_file: Path, config: CleanConfig):
    print("Running cleaning step...")
    out = Path("./cleaned_data.parquet")
    # ... process and write ...
    return out  # Decorator logs this and returns an Artifact

# --- Pattern 2: The Splitter (Return a Dict of Paths) ---
@tracker.task()
def split_data(clean_artifact):
    # ... process ...
    return {
        "train": Path("./train.csv"),
        "test": Path("./test.csv")
    }

# Execution
# If run twice, the second execution returns instantly via Cache Hit.
conf = CleanConfig(threshold=0.8)

clean_art = clean_data("raw.csv", conf)
datasets = split_data(clean_art) # datasets is Dict[str, Artifact]
```

### 2. Wrapping Legacy Code ("Black Box" Models)

For models that read config files from disk or dump side-effects into folders (like ActivitySim or loose scripts), use `depends_on` and `capture_dir`.

```python
import consist
import legacy_lib # A library that implicitly reads 'sim_config.yaml'

@tracker.task(
    # Hashes these files into the run identity, even though the function doesn't take them as args
    depends_on=["sim_config.yaml"], 
    
    # Watches this folder and auto-logs any NEW or MODIFIED files
    capture_dir="./model_outputs",
    capture_pattern="*.csv"
)
def run_simulation(upstream_data):
    # 'upstream_data' is passed just to link the graph lineage.
    legacy_lib.run()
    # MUST return None when using capture_dir.
    
# Returns a List[Artifact] of whatever the legacy code produced
results = run_simulation(clean_art)
```

### 3. Observability & Introspection

Consist provides tools to debug runs while you work in a notebook or IDE.

```python
# A. Runtime Metrics
# Inject metrics deep inside your code without passing objects around
def train():
    acc = 0.98
    consist.log_meta(accuracy=acc, tags=["production"])

# B. Immediate History
# Check what just happened
print(tracker.last_run) 
# <ConsistRecord run_id='task_a_123' outputs=2>

if tracker.last_run.cached_run:
    print("🚀 It was a cache hit!")

# C. Database History
# Get a DataFrame of all runs
df = tracker.history(limit=5)
print(df[["model_name", "status", "created_at"]])
```

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

### 📦 Container-Native
Treats Docker/Singularity containers as "Pure Functions".
*   Tracks `Image Digest + Command` as configuration.
*   Tracks mounted volumes as Inputs/Outputs.

---

## Contributing

Consist is currently in active development for the PILATES project.

**Requirements:**
*   Python 3.10+
*   DuckDB
*   Docker (Optional, for container support)