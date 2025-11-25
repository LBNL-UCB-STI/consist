# Consist

**Consist** is the new provenance, caching, and data virtualization layer for PILATES.

It replaces the legacy `DuckDBManager` and `records.py` logic with a robust, standalone system designed for:
1.  **Reproducibility:** Merkle-DAG based run identity (Inputs + Config + Code).
2.  **Observability:** Dual-write logging (`consist.json` for humans, `provenance.duckdb` for analytics).
3.  **Flexibility:** "Hybrid Views" that allow querying data transparently whether it lives in raw Parquet files or a managed Database.

## Core Architecture

**Philosophy:** *"JSON for Truth, SQL for Power."*

### 1. The Tracker (`consist.core.tracker`)
The central orchestrator. It manages the lifecycle of a **Run** and logs **Artifacts** (files/data).
- **Dual-Write:** Synchronously writes to a human-readable JSON log and an analytic DuckDB index.
- **Path Virtualization:** Converts absolute paths (e.g., `/mnt/data/file.csv`) into portable URIs (e.g., `inputs://file.csv`) to enable run portability across machines.

### 2. The Materializer (`consist.integrations.dlt_loader`)
A bridge using `dlt` (Data Load Tool) to ingest data into the global database.
- **Strict Mode:** Validates data against a Pydantic/SQLModel schema.
- **Quick Mode:** Auto-infers schema from data.
- **Consist Protocol:** Automatically injects system columns (`consist_run_id`, `consist_year`, etc.) for traceability.

### 3. The Virtualizer (`consist.core.views`)
A "View Factory" that abstracts storage details.
- **Hybrid Views:** Generates SQL `UNION ALL BY NAME` views that combine "Cold" data (raw files) and "Hot" data (materialized rows) into a single queryable table.
- **Schema Drift:** Gracefully handles missing or extra columns between runs.

## Run Identity & Caching

Consist automatically generates a cryptographic signature for every run to enable safe caching and "forking" of workflows.

**Identity Formula:** `H_run = SHA256( H_code + H_config + H_inputs )`

### Pseudocode & Configuration

The hashing happens automatically when you use the `Tracker`.

```python
from consist.core.tracker import Tracker

# Option A: Standard Mode (Secure)
# - Input files are hashed by reading their full content (SHA256).
# - Good for small data or verifying absolute integrity.
tracker = Tracker(run_dir="./runs")

# Option B: Fast Mode (Performance)
# - Input files are hashed by metadata (File Size + Modification Time).
# - Essential for workflows with 10GB+ input files.
tracker = Tracker(run_dir="./runs", hashing_strategy="fast")

# --- usage ---

config = {
    "scenario": "high_growth",
    "random_seed": 12345,
    "ui_settings": {"color": "blue"} # 'Noise' keys can be excluded
}

# The Tracker automatically:
# 1. Canonicalizes the config (sorts keys, handles types).
# 2. Captures the current Git Commit SHA (H_code).
# 3. Computes H_config.
with tracker.start_run("run_1", config=config):
    
    # 4. As you log inputs, Tracker computes H_inputs (Content or Metadata)
    tracker.log_artifact("large_data.csv", direction="input")

# RESULT:
# Check 'consist.json'. You will see unique signatures that
# prove exactly what code and data produced this result.
```

## Setup

This project is managed with `uv`.

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest
```

## Usage Example

```python
import consist
from consist.core.tracker import Tracker
from pathlib import Path

# Initialize
tracker = Tracker(
    run_dir=Path("./runs/experiment_1"), 
    db_path="./provenance.duckdb"
)

# --- Step 1: Generation ---
with tracker.start_run("run_id_101", model="asim", year=2010):
    
    # 1. Log an input using the global API
    # (No need to pass 'tracker' into your functions!)
    consist.log_artifact("/path/to/input.csv", key="households", direction="input")
    
    # 2. Run simulation logic...
    # (Assume we write a file called 'outputs.csv' here)
    
    # 3. Log the output
    # This returns an Artifact object with a portable URI and runtime path
    persons_artifact = consist.log_artifact("outputs.csv", key="persons", direction="output")
    
    # 4. (Optional) Ingest into DuckDB for analysis
    consist.ingest(
        artifact=persons_artifact,
        data=[{"id": 1, "income": 50000}]
    )

# --- Step 2: Consumption (Chaining Runs) ---
# We can pass artifacts from previous runs directly into new ones!
with tracker.start_run("run_id_102", model="post_processor"):
    
    # The artifact object knows where it lives. 
    # Consist resolves it automatically for the current context.
    consist.log_artifact(persons_artifact, direction="input")
    
    # Create a unified view to query it
    tracker.create_view("v_persons", "persons")
```

## Current Status
*Active Development - Phase 1 & 3*

- [x] Core Data Model (SQLModel)
- [x] Tracker Implementation
- [x] dlt Integration
- [x] Hybrid View Generation
- [x] Canonical Config Hashing
- [x] Run Identity (Git + Config + Inputs)
- [x] Artifact Object Chaining (Pipeline API)
- [x] Global Context (`import consist`)
- [ ] Run Forking/Caching logic
- [ ] SQL Transformers