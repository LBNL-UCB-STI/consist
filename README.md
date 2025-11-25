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
from consist.tools.tracker import Tracker
from pathlib import Path

# Initialize
tracker = Tracker(
    run_dir=Path("./runs/experiment_1"), 
    db_path="./provenance.duckdb"
)

config = {
    "scenario": "high_growth",
    "random_seed": 12345,
    "ui_settings": {"color": "blue"} # 'Noise' keys can be excluded
}

# The Tracker automatically:
# 1. Canonicalizes the config (sorts keys, handles types).
# 2. Captures the current Git Commit SHA (H_code).
# 3. Computes H_config.

# --- Step 1: Generation ---
with tracker.start_run("run_id_101", model="asim", year=2010, config=config):
    
    # 1. Log an input
    tracker.log_artifact("/path/to/input.csv", key="households", direction="input")
    
    # 2. Run simulation logic...
    # (Assume we write a file called 'outputs.csv' here)
    
    # 3. Log the output
    # This returns an Artifact object with a portable URI (e.g. "./outputs.csv")
    # AND a runtime cache of the absolute path.
    persons_artifact = tracker.log_artifact("outputs.csv", key="persons", direction="output")
    
    # 4. (Optional) Ingest into DuckDB for analysis
    tracker.ingest(
        artifact=persons_artifact,
        data=[{"id": 1, "income": 50000}]
    )

# --- Step 2: Consumption (Chaining Runs) ---
# We can pass artifacts from previous runs directly into new ones!
with tracker.start_run("run_id_102", model="post_processor"):
    
    # No need to manually resolve paths. The artifact object knows where it lives.
    tracker.log_artifact(persons_artifact, direction="input")
    
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
- [ ] Run Forking/Caching logic
- [ ] SQL Transformers