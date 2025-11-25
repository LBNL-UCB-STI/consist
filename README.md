# Consist

**Consist** is the new provenance, caching, and data virtualization layer for PILATES.

It replaces the legacy `DuckDBManager` and `records.py` logic with a robust, standalone system designed for:
1.  **Reproducibility:** Merkle-DAG based run identity (Inputs + Config + Code).
2.  **Observability:** Dual-write logging (`consist.json` for humans, `provenance.duckdb` for analytics).
3.  **Flexibility:** "Hybrid Views" that allow querying data transparently whether it lives in raw Parquet files or a managed Database.

## Core Architecture

**Philosophy:** *"JSON for Truth, SQL for Power."*

### 1. The Tracker (`consist.tools.tracker`)
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
    run_dir=Path("./runs"), 
    db_path="./provenance.duckdb"
)

# Start a tracked execution context
with tracker.start_run("run_id_123", model="asim", year=2010):
    
    # Log an input file
    tracker.log_artifact("/path/to/input.csv", key="households", direction="input")
    
    # ... Run simulation logic ...
    
    # Ingest output data to DB
    tracker.ingest(
        artifact=tracker.log_artifact("outputs.csv", key="persons"),
        data=[{"id": 1, "income": 50000}]
    )

# Create a unified view (reads from files OR db)
tracker.create_view("v_persons", "persons")
```

## Current Status
*Active Development - Phase 1 & 3*

- [x] Core Data Model (SQLModel)
- [x] Tracker Implementation
- [x] dlt Integration
- [x] Hybrid View Generation
- [ ] Canonical Config Hashing (In Progress)
- [ ] Run Forking/Caching logic
- [ ] SQL Transformers