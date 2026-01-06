# Glossary

This page defines key terms used in Consist documentation.

---

## Artifact

A file produced or consumed by a run, with provenance metadata attached. Artifacts record:
- The file path and format (CSV, Parquet, HDF5, etc.)
- Which run created or used it (for inputs)
- A content hash (SHA256) for integrity checking
- Optional ingestion status (whether it was stored in DuckDB)

**Example**: A Parquet file `results.parquet` is an Artifact if it was produced by a run and tracked by Consist.

**See also**: Run, Provenance, Ingestion

---

## Cache Hit

When Consist skips execution because it finds a previous run with an identical signature. The cached results (artifact metadata and optionally file copies) are returned instantly.

**Example**: You run a function with config `{"threshold": 0.5}` on input `data.csv`, then re-run with the exact same config and input. Second execution is a cache hit—no computation happens.

**Opposite**: Cache miss (run must execute)

**See also**: Signature, Cache Miss

---

## Cache Miss

When Consist cannot find a matching cached result, so the function must execute. The outputs are recorded as a new run and new artifacts are created.

**See also**: Cache Hit, Signature

---

## Config

Dictionary of parameters that affect computation, and are included in the cache signature. Example:

```python
tracker.run(
    fn=my_model,
    config={"year": 2030, "scenario": "baseline"},  # Part of signature
    inputs={...},
    outputs=[...]
)
```

Changing config invalidates cache and triggers re-execution. Config is hashed (not stored as-is) to allow large nested dictionaries.

**See also**: Facet, Signature

---

## Coupler

A Consist pattern for orchestrating multi-step workflows. A coupler passes artifacts from one step's outputs to the next step's inputs, linking them through scenario trees. Useful for complex loops and iterative computations.

**Example**: In a transportation model with feedback loops (trip distribution → mode choice → assignment → congestion update), couplers link each step's outputs to the next step's inputs.

**See also**: Scenario, Run, Trace

---

## DLT (Data Load Tool)

An optional Python library for loading data into data warehouses. Consist integrates with DLT to materialize artifacts into DuckDB with provenance columns (run_id, artifact_id, etc.). Using DLT is optional; you can ingest manually or load files directly.

**See also**: Ingestion, Materialization

---

## Facet

A small, queryable piece of metadata extracted from config. Unlike config (which is hashed and not stored), facets are indexed in DuckDB for filtering.

**Use case**: Your config is a 10 MB YAML file (too large to store). You extract `{"year": 2030, "parking_cost": 5.0}` as facets and query runs by year/parking_cost.

**Example**:
```python
tracker.run(
    fn=my_model,
    config={"huge_model_config": ...},  # Not queryable directly (too large)
    facet={"year": 2030, "scenario": "baseline"},  # Indexed and queryable
    inputs={...},
    outputs=[...]
)

# Later: query all 2030 runs
df = tracker.find_runs(facet_year=2030)
```

**See also**: Config, Signature

---

## Ghost Mode

Consist's ability to recover artifacts that exist only in the provenance database (DuckDB), not as physical files. If you delete the original file but it was ingested, you can still load it via `consist.load(artifact)`.

**Use case**: Re-running a pipeline against archived input data. If inputs were previously ingested, Consist recovers them from the database instead of requiring the original files.

**See also**: Ingestion, Materialization

---

## Hydration

The process of reconstructing artifact metadata (what run created it, with what config) without copying file bytes. Cache hits hydrate artifacts into the current run context.

**Hydration ≠ copying files.** A hydrated artifact has provenance metadata but may not have file bytes copied to the new run's directory.

**See also**: Materialization, Cache Hit

---

## Ingestion

Loading artifact data into DuckDB for SQL-native analysis. Optional; you can use Consist without ingesting (just track files).

**Use case**: You want to query 50 Parquet files across 50 runs in SQL without loading them all into memory.

**Process**:
1. Artifact is created/logged by a run
2. `tracker.ingest(artifact, data=df)` stores the data in DuckDB
3. Later: query in SQL across all ingested data

**See also**: Materialization, Ghost Mode, Hybrid View

---

## Lineage

The complete dependency chain showing where a result came from. Lineage tracks: which run created an artifact, which inputs that run used, which runs created those inputs, etc.

**Example**: `consist lineage traffic_volumes` shows:
```
traffic_volumes (artifact)
├── created by: traffic_simulation run
│   ├── input: assigned_trips
│   │   └── created by: assignment run
│   │       └── input: trip_tables
│   │           └── created by: mode_choice run
│   │               └── ...
```

**See also**: Provenance, Run, Artifact

---

## Materialization

Storing artifact data (actual bytes) in DuckDB, making it recoverable even if the original file is deleted. More complete than hydration.

**Materialization = copying bytes into the database**
**Hydration = recovering metadata without bytes**

**See also**: Hydration, Ingestion, Ghost Mode

---

## Provenance

Complete history of where a result came from: code version, configuration, input data, and compute environment. Consist records provenance automatically for every run.

**Why it matters**: Reproducibility ("Can I re-run this exactly?"), Accountability ("Which config made this figure?"), Debugging ("Why did this change?")

**See also**: Artifact, Lineage, Signature

---

## Run

A single execution of a tracked function or workflow step. A run records:
- Input artifacts and configuration
- Execution status (completed, failed, cached)
- Output artifacts
- Timing (start/end times)
- Tags and metadata

**Example**:
```python
result = tracker.run(
    fn=clean_data,
    inputs={"raw_path": "raw.csv"},
    config={"threshold": 0.5},
    outputs=["cleaned"],
)
```

This creates a Run with one input artifact, one config dict, and one output artifact.

**See also**: Artifact, Scenario

---

## Scenario

A grouping of related runs. Scenarios are useful for organizing multi-variant studies or iterative workflows.

**Example**: "baseline_2030" scenario contains 5 related runs:
- Year 2030, baseline policy, iteration 0
- Year 2030, baseline policy, iteration 1
- Year 2030, baseline policy, iteration 2
- ...

**Important**: Consist uses "scenario" differently from policy modeling jargon. In Consist, a scenario is a parent run grouping; in transportation modeling, "baseline scenario" and "growth scenario" are policy variants. Don't confuse the two.

**See also**: Run, Coupler, Trace

---

## Signature

A fingerprint (SHA256 hash) of code version + config + input artifact hashes. Identical signatures = identical outputs (assuming deterministic functions). Used as the cache key.

**How it works**:
1. Function code is hashed (git commit SHA + modified files)
2. Config dict is hashed
3. Input file hashes are computed
4. All three are combined: `signature = SHA256(code + config + inputs)`
5. This signature is the cache key

**Why**: If you re-run with the same signature, Consist knows the result will be the same, so it returns the cached version instantly.

**See also**: Cache Hit, Config, Artifact

---

## Trace

The execution path through a multi-step workflow, showing which runs were executed, which were cache hits, and what artifacts were passed between them.

**See also**: Scenario, Coupler, Lineage

---

## Virtualization (Data Virtualization)

Querying multiple artifacts as if they were a single table, without loading all data into memory. DuckDB handles data movement lazily.

**Example**: Query 50 Parquet files across 50 runs:
```sql
SELECT year, mode, COUNT(*) as trips
FROM consist_view_trips
WHERE scenario IN ('baseline', 'high_growth')
GROUP BY year, mode
```

Consist creates a virtual SQL view that queries each file as needed, not loading all at once.

**See also**: Hybrid View, Ingestion

---

## Hybrid View

A SQL view that combines:
1. **Hot data**: Ingested artifacts stored in DuckDB
2. **Cold data**: Raw files (Parquet, CSV) queried on-the-fly

Hybrid views let you query across runs without requiring all data to be ingested, reducing storage overhead.

**See also**: Ingestion, Virtualization

