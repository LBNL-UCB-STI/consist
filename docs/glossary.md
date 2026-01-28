# Glossary

This page defines key terms used in Consist documentation.

---

## Artifact

A file (CSV, Parquet, HDF5, etc.) produced or used by a run, with metadata attached to track its origin and integrity. Think of it as a "named, tracked file" that Consist remembers who created it and can verify hasn't been corrupted.

Artifacts record:
- The file path and format (CSV, Parquet, HDF5, etc.)
- Which run created or used it (for inputs)
- A content hash (SHA256) for integrity checking
- Optional ingestion status (whether it was stored in DuckDB)

**Research example**: When you publish results (a transportation demand forecast, climate projections, or zoning capacity map), each output file is an Artifact. You can ask "who created this file?" (`consist lineage traffic_volumes`), verify it hasn't been corrupted, and trace it back to the exact code version and config that produced it.

**See also**: Run, Provenance, Ingestion

---

## Cache Hit

When Consist skips execution because it finds a previous run with an identical signature. The cached results (artifact metadata and optionally file copies) are returned instantly.

**Example**: You run a function with config `{"threshold": 0.5}` on input `data.csv`, then re-run with the exact same config and input. Second execution is a cache hit—no computation happens.

**Research example**: In a parameter sweep (testing 20 demand elasticity values), the first run re-executes your demand model. Runs 2-20 are all cache hits for preprocessing (same input data, same code) but misses for the demand model (different elasticity). Consist skips 19 preprocessing steps, saving hours.

**Opposite**: Cache miss (run must execute)

**See also**: Signature, Cache Miss

---

## Cache Miss

When Consist cannot find a matching cached result, so the function must execute. The outputs are recorded as a new run and new artifacts are created.

**See also**: Cache Hit, Signature

---

## Canonical Hashing

Converting configuration data (dicts, YAML, etc.) into a single, consistent fingerprint, regardless of field order or how numbers are formatted. This ensures `{"a": 1, "b": 2}` and `{"b": 2, "a": 1}` produce the same hash, so Consist treats them as identical configurations.

**Why it matters**: Without canonical hashing, the same config in different orders would produce different cache keys, breaking reproducibility.

**See also**: Signature, Config

---

## Config

Dictionary of parameters that affect computation, and are included in the cache signature. Example:

```python
import consist
from consist import use_tracker

with use_tracker(tracker):
    consist.run(
        fn=my_model,
        config={"year": 2030, "scenario": "baseline"},  # Part of signature
        inputs={...},
        outputs=[...]
    )
```

Changing config invalidates cache and triggers re-execution. Config is hashed (not stored as-is) to allow large nested dictionaries.

**Research example**: Your config might be a 50MB ActivitySim parameter file. Consist hashes it into the cache key, so if a colleague changes a mode choice coefficient, Consist automatically knows to re-run your demand model (cache miss). Without this, you'd have to manually remember which config changes require re-running which steps.

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

A small, queryable subset of configuration (e.g., `{"year": 2030, "scenario": "baseline"}`) that's indexed in DuckDB so you can filter runs. Use facets when you want to ask "show me all runs where year=2030" without storing the entire 50MB config file.

Unlike identity config (which is hashed into the cache key), facets are stored queryably in the database for filtering and analysis.

**Use case**: Your config is a 10 MB YAML file (too large to store). You extract `{"year": 2030, "parking_cost": 5.0}` as facets and query runs by year/parking_cost.

**Example**:
```python
import consist
from consist import use_tracker

with use_tracker(tracker):
    consist.run(
        fn=my_model,
        config={"huge_model_config": ...},  # Not queryable directly (too large)
        facet={"year": 2030, "scenario": "baseline"},  # Indexed and queryable
        inputs={...},
        outputs=[...]
    )

# Later: query all 2030 runs
df = tracker.find_runs(facet_year=2030)
```

**Research example**: You're running demand models for 10 years (2020-2050) × 3 scenarios (baseline, transit-friendly, congestion pricing). You set `facet={"year": 2030, "scenario": "transit-friendly"}` for each run. Later, your colleague asks "show me all 2040 sensitivity tests." You query `facet_year=2040` and get 50 runs instantly—no manual searching through 500 run directories.

**See also**: Config, Signature

---

## Ghost Mode

Consist's ability to recover artifacts that exist only in the provenance database (DuckDB), not as physical files. If you delete the original file but it was ingested, you can still load it via `consist.load(artifact)` (returns a DuckDB Relation) or `consist.load_df(artifact)`.

**Use case**: Re-running a pipeline against archived input data. If inputs were previously ingested, Consist recovers them from the database instead of requiring the original files.

**See also**: Ingestion, Materialization

---

## Hydration

Recovering the metadata and location information about a previous run's output without copying the file bytes. On a cache hit, Consist "hydrates" the output artifact so you know where it came from and can access it, but doesn't necessarily copy it to your current run directory.

**Hydration ≠ copying files.** A hydrated artifact has provenance metadata but may not have file bytes copied to the new run's directory. By default, Consist recovers the information but doesn't copy files (saves disk space). You opt in to copying files when needed.

**See also**: Materialization, Cache Hit

---

## Identity Config

The full set of configuration parameters that affect a run's cache signature; if identity config changes, the run must re-execute. Unlike facets (which are just for querying), identity config is hashed into the cache key to ensure cache invalidation when parameters change.

**Example**: If your `config` dict contains `{"year": 2030, "mode_choice_coefficient": 0.5}`, both values are hashed into the signature. Changing either value invalidates cache.

**See also**: Config, Facet, Signature

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

## Merkle DAG

A chain of computations where each step's inputs are linked to the outputs of previous steps, creating an unbreakable record of data lineage. Like a railroad consist (the specific order of locomotives and cars), each simulation year depends on the previous year's output.

**Why it matters**: This structure enables Consist to detect when cached results are valid (all upstream inputs haven't changed) and to recover missing data if needed.

**See also**: Signature, Lineage, Artifact

---

## Materialization

Saving the actual bytes of a data file into DuckDB (the provenance database) so it's recoverable even if the original file gets deleted. If you ingest a 10GB result file, DuckDB stores a copy so you can retrieve it later without the original file.

**Materialization = copying bytes into the database**
**Hydration = recovering metadata without bytes**

**See also**: Hydration, Ingestion, Ghost Mode

---

## Provenance

Complete history of where a result came from: code version, configuration, input data, and compute environment. Consist records provenance automatically for every run.

**Why it matters**: Reproducibility ("Can I re-run this exactly?"), Accountability ("Which config made this figure?"), Debugging ("Why did this change?")

**Research example**: You published a land-use forecast that shows 10% job growth in downtown. A policy maker asks "is that Figure 3a or Figure 3b from the report?" You run `consist show <run_id>` and instantly see: code version (commit SHA), exact config parameters (zoning policy, density cap), which parcel survey data, and when it ran. You can reproduce it exactly or change one parameter and show the difference. Without provenance, you'd spend hours reconstructing assumptions.

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
import consist
from consist import use_tracker

with use_tracker(tracker):
    result = consist.run(
        fn=clean_data,
        inputs={"raw_path": "raw.csv"},
        config={"threshold": 0.5},
        outputs=["cleaned"],
    )
```

This creates a Run with one input artifact, one config dict, and one output artifact.

**Research example**: In climate modeling, each year's downscaling (e.g., 2030 temperature downscaling) is a separate Run. The provenance database stores 20 runs (2031-2050) all under one scenario, linked by data flow (year 2030's output feeds year 2031's input). You can query: "which runs used GCM model X?" or "what was the total compute time across all 2050 runs?" or "trace 2050's data back to the original global model."

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

A unique fingerprint of a run created by hashing together your code version, configuration parameters, and input data. It's like a "run ID" that Consist compares across executions to detect when the same inputs, code, and config have been run before.

**How it works**:
1. Function code is hashed (git commit SHA + modified files)
2. Config dict is hashed deterministically (canonical hashing)
3. Input file hashes are computed
4. All three are combined: `signature = SHA256(code + config + inputs)`
5. This signature is the cache key

**Why**: If you re-run with the same signature, Consist knows the result will be the same, so it returns the cached version instantly. Identical signatures = identical outputs (assuming deterministic functions).

**See also**: Cache Hit, Config, Artifact, Canonical Hashing

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
