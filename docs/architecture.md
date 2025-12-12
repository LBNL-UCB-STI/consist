# Architecture

## How Caching Works

Consist identifies runs using a three-part signature:

```
SHA256(code_hash | config_hash | input_hash)
```

**Code hash**: Derived from your Git commit SHA by default. Uncommitted changes append `-dirty-<timestamp>` to invalidate cache. For `@task` decorators, you can hash at different granularities:

| Strategy | Scope | Use Case |
|----------|-------|----------|
| Git SHA (global) | Entire repository | Production runs requiring full reproducibility |
| Module (default) | Single `.py` file | Development; captures helpers in the same file |
| Source (atomic) | Function body only | Pure functions; ignores comments and unrelated changes |

**Config hash**: Canonical representation of configuration, normalized for dictionary ordering and numeric type variations. Pydantic models are serialized deterministically.

**Input hash**: For Consist-produced artifacts, this references the *signature of the producing run* (Merkle linking). For raw files, Consist hashes file contents or metadata depending on the hashing strategy (`full` vs `fast`).

This Merkle DAG structure means:
- Changing a parameter invalidates only downstream runs that depend on it
- Identical inputs produce cache hits across machines (given the same code version)
- Provenance validity depends on the lineage graph, not file existence

### Cache Modes

| Mode | Behavior |
|------|----------|
| `reuse` (default) | Return cached result if signature matches |
| `overwrite` | Always execute, update cache with new result |
| `readonly` | Use cache but don't persist new results (sandbox mode) |

### Ghost Mode

Intermediate files can be deleted to save disk space. As long as the provenance database records the lineage and content hashes, Consist can:
- Verify that a cached result is valid (signature matches)
- Identify which upstream run produced a missing artifact
- Re-execute only the necessary steps to regenerate data

This is useful for long-running studies where you want to keep provenance forever but can't store every intermediate file.

---

## Data Model

Consist uses two core entities with a many-to-many relationship:

| Entity | Purpose |
|--------|---------|
| `Run` | Execution context: model name, config, timestamps, status, parent linkage |
| `Artifact` | File metadata: path (as URI), content hash, driver, schema reference |
| `RunArtifactLink` | Connects runs to their input and output artifacts with direction metadata |

Key fields for workflow tracking:
- `Run.parent_run_id` — Links scenario steps to their parent scenario
- `Run.scenario_id` — Denormalized for easy filtering
- `Run.year` — Simulation year for time-series workflows
- `Run.tags` — String labels for filtering (stored as JSON array)
- `Artifact.hash` — SHA256 content hash for deduplication and verification

---

## Dual-Write Persistence

Consist maintains two synchronized records for resilience:

```
┌──────────────────┐
│     Tracker      │
└────────┬─────────┘
         │
    ┌────┴────┬────────────┐
    ▼         ▼            ▼
┌────────┐ ┌──────────┐ ┌──────────┐
│  JSON  │ │  DuckDB  │ │  Events  │
│Snapshot│ │ Database │ │ Manager  │
└────────┘ └──────────┘ └──────────┘
```

**Write order (safety guarantee):**
1. Update in-memory model
2. Flush to `consist.json` (atomic write) ← **Source of truth**
3. Attempt DB sync (catch errors, log warning, never crash)

**JSON snapshots** (`consist.json` per run): Portable, human-readable, version-controllable. Each run directory contains a complete record that survives database corruption.

**DuckDB database**: Enables fast queries across runs, artifacts, and lineage. Can be rebuilt from JSON snapshots if needed. Handles concurrent access with retry logic.

---

## Path Virtualization

Absolute paths break portability. Consist stores relative URIs and resolves them at runtime.

```
User logs: /mnt/data/land_use.csv
           ↓
Tracker detects mount: mounts={"inputs": "/mnt/data"}
           ↓
Stored URI: inputs://land_use.csv
```

**Workspace URIs**: For run-specific output directories, Consist mounts the current run's directory as `workspace://`. Historical paths are resolved via metadata stored in `Run.meta["_physical_run_dir"]`.

This means:
- Provenance stays valid when data moves between machines
- Teams can share databases without path conflicts
- Cloud and local storage can coexist

---

## Data Virtualization

### SQL Views

Register SQLModel schemas to query artifacts across runs as unified tables:

```python
class Person(SQLModel, table=True):
    person_id: int = Field(primary_key=True)
    age: int

tracker = Tracker(schemas=[Person])

# Access the view
VPerson = tracker.views.Person

# Query across all runs
query = select(VPerson).where(VPerson.consist_year == 2030)
```

Views automatically include system columns:
- `consist_run_id` — Which run produced this row
- `consist_scenario_id` — Parent scenario identifier
- `consist_year` — Simulation year
- `consist_artifact_id` — Source artifact

### Hybrid Views

Consist creates "hybrid" views that union:
- **Hot data**: Rows ingested into DuckDB tables (fast queries)
- **Cold data**: Parquet/CSV files on disk (no duplication)

This lets you query terabytes of simulation output without loading everything into memory.

### Matrix Views (N-Dimensional)

For Zarr/NetCDF arrays, `MatrixViewFactory` creates lazy xarray Datasets:

1. Query the artifact catalog for matching Zarr stores
2. Open each store lazily (no data loaded)
3. Concatenate along `run_id` dimension with `year`/`iteration` as coordinates

```python
ds = tracker.matrix.load("skim_matrices", variables=["travel_time"])
# ds is an xarray.Dataset with dims: (run_id, origin, destination)
```

---

## Container Integration

Containers are treated as pure functions where the image digest becomes part of the cache signature:

```
Signature = SHA256(image_digest | command | env | mount_hashes | input_signatures)
```

On cache hit:
1. Verify outputs exist (or are in ghost mode)
2. Relink artifacts to current run
3. Hydrate files to requested host paths (copy from cache)

On cache miss:
1. Execute container
2. Capture declared outputs
3. Record image digest and mount metadata

Backend abstraction supports Docker and Singularity/Apptainer for HPC environments.

---

## Event Hooks

Register callbacks for run lifecycle events:

```python
tracker.events.on_run_complete(lambda run: notify_slack(run.id))
tracker.events.on_run_failed(lambda run, error: log_to_sentry(error))
```

Events are emitted but failures in hooks don't crash the run—they're logged and the workflow continues.

---

## Context Stack

Consist maintains a thread-local stack of active trackers, allowing nested contexts and implicit tracker resolution:

```python
with tracker.scenario("baseline") as sc:
    # consist.log_artifact() finds the active tracker automatically
    with sc.step(name="simulate"):
        consist.log_dataframe(df, key="results")  # No tracker= needed
```

This enables clean APIs where most functions don't require explicit tracker parameters.