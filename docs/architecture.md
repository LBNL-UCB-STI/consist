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

Consist enables "Ghost Mode" — the ability to delete intermediate files while preserving provenance and recoverability. As long as the provenance database records the lineage and content hashes, Consist can:

- Verify that a cached result is valid (signature matches)
- Identify which upstream run produced a missing artifact
- Re-execute only the necessary steps to regenerate data

**When You Need This**

Long-running research workflows accumulate massive intermediate datasets that you want to keep for lineage but eventually need to reclaim space. Consider a 30-year climate simulation:

```
Year 2020: Compute temperature fields → Store (500GB) → Use as input for 2021
Year 2021: Compute temperature fields → Store (500GB) → Use as input for 2022
... (31 years × 500GB = 15.5TB)
Year 2050: Final results published
```

Once you've published results (2030–2050), you can safely delete 2020–2029 intermediate files. The provenance database still tracks what those files contained (via content hashes). If you later need to re-run 2031, Consist automatically:
1. Detects that 2030's output is missing from disk
2. Checks if 2030's signature matches a cached computation
3. Re-runs 2030 (which depends on 2029, which depends on 2028, etc.)
4. Materializes only the files needed for the downstream run

**How It Works**

When you log an artifact in Consist, three pieces of information are persisted:

- **Content hash** (SHA256 of file bytes) — Stored in the database
- **URI and metadata** — Stored as provenance
- **Actual file** — On disk (optional in Ghost Mode)

On cache hits, if a file is missing from disk:
```python
# Run 2030 tries to load output from 2029 (now deleted)
with tracker.start_run("2030"):
    upstream_data = consist.log_artifact("year_2029_temps.parquet")
    # Ghost Mode: URI and hash exist in DB, file is missing
    # Consist checks the signature and re-runs 2029 if needed
    df = upstream_data.load()  # Transparently materializes from re-run
```

The re-execution respects the same cache key (code + config + inputs), so if inputs haven't changed, no additional computation is triggered—Consist reuses the prior computation and materializes its output on-demand.

**Best Practices**

- Use Ghost Mode for intermediate outputs in long-running studies, not critical published results.
- Keep the provenance database (`provenance.duckdb`) on reliable storage; it's the source of truth for recovery.
- Archive deleted files alongside the database for offline recovery if needed.
- Test recovery workflows (intentional deletion + re-run) before relying on Ghost Mode in production.

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

### Configuration Management: Tracking Large, Complex Configs

Scientific simulations often involve complex, large configuration files: ActivitySim YAML and csv spanning 5MB, BEAM routing configs, climate model parameter trees. Consist provides strategies to make these configurations part of your provenance without storing them as bulky database fields.

#### Why This Matters: Configuration as Provenance

Configuration is critical for reproducibility: the same code + different config = different results. Consist must:
- Track config as part of the cache key (so changing parameters invalidates cache)
- Make config searchable (so you can find "all runs with alpha=0.5")
- Avoid bloating the provenance database with massive YAML files

The challenge: A 50MB ActivitySim config file is too large to store as a queryable blob, but you can't just ignore it.

#### Three Configuration Strategies

Consist separates configuration into three patterns. Choose based on your workflow:

**Strategy 1: Identity Config (For Cache Keys, Not Queries)**

Use this when configuration is important for caching but doesn't need to be queryable in the database.

```python
from pydantic import BaseModel

class SimulationConfig(BaseModel):
    alpha: float = 0.5
    beta: float = 1.0

tracker.start_run(
    "run_001",
    config=SimulationConfig(alpha=0.5, beta=1.0)
)
```

Consist hashes the config deterministically and includes it in the run signature. Changes to alpha or beta invalidate the cache. Use when: Config is part of the cache key but you don't need to filter by it.

**Strategy 2: Facet (Queryable Configuration)**

Use this when you want to search and filter runs by configuration values.

```python
tracker.start_run(
    "run_001",
    config=SimulationConfig(alpha=0.5, beta=1.0),
    facet={"alpha": 0.5, "beta": 1.0, "scenario": "baseline"}
)
```

Consist stores the facet as a small JSON snapshot in DuckDB and deduplicates identical facets. You can query: "Find all runs where alpha > 0.4 and scenario='baseline'". Use when: You're running parameter sweeps and want to find results by parameter value.

**Strategy 3: Hash-Only Attachments (For Large Files, No Database Storage)**

Use this when configuration files are too large to query and don't need to be stored.

```python
tracker.start_run(
    "run_001",
    hash_inputs=[
        Path("./activitysim_config"),  # 50MB directory
        Path("./beam_config.hocon"),   # 30MB config file
    ]
)
```

Consist computes a fingerprint (SHA256 hash) of the config files. This fingerprint becomes part of the run signature (changing config invalidates cache). The actual config content is not stored. Use when: Configuration files are 10MB+ or you don't need to query by config values.

#### Combining Strategies: The Typical Pattern

Most real workflows use all three:

```python
import consist
from pydantic import BaseModel

class SimConfig(BaseModel):
    year: int
    scenario: str
    demand_elasticity: float

# Start run with all three
result = tracker.start_run(
    "baseline_2030",
    config=SimConfig(
        year=2030,
        scenario="baseline",
        demand_elasticity=0.8
    ),
    facet={
        # Queryable fields: year and scenario
        "year": 2030,
        "scenario": "baseline",
    },
    hash_inputs=[
        # Large external configs hashed for cache key but not stored
        Path("./activitysim"),
        Path("./network_config.yaml")
    ]
)
```

Result:
- Cache key includes: config hash + ActivitySim hash + network hash
- Database stores only the small facet (year, scenario)
- You can query by year/scenario; ActivitySim changes tracked via fingerprints

#### Database Tables

Consist uses these tables for configuration tracking:

- `config_facet`: Deduplicated facet JSON keyed by a canonical hash, namespaced by run model
- `run_config_kv`: Flattened, typed key/value index for facet filtering

Note: Use `Run.year` (a first-class indexed column) for time-series filtering rather than duplicating year in the facet.

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

## Testing & Coverage Focus

The test suite prioritizes correctness for production workflows and portability:

- Cache hydration across run directories (metadata-only, requested/all outputs, missing-input reconstruction, permission/mount issues).
- Path virtualization and mount resolution (workspace URIs, symlink handling, stale/moved run directories).
- Persistence resilience (lock retries, constraint conflict handling, JSON snapshot safety).
- Ingestion and data virtualization (DLT ingestion paths, strict schema validation, hybrid view behavior).
- CLI/query helpers for inspection workflows (filters, preview error modes).

---

## Path Virtualization

Absolute paths break portability. Consist stores relative URIs and resolves them at runtime.
For a focused guide, see [Mounts & Portability](mounts-and-portability.md).

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

Consist maintains a context-local stack of active trackers, allowing nested contexts and implicit tracker resolution:

```python
import consist
from consist import use_tracker

with use_tracker(tracker):
    with consist.scenario("baseline") as sc:
        # consist.log_artifact() finds the active tracker automatically
        with sc.step(name="simulate"):
            consist.log_dataframe(df, key="results")  # No tracker= needed
```

This enables clean APIs where most functions don't require explicit tracker parameters.
