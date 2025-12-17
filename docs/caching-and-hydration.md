# Caching and Artifact Hydration Patterns

Consist’s core loop is:
1. **Declare inputs** (files or upstream artifacts)
2. **Compute a run signature** (code + config + inputs)
3. **Identify existing prior results if they exist** (caching)
4. **Load data from the best available source if they are needed, create them if not** (Look on disk first; database when appropriate; run the model if all else fails)

This page focuses on practical patterns for making caching reliable, keeping pipelines portable, and choosing when/how artifacts should be “hydrated” (materialized or recovered) for downstream work.

---

## Key Concepts and Terminology

This page uses a few terms very precisely. The goal is to separate **provenance objects**
(Artifacts and Runs) from **physical bytes on disk** (files/directories) and from
**database-backed data** (DuckDB tables).

### “Hydrate” vs “materialize” (recommended vocabulary)

Consist workflows often need two different actions when accessing previously-cached data:

1) **Artifact hydration (record/object hydration)**  
   Creating or retrieving an `Artifact` object that has the right identity and metadata
   (e.g., `id`, `uri`, `run_id`, `meta`, and a resolvable `.path` via mounts) so it can be
   passed around as provenance.

   - This is what Consist does on a cache hit in the core `Tracker`: it “hydrates”
     the cached *output artifacts* into the new run context as `Artifact` objects.
   - Artifact hydration does **not** imply that files are copied, downloaded, or re-created.

2) **Physical materialization (bytes-on-disk materialization)**  
   Ensuring the corresponding artifact data *exists at a specific filesystem location*
   (e.g., copying a cached file into a new scratch directory, extracting it, or otherwise
   creating bytes on disk).

   - This is not done automatically by the core `Tracker` cache path.
   - Some integrations (notably the containers API) intentionally materialize cached outputs
     because callers expect host paths to exist.

When reading “hydrate” below, assume **artifact hydration** unless the text explicitly says
“materialize bytes on disk”.

### Runs, signatures, and cache hits

Every run has a **signature** derived from:
- **code hash** (tracked via `IdentityManager`)
- **config hash**
- **input hash** (derived from declared input artifacts)

If Consist finds a previously **completed** run with the same signature and its outputs are still valid, the new run becomes a **cache hit** and reuses the prior outputs.

Cache behavior is controlled via `cache_mode`:
- `reuse` (default): reuse matching completed runs
- `overwrite`: always execute (no cache lookup)
- `readonly`: avoid persisting changes (useful for inspection)

### Artifact identity vs. artifact bytes

Consist distinguishes:
- **provenance identity**: where an artifact came from (e.g., `artifact.run_id` links to the producing run)
- **physical bytes**: the actual on-disk file contents

For Consist-produced artifacts, inputs are typically hashed by linking to the producing run’s signature (Merkle-style). For raw files (no `run_id`), Consist hashes file contents/metadata depending on configuration.

### Portability and path resolution

An artifact has a portable `artifact.uri` and a runtime-resolved `artifact.path`.
- `tracker.resolve_uri(uri)` translates a portable URI into a host filesystem path using mounts.
- On cache hits, Consist performs **artifact hydration**: it attaches cached output `Artifact`
  objects to the active run context so downstream code can reference them.

This is what keeps pipelines portable across different machines/scratch directories: code consumes
`Artifact` objects and resolves paths through the tracker instead of embedding absolute paths.

---

## Pattern 1: Step caching with explicit artifact handoff

For multi-step workflows, the most robust pattern is:
- each step logs outputs as artifacts
- the next step declares those artifacts as inputs

Using `tracker.scenario(...)`:

```python
with tracker.scenario("my_scenario") as scenario:
    with scenario.step("ingest"):
        raw = tracker.log_artifact("raw.csv", key="raw", direction="output")
        scenario.coupler.set("raw", raw)

    # `input_keys=[...]` declares step inputs by Coupler key without repeating
    # `scenario.coupler.require(...)` in the `inputs=[...]` list.
    with scenario.step("transform", input_keys=["raw"]):
        raw_art = scenario.coupler.require("raw")
        df = consist.load(raw_art, tracker=tracker)
```

Notes:
- Prefer `scenario.coupler.require("raw")` over `get("raw")` in tests and templates; it fails loudly if a predecessor didn’t set the key.
- Always pass upstream artifacts through `inputs=[...]` so caching and provenance are correct.
- Step bodies still execute on cache hits; Consist does not “skip the Python block”.
  - To make cache-agnostic step code easier to write, `tracker.log_artifact(..., direction="output")` will return the hydrated cached output when the current run is a cache hit and the output `key` matches a cached output.
  - If you attempt to log a *new* output key on a cache hit, Consist raises a `RuntimeError` (use `cache_mode="overwrite"` if you need to produce new outputs).

---

## Pattern 2: Task caching (`@tracker.task`) and chained artifacts

The `@tracker.task()` decorator is designed for “function-shaped steps”:
- it automatically starts/ends a run
- it turns `Path` arguments into input artifacts
- it turns returned `Path`s into output artifacts
- it supports caching via `cache_mode`

```python
@tracker.task()
def clean(raw_file: Path) -> Path:
    ...
    return cleaned_path

@tracker.task()
def analyze(cleaned: Artifact) -> Path:
    # You can pass artifacts directly; the decorator resolves `.path` for execution
    ...
    return analysis_path
```

If the `clean(...)` signature matches a prior completed run, calling it again returns cached outputs without re-executing the function body.

---

## Pattern 3: Resuming after failures (partial pipeline reuse)

A common operational workflow is:
- early steps succeed (and are cached)
- a late step fails (e.g., ENOSPC / transient compute failure)
- re-run should reuse upstream cached results and continue from the failure point

This works when:
- upstream outputs are logged as artifacts
- downstream steps declare those artifacts as inputs
- failed runs are not reused for caching (Consist only reuses completed runs)

In practice:
- re-running will cache-hit earlier steps
- the failed step will re-execute because there is no valid completed run to reuse

---

## Pattern 4: Hot vs. cold data, and database-backed recovery

Consist supports two complementary storage strategies:

### Cold data (files on disk)
- Artifacts are logged with file paths/URIs.
- Loading uses the file directly.
- Best for large binary formats or when you want to keep the database slim.

### Hot data (ingested into DuckDB)
- `tracker.ingest(artifact)` materializes the artifact into DuckDB.
- The artifact is marked with `artifact.meta["is_ingested"] = True`.
- This enables query performance, schema tracking, and optional recovery if the file disappears.

---

## Database fallback policy in `consist.load`

When `consist.load(...)` can’t find a file on disk, it may be able to recover data from DuckDB
(if the artifact was ingested). This is controlled by `db_fallback`.

Important: DB fallback controls **data recovery at load time**. It is distinct from:
- cache-hit artifact hydration (record/object hydration), and
- physical materialization (copying bytes into a target directory).

- `db_fallback="inputs-only"` (default): allow DB recovery **only** if:
  - there is an active run context
  - the current run is **not** a cache hit
  - the artifact is a **declared input** to that active run

- `db_fallback="always"`: allow DB recovery whenever the artifact is ingested and a tracker DB is available (useful for interactive inspection).
- `db_fallback="never"`: disable DB recovery entirely.

Why the default is strict:
- It prevents “silent DB reads” in contexts where an artifact isn’t actually part of the executing run’s declared inputs.
- It nudges pipelines toward explicit provenance: if you want DB recovery during execution, declare the artifact as an input.

Example: explicit inspection outside a run:

```python
df = consist.load(artifact, tracker=tracker, db_fallback="always")
```

Example: execution-time recovery (artifact must be an input):

```python
with tracker.start_run("step", model="my_step", inputs=[artifact], cache_mode="overwrite"):
    df = consist.load(artifact, tracker=tracker)  # defaults to inputs-only
```

---

## What exactly happens on a cache hit?

This is the most common source of confusion, so here is the explicit behavior.

### Core `Tracker` cache hits (tasks/scenarios)

On a cache hit, Consist:
- Finds a matching **completed** prior run with the same signature.
- Validates cached outputs are “usable” (output files exist OR are marked ingested).
- **Hydrates artifact objects** for the cached outputs into the current run context.

On a cache hit, Consist does **not**:
- Copy files into a new run directory.
- Recreate missing files from DuckDB automatically.
- Guarantee that `artifact.path` exists on disk without an explicit load/materialization step.

If you need bytes, you typically:
- call `consist.load(...)` for tabular artifacts (disk first, then DB fallback per `db_fallback`), or
- implement explicit file copy/materialization for non-tabular artifacts.

### Optional cache-hit materialization (copy bytes on disk)

Core Consist supports an **opt-in** policy to physically materialize cached outputs on cache hits
by copying bytes from the cached artifact’s resolved path to a destination you provide.

This is intentionally **copy-only** materialization (Phase 1):
- It copies from the cached source path if it exists.
- It does not reconstruct files from DuckDB.

Enable it at run start:

```python
with tracker.start_run(
    "r2",
    model="step",
    materialize_cached_outputs="all",
    materialize_cached_outputs_dir=Path("some/output/dir"),
):
    ...
```

Or materialize only specific cached outputs (by artifact key):

```python
with tracker.start_run(
    "r2",
    model="step",
    materialize_cached_outputs="requested",
    materialize_cached_output_paths={"features": Path("outputs/features.csv")},
):
    ...
```

Defaults remain unchanged: `materialize_cached_outputs="never"` (artifact hydration only).

### Containers integration cache hits (requested outputs)

The containers API is intentionally different: on a cache hit it attempts to **copy cached outputs**
to the requested host output paths so downstream tooling sees real files. Internally this now uses
the same copy-based materialization helper as core runs (policy concept is shared), but containers
default to “requested outputs” because the caller explicitly provided host output paths.

This is an integration-level choice for ergonomics; it should not be assumed for core `Tracker`
cache hits.

---

## Practical Guidance

### Make caching predictable
- Declare all true dependencies as inputs (`inputs=[...]` or task parameters).
- Keep configs deterministic (avoid embedding timestamps/randomness unless intended).
- Prefer stable code identity in tests (Consist test suite patches the code hash for determinism).

### Decide what to ingest
Ingest when you want:
- queryable “hot” tables in DuckDB
- schema profiling/validation
- optional recovery if files are deleted

Keep as cold files when:
- you don’t want to duplicate storage in DuckDB
- artifacts are huge or already live in an external system

### Safe deletion and long-running pipelines
Deleting old artifacts can be safe when:
- you’re confident they will not be loaded again, or
- they were ingested and you intentionally allow DB recovery (usually by declaring them as inputs in a non-cached run, or by using `db_fallback="always"` in explicit analysis tooling).

If you want to aggressively GC the workspace while keeping provenance usable, ingestion + intentional DB fallback is the strategy to reach for.
