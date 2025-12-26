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
  - If code attempts to produce a *new or different* output on a cache hit, Consist demotes the cache hit to an executing run (so provenance remains truthful).

### Function-shaped steps (skip on cache hit)

If you have an expensive callable (including imported functions or bound methods) and you want Consist to skip executing it on cache hits, use `ScenarioContext.run_step(...)`.

`output_paths={...}` values are interpreted as:
- relative paths → relative to the step run directory (`t.run_dir`)
- URI-like strings (`"scheme://..."`) → resolved via mounts (`tracker.resolve_uri`)
- absolute paths → used as-is

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
- By default, **skips** filesystem checks for cached outputs for speed.
  - Pass `validate_cached_outputs="eager"` if you want to require that output files exist (or are ingested).
- **Hydrates artifact objects** for the cached outputs into the current run context.
  - Paths are resolved on demand (e.g., `cached_output()`).
  - `Artifact.path` lazily resolves via the active tracker; no filesystem checks happen until you access it.

On a cache hit, Consist does **not**:
- Copy files into a new run directory.
- Recreate missing files from DuckDB automatically.
- Guarantee that `artifact.path` exists on disk without an explicit load/materialization step.

If you need bytes, you typically:
- call `consist.load(...)` for tabular artifacts (disk first, then DB fallback per `db_fallback`), or
- implement explicit file copy/materialization for non-tabular artifacts.

### Cache misses that depend on cached inputs

If a run is **not** a cache hit but its inputs include artifacts produced by cached runs,
those input artifacts are still referenced by **portable URIs** (e.g., `./outputs/foo.parquet`).
By default, Consist resolves those URIs into the **current** run directory, which may not
contain the bytes from the original run.

To make “extend a scenario in a new run_dir” workflows work without rerunning cached steps,
use the cache hydration policy:

```python
with tracker.start_run(
    "next_step",
    model="step",
    inputs=[cached_artifact],
    cache_hydration="inputs-missing",
):
    ...
```

With `cache_hydration="inputs-missing"`, Consist will:
- detect missing input paths in the current run_dir,
- locate the original run directory recorded in provenance metadata, and
- copy those input bytes into the current run_dir before execution.

This keeps cache misses fast while still ensuring the executing step can read its inputs.

If the original on-disk input is missing but the artifact was ingested, Consist will
attempt to reconstruct the input from DuckDB **for CSV and Parquet artifacts only**.
Other drivers raise a `ValueError` to avoid silent or lossy conversions.

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
    cache_hydration="outputs-all",
    materialize_cached_outputs_dir=Path("some/output/dir"),
):
    ...
```

Or materialize only specific cached outputs (by artifact key):

```python
with tracker.start_run(
    "r2",
    model="step",
    cache_hydration="outputs-requested",
    materialize_cached_output_paths={"features": Path("outputs/features.csv")},
):
    ...
```

Defaults remain unchanged: `cache_hydration="metadata"` (artifact hydration only).

Validation rules (error cases):
- `outputs-requested` requires `materialize_cached_output_paths` and rejects `materialize_cached_outputs_dir`.
- `outputs-all` requires `materialize_cached_outputs_dir` and rejects `materialize_cached_output_paths`.
- `metadata`/`inputs-missing` reject both materialization args.

Operational notes:
- `outputs-requested` will log a warning if you ask for keys that are not present in the cached outputs.
- `outputs-all` raises `FileNotFoundError` if any cached output source file is missing.

### Containers integration cache hits (requested outputs)

The containers API is intentionally different: on a cache hit it attempts to **copy cached outputs**
to the requested host output paths so downstream tooling sees real files. Internally this now uses
the same copy-based materialization helper as core runs (policy concept is shared), but containers
default to “requested outputs” because the caller explicitly provided host output paths.

This is an integration-level choice for ergonomics; it should not be assumed for core `Tracker`
cache hits.

---

## Hydration policy table

| Workflow need | `cache_hydration` | When bytes are copied | Notes |
| --- | --- | --- | --- |
| Fast cache hits, metadata only | `metadata` | Never | Default; paths may not exist in new run dirs. |
| Extend a scenario in a new `run_dir` | `inputs-missing` | On cache misses, for missing inputs only | Ensures executing steps can read cached inputs. |
| Materialize a few outputs on cache hits | `outputs-requested` | On cache hits, for requested outputs | Requires `materialize_cached_output_paths`. |
| Make all cached outputs exist locally | `outputs-all` | On cache hits, for all outputs | Requires `materialize_cached_outputs_dir`. |

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

---

## Cache-Hit Performance and Diagnostics

Consist’s cache-hit path is optimized to minimize filesystem work by default.
The main knobs are:

- `validate_cached_outputs="lazy"` (default) skips per-output existence checks.
- `cache_hydration="metadata"` (default) avoids copying bytes for cached outputs.

### Cache and hydration knobs (summary)

- `cache_mode`:
  - `reuse` (default): reuse cached runs when signatures match.
  - `overwrite`: always execute and replace cached outputs.
  - `readonly`: read cache but avoid persisting new outputs.
- `validate_cached_outputs`:
  - `lazy` (default): skip filesystem checks for cached outputs.
  - `eager`: require output files to exist (or be ingested) for a cache hit.
- `cache_hydration`:
  - `metadata` (default): hydrate artifact objects only; no bytes copied.
  - `inputs-missing`: on cache misses, copy missing input bytes into the current `run_dir`.
  - `outputs-requested`: on cache hits, copy only specified cached outputs to requested paths.
  - `outputs-all`: on cache hits, copy all cached outputs into a target directory.
- Scenario default: pass `step_cache_hydration="inputs-missing"` to `tracker.scenario(...)`
  to apply a default policy to all steps unless overridden.
- `materialize_cached_output_paths`: required for `outputs-requested`.
- `materialize_cached_outputs_dir`: required for `outputs-all`.

For large iterative workflows, most cache-hit time is spent on:

- fetching cached outputs from the database
- hashing raw-file inputs (when inputs are not Consist-produced artifacts)

You can enable lightweight diagnostics to understand where time is going:

- `CONSIST_CACHE_TIMING=1` logs timing for cache-hit phases during `begin_run`
  (signature prefetch, input hashing, cache lookup, validation, hydration).
- `CONSIST_CACHE_DEBUG=1` logs cache hit/miss details and signatures.

If you need strict safety for missing outputs, use `validate_cached_outputs="eager"`,
accepting the additional filesystem checks.

---

## Pattern 5: Shared input bundles via a DuckDB file

If you want collaborators to start with **all required inputs already packaged in a
standalone DuckDB file**, use a dedicated "bundle" run that logs inputs as outputs
and ingests them. Sharing the DB file is enough to reproduce inputs without shipping
the original raw files.

### Bundle creation (producer)

```python
tracker = Tracker(run_dir="bundle_build", db_path="inputs.duckdb")

with tracker.start_run("inputs_bundle_v1", model="input_bundle", cache_mode="overwrite"):
    households = tracker.log_artifact("households.csv", key="households", direction="output")
    persons = tracker.log_artifact("persons.csv", key="persons", direction="output")
    tracker.ingest(households)
    tracker.ingest(persons)
```

Packaging steps:
1. Run the bundle creation code once.
2. Share the resulting DuckDB file (`inputs.duckdb`) with collaborators.
3. Collaborators only need the DB file; they do **not** need the original CSV files.

### Bundle consumption (consumer)

```python
tracker = Tracker(run_dir="runs", db_path="inputs.duckdb")

bundle_outputs = tracker.load_input_bundle("inputs_bundle_v1")
inputs = list(bundle_outputs.values())

with tracker.start_run(
    "simulate",
    model="simulate",
    inputs=inputs,
    cache_hydration="inputs-missing",
):
    ...
```

Notes:
- The bundle is identified **only by run_id**; no hard-coded hashes are needed.
- `cache_hydration="inputs-missing"` will reconstruct CSV/Parquet inputs from the
  shared DuckDB file if the original bytes are missing.

### Multi-model workflows from one bundle

Bundles can contain inputs for multiple downstream steps. Each run selects only the
inputs it needs:

```python
bundle_outputs = tracker.load_input_bundle("inputs_bundle_v1")

with tracker.start_run(
    "model_a",
    model="model_a",
    inputs=[bundle_outputs["input_a"]],
    cache_hydration="inputs-missing",
):
    ...

with tracker.start_run(
    "model_b",
    model="model_b",
    inputs=[bundle_outputs["input_b"]],
    cache_hydration="inputs-missing",
):
    ...
```
