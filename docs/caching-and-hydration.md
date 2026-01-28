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

## Hydration vs Materialization: Recovering Information vs. Ensuring Files Exist

When Consist finds a cached result, it needs to "bring that result back" into your new run. But **what does 'bringing back' mean?** This section clarifies the distinction between recovering information about a previous result versus ensuring the actual files exist on your current system.

### The Two Types of Recovery

Imagine you run a climate simulation in `/scratch/simulation_2024/` and it produces a 50GB results file. Six months later, you run a new analysis with the same inputs. Consist recognizes a cache hit. Now:

**Artifact Hydration** = Recovering the *information* about that result: "This result came from run `abc123`, it's a Parquet file called `results.parquet`, and it contains monthly precipitation data." Consist creates an `Artifact` object with the metadata, URIs, and provenance—all the *information* needed to reference and load the result.

**Materialization** = Ensuring the *actual bytes* exist on your new system: copying the 50GB `results.parquet` file from its original location into your new run directory so your analysis code can read it from disk.

The key insight: **Hydration is fast and automatic; materialization is optional and explicit.**

### Why Consist Does This

By default, Consist only hydrates metadata. This saves disk space and time:
- Metadata recovery is instant (lookup in provenance database)
- Files stay in their original location or database
- You pay for file copying *only if you ask for it*

This is useful when:
- You're running 100 variations of an analysis on the same simulated data—you don't need 100 copies of that 50GB results file
- Your downstream code can load from the original file path (via mounted storage or database fallback)
- You want minimal disk overhead for large-scale parameter sweeps

### When to Materialize: Making Cached Files Local

You'll want to materialize cached outputs (copy bytes to your current run) in a few cases:

1. **Extending a scenario in a new workspace:** You cached results in `/workspace/2024`, now you want to continue analysis in `/workspace/2025`. Use `cache_hydration="inputs-missing"` to automatically copy input files your executing steps need.

2. **Preparing outputs for external tools:** Some tools expect files to exist locally. Use `cache_hydration="outputs-requested"` with `materialize_cached_output_paths` to copy specific cached outputs you need.

3. **Ensuring full reproducibility locally:** You want all results locally accessible without remote mounts. Use `cache_hydration="outputs-all"` to copy all cached outputs into a local directory.

### Example: Climate Data Workflow

```python
# First run: simulate climate for years 2020-2030, takes 24 hours
with tracker.start_run(
    "climate_baseline",
    scenario_id="baseline",
    model="climate_sim",
    year=2030,
    cache_mode="overwrite"
):
    results = run_climate_model()  # 50GB output
    tracker.log_artifact(results, key="precip")

# Later: analysis that only needs metadata about the result
with tracker.start_run(
    "analyze_trends",
    scenario_id="baseline",
    model="analysis",
    inputs=[cached_precip_artifact],
    cache_hydration="metadata",  # Default: no copying
):
    # Consist hydrated the Artifact object with metadata
    # You can query properties, compute signatures, check provenance
    print(f"Result came from run {cached_precip_artifact.run_id}")

    # If you need the actual bytes, you explicitly load them:
    df = consist.load_df(cached_precip_artifact)  # Loads from original location

# Later: need to ensure files exist locally for external tool
with tracker.start_run(
    "export_for_visualization",
    scenario_id="baseline",
    model="export",
    cache_hydration="outputs-requested",
    materialize_cached_output_paths={
        "precip": Path("./local_outputs/precip.parquet")
    }
):
    # Consist copies the cached precip.parquet into ./local_outputs/
    # Now your external tool can see real files
    subprocess.run(["qgis", "export_for_visualization.py"])
```

### Default Behavior: Metadata-Only Hydration

By default, `cache_hydration="metadata"`:
- Fast: no filesystem operations
- Efficient: no disk duplication
- Requires explicit `consist.load_df()` or `consist.load()` when you need bytes

This is the right default for scientific workflows because:
- You often analyze cached results programmatically (via `consist.load_df()` or `consist.load()`)
- Large simulations benefit from avoiding disk copies
- Provenance and signature verification don't need bytes on disk

### Practical Decision Table

| Your Need | Use This | Tradeoff |
|-----------|----------|----------|
| Run parameter sweeps on cached simulation | `metadata` (default) | No disk copy; must load data via `consist.load_df()`/`consist.load()` |
| Extend analysis in a new workspace | `inputs-missing` | Copies only missing input files; fast on cache hits |
| Run external tool that needs local files | `outputs-requested` | Copy only what you ask for; must list outputs explicitly |
| Ensure all cached data is accessible locally | `outputs-all` | Copies everything; uses disk space but guarantees file paths exist |

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
If you already know the content hash (for example, after copying or moving a file), you can pass `content_hash=` to `log_artifact` to reuse it and skip hashing the path on disk. To avoid accidentally mutating existing provenance, Consist ignores mismatched overrides unless `force_hash_override=True`. Use `validate_content_hash=True` to verify the override against on-disk data.

### Portability and path resolution

An artifact has a portable `artifact.container_uri` and a runtime-resolved `artifact.path`.
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

Using `consist.scenario(...)`:

```python
import consist
from consist import use_tracker

with use_tracker(tracker):
    with consist.scenario("my_scenario") as scenario:
        with scenario.trace("ingest"):
            consist.log_artifact("raw.csv", key="raw", direction="output")

        # `inputs=[...]` declares step inputs by Coupler key without repeating
        # `scenario.coupler.require(...)` in the `inputs=[...]` list.
        with scenario.trace("transform", inputs=["raw"]):
            raw_art = scenario.coupler.require("raw")
            df = consist.load_df(raw_art, tracker=tracker)
```

Notes:
- Prefer `scenario.coupler.require("raw")` over `get("raw")` in tests and templates; it fails loudly if a predecessor didn’t set the key.
- Always pass upstream artifacts through `inputs=[...]` so caching and provenance are correct.
- Step bodies still execute on cache hits; Consist does not “skip the Python block”.
  - To make cache-agnostic step code easier to write, `tracker.log_artifact(..., direction="output")` will return the hydrated cached output when the current run is a cache hit and the output `key` matches a cached output.
  - If code attempts to produce a *new or different* output on a cache hit, Consist demotes the cache hit to an executing run (so provenance remains truthful).

### Function-shaped steps (skip on cache hit)

If you have an expensive callable (including imported functions or bound methods) and you want Consist to skip executing it on cache hits, use `ScenarioContext.run(...)`.

`output_paths={...}` values are interpreted as:
- relative paths → relative to the step run directory (`t.run_dir`)
- URI-like strings (`"scheme://..."`) → resolved via mounts (`tracker.resolve_uri`)
- absolute paths → used as-is

---

## Pattern 2: Cached runs with `consist.run`

Use `consist.run(...)` with declared outputs to get cache-aware execution for function-shaped steps.

```python
def clean(raw_file: Path):
    ...
    return {"cleaned": cleaned_df}

import consist
from consist import use_tracker

with use_tracker(tracker):
    result = consist.run(
        fn=clean,
        inputs={"raw_file": Path("raw.csv")},
        outputs=["cleaned"],
        load_inputs=True,
    )
```

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

Ingestion and hybrid views are covered in the dedicated guide:
[Ingestion & Hybrid Views](ingestion-and-hybrid-views.md). That doc explains when to ingest,
how schema validation works, and how DB fallback behaves when files are missing.

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
- call `consist.load_df(...)` (or `consist.load(...)` for Relations) for tabular artifacts (disk first; see Ingestion & Hybrid Views for DB fallback), or
- implement explicit file copy/materialization for non-tabular artifacts.

If you want to work with a DuckDB Relation directly, use `consist.load_relation(...)` as a
context manager to ensure the underlying DuckDB connection is closed. If you keep Relations
alive in memory, Consist will warn once the active relation count crosses the
`CONSIST_RELATION_WARN_THRESHOLD` (default 100).

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

Most users should stick with the default `metadata` mode. Use the other modes only when downstream tools require the actual files to exist on disk.

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
- Scenario default: pass `step_cache_hydration="inputs-missing"` to `consist.scenario(...)`
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

Example output with `CONSIST_CACHE_TIMING=1`:

```text
[cache_timing] signature_prefetch=2.1ms input_hashing=145.8ms cache_lookup=3.4ms validate=0.6ms hydration=0.2ms
```

Interpretation: input hashing dominates, so consider ingesting large tabular inputs or passing Consist-produced artifacts instead of raw files.

Example output with `CONSIST_CACHE_DEBUG=1`:

```text
[cache_debug] hit=True signature=7e9c1c inputs=3 outputs=2 hydration=metadata
```

Interpretation: the cache hit returned metadata only; if downstream tools need files, select an outputs hydration mode.

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
