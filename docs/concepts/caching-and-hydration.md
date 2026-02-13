# Caching and Artifact Hydration Patterns

Consist's core loop: declare inputs → compute signature → check cache → load or execute.

!!! info "Terminology"
    **Hydration** recovers metadata and provenance about a prior run's outputs without copying file bytes. **Materialization** ensures file bytes exist on disk (copying them from their original location). A cache hit always hydrates; it only materializes if you request it via a specific hydration policy.

This page covers patterns for making caching reliable, keeping pipelines portable, and choosing when artifacts should be hydrated or materialized.

---

## Terms Used Precisely

This page separates **provenance objects** (Artifacts, Runs) from **physical bytes** (files on disk) and **database-backed data** (DuckDB tables).

**Cache Hit**: Consist finds a prior run with the same signature and returns cached artifact metadata.

**Hydration**: Recovering metadata and location information about a previous run's outputs without copying file bytes.

**Materialization**: Copying artifact bytes to disk (or into DuckDB) so files exist in a new location.

---

## Hydration vs Materialization

When Consist finds a cached result, it "brings back" information about that result—but what does that mean?

Consider a electric grid simulation in `/scratch/simulation_2024/` that produces a 50GB results file. Six months later, you run analysis with the same inputs. Consist recognizes a cache hit.

**Artifact Hydration** recovers *information*: the result came from run `abc123`, it's a Parquet file called `results.parquet`, it contains monthly precipitation data. Consist creates an `Artifact` object with metadata, URIs, and provenance.

**Materialization** ensures *bytes exist*: copying the 50GB file from its original location into your new run directory.

Hydration is fast and automatic. Materialization is optional and explicit.

### Default Behavior

Consist hydrates metadata only. Metadata recovery is instant (database lookup). Files stay in their original location. You pay for copying only when requested.

This matters for parameter sweeps: 100 demand model variations do not require 100 copies of a 50GB input file. Downstream code loads from the original path or database fallback.

### When to Materialize

Materialize cached outputs (copy bytes to your current run) in three cases:

1. **Extending a scenario in a new workspace**: Cached results in `/workspace/2024`, continuing in `/workspace/2025`. Use `cache_hydration="inputs-missing"` to copy input files.

2. **Preparing outputs for external tools**: Tools expect local files. Use `cache_hydration="outputs-requested"` with `materialize_cached_output_paths`. With `consist.run(...)` or `sc.run(...)`, use `output_paths={...}`.

3. **Ensuring local reproducibility**: All results accessible without remote mounts. Use `cache_hydration="outputs-all"` to copy all cached outputs.

### Example: Electric Grid Modeling Workflow

``` python
from pathlib import Path

with tracker.start_run(  # (1)!
    "grid_dispatch_baseline",
    model="grid_sim",
    year=2024,
    cache_mode="overwrite"  # (2)!
):
    results = run_dispatch_model()  # (3)!
    tracker.log_artifact(results, key="dispatch")  # (4)!

with tracker.start_run(  # (5)!
    "reliability_analysis",
    model="analysis",
    inputs=[cached_dispatch_artifact],  # (6)!
    cache_hydration="metadata",  # (7)!
):
    print(f"Result came from run {cached_dispatch_artifact.run_id}")  # (8)!

    df = consist.load_df(cached_dispatch_artifact)  # (9)!

with tracker.start_run(  # (10)!
    "export_for_reporting",
    model="export",
    cache_hydration="outputs-requested",  # (11)!
    materialize_cached_output_paths={
        "dispatch": Path("./local_outputs/dispatch.parquet")  # (12)!
    }
):
    subprocess.run(["report_generator", "annual_summary.py"])  # (13)!
```

1. First run: simulate regional grid dispatch for 2024, takes 18 hours.
2. **Overwrite mode**: Forces re-execution and overwrites any cached results. Use for baseline runs or when upstream data has changed outside Consist's tracking.
3. Dispatch models produce large outputs—hourly resolution across 8,760 hours for all generators and transmission lines.
4. **Artifact logging**: Consist records the file's hash, location, and lineage. This artifact can now be referenced as an input to downstream runs.
5. Later: analysis that only needs metadata about the result.
6. **Input declaration**: Tells Consist this run depends on the dispatch artifact. If the upstream run is invalidated, this run's cache is also invalidated.
7. **Metadata-only hydration**: Consist loads the Artifact object (provenance, signatures, URIs) without copying the 40GB file. Default behavior—saves disk space.
8. Even without file bytes, you can query provenance: which run produced this, when, with what config.
9. **Explicit loading**: When you actually need the data, `load_df()` reads from the original location (or database if ingested). This is the only point where bytes move.
10. Later: need to ensure files exist locally for regulatory reporting tool.
11. **Outputs-requested mode**: Tells Consist to copy cached output files to disk. Use when external tools need real files, not just metadata.
12. **Materialization path**: Specifies where Consist should copy the cached artifact. The external reporting tool will find the file here.
13. Now the regulatory reporting tool can access real files at known paths—Consist handled the copying.

### Default Behavior: Metadata-Only Hydration

The default `cache_hydration="metadata"` performs no filesystem operations and no disk duplication. Use `consist.load_df()` or `consist.load()` when you need bytes.

This default suits scientific workflows: large simulations benefit from avoiding disk copies, and provenance verification does not need bytes on disk.

### Practical Decision Table

| Your Need | Use This | Tradeoff |
|-----------|----------|----------|
| Run parameter sweeps on cached simulation | `metadata` (default) | No disk copy; must load data via `consist.load_df()`/`consist.load()` |
| Extend analysis in a new workspace | `inputs-missing` | Copies only missing input files; fast on cache hits |
| Run external tool that needs local files | `outputs-requested` | Copy only what you ask for; must list outputs explicitly |
| Ensure all cached data is accessible locally | `outputs-all` | Copies everything; uses disk space but guarantees file paths exist |

### Signatures and Cache Behavior

Every run's **signature** is derived from code hash, config hash, and input hash. If Consist finds a completed run with the same signature and valid outputs, the new run is a **cache hit** and reuses prior outputs.

#### Intentional Cache Invalidation

Sometimes you need to invalidate caches globally or for a specific step (e.g., after a schema change or a bug fix that didn't change the config signature).

- **Global Invalidation**: Increment `Tracker(cache_epoch=N)`. This bumps the version for every run in that tracker.
- **Scenario Invalidation**: Use `tracker.scenario(..., cache_epoch=N)`.
- **Step Invalidation**: Use `@define_step(cache_version=N)`.

The `cache_epoch` and `cache_version` are folded into the run's identity hash, so a bump guarantees a cache miss.

For decorator defaults, callable metadata, and name templates, see
**[Decorators & Metadata](decorators-and-metadata.md)**.

```python
tracker = Tracker(run_dir="runs", cache_epoch=2)

@define_step(cache_version=3)
def simulate(...) -> None:
    ...

with tracker.scenario("baseline", cache_epoch=4) as sc:
    sc.run(simulate, ...)
```

| `cache_mode` | Behavior |
|--------------|----------|
| `reuse` (default) | Reuse matching completed runs |
| `overwrite` | Always execute (no cache lookup) |
| `readonly` | Read cache but avoid persisting changes |

### Artifact Identity vs. Bytes

Consist distinguishes **provenance identity** (where an artifact came from, via `artifact.run_id`) from **physical bytes** (on-disk file contents).

For Consist-produced artifacts, inputs are hashed by linking to the producing run's signature (Merkle-style). For raw files (no `run_id`), Consist hashes file contents or metadata depending on configuration.

To skip hashing when you know the content hash (e.g., after copying a file), pass `content_hash=` to `log_artifact`. Consist ignores mismatched overrides unless `force_hash_override=True`. Use `validate_content_hash=True` to verify the override against on-disk data.

### Portability and Path Resolution

An artifact has a portable `artifact.container_uri` and a runtime-resolved `artifact.path`. Call `tracker.resolve_uri(uri)` to translate a portable URI into a host filesystem path using mounts.

On cache hits, Consist attaches cached output `Artifact` objects to the active run context. Code consumes `Artifact` objects and resolves paths through the tracker, keeping pipelines portable across machines.

---

## Pattern 1: Step Caching with Explicit Artifact Handoff

For multi-step workflows, each step logs outputs as artifacts and the next step declares those artifacts as inputs.

``` python
import consist
from consist import use_tracker

with use_tracker(tracker):
    with consist.scenario("my_scenario") as scenario:
        with scenario.trace("ingest"):
            consist.log_artifact("raw.csv", key="raw", direction="output")

        with scenario.trace("transform", inputs=["raw"]):  # (1)!
            raw_art = scenario.coupler.require("raw")  # (2)!
            df = consist.load_df(raw_art, tracker=tracker)
```

1. `inputs=[...]` declares step inputs by Coupler key without repeating.
2. `scenario.coupler.require(...)` in the `inputs=[...]` list.

Use `scenario.coupler.require("raw")` over `get("raw")`—it fails loudly if a predecessor did not set the key. Pass upstream artifacts through `inputs=[...]` for correct caching and provenance.

!!! note "Step bodies execute on cache hits"
    Consist does not skip Python blocks. On cache hits, `tracker.log_artifact(..., direction="output")` returns the hydrated cached output when the `key` matches. If code produces a different output, Consist demotes the cache hit to an executing run.

### Function-Shaped Steps (Skip on Cache Hit)

Use `ScenarioContext.run(...)` to skip execution on cache hits. The `output_paths={...}` values are interpreted as relative paths (to `t.run_dir`), URI-like strings (resolved via mounts), or absolute paths.

---

## Pattern 2: Cached Runs with `consist.run`

Use `consist.run(...)` with declared outputs for cache-aware function execution.

```python
def clean(raw_file: Path):
    ...
    return {"cleaned": cleaned_df}

import consist
from consist import ExecutionOptions, use_tracker

with use_tracker(tracker):
    result = consist.run(
        fn=clean,
        inputs={"raw_file": Path("raw.csv")},
        outputs=["cleaned"],
        execution_options=ExecutionOptions(load_inputs=True),
    )
```

---

## Pattern 3: Resuming After Failures

When early steps succeed (and cache) but a late step fails, re-running reuses upstream cached results and continues from the failure point.

Requirements: upstream outputs are logged as artifacts, downstream steps declare those artifacts as inputs, and Consist only reuses completed runs. Re-running cache-hits earlier steps; the failed step re-executes because no valid completed run exists.

---

## Pattern 4: Hot vs. Cold Data

See [Data Materialization](data-materialization.md) for ingestion patterns, schema validation, and database fallback behavior.

---

## What Happens on a Cache Hit?

### Core Tracker Behavior

On a cache hit, Consist:

- Finds a matching **completed** prior run with the same signature
- Skips filesystem checks for cached outputs (for `run(...)`, use
  `cache_options=CacheOptions(validate_cached_outputs="eager")` to require files
  exist or are ingested)
- Hydrates artifact objects into the current run context; paths resolve lazily via the active tracker

On a cache hit, Consist does **not**:

- Copy files into a new run directory
- Recreate missing files from DuckDB automatically
- Guarantee that `artifact.path` exists on disk

To access bytes: call `consist.load_df(...)` for DataFrames or `consist.load(...)` for Relations. Use `consist.load_relation(...)` as a context manager to ensure the DuckDB connection closes. Consist warns when active relation count exceeds `CONSIST_RELATION_WARN_THRESHOLD` (default 100).

### Cache Misses with Cached Inputs

If a run is not a cache hit but its inputs include artifacts from cached runs, those inputs are referenced by portable URIs (e.g., `./outputs/foo.parquet`). By default, URIs resolve to the current run directory, which may not contain bytes from the original run.

To extend a scenario in a new `run_dir` without re-running cached steps, use the cache hydration policy:

```python
with tracker.start_run(
    "next_step",
    model="step",
    inputs=[cached_artifact],
    cache_hydration="inputs-missing",
):
    ...
```

With `cache_hydration="inputs-missing"`, Consist detects missing input paths, locates the original run directory from provenance, and copies input bytes into the current `run_dir` before execution.

If the original file is missing but the artifact was ingested, Consist reconstructs inputs from DuckDB for CSV and Parquet artifacts only. Other drivers raise a `ValueError` to avoid silent or lossy conversions.

### Optional Cache-Hit Materialization

Consist supports opt-in materialization: copying bytes from a cached artifact's resolved path to a destination you provide. This is copy-only—it does not reconstruct files from DuckDB.

Enable at run start:

```python
with tracker.start_run(
    "r2",
    model="step",
    cache_hydration="outputs-all",
    materialize_cached_outputs_dir=Path("some/output/dir"),
):
    ...
```

Materialize specific cached outputs by artifact key:

```python
with tracker.start_run(
    "r2",
    model="step",
    cache_hydration="outputs-requested",
    materialize_cached_output_paths={"features": Path("outputs/features.csv")},
):
    ...
```

Equivalent with `consist.run(...)`:

```python
from consist import CacheOptions

result = consist.run(
    fn=step,
    cache_options=CacheOptions(cache_hydration="outputs-requested"),
    output_paths={"features": Path("outputs/features.csv")},
)
```

Default is `cache_hydration="metadata"` (artifact hydration only). For
`consist.run(...)` or `sc.run(...)`, use `output_paths={...}` with
`cache_options=CacheOptions(cache_hydration="outputs-requested")`.

| Policy | Requires | Rejects |
|--------|----------|---------|
| `outputs-requested` | `materialize_cached_output_paths` or `output_paths` | `materialize_cached_outputs_dir` |
| `outputs-all` | `materialize_cached_outputs_dir` | `materialize_cached_output_paths` |
| `metadata` / `inputs-missing` | — | Both materialization args |

`outputs-requested` logs a warning for missing keys. `outputs-all` raises `FileNotFoundError` if any cached output source file is missing.

### Containers Integration

The containers API defaults to copying cached outputs to requested host output paths so downstream tooling sees real files. This uses the same copy-based materialization helper as core runs, but containers default to "requested outputs" because the caller explicitly provides host output paths.

---

## Hydration Policy Summary

| Workflow | `cache_hydration` | When Bytes Copy | Notes |
|----------|-------------------|-----------------|-------|
| Fast cache hits | `metadata` | Never | Default; paths may not exist in new run dirs |
| Extend scenario in new `run_dir` | `inputs-missing` | Cache misses, missing inputs only | Ensures executing steps read cached inputs |
| Materialize specific outputs | `outputs-requested` | Cache hits, requested outputs | Requires `materialize_cached_output_paths` or `output_paths` |
| All outputs local | `outputs-all` | Cache hits, all outputs | Requires `materialize_cached_outputs_dir` |

Use `metadata` (the default) unless downstream tools require files on disk.

---

## See Also

- **[Data Materialization](data-materialization.md)** — When to ingest data and use hybrid views
- **[Config Management](config-management.md)** — How configuration affects cache invalidation

---

## Practical Guidance

### Make Caching Predictable

Declare all dependencies as inputs (`inputs=[...]` or task parameters). Keep configs deterministic—avoid embedding timestamps or randomness unless intended. Prefer stable code identity in tests (the Consist test suite patches the code hash for determinism).

### Decide What to Ingest

Ingest for queryable DuckDB tables, schema profiling, and optional recovery if files are deleted. Keep as cold files when you do not want to duplicate storage or artifacts are large and already in an external system.

### Safe Deletion

Delete artifacts when you are confident they will not be loaded again, or when they were ingested and you allow DB recovery (via `db_fallback="always"` or by declaring them as inputs). For aggressive GC while keeping provenance usable, use ingestion + intentional DB fallback.

---

## Cache-Hit Performance and Diagnostics

Consist's cache-hit path minimizes filesystem work by default: `validate_cached_outputs="lazy"` skips per-output existence checks, and `cache_hydration="metadata"` avoids copying bytes.

### Configuration Summary

| Parameter | Options |
|-----------|---------|
| `cache_mode` | `reuse` (default), `overwrite`, `readonly` |
| `validate_cached_outputs` | `lazy` (default), `eager` |
| `cache_hydration` | `metadata` (default), `inputs-missing`, `outputs-requested`, `outputs-all` |
| `step_cache_hydration` | Scenario-level default for all steps |
| `materialize_cached_output_paths` | Required for `outputs-requested` |
| `materialize_cached_outputs_dir` | Required for `outputs-all` |

For large iterative workflows, cache-hit time is spent fetching cached outputs and hashing raw-file inputs. Enable diagnostics:

- `CONSIST_CACHE_TIMING=1`: logs timing for signature prefetch, input hashing, cache lookup, validation, hydration
- `CONSIST_CACHE_DEBUG=1`: logs cache hit/miss details and signatures

```text
[cache_timing] signature_prefetch=2.1ms input_hashing=145.8ms cache_lookup=3.4ms validate=0.6ms hydration=0.2ms
```

If input hashing dominates, ingest large tabular inputs or pass Consist-produced artifacts instead of raw files.

```text
[cache_debug] hit=True signature=7e9c1c inputs=3 outputs=2 hydration=metadata
```

If downstream tools need files, select an outputs hydration mode. Use `validate_cached_outputs="eager"` for strict output existence checks.

---

## Pattern 5: Shared Input Bundles

Package all required inputs in a standalone DuckDB file. Create a "bundle" run that logs inputs as outputs and ingests them. Share the DB file to reproduce inputs without shipping original raw files.

### Bundle Creation (Producer)

```python
tracker = Tracker(run_dir="bundle_build", db_path="inputs.duckdb")

with tracker.start_run("inputs_bundle_v1", model="input_bundle", cache_mode="overwrite"):
    households = tracker.log_artifact("households.csv", key="households", direction="output")
    persons = tracker.log_artifact("persons.csv", key="persons", direction="output")
    tracker.ingest(households)
    tracker.ingest(persons)
```

Run the bundle creation code once. Share `inputs.duckdb` with collaborators—they do not need the original CSV files.

### Bundle Consumption (Consumer)

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

The bundle is identified by `run_id` only—no hard-coded hashes. With `cache_hydration="inputs-missing"`, Consist reconstructs CSV/Parquet inputs from the shared DuckDB file if original bytes are missing.

### Multi-Model Workflows

Bundles contain inputs for multiple downstream steps. Each run selects the inputs it needs:

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
