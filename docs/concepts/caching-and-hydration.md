# Caching and Hydration

Consist's execution loop is:

```text
declare inputs -> compute signature -> check cache -> hydrate or execute
```

This page focuses on the run signature, cache hit/miss behavior, hydration
versus materialization, and the policy knobs you are likely to use. For
runnable workflow patterns, see the [Usage Guide](../usage-guide.md).

## Core Terms

| Term | Meaning |
|---|---|
| Signature | Deterministic fingerprint built from code identity, config identity, and input identity. Used for cache lookup. |
| Cache hit | A completed prior run has the same signature and reusable outputs. |
| Cache miss | No reusable completed run matches, so Consist executes the step. |
| Hydration | Recover artifact metadata, paths, and provenance records. |
| Materialization | Ensure artifact bytes exist at a target filesystem path. |
| Ghost mode | Recovery path when provenance exists but original files are missing. |

Hydration and materialization are deliberately separate. A cache hit can return
artifact metadata without copying bytes.

## Run Signature

For `Tracker.run(...)`, `Tracker.trace(...)`, `ScenarioContext.run(...)`, and
`ScenarioContext.trace(...)`, the practical signature model is:

```text
signature = code identity + config identity + input identity
```

### Code Identity

By default, Consist uses repository Git state. For function-shaped steps, you
can narrow code identity with `CacheOptions`:

```python
from consist import CacheOptions

cache_options = CacheOptions(
    code_identity="callable_module",
    code_identity_extra_deps=["shared/helpers.py"],
)
```

Supported modes are:

| Mode | Use when |
|---|---|
| `repo_git` | Repository state is the right invalidation boundary. |
| `callable_module` | Unrelated repo edits should not invalidate this callable. |
| `callable_source` | Only the function source should drive code identity. |

Use `code_identity_extra_deps` when a callable-scoped mode also depends on
helper files.

### Config Identity

`config={...}` contributes to the signature. Adapters can also contribute
canonical config identity through `adapter=...`. Query-oriented facets are for
filtering and grouping; do not rely on them as a replacement for identity
inputs or config.

See [Config Management](config-management.md) for config/facet guidance.

### Input Identity

Declared `inputs={...}` contribute to lineage and input hashing. Produced
Consist artifacts link through their producing run signature. Raw files are
hashed according to the active hashing strategy.

Use `identity_inputs=[...]` for additional hash-only files or directories that
should affect the signature without being passed as callable inputs.

## Hit and Miss Behavior

On a cache hit, Consist:

- Finds a matching completed prior run.
- Hydrates the prior output artifacts into the current result.
- Preserves lineage to the producing run.
- Skips callable execution for `run(...)`-style function execution.

On a cache hit, Consist does not automatically copy every output file into the
new run directory. That only happens when a materialization policy asks for it.

On a cache miss, Consist executes the step, records a new run, logs declared
outputs, and stores enough identity metadata to explain future hits and misses.

`trace(...)` is different from function-shaped `run(...)`: the Python block is
entered so you can perform explicit lifecycle work. Use `run(...)` when
skip-on-hit callable execution is the important behavior.

## Diagnosing Cache Misses

Start with the persisted identity summary:

```python
run = tracker.get_run("run_id")
summary = run.identity_summary

print(summary["signature"])
print(summary["code_version"])
print(summary["config_hash"])
print(summary["input_hash"])
print(summary.get("identity_inputs"))
```

If a run re-executed, inspect the cache miss explanation:

```python
explanation = run.meta.get("cache_miss_explanation")

if explanation:
    print(explanation["reason"])
    print(explanation.get("candidate_run_id"))
    print(explanation.get("matched_components"))
    print(explanation.get("mismatched_components"))
    print(explanation.get("details", {}))
```

Common reasons include config drift, input drift, code drift, invalid cached
outputs, or no similar prior run.

## Hydration vs Materialization

Hydration recovers information about an artifact:

- artifact key
- producing run
- logical URI and resolved path
- metadata and format
- canonical fingerprint on `artifact.hash`

Materialization moves or recreates bytes:

- copy a cached output to a requested path
- stage an input file for a path-bound tool
- export an ingested CSV/Parquet artifact from DuckDB fallback
- restore historical outputs into a restart workspace

Default cache-hit behavior is metadata hydration. This keeps parameter sweeps
and large simulations from duplicating files unnecessarily.

Use `consist.load_df(...)`, `consist.load(...)`, or `consist.load_relation(...)`
when you need to read artifact bytes. See
[Data Materialization](data-materialization.md) for ingestion and database
fallback behavior.

## Cache Hydration Policies

Set cache-hit materialization behavior with
`CacheOptions(cache_hydration=...)` on `run(...)`, or the equivalent lifecycle
argument on lower-level APIs.

| Policy | Behavior | Typical use |
|---|---|---|
| `metadata` | Hydrate artifact records only. | Default for provenance and most analysis. |
| `inputs-missing` | Materialize missing declared inputs before execution. | Continue workflows across run directories. |
| `outputs-requested` | Materialize selected cached outputs to requested paths. | External tools need known output filenames. |
| `outputs-all` | Materialize all cached outputs under a target directory. | Build a self-contained local copy. |

For `outputs-requested`, provide explicit output paths. For `outputs-all`,
provide a target output directory. `run(...)` accepts the high-level
`output_paths={...}` pattern for requested outputs.

## Input Binding and Hydration

`input_binding` controls what the callable receives. `cache_hydration` controls
which cached bytes are made available. They often work together, but they are
not the same knob.

| Input binding | Callable receives | Hydration concern |
|---|---|---|
| `"paths"` | Local `Path` objects | Inputs must resolve to usable local files or directories. |
| `"loaded"` | Loaded Python objects | Consist can load from files or supported ingested storage. |
| `"none"` | Nothing automatically | Inputs still affect identity and lineage. |

Path-bound workflows often use `cache_hydration="inputs-missing"` when work
moves across run directories. Loaded workflows can often keep
`cache_hydration="metadata"` and load data on demand.

## Requested Input Staging

Requested input staging is input-side materialization attached to normal run
execution. Use it when a declared input has a canonical provenance identity but
the callable needs it at an exact workspace-local path.

```python
from pathlib import Path

from consist import ExecutionOptions

result = tracker.run(
    fn=run_tool,
    inputs={"config_path": config_artifact},
    outputs=["report"],
    execution_options=ExecutionOptions(
        input_binding="paths",
        input_materialization="requested",
        input_materialization_mode="copy",
        input_paths={"config_path": Path("./workspace/tool-config.yaml")},
    ),
)
```

This staging runs before execution on cache misses and before returning cached
results on cache hits. It keeps `inputs={...}` as the source of identity and
lineage while making fixed-path tools practical.

Use low-level `stage_artifact(...)` or `stage_inputs(...)` only when staging is
needed outside a run lifecycle. See [Materialization](../api/materialize.md).

## Output Recovery and Historical Hydration

For restart or archive workflows, prefer `hydrate_run_outputs(...)` over
manually copying old output paths. It returns keyed hydration records that say
which requested outputs were restored, where they landed, and whether the
resulting detached artifact is resolvable.

Recovery sources are checked in this order:

1. Per-call `source_root=...`
2. Historical source path or recorded mount root
3. `artifact.meta["recovery_roots"]`
4. DuckDB export fallback for ingested CSV/Parquet artifacts

See [Materialization](../api/materialize.md) for the detailed status meanings
and archive helpers.

## Policy Summary

| Goal | Prefer |
|---|---|
| Fast reruns without copying large outputs | `cache_hydration="metadata"` |
| Continue a workflow when cached inputs are missing locally | `cache_hydration="inputs-missing"` |
| Give a path-bound callable exact input filenames | `ExecutionOptions(input_materialization="requested", input_paths={...})` |
| Give an external consumer selected cached outputs | `cache_hydration="outputs-requested"` plus requested output paths |
| Make all cached outputs local | `cache_hydration="outputs-all"` |
| Rehydrate old run outputs into a new workspace | `hydrate_run_outputs(...)` |
| Query or recover tabular bytes from DuckDB | Ingest artifacts; see [Data Materialization](data-materialization.md) |

## Intentional Invalidation

Use explicit version knobs when a schema or semantic change should invalidate
cache even if file hashes did not change:

```python
from consist import CacheOptions, Tracker, define_step

tracker = Tracker(run_dir="runs", cache_epoch=2)


@define_step(cache_version=3)
def simulate(...):
    ...


result = tracker.run(
    fn=simulate,
    cache_options=CacheOptions(cache_version=4),
)
```

Use the narrowest invalidation boundary that matches the change: step version,
scenario epoch, or tracker epoch.

## Related Pages

- [Usage Guide](../usage-guide.md): choosing `run`, `trace`, `scenario`, input
  binding, and step references.
- [Data Materialization](data-materialization.md): ingestion, hot/cold data,
  hybrid views, and loader fallback.
- [Materialization](../api/materialize.md): input staging,
  `hydrate_run_outputs(...)`, and recovery status objects.
- [Run API](../api/run.md): accepted options and generated reference.
- [Core Concepts Overview](overview.md): one-line definitions and the mental
  model for runs, artifacts, scenarios, and couplers.
