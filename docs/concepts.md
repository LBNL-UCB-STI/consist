# Concepts

This page establishes the core mental model for Consist before diving into API details.

## Core abstractions

- **Run**: A single execution with tracked inputs, config, and outputs. Runs are identified by a signature that lets Consist reuse prior results.
- **Artifact**: A file produced or consumed by a run. Artifacts carry provenance metadata such as content hash and creator run.
- **Scenario**: A collection of related runs (an experiment or study) that lets you compare variants cleanly.
- **Coupler**: A lightweight helper that passes artifacts between steps in a scenario so each step records lineage without manual wiring.

## How caching works

Consist computes a signature from the code version, config, and input artifact hashes:

```text
signature = hash(code_version + config + input_hashes)
```

- Same signature: return cached outputs.
- Different signature: execute and record new lineage.

This means you can safely re-run a workflow with the same inputs and config without redoing work, while still getting new results when anything changes.
On cache hits, Consist returns output artifact metadata without copying files; load or hydrate outputs only when you need bytes.

## The config vs facet distinction

This is a key concept for making runs both reproducible and queryable.

When you configure a run, you make a choice: should this parameter trigger a re-run if it changes (cache invalidation), or should it be searchable so you can find "all runs where this parameter had that value"?

**Config** (affects reproducibility): Parameters that change behavior. Consist hashes config values into your run's signature. If config changes, the signature changes, and cached results don't apply—you must re-run. Use this for anything that should invalidate the cache.

**Facet** (enables filtering): Queryable metadata you want to search by. Facets are stored in the database without affecting caching. You can ask "show all runs where year=2030 and scenario='baseline'" without storing the entire config. Use this when you want analytics without cache invalidation.

**Practical example**: You have a 100KB ActivitySim config file. Store the whole file as `config=...` (it hashes into the signature, so changes trigger re-runs). But extract small, queryable pieces: `facet={"year": 2030, "mode_choice_coefficient": 0.5}`. Now you can search "find runs where coefficient > 0.4" without bloating the database with raw csv inputs.

**Quick decision tree:**
- "Should changing this parameter re-run my model?" → `config`
- "Do I want to search/filter by this value later?" → `facet`
- "Both?" → Use both. A value can be in config (cache invalidation) and facet (queryable).

| Parameter     | Affects Cache? | Queryable?      | Use For                                        |
|---------------|----------------|-----------------|------------------------------------------------|
| `config=`     | Yes            | No (by default) | Parameters that change behavior                |
| `facet=`      | No             | Yes (indexed)   | Metadata for filtering/grouping                |
| `facet_from=` | —              | Yes             | Extract specific keys from config for querying |

## How inputs and outputs are treated

- **Inputs** are files or values that influence computation. File inputs are hashed by content to ensure the signature reflects the actual data.
- **Outputs** are named artifacts you declare when you call `consist.run(...)` (or `tracker.run(...)`). Consist stores their paths and provenance metadata for later lookup and querying.

To keep outputs portable, write them under `tracker.run_dir` or a mounted `outputs://` root. That keeps artifact paths relative and consistent across machines.

### Input mappings and auto-loading

Inputs can be passed as a list (hash-only) or a mapping (hash + parameter injection). When you use a mapping, Consist matches input keys to function parameters and auto-loads those artifacts into the call by default. If you want to pass raw paths instead, set `load_inputs=False` and pass the paths explicitly (for example, via `runtime_kwargs`).

Concrete example:

```python
import pandas as pd

def summarize_trips(trips_df):
    return trips_df["distance_miles"].mean()

# Auto-loading: mapping keys match function params, so Consist loads the DataFrame.
result = consist.run(
    fn=summarize_trips,
    inputs={"trips_df": trips_artifact},
)

def summarize_trips_from_path(trips_path: str):
    df = pd.read_parquet(trips_path)
    return df["distance_miles"].mean()

# No auto-loading: you get the raw path and load it yourself.
result = consist.run(
    fn=summarize_trips_from_path,
    inputs={"trips_path": trips_artifact},
    load_inputs=False,
    runtime_kwargs={"trips_path": trips_artifact.path},
)
```

## When to use each pattern

- `consist.run(...)` (or `tracker.run(...)`) for standalone steps or when calling library functions.
- `scenario.trace(...)` for inline code blocks within a workflow.
- `scenario.run(...)` when you want cache-skip behavior (the function does not execute on cache hit).

If you are unsure, start with `consist.run(...)` inside a `use_tracker(...)` scope. It is the simplest pattern and works well for most first-time use cases.
