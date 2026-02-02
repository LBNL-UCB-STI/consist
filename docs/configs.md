# Configuration, Identity, and Facets

This guide explains how Consist handles configuration data, how it impacts
run identity hashing, and how to make config data queryable in the database.

## Overview

Consist supports three complementary configuration channels per run:

1. **Identity config**: `config=...` (hashed into the run signature)
2. **Queryable facet**: `facet=...` (persisted in DuckDB and optionally indexed)
3. **Hash-only inputs**: `hash_inputs=[...]` (file/dir digests folded into identity)

Use `config` for the full run configuration, `facet` for a small subset of
fields you want to query, and `hash_inputs` for external config trees or
files that should affect caching without being stored as structured metadata.

## API Summary

You can pass config parameters via the high-level APIs:

- `consist.run(...)` / `consist.trace(...)`
- `Tracker.run(...)` / `Tracker.trace(...)` (explicit tracker form)
- `ScenarioContext.run(...)`
- `ScenarioContext.trace(...)`

Relevant arguments:

- `config: dict | BaseModel | None`
- `config_plan: ConfigPlan | None`
- `facet: dict | BaseModel | None`
- `facet_from: list[str] | None`
- `hash_inputs: list[Path | str | (label, Path|str)] | None`
- `facet_schema_version: str|int|None`
- `facet_index: bool` (default `True`)

## Identity Hashing

A run signature is derived from:

- the **config hash**
- the **input hash** (logged inputs)
- the **code hash**

The config hash includes:

- the `config` payload (normalized JSON)
- select run fields (`model`, `year`, `iteration`) under a reserved
  `__consist_run_fields__` key to avoid cache hits across distinct runs

This means changing `model`, `year`, or `iteration` will change the run
signature even if `config` is unchanged.

## Identity Config (`config`)

- Drives `Run.config_hash` and cache identity.
- Stored in the per-run `consist.json` snapshot under `config`.
- Not stored as structured/queryable data in DuckDB.

If you pass a Pydantic model, it is serialized via `model_dump()` for hashing
and JSON snapshotting.

## Facets (`facet` and `facet_from`)

Facets are compact, queryable config subsets persisted to DuckDB.
They are intended to be small, stable, and useful for filtering.

Facet sources:

- Explicit `facet=...`
- `facet_from=["key", ...]` extracts top-level keys from `config`
- `to_consist_facet()` on a Pydantic config model (if implemented)

Behavior notes:

- Facets are only persisted when explicitly provided, extracted via
  `facet_from`, or produced by `to_consist_facet()`.
- `facet_from` raises `KeyError` if any key is missing in `config`.
- If both `facet` and `facet_from` are provided, the extracted values are
  merged into the explicit facet, and explicit keys win.
- If `facet_schema_version` is not provided and the config model exposes
  `facet_schema_version`, Consist records that value automatically.

**Guardrails** (enforced silently; no error raised):

| Limit | Behavior when exceeded |
|-------|----------------------|
| Facet > 16 KB (canonical JSON) | Facet not persisted; run continues |
| Facet > 500 KV rows | Facet not indexed; run continues |

## Hash-Only Inputs (`hash_inputs`)

Use `hash_inputs` to include file or directory content in identity hashing
without logging inputs in provenance.

- Files are hashed via the configured `hashing_strategy`.
- Directories are hashed deterministically via a sorted walk.
- Dotfiles are ignored by default.

`hash_inputs` is **path-only** (files or directories). Artifacts are
intentionally excluded to keep identity hashing deterministic and avoid
implicit hydration.

Digests are recorded in:

- `config["__consist_hash_inputs__"]` (identity-only payload)
- `run.meta["consist_hash_inputs"]` (audit/debugging)

### When to Use Hash-Only Inputs

`hash_inputs` solves a specific problem: **Configuration files that must affect the cache key but are too large or unstructured to query in the database.**

**The Trade-off**

Normally, you pass configuration two ways:

1. **Via `config=`** — Stored in the JSON run snapshot and hashed into identity, but not queryable by default.
2. **Not tracked** — Smaller footprint, but changes to external files don't invalidate the cache, risking incorrect cache hits.

`hash_inputs` is the middle ground: **Hash the files so cache keys change when they do, but don't store the content.**

**Example: ActivitySim Configuration**

ActivitySim projects use a 5MB+ directory of YAML and csv configuration files. The config affects simulation output, so it must change the cache signature. But the YAML is:
- Too large to store as a queryable facet (exceeds the 16 KB limit)
- Unstructured (hundreds of files with complex nested keys)
- Not useful for filtering runs (you don't query "find runs where beam.memory=180")

Instead, hash the config directory:

```python
import consist
from pathlib import Path
from consist import use_tracker

asim_config_dir = Path("./configs/activitysim")

with use_tracker(tracker):
    # First run with baseline config
    result1 = consist.run(
        fn=run_activitysim,
        name="asim_baseline",
        config={"scenario": "baseline"},
        hash_inputs=[("asim_config", asim_config_dir)],
    )

# Later: You edit the YAML config (change a parameter)
# The directory hash changes → signature changes → cache miss
# ActivitySim re-runs with the new config

    # Second run with updated config
    result2 = consist.run(
        fn=run_activitysim,
        name="asim_baseline",
        config={"scenario": "baseline"},  # Same config dict
        hash_inputs=[("asim_config", asim_config_dir)],  # Different hash
    )
# Different signature (hash_inputs changed) → fresh run
```

**Comparison: config vs hash_inputs**

| Approach | Space | Queryable | Cache Behavior |
|---|---|---|---|
| `config={"yaml": large_dict}` | JSON snapshot only | No (by default) | Cache respects changes |
| `hash_inputs=[path]` | Minimal | No | Cache respects changes |
| Ignore files | Minimal | N/A | Cache miss if files change (BAD) |

For large, unstructured configs like ActivitySim YAML, use `hash_inputs`.

**Audit Trail**

Even though the file content isn't stored, Consist records the hash for debugging:

```python
run = tracker.get_run("asim_baseline_001")
print(run.meta["consist_hash_inputs"])
# Output: {"asim_config": "sha256:a1b2c3..."}
```

This lets you correlate runs with specific configuration states without storing the full config.

## Querying Facets in DuckDB

Facet persistence creates two tables:

- `config_facet`: deduplicated facet JSON blobs
- `run_config_kv`: flattened key/value index for filtering

Flattened keys use dot-notation, with dots in raw keys escaped as `\.`.

Examples:

```python
tracker.find_runs_by_facet_kv(
    namespace="beam",
    key="memory_gb",
    value_num=180,
)
```

For range queries, filter results in Python or use direct SQL on the `run_config_kv` table.

You can also fetch facets directly:

```python
facet = tracker.get_config_facet(facet_id)
```

## Model-Specific Config Adapters

For complex, file-based configurations (such as ActivitySim YAML/CSV configs),
Consist provides **config adapters**—model-specific modules that discover,
canonicalize, and ingest configuration data into queryable tables.

Unlike in-memory configs (dicts/Pydantic models), adapters handle:
- **Discovery**: Locating config files and computing content hashes
- **Canonicalization**: Converting config metadata into artifacts and ingestable schemas
- **Ingestion**: Persisting calibration-sensitive parameters as queryable tables

This decouples model-specific parsing logic from Consist core, making it easy
to add new model types without coupling them to the framework.

**Available adapters:**

- [ActivitySim Config Adapter](integrations/config_adapters_activitysim.md) — Discover, canonicalize, and query ActivitySim YAML/CSV configurations
- [BEAM Config Adapter](integrations/config_adapters_beam.md) — Canonicalize HOCON configs and query key/value parameters

For detailed usage and API reference, see the [Config Adapters Integration Guide](integrations/config_adapters.md).

## Examples

### consist.run with config + facet

```python
import consist
from consist import use_tracker

with use_tracker(tracker):
    consist.run(
        fn=run_beam_step,
        name="beam",
        config=beam_cfg,
        facet={"memory_gb": beam_cfg.memory_gb},
        hash_inputs=[("beam_hocon", beam_cfg_path)],
    )
```

### consist.run with config_plan (config adapters)

```python
from consist.integrations.activitysim import ActivitySimConfigAdapter

import consist
from consist import use_tracker

adapter = ActivitySimConfigAdapter()
plan = tracker.prepare_config(adapter, [overlay_dir, base_dir])

with use_tracker(tracker):
    consist.run(
        fn=run_activitysim,
        name="activitysim",
        config={"scenario": "baseline"},
        config_plan=plan,
        cache_mode="reuse",
    )
```

### ScenarioContext.run with facet_from

```python
import consist
from consist import use_tracker

with use_tracker(tracker):
    with consist.scenario("demo", tags=["travel"]) as sc:
        sc.run(
            fn=run_activitysim,
            name="activitysim",
            config=asim_cfg,
            facet_from=["scenario", "sample_rate"],
            hash_inputs=[("asim_yaml", asim_config_dir)],
        )
```
