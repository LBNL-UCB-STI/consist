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

- `Tracker.run(...)`
- `Tracker.trace(...)`
- `ScenarioContext.run(...)`
- `ScenarioContext.trace(...)`

Relevant arguments:

- `config: dict | BaseModel | None`
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

Guardrails:

- Facets larger than 16 KB (canonical JSON) are not persisted.
- Facets that would produce more than 500 KV rows are not indexed.

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

## Querying Facets in DuckDB

Facet persistence creates two tables:

- `config_facet`: deduplicated facet JSON blobs
- `run_config_kv`: flattened key/value index for filtering

Flattened keys use dot-notation, with dots in raw keys escaped as `\.`.

Examples:

```python
tracker.find_runs_by_facet_kv(
    key="beam.memory_gb",
    op=">=",
    value_num=180,
    namespace="beam",
)
```

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

- [ActivitySim Config Adapter](integrations/config_adapters.md#activitysim) — Discover, canonicalize, and query ActivitySim YAML/CSV configurations

For detailed usage and API reference, see the [Config Adapters Integration Guide](integrations/config_adapters.md).

## Examples

### Tracker.run with config + facet

```python
tracker.run(
    fn=run_beam_step,
    name="beam",
    config=beam_cfg,
    facet={"memory_gb": beam_cfg.memory_gb},
    hash_inputs=[("beam_hocon", beam_cfg_path)],
)
```

### ScenarioContext.run with facet_from

```python
with tracker.scenario("demo", tags=["travel"]) as sc:
    sc.run(
        fn=run_activitysim,
        name="activitysim",
        config=asim_cfg,
        facet_from=["scenario", "sample_rate"],
        hash_inputs=[("asim_yaml", asim_config_dir)],
    )
```
