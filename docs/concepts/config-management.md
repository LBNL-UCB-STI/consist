# Config, Facets, and Identity Inputs

When configuring a run, decide whether each parameter should trigger
re-execution on change, be searchable for later filtering, or both.

---

## Decision Tree

- **Should changing this parameter re-run my model?** → `config`
- **Do I need to search/filter by this value later?** → `facet`
- **Both?** → Use both. A value can appear in `config` (cache invalidation) and `facet` (queryable).
- **Large external config files that must affect the cache key?** → `identity_inputs`

---

## Reference Table

| Parameter        | Affects Cache? | Queryable?      | Use For                                          |
|------------------|----------------|-----------------|--------------------------------------------------|
| `config=`        | Yes            | No (by default) | Parameters that change behavior                  |
| `facet=`         | No             | Yes (indexed)   | Metadata for filtering and grouping              |
| `facet_from=`    | —              | Yes             | Extract keys from `config` for queries           |
| `identity_inputs=` | Yes          | No              | Large or file-based configs that affect identity |

---

## The `config` Argument

`config` accepts a plain `dict` or a Pydantic model. It is:

- **Hashed** into the run signature (cache identity).
- **Stored** in the per-run `consist.json` snapshot under `config`.
- **Not** stored as structured/queryable data in DuckDB by default.

If you pass a Pydantic model, it is serialized via `model_dump()` before hashing
and snapshotting.

### Config Hashing

Consist uses **canonical hashing**: converting dicts (and YAML/JSON) into a
deterministic fingerprint regardless of field order or number formatting.
Both `{"a": 1, "b": 2}` and `{"b": 2, "a": 1}` produce the same hash, so
the cache stays stable when config dicts are built in different orders.

The run signature is derived from:

- the **config hash** (the `config` payload, normalized)
- the **input hash** (logged inputs)
- the **code hash**

A few run-level fields — `model`, `year`, and `iteration` — are also folded
into the config hash under a reserved `__consist_run_fields__` key. When set,
`cache_epoch` and `cache_version` are included there too. This means changing
those values will change the signature even if `config` is otherwise identical.

---

## Facets

Facets are compact, queryable config subsets persisted to DuckDB. They do not
affect the cache key — they exist purely for filtering and grouping runs after
the fact.

**Sources:**

- `facet={"key": value, ...}` — explicit facet payload
- `facet_from=["key", ...]` — extracts top-level keys from `config` by name
- `to_consist_facet()` on a Pydantic model — if implemented

If both `facet` and `facet_from` are provided, extracted values are merged into
the explicit facet dict; explicit keys win on collision. `facet_from` raises
`KeyError` if any listed key is missing from `config`.

### Guardrails

| Limit | Behavior when exceeded |
|-------|------------------------|
| Facet > 16 KB (canonical JSON) | Facet not persisted; run continues |
| Facet > 500 KV rows | Facet not indexed; run continues |

No error is raised — Consist degrades silently to avoid blocking runs.

### Querying Facets in DuckDB

Facet persistence creates indexed tables:

- `config_facet` — deduplicated facet JSON blobs
- `run_config_kv` — flattened key/value index for filtering
- `artifact_facet` — artifact-level facet blobs
- `artifact_kv` — flattened scalar key/value index for artifact filtering

Flattened keys use dot-notation; dots in raw key names are escaped as `\.`.

```python
tracker.find_runs_by_facet_kv(
    namespace="beam",
    key="memory_gb",
    value_num=180,
)

# Or fetch a full facet blob by ID:
facet = tracker.get_config_facet(facet_id)
```

### Practical Example

```python
consist.run(
    fn=my_model,
    config={"full_config_path": "activitysim_config.yaml"},  # (1)!
    facet={"year": 2030, "mode_choice_coefficient": 0.5},    # (2)!
    inputs={...},
    outputs=[...],
)
```

1. Hashed into cache key; changes trigger re-runs.
2. Queryable in DuckDB; does not affect the cache key.

This enables queries like `mode_choice_coefficient > 0.4` without bloating
the database with raw config files, while still invalidating the cache
when the full config changes.

---

## Identity Inputs

`identity_inputs` solves a specific problem: **configuration files that must
affect the cache key but are too large or unstructured to store as queryable
metadata.**

Use `identity_inputs` to include file or directory content in the run signature
without logging those files as provenance inputs.

- Files are hashed via the configured `hashing_strategy`.
- Directories are hashed deterministically via a sorted walk.
- Dotfiles are ignored by default.
- Artifacts are intentionally excluded — `identity_inputs` is path-only.

### When to Use Identity Inputs

Normally, large external configs fall into one of two unsatisfying patterns:

| Approach | Space | Queryable | Cache behavior |
|---|---|---|---|
| `config={"yaml": large_dict}` | JSON snapshot only | No (by default) | Respects changes |
| Not tracked at all | None | No | **Stale cache if files change** |

`identity_inputs` is the middle ground: hash the files so the cache key changes
when they do, without storing the file content.

**Example: ActivitySim**

ActivitySim projects use a 5 MB+ directory of YAML and CSV config files.
The config affects output, so changes must invalidate the cache. But the YAML
is too large for a facet (exceeds 16 KB), unstructured, and not useful for
database filtering.

```python
asim_config_dir = Path("./configs/activitysim")

with use_tracker(tracker):
    result = consist.run(
        fn=run_activitysim,
        name="asim_baseline",
        config={"scenario": "baseline"},
        identity_inputs=[("asim_config", asim_config_dir)],  # (1)!
    )
```

1. The directory is hashed. If any file inside changes, the signature changes
   and ActivitySim re-runs.

### Concise and Labeled Forms

Each entry in `identity_inputs` can be a bare path or a labeled tuple:

```python
# Concise: bare paths (labels auto-derived from the project-relative path when possible)
identity_inputs = [config_root, coeffs_csv]

# Explicit: labeled tuples (stable names in identity summaries)
identity_inputs = [
    ("asim_config", config_root),
    ("tour_mode_coeffs", coeffs_csv),
]
```

### Audit Trail

Even though file content is not stored, Consist records the hash for debugging:

```python
run = tracker.get_run("asim_baseline_001")
print(run.meta["consist_hash_inputs"])
# {"asim_config": "a1b2c3..."}
print(run.identity_summary["identity_inputs"])
```

---

## ConfigAdapters

`ConfigAdapter` is an integration plugin for tools with complex, file-based
configuration — ActivitySim YAML, BEAM HOCON, MATSim XML, and similar. An
adapter handles discovery, canonicalization, and optional ingestion of
model-specific config into queryable tables.

Most users do not need to write or use a `ConfigAdapter`. If your config fits
in a `dict` or Pydantic model, `config=` is sufficient.

When you do need one, pass it via `adapter=`:

```python
from consist.integrations.activitysim import ActivitySimConfigAdapter

adapter = ActivitySimConfigAdapter(root_dirs=[overlay_dir])

consist.run(
    fn=run_activitysim,
    name="activitysim",
    config={"scenario": "baseline"},
    adapter=adapter,
    identity_inputs=[("asim_config", overlay_dir)],
)
```

For details on writing or using adapters, see the
[Config Adapters Integration Guide](../integrations/config_adapters.md).

---

## Examples

### `run` with config and facet

```python
import consist
from consist import use_tracker

with use_tracker(tracker):
    consist.run(
        fn=run_beam_step,
        name="beam",
        config=beam_cfg,
        facet={"memory_gb": beam_cfg.memory_gb},
        identity_inputs=[("beam_hocon", beam_cfg_path)],
    )
```

### Scenario run with `facet_from`

```python
with use_tracker(tracker):
    with consist.scenario("demo", tags=["travel"]) as sc:
        sc.run(
            fn=run_activitysim,
            name="activitysim",
            config=asim_cfg,
            facet_from=["scenario", "sample_rate"],
            identity_inputs=[("asim_yaml", asim_config_dir)],
        )
```

---

## See Also

- **[Caching & Hydration](caching-and-hydration.md)** — How config changes affect cache behavior
- **[Config Adapters](../integrations/config_adapters.md)** — Integration guide for ActivitySim, BEAM, and custom adapters
