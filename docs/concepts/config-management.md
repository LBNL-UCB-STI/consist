# Config Management: Caching vs. Filtering

When configuring a run, decide whether each parameter should trigger re-execution on change or be searchable for later filtering—or both.

---

## Decision Tree

- **Should changing this parameter re-run my model?** → `config`
- **Do I need to search/filter by this value later?** → `facet`
- **Both?** → Use both. A value can appear in config (cache invalidation) and facet (queryable).

---

## Reference Table

| Parameter     | Affects Cache? | Queryable?      | Use For                              |
|---------------|----------------|-----------------|--------------------------------------|
| `config=`     | Yes            | No (by default) | Parameters that change behavior      |
| `facet=`      | No             | Yes (indexed)   | Metadata for filtering/grouping      |
| `facet_from=` | —              | Yes             | Extract keys from config for queries |

---

## The Distinction

**Config** affects reproducibility: Consist hashes config values into the run signature. If config changes, the signature changes, and cached results do not apply—the run re-executes.

**Facet** enables filtering: Facets are indexed in DuckDB without affecting caching. Query "all runs where year=2030 and scenario='baseline'" without storing entire configs.

---

## Practical Example

Store a 100KB ActivitySim config file as `config=...` (hashes into signature; changes trigger re-runs). Extract small, queryable pieces as facets:

``` python
consist.run(
    fn=my_model,
    config={"full_config_path": "activitysim_config.yaml"},  # (1)!
    facet={"year": 2030, "mode_choice_coefficient": 0.5},   # (2)!
    inputs={...},
    outputs=[...]
)
```

1. Hashed; changes invalidate cache.
2. Queryable; doesn't affect cache.

This approach enables:

1. Queries like `mode_choice_coefficient > 0.4` without bloating the database with raw config files
2. Cache misses when the full config file changes (reproducibility)
3. Post-hoc filtering by year or coefficient

**Example**: Running demand models for 10 years (2020–2050) × 3 scenarios (baseline, transit-friendly, congestion pricing). Set `facet={"year": 2030, "scenario": "transit-friendly"}` for each run. Query `facet_year=2040` to retrieve all 2040 runs instantly—no manual search through 500 directories.

## Config Hashing

Consist uses **canonical hashing**: converting dictionaries (and YAML/JSON) into a deterministic fingerprint regardless of field order or number formatting. Both `{"a": 1, "b": 2}` and `{"b": 2, "a": 1}` produce the same hash, so cache remains stable when config dicts are built in different orders.

---

## Config Adapters

Building an integration for ActivitySim, MATSim, or similar tools requires a **config adapter**—a function that transforms the tool's native config into a Consist-compatible format.

### When to Use Each Adapter

| Adapter | Purpose | Required? |
|---------|---------|-----------|
| `canonicalize_config()` | Transform native config into a deterministic dict for hashing. Exclude irrelevant fields (logging, caches). | Yes |
| `prepare_config()` | Merge scenario parameters into native config for execution. Apply templating or defaults. | No |

If you need only one, implement `canonicalize_config()`—it is the minimum requirement for caching.

### `canonicalize_config(native_config) -> dict`

Returns a stable representation for cache signatures:

``` python
def canonicalize_config(activitysim_config_dict):
    return {
        "year": activitysim_config_dict["year"],
        "scenario": activitysim_config_dict["scenario"],
        "mode_choice_coefficients": activitysim_config_dict["coefficients"],  # (1)!
    }
```

1. Return only the fields that affect model behavior.

### `prepare_config(native_config, scenario_id, year) -> dict`

Returns the full config the tool needs at runtime:

``` python
def prepare_config(base_config, scenario_id, year):
    config = base_config.copy()
    config["scenario_id"] = scenario_id
    config["year"] = year
    return config
```

---

## See Also

- **[Caching & Hydration](caching-and-hydration.md)** — How config changes affect cache behavior
- **[Config Adapters](../integrations/config_adapters.md)** — Detailed integration guide for adapters
