
# Config Adapters

Config adapters provide model-specific interfaces to discover, canonicalize, and
ingest complex file-based configurations. They enable tracking of calibration-sensitive
parameters as queryable database tables while preserving full config provenance via artifacts.

## Overview

### When to Use Config Adapters

Use config adapters when your model configuration:

- Lives in **multiple files** across a directory hierarchy (YAML, CSV, HOCON, etc.)
- Has **layered inheritance** or cascade logic (e.g., `inherit_settings: true`)
- Contains **calibration-sensitive parameters** you want to query and compare across runs
- Needs **content hashing** for provenance and cache identity
- Should remain **decoupled** from Consist core (no hard dependency on your model's libraries)

### When to Use In-Memory Configs Instead

Use the standard [config API](../configs.md) if:

- Configuration fits in a Python dict or Pydantic model
- You don't need to track file-based config artifacts
- A simple facet extraction suffices for your use case

### Architecture

Config adapters implement three phases:

```
Config Directory
    ↓
discover()           ← Locate files, compute content hash
    ↓
CanonicalConfig
    ↓
canonicalize()       ← Generate artifact specs + ingest specs
    ↓
CanonicalizationResult (artifacts + ingestables)
    ↓
tracker.canonicalize_config()  ← Log artifacts, ingest to DB
    ↓
Queryable Tables + Full Provenance
```

---

## Adapter Guides

Detailed, model-specific implementations live here:

- [ActivitySim Config Adapter](config_adapters_activitysim.md)
- [BEAM Config Adapter](config_adapters_beam.md)

These guides include discovery/canonicalization behaviors, ingestion schemas, and
query examples. The API patterns are shared: you construct an adapter, call
`tracker.canonicalize_config(...)` or `tracker.prepare_config(...)`, and then query
the resulting cache tables.

### Extensibility

Config adapters follow a Protocol interface, making it straightforward to add support
for other models. Future adapters might include:

- **Additional HOCON/YAML models**: Layered config discovery + calibration-sensitive key extraction
- **Custom Models**: Any model with file-based config that needs parameterization tracking

If you'd like to add an adapter for your own model, consult the
[ActivitySim adapter source](https://github.com/LBNL-UCB-STI/consist/tree/main/src/consist/integrations/activitysim)
as a reference implementation.

---

## See Also

- [Configuration, Identity, and Facets](../configs.md) — In-memory config and facets
- [Ingestion and Hybrid Views](../ingestion-and-hybrid-views.md) — DLT-based data ingestion
- [Caching and Hydration](../caching-and-hydration.md) — Run signature and cache identity
