# Config Adapters

Config adapters provide model-specific interfaces to discover, canonicalize, and ingest complex file-based configurations. They enable tracking of calibration-sensitive parameters as queryable database tables while preserving full config provenance via artifacts.

## Decision Guide

| Criterion | Use Config Adapters | Use In-Memory Config |
|-----------|--------------------|--------------------|
| Config location | Multiple files (YAML, CSV, HOCON) | Python dict or Pydantic model |
| Config structure | Layered inheritance, file hierarchies | Flat or simple nesting |
| Queryable parameters | Calibration-sensitive values | Simple facets suffice |
| File tracking | Need content hashing per file | No file artifacts needed |

For in-memory configuration, see [Configuration, Identity, and Facets](../configs.md).

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
