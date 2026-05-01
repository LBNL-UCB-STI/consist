# Config Adapters

Config adapters provide model-specific interfaces to discover, canonicalize, and ingest complex file-based configurations. They enable tracking of calibration-sensitive parameters as queryable database tables while preserving full config provenance via artifacts.

## Decision Guide

| Criterion | Use Config Adapters | Use In-Memory Config |
|-----------|--------------------|--------------------|
| Config location | Multiple files (YAML, CSV, HOCON) | Python dict or Pydantic model |
| Config structure | Layered inheritance, file hierarchies | Flat or simple nesting |
| Queryable parameters | Calibration-sensitive values | Simple facets suffice |
| File tracking | Need content hashing per file | No file artifacts needed |

For in-memory configuration, see [Config, Facets, and Identity Inputs](../concepts/config-management.md).

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
CanonicalizationResult (identity + artifacts + ingestables)
    ↓
tracker.canonicalize_config()  ← Log artifacts, ingest to DB
    ↓
Queryable Tables + Full Provenance
```

Adapter-backed runs also persist `run.meta["config_identity_manifest"]`, a
structured manifest of the adapter identity inputs. This is part of the adapter
contract: custom adapters must return `CanonicalizationResult(..., identity=...)`.
Cache-miss explanations use that manifest before falling back to facets or JSON
snapshots, so config misses can point to changed, added, or removed config
references and files, reference status changes such as `missing_optional` to
`resolved`, and changed adapter identity options.

Config identity is still separate from runtime materialization. If a container
mounts a broad mutable directory whose meaningful children are already declared
as artifacts, that directory de-duplication belongs to the container/input
identity layer rather than the config adapter manifest.

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

- [Config, Facets, and Identity Inputs](../concepts/config-management.md) — In-memory config, facets, and identity inputs
- [Data Virtualization & Materialization Strategy](../concepts/data-materialization.md) — DLT-based data ingestion
- [Caching and Artifact Hydration Patterns](../concepts/caching-and-hydration.md) — Run signature and signature components
