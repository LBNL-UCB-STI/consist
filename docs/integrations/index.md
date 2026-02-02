# Integrations

These integrations extend Consist for container execution, data ingestion, and configuration management.

## When to Use Each Integration

| Integration | Use when |
|-------------|----------|
| **Container** | Wrapping existing tools (ActivitySim, SUMO, BEAM) without source modification |
| **DLT Loader** | Cross-run SQL queries on 100K+ row datasets with schema validation |
| **Config Adapters** | Tracking file-based configs (YAML/HOCON hierarchies) with queryable parameters |

---

## Container Integration

Execute Docker and Singularity containers with provenance tracking.

- Image digest-based caching for reproducibility
- Volume mounting and output capture
- [Full guide](../containers-guide.md) | [API reference](containers.md)

---

## DLT Loader

Schema-validated data ingestion with DuckDB.

- Automatic provenance column injection (`consist_run_id`, etc.)
- Cross-run SQL queries with registered schemas
- [Full guide](../dlt-loader-guide.md) | [API reference](dlt_loader.md)

---

## Config Adapters

Model-specific configuration discovery and tracking.

- Automatic discovery of YAML/HOCON hierarchies
- Queryable configuration tables for sensitivity analysis
- [Full guide](config_adapters.md) | [ActivitySim](config_adapters_activitysim.md) | [BEAM](config_adapters_beam.md)
