---
icon: lucide/puzzle
---

# Integrations

These integrations extend Consist's capabilities for container execution, data ingestion, and configuration management.

## Core Integrations

### [Container Integration](../containers-guide.md)

Execute Docker and Singularity containers with provenance tracking.

- Run ActivitySim, SUMO, BEAM, and other existing tools
- Image digest-based caching for reproducibility
- Covers volume mounting, output handling, and debugging
- [Full guide](../containers-guide.md) | [API reference](containers.md)

### [DLT Loader](../dlt-loader-guide.md)

Schema-validated data ingestion with DuckDB.

- Ingest Parquet, CSV, DataFrames with type enforcement
- Automatic provenance column injection
- Cross-run SQL queries with registered schemas
- [Full guide](../dlt-loader-guide.md) | [API reference](dlt_loader.md)

### [Config Adapters](config_adapters.md)

Model-specific configuration discovery and tracking.

- ActivitySim: Track and query calibration parameters across runs
- Automatic discovery of YAML hierarchies and CSV references
- Queryable configuration tables for sensitivity analysis
- [Full guide](config_adapters.md)
