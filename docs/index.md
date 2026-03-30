---
hide:
  - navigation
  - title
---

<div id="home-page"></div>

<div class="home-hero">
  <picture class="home-hero__logo">
    <source media="(prefers-color-scheme: dark)" srcset="assets/logo-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="assets/logo.png">
    <img src="assets/logo.png" alt="Consist" width="320">
  </picture>
  <p class="tagline">Provenance tracking and intelligent caching for scientific simulation workflows.</p>
</div>

## Start Here

Follow this path in order if you are new to Consist:

1. [Installation](getting-started/installation.md)
2. [Quickstart](getting-started/quickstart.md)
3. [First Workflow](getting-started/first-workflow.md)
4. [Core Concepts](concepts/overview.md)
5. [Usage Guide](usage-guide.md)
6. [Example Gallery](examples.md)
7. [Advanced Usage](advanced/index.md)

This path takes you from a fresh environment to a working multi-step cached
pipeline, then into deeper usage patterns.

!!! note

    - Python 3.11+
    - Base install:
    `pip install consist`
    - For the first workflow tutorial (Parquet writes):
    run `pip install "consist[parquet]"`
    - See [Installation](getting-started/installation.md) for complete options,
    including source installs and optional extras.

## What is Consist?

Consist is a Python library for **provenance tracking and intelligent caching**
in scientific simulation workflows. Tasks are ordinary Python functions;
Consist records lineage without restructuring your code or introducing implicit
dependencies.

It helps you:

- Answer "what exactly produced this result?"—code version, config, and inputs, all queryable after the fact
- Skip redundant computation: cache hits fire automatically when code, config, and inputs are unchanged
- Wire multi-step pipelines explicitly via artifact references, not name-based injection or global state
- Query and compare results across runs using DuckDB-backed SQL
- Keep pipelines portable across machines via URI + mount resolution

## Secondary Navigation

After completing the onboarding path above, use these role/topic guides for
deeper work.

=== "By Role"

    - **Simulation developers**: [Architecture](architecture.md),
      [Config Adapters](integrations/config_adapters.md),
      [Container Integration](containers-guide.md)
    - **Pipeline operators**: [CLI Reference](cli-reference.md),
      [DB Maintenance Guide](db-maintenance.md),
      [Troubleshooting](troubleshooting.md)
    - **Researchers**: [Data Materialization](concepts/data-materialization.md),
      [Mounts & Portability](mounts-and-portability.md),
      [Glossary](glossary.md)

=== "By Topic"

    - **Caching and reuse**: [Caching & Hydration](concepts/caching-and-hydration.md)
    - **Configuration and identity**: [Config Management](concepts/config-management.md)
    - **SQL analytics and ingestion**:
      [Data Materialization](concepts/data-materialization.md),
      [DLT Loader Guide](dlt-loader-guide.md), [Schema Export](schema-export.md)
    - **Workflow patterns**: [Usage Guide](usage-guide.md),
      [Workflow Contexts API](api/workflow.md)
    - **Programmatic API**: [API Reference](api/index.md)

## Common follow-up tasks

| I want to...                           | Go to                                                                                   |
|----------------------------------------|-----------------------------------------------------------------------------------------|
| Speed up my pipeline                   | [Caching & Hydration](concepts/caching-and-hydration.md)                                |
| Debug a cache miss                     | [Troubleshooting](troubleshooting.md)                                                   |
| Operate or repair the provenance DB    | [DB Maintenance Guide](db-maintenance.md)                                               |
| Find which config produced a result    | [`consist lineage`](cli-reference.md#consist-lineage)                                   |
| Compare results across scenarios       | [Data Materialization](concepts/data-materialization.md)                                |
| Ingest data for SQL analysis           | [Data Materialization](concepts/data-materialization.md)                                |
| Understand config vs. facets           | [Config Management](concepts/config-management.md)                                      |
| Share a reproducible study             | [Mounts & Portability](mounts-and-portability.md)                                       |
| Integrate with ActivitySim/BEAM/MATSim | [Config Adapters](integrations/config_adapters.md) or [Containers](containers-guide.md) |

## Built on Open Standards

Consist relies on modern, high-performance data engineering tools:

- **[DuckDB](https://duckdb.org/)**: A fast and powerful embedded database that enables provenance queries and data 
  virtualization.
- **[SQLModel](https://sqlmodel.tiangolo.com/)**: Combines SQLAlchemy and Pydantic for robust, type-safe data modeling
  and schema validation.
- **[DLT (Data Load Tool)](https://dlthub.com/)**: Handles robust, schema-aware data ingestion from diverse sources into
  your provenance database.
- **[Apache Parquet & Zarr](https://parquet.apache.org/)**: Industry-standard formats for efficient, compressed storage
  of tabular and multi-dimensional scientific data.

## Learn More

See [Core Concepts](concepts/overview.md) for a complete mental model, or [Glossary](glossary.md) for quick term
definitions.
