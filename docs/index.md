---
hide:
  - navigation
---
# 
<p align="center">
  <img src="assets/logo.png" alt="Consist" width="320" class="logo-light">
</p>

<p align="center" class="tagline">
  Track provenance. Skip redundant computation. Query results across runs.
</p>

---

## What is Consist?

Consist is a Python library for **automatic provenance tracking and intelligent caching** in scientific simulation workflows.

**Problem**: Simulation pipelines are expensive (often 24-48 hour runs), produce massive files, and require exact reproducibility. When you re-run with the same inputs, you waste days recomputing identical results. Without provenance tracking, you can't answer "which configuration produced this forecast?" or "did Figure 3 come from v1.2 or v1.3?"

**Solution**: Consist automatically:

- Tracks **provenance**: what code version, configuration, and inputs produced each result
- Detects **redundant computation**: reuses results when code + config + inputs match (cache hits)
- Enables **SQL queries** across runs: ingest results into DuckDB for analytics without loading everything into memory
- Keeps workflows **portable**: run the same pipeline on different machines and it "just works"

---

## Getting Started

Choose your path:

**New to provenance tracking?**

1. [Install & Quickstart](getting-started/installation.md) — 5 minutes
2. [Core Concepts](concepts/overview.md) — Mental model
3. [First Workflow](getting-started/first-workflow.md) — Multi-step pipeline
4. [Examples](examples.md) — Runnable notebooks

**Integrating Consist into a tool (ActivitySim, MATSim, BEAM)?**

1. [Core Concepts](concepts/overview.md)
2. [Architecture](architecture.md) — System design
3. [Config Adapters](integrations/config_adapters.md) — Write an adapter
4. [Containers](containers-guide.md) — If tracking Docker/Singularity

**Optimizing performance or debugging cache issues?**

1. [Caching & Hydration](concepts/caching-and-hydration.md)
2. [Troubleshooting](troubleshooting.md)
3. [CLI Reference](cli-reference.md)

---

## By User Type

<div class="grid cards" markdown>

-   **Simulation developers**

    ---

    Building or maintaining tools like ActivitySim, MATSim, BEAM?

    → [Core Concepts](concepts/overview.md) | [Architecture](architecture.md) | [Config Adapters](integrations/config_adapters.md)

-   **Practitioners & MPO staff**

    ---

    Prefer command-line over Python?

    → [CLI Reference](cli-reference.md) — query and compare results without code

-   **Researchers**

    ---

    Building reproducible workflows for publication?

    → [Core Concepts](concepts/overview.md) | [Data Materialization](concepts/data-materialization.md) | [Mounts & Portability](mounts-and-portability.md)

</div>

---

## By Topic

### Beginner

- [Core Concepts](concepts/overview.md) – Mental models and core ideas
- [Quickstart](getting-started/quickstart.md) – 5-minute first run
- [First Workflow](getting-started/first-workflow.md) – Build a multi-step pipeline
- [Example Notebooks](examples.md) – Runnable examples with explanations

### Intermediate

- [Config Management](concepts/config-management.md) – Config hashing and queryable metadata (facets)
- [Caching & Hydration](concepts/caching-and-hydration.md) – Caching patterns and data recovery
- [Data Materialization Strategy](concepts/data-materialization.md) – When to ingest data and use hybrid views
- [DLT Loader Integration](dlt-loader-guide.md) – Schema-validated data ingestion
- [Mounts & Portability](mounts-and-portability.md) – Reproducible workflows across machines

### Advanced

- [Architecture](architecture.md) – System design and implementation details
- [Container Integration](containers-guide.md) – Docker/Singularity support
- [Config Adapters](integrations/config_adapters.md) – Building adapters for external tools
- [CLI Reference](cli-reference.md) – All command-line tools
- [Troubleshooting](troubleshooting.md) – Common issues and solutions
- [FAQ](faq.md) – Frequently asked questions
- [API Reference](api/index.md) – Complete function and class documentation

---

## Common Tasks

| I want to... | Go to |
|--------------|-------|
| Speed up my pipeline | [Caching & Hydration](concepts/caching-and-hydration.md) |
| Debug a cache miss | [Troubleshooting](troubleshooting.md) |
| Find which config produced a result | [`consist lineage`](cli-reference.md#consist-lineage) |
| Compare results across scenarios | [Data Materialization](concepts/data-materialization.md) |
| Ingest data for SQL analysis | [Data Materialization](concepts/data-materialization.md) |
| Understand config vs. facets | [Config Management](concepts/config-management.md) |
| Share a reproducible study | [Mounts & Portability](mounts-and-portability.md) |
| Integrate with ActivitySim/BEAM/MATSim | [Config Adapters](integrations/config_adapters.md) or [Containers](containers-guide.md) |

---

## Built on Open Standards

Consist relies on modern, high-performance data engineering tools:

- **[DuckDB](https://duckdb.org/)**: The "SQLite for Analytics" powers our lightning-fast provenance queries and data virtualization.
- **[SQLModel](https://sqlmodel.tiangolo.com/)**: Combines SQLAlchemy and Pydantic for robust, type-safe data modeling and schema validation.
- **[DLT (Data Load Tool)](https://dlthub.com/)**: Handles robust, schema-aware data ingestion from diverse sources into your provenance database.
- **[Apache Parquet & Zarr](https://parquet.apache.org/)**: Industry-standard formats for efficient, compressed storage of tabular and multi-dimensional scientific data.

---

## Learn More

See [Core Concepts](concepts/overview.md) for a complete mental model, or [Glossary](glossary.md) for quick term definitions.
