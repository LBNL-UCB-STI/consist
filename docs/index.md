---
hide:
  - navigation
---

# Consist

Consist automatically records what code, configuration, and input data produced each result in your simulation pipeline. It uses this to avoid redundant computation, lets you query results across runs, and traces lineage from any output back to its sources.

---

## Getting Started

**New to Consist?** Follow this path:

### 1. **[Install & Quickstart](getting-started/installation.md)**

   * Install Consist from source and run a small first example
   * Creates your first run and provenance database

### 2. **[Concepts](concepts.md)**

   * Learn the mental model: Runs, Artifacts, Scenarios, and how caching works
   * Start here to understand core ideas

### 3. **[Usage Guide](usage-guide.md)**

   * Build your first multi-step workflow
   * Learn patterns for runs, scenarios, and cross-run querying

### 4. **[Example Notebooks](examples.md)**

   * Run the quickstart notebook to see caching in action
   * Follow other examples for your domain (parameter sweeps, iterative workflows, multi-scenario analysis)

---

## By User Type

**I develop or maintain simulation tools (ActivitySim, MATSim, etc.):**
- Start with [Concepts](concepts.md) for mental models
- Read [Usage Guide](usage-guide.md) for API patterns and integration examples
- See the [Container Integration Guide](containers-guide.md) for wrapping legacy code

**I'm an MPO official or practitioner:**
- If you prefer not to write Python, use the [CLI reference](cli-reference.md) to query and compare results from the command line
- Example: `consist lineage traffic_volumes` shows what produced your result

**I'm building research workflows:**
- Start with [Concepts](concepts.md)
- Read [Ingestion & Hybrid Views](ingestion-and-hybrid-views.md) for SQL-native analysis
- See [Mounts & Portability](mounts-and-portability.md) for reproducible, shareable studies

---

## By Topic

### Beginner

- [Concepts](concepts.md) – Mental models and core ideas
- [Caching Fundamentals](caching-fundamentals.md) – How signature-based caching works
- [Usage Guide: Basic Runs & Scenarios](usage-guide.md#choosing-your-pattern) – Simple workflows
- [Example: Quickstart](examples.md#quickstart) – 5-minute first run

### Intermediate

- [Configuration & Facets](configs.md) – Config hashing and queryable metadata
- [Caching & Hydration](caching-and-hydration.md) – Caching patterns and data management policies
- [Ingestion & Hybrid Views](ingestion-and-hybrid-views.md) – SQL queries across runs
- [DLT Loader Integration](dlt-loader-guide.md) – Schema-validated data ingestion with DuckDB
- [Mounts & Portability](mounts-and-portability.md) – Reproducible workflows with external data

### Advanced / Reference

- [Architecture](architecture.md) – Implementation details and design decisions
- [Container Integration](containers-guide.md) – Docker/Singularity with ActivitySim, SUMO, and other tools
- [CLI Reference](cli-reference.md) – All command-line tools
- [Troubleshooting](troubleshooting.md) – Common errors, debugging, and solutions
- [API Overview](api/index.md) – Function and class reference

---

## Key Terms

- **Run**: A single execution with tracked inputs, config, and outputs. Runs are the unit of cache reuse and comparison, so keeping them consistent lets you skip expensive recomputation.
- **Artifact**: A file with provenance metadata attached. Artifacts are the concrete outputs you can trace, load, or hydrate later, which is how Consist keeps results reproducible.
- **Signature**: Fingerprint of code + config + inputs. Identical signatures lead to a cache hit.
- **Scenario**: A parent run grouping related child runs
- **Provenance**: Complete history of where a result came from

See [Glossary](glossary.md) for full definitions of all terms, or [Concepts](concepts.md) for detailed explanations with examples.

---

## Common Tasks

**I need to speed up my pipeline:**
→ [Caching & Hydration](caching-and-hydration.md)

**I want to debug a cache miss or other issue:**
→ [Troubleshooting](troubleshooting.md)

**I want to know which config produced a result:**
→ [CLI: `consist lineage`](cli-reference.md#consist-lineage)

**I want to compare results across 50 scenarios:**
→ [Usage Guide: Cross-Run Queries](usage-guide.md#querying-results)

**I want to ingest data for SQL analysis across runs:**
→ [DLT Loader Integration](dlt-loader-guide.md)

**I want to share my study for reproducibility:**
→ [Mounts & Portability](mounts-and-portability.md)

**I want to integrate with an existing tool (ActivitySim, SUMO, BEAM):**
→ [Container Integration Guide](containers-guide.md)
