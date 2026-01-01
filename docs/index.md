# Consist

Consist automatically records what code, configuration, and input data produced each result in your simulation pipeline. It uses this to skip redundant computation, let you query results across runs, and trace lineage from any output back to its sources.

---

## Getting Started (Start Here!)

**New to Consist?** Follow this path:

1. **[Concepts](concepts.md)** (5 min read)
   - Learn the mental model: Runs, Artifacts, Scenarios, and how caching works
   - Start here to understand core ideas

2. **[Usage Guide](usage-guide.md)** (20 min read)
   - Build your first multi-step workflow
   - Learn patterns for runs, scenarios, and cross-run querying

3. **[Example Notebooks](examples.md)**
   - Run the quickstart notebook to see caching in action
   - Follow other examples for your domain (parameter sweeps, iterative workflows, multi-scenario analysis)

---

## By User Type

**I develop or maintain simulation tools (ActivitySim, SUMO, etc.):**
- Start with [Concepts](concepts.md) for mental models
- Read [Usage Guide](usage-guide.md) for API patterns and integration examples
- See [Container Support](usage-guide.md#container-support) for wrapping legacy code

**I'm an MPO official or practitioner:**
- Skip code examples. See [CLI reference](cli-reference.md) to query and compare results from the command line
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
- [Usage Guide: Basic Runs & Scenarios](usage-guide.md#runs-and-outputs) – Simple workflows
- [Example: Quickstart](examples.md#00_quickstart) – 5-minute first run

### Intermediate

- [Configuration & Facets](configs.md) – Config hashing and queryable metadata
- [Caching & Hydration](caching-and-hydration.md) – Caching patterns and data management policies
- [Ingestion & Hybrid Views](ingestion-and-hybrid-views.md) – SQL queries across runs
- [Mounts & Portability](mounts-and-portability.md) – Reproducible workflows with external data

### Advanced / Reference

- [Architecture](architecture.md) – Implementation details and design decisions
- [CLI Reference](cli-reference.md) – All command-line tools
- [API Overview](api/index.md) – Function and class reference

---

## Key Terms

- **Run**: A single execution with tracked inputs, config, and outputs
- **Artifact**: A file with provenance metadata attached
- **Signature**: Fingerprint of code + config + inputs. Identical signatures = cache hit
- **Scenario**: A parent run grouping related child runs
- **Provenance**: Complete history of where a result came from

See [Glossary](glossary.md) for full definitions of all terms, or [Concepts](concepts.md) for detailed explanations with examples.

---

## Common Tasks

**I need to speed up my pipeline:**
→ [Caching & Hydration](caching-and-hydration.md)

**I want to know which config produced a result:**
→ [CLI: `consist lineage`](cli-reference.md#trace-lineage)

**I want to compare results across 50 scenarios:**
→ [Usage Guide: Cross-Run Queries](usage-guide.md#querying-across-runs)

**I want to share my study for reproducibility:**
→ [Mounts & Portability](mounts-and-portability.md)

**I want to integrate with an existing tool (ActivitySim, SUMO, etc.):**
→ [Usage Guide: Integration Patterns](usage-guide.md#advanced-patterns)
