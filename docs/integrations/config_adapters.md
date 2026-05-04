# Config Adapters

Many simulation models store their configuration as a directory of YAML, CSV,
or HOCON files — sometimes dozens of them, with inheritance and overrides spread
across nested folders. Passing that as `config={"path": "configs/"}` to Consist
would miss changes inside the files, and dumping the entire config tree as a
dict snapshot would produce enormous, hard-to-query records.

Config adapters solve this. They know how a specific model organizes its config
files, so they can walk the directory, hash each file individually, extract the
parameters that matter for calibration, and register everything as queryable
artifacts — without you having to write any of that discovery logic yourself.

The result: cache invalidation that catches any config file change, plus SQL
tables of key parameters you can query across runs.

## When to use an adapter vs. plain config

| | Use an adapter | Use `config=` directly |
|---|---|---|
| Config location | Directory of YAML/CSV/HOCON files | Python dict or Pydantic model |
| Config structure | Layered inheritance, file hierarchies | Flat or lightly nested |
| Queryable parameters | Calibration-sensitive values across many files | Simple facets suffice |
| File tracking | Need per-file content hashing and artifact lineage | No file artifacts needed |

For in-memory configuration, see [Config, Facets, and Identity Inputs](../concepts/config-management.md).

---

## What an adapter does

When you call `tracker.canonicalize_config(adapter=my_adapter)`, Consist runs
three steps:

1. **Discover** — walks the config directory, locates relevant files, and
   computes a content hash per file
2. **Canonicalize** — extracts calibration-sensitive parameters, builds artifact
   specs for each config file, and constructs the identity payload that will
   contribute to the run signature
3. **Ingest** — logs each config file as a tracked artifact and writes extracted
   parameters to queryable DuckDB tables

After this, every run backed by that adapter records exactly which config files
were used, what their content was, and which parameters changed between runs.
When a cache miss occurs, Consist can tell you which specific files or keys
changed — not just that "config changed."

---

## Adapter guides

- [ActivitySim Config Adapter](config_adapters_activitysim.md)
- [BEAM Config Adapter](config_adapters_beam.md)

Each guide covers discovery behavior, which parameters are extracted, the
ingestion schema, and query examples.

## Writing your own adapter

Adapters follow a Python Protocol, so you can add support for any model with
file-based configuration. The [ActivitySim adapter source](https://github.com/LBNL-UCB-STI/consist/tree/main/src/consist/integrations/activitysim)
is the best reference: it covers layered YAML discovery, CSV extraction, and
the `CanonicalizationResult` structure that Consist expects back.

The core contract: your adapter's `canonicalize()` method must return a
`CanonicalizationResult` with an `identity=` payload. Consist uses that manifest
for cache-miss explanations — so misses can report exactly which config
references were added, removed, or changed, rather than falling back to a
generic "config changed" message.

---

## See Also

- [Config, Facets, and Identity Inputs](../concepts/config-management.md) — In-memory config, facets, and identity inputs
- [Data Virtualization & Materialization Strategy](../concepts/data-materialization.md) — DLT-based data ingestion
- [Caching and Artifact Hydration Patterns](../concepts/caching-and-hydration.md) — Run signature and caching behavior
