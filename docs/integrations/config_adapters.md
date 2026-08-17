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
- [GTFS Support](gtfs.md)

Each guide covers discovery behavior, which parameters are extracted, the
ingestion schema, and query examples.

GTFS is a little different from the other adapters: it is a standardized feed
format, not a bespoke model config tree. If you only need the high-level
transit story, start with [GTFS Support](gtfs.md). If you are wiring GTFS into
BEAM, use the [BEAM guide](config_adapters_beam.md) as the entry point.

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

## Read canonicalization facts inside a wrapped step

When a Python step needs to stage or inspect the files that its configuration
references, request a `RunContext`. After Consist applies the config plan and
before it calls the step, `ctx.canonicalization` exposes the immutable facts
from the same canonicalization pass:

```python
from consist import ExecutionOptions, RunContext


def launch_model(ctx: RunContext) -> None:
    snapshot = ctx.canonicalization
    if snapshot is None:  # no adapter was configured for this run
        return

    for item in snapshot.references:
        reference = item.reference
        print(reference.config_key, item.resolved_path, item.artifact_keys)


tracker.run(
    fn=launch_model,
    name="model",
    adapter=adapter,
    execution_options=ExecutionOptions(inject_context="ctx"),
)
```

`reference.raw_value` is the value found in the model configuration and
`reference.canonical_value` is Consist's portable identity value.
`item.resolved_path` is only the local path observed during canonicalization.
It is not an execution path: staging, copying, rewriting, and container mount
translation remain downstream responsibilities. Likewise, an empty
`item.artifact_keys` tuple accurately means Consist did not log an artifact for
that reference; it does not authorize consumers to guess one from a filename.

When an adapter selects one semantically distinct artifact under a broader
reference, `item.artifact_members` records its role, exact persisted artifact
key, observed local path, and narrow selection metadata. Each member key is
also present in `item.artifact_keys`. Member paths remain canonicalization-time
observations, not portable identities or final execution paths.

This view is available only to Python callables with injected context. Cache
hits do not execute the callable, and native container runs do not receive a
Python context or a serialized snapshot.

---

## See Also

- [Config, Facets, and Identity Inputs](../concepts/config-management.md) — In-memory config, facets, and identity inputs
- [Data Storage and Ingestion Strategy](../concepts/data-materialization.md) — DLT-based data ingestion
- [Caching and Artifact Hydration Patterns](../concepts/caching-and-hydration.md) — Run signature and caching behavior
