# Concepts

This page establishes the core mental model for Consist before diving into API details.

## Core abstractions

- **Run**: A single execution with tracked inputs, config, and outputs. Runs are identified by a signature that lets Consist reuse prior results.
- **Artifact**: A file produced or consumed by a run. Artifacts carry provenance metadata such as content hash and creator run.
- **Scenario**: A collection of related runs (an experiment or study) that lets you compare variants cleanly.
- **Coupler**: A lightweight helper that passes artifacts between steps in a scenario so each step records lineage without manual wiring.

## How caching works

Consist computes a signature from the code version, config, and input artifact hashes:

```text
signature = hash(code_version + config + input_hashes)
```

- Same signature: return cached outputs.
- Different signature: execute and record new lineage.

This means you can safely re-run a workflow with the same inputs and config without redoing work, while still getting new results when anything changes.

## The config vs facet distinction

This is a key concept for making runs both reproducible and queryable.

| Parameter     | Affects Cache? | Queryable?      | Use For                                        |
|---------------|----------------|-----------------|------------------------------------------------|
| `config=`     | Yes            | No (by default) | Parameters that change behavior                |
| `facet=`      | No             | Yes (indexed)   | Metadata for filtering/grouping                |
| `facet_from=` | —              | Yes             | Extract specific keys from config for querying |

Guidance:
- Put anything that should trigger a re-run into `config`.
- Put metadata you want to filter or group by into `facet`.
- Use `facet_from` when a config value is useful for querying but should still affect the cache.

## How inputs and outputs are treated

- **Inputs** are files or values that influence computation. File inputs are hashed by content to ensure the signature reflects the actual data.
- **Outputs** are named artifacts you declare when you call `consist.run(...)` (or `tracker.run(...)`). Consist stores their paths and provenance metadata for later lookup and querying.

To keep outputs portable, write them under `tracker.run_dir` or a mounted `outputs://` root. That keeps artifact paths relative and consistent across machines.

## When to use each pattern

- `consist.run(...)` (or `tracker.run(...)`) for standalone steps or when calling library functions.
- `scenario.trace(...)` for inline code blocks within a workflow.
- `scenario.run(...)` when you want cache-skip behavior (the function does not execute on cache hit).

If you are unsure, start with `consist.run(...)` inside a `use_tracker(...)` scope. It is the simplest pattern and works well for most first-time use cases.
