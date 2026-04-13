# Materialization

For historical output recovery, start with `hydrate_run_outputs(...)`.

It is the clearest API for restart, archive-mirror recovery, and
cross-workspace reuse because it answers the practical questions in one call:

- Which keys did I ask for?
- Where did each one land?
- Can I use the returned artifact right now?

`materialize_run_outputs(...)` performs the same underlying copy/export work
but returns the older aggregate
[`MaterializationResult`](#consist.core.materialize.MaterializationResult).
Keep it for compatibility or summary-style reporting; use
`hydrate_run_outputs(...)` for new workflows.

## Recommended Workflow

Hydrate only the outputs you need into a fresh workspace root:

```python
from pathlib import Path

hydrated = tracker.hydrate_run_outputs(
    "prior_run_id",
    keys=["persons", "households"],
    target_root=Path("restored_workspace"),
    source_root=Path("/archive/outputs_mirror"),  # optional
)
```

Inspect the keyed results and reuse the detached artifacts directly:

```python
persons = hydrated["persons"]

print(persons.status)
print(persons.path)

if persons.resolvable:
    path = persons.artifact.as_path()
    print(path)
```

In this flow:

- `hydrated["persons"].status` tells you what happened for that key.
- `hydrated["persons"].path` tells you where Consist expected or created the
  destination.
- `hydrated["persons"].artifact` is a detached artifact view. When
  `resolvable` is `True`, `artifact.as_path()` points at the hydrated
  destination in the new workspace.

## Status Meanings

| Status | Meaning |
|---|---|
| `materialized_from_filesystem` | Copied from historical cold bytes on disk |
| `materialized_from_db` | Exported from DuckDB for an ingested CSV/Parquet artifact |
| `preserved_existing` | Destination already existed and was reused |
| `skipped_unmapped` | No safe historical relative-path mapping was available |
| `missing_source` | Historical bytes were unavailable and no DB fallback applied |
| `failed` | Recovery was attempted but failed due to a collision, policy check, or copy/export error |

## When To Use `materialize_run_outputs(...)`

Use `materialize_run_outputs(...)` only when you intentionally want the older
aggregate summary buckets:

- `materialized_from_filesystem`
- `materialized_from_db`
- `skipped_existing`
- `skipped_unmapped`
- `skipped_missing_source`
- `failed`

If you are writing new restart or recovery logic, prefer
`hydrate_run_outputs(...)`.

## Reference

This page keeps the story focused on the recommended workflow. For full API
signatures and attribute details, use the generated reference below.

::: consist.core.materialize
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - HydratedRunOutput
        - HydratedRunOutputsResult
        - MaterializationResult
      filters:
        - "!^_"
