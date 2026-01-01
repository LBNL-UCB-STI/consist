# Mounts & Portability

Consist stores portable URIs instead of absolute filesystem paths so runs can move
between machines without breaking lineage. This page explains how mounts, workspace
URIs, and historical path resolution work.

---

## Mounts at a glance

Mounts map a short scheme name to a real path on disk:

```python
from consist import Tracker

tracker = Tracker(
    run_dir="./runs",
    db_path="./provenance.duckdb",
    mounts={
        "inputs": "/shared/inputs",
        "scratch": "/scratch/users/MY_USERNAME",
    },
)
```

When you log a path under a mount, Consist stores a URI such as:

```
inputs://land_use.csv
scratch://temp/output.parquet
```

This keeps provenance portable and lets each user remap mounts on their machine.

---

## Workspace URIs (run-local outputs)

Paths under the run directory are stored relative to the active run:

```
./outputs/model.csv
```

Internally this is treated as a workspace URI and resolved using the run's
`_physical_run_dir` metadata. That field records the absolute run directory used
when the run executed.

Implications:
- Cache hits can hydrate artifacts even when the current run directory differs.
- Moving or deleting the original run directory will break byte-level access,
  but metadata-only cache hits still work.

---

## Historical path resolution

When Consist needs bytes from a historical run (e.g., cache hydration or
`inputs-missing`), it resolves paths in this order:

1) If the URI uses `workspace://` or `./`, resolve relative to the original run’s
   `_physical_run_dir`.
2) If the URI uses a mount scheme (e.g., `inputs://`), resolve using the current
   tracker mounts.
3) Otherwise, treat the URI as an absolute path.

If a mount is missing or points somewhere else, materialization will warn and skip
missing files rather than crashing (unless explicitly set to raise).

---

## Sharing a database across machines

Sharing a DuckDB provenance file across a team is supported, but you must keep
mounts consistent in intent even if the physical paths differ.

Recommended practice:
- Agree on mount *names* (`inputs`, `outputs`, `scratch`, `shared`).
- Each user maps those names to their local filesystem.
- Store the DB in a shared location with write access controls.

If a user hits a cache hit but cannot access the source filesystem, Consist will
log a warning and proceed without materializing the files.

---

## Best practices

- Prefer mounts for shared data directories; avoid absolute paths in artifacts.
- Keep run directories local and disposable; treat cached outputs as rehydratable.
- Use `cache_hydration="outputs-requested"` for only the outputs you need.
- Use `cache_hydration="inputs-missing"` to backfill inputs when a run moves
  across machines or directories.

---

## Troubleshooting

- **Missing file on cache hit**: Check that mounts map to the correct root and the
  original run directory still exists for workspace URIs.
- **Moved run directory**: Cache metadata is still valid, but byte materialization
  will warn because `_physical_run_dir` no longer points to the original location.
- **Permission denied**: Consist warns and continues; adjust mount permissions or
  use a shared accessible path for cached outputs you need to materialize.

---
