# Mounts & Portability

Consist stores portable URIs instead of absolute filesystem paths so runs can move
between machines without breaking lineage. This page explains how mounts, workspace
URIs, and historical path resolution work.

---

## Getting Started with Mounts

When you set up Consist for your research project, you'll define **mounts** — mappings from short names to directories on your filesystem. These mounts let each team member keep their data in different locations while sharing a common provenance database.

**The Workflow**

1. **Identify your data directories**: Where are your inputs? Where do you write outputs? Where is temporary scratch space?
2. **Assign mount names**: Choose memorable names like `inputs`, `outputs`, `scratch`, `shared`.
3. **Define mounts in your Tracker**: Map those names to real paths on each person's machine.
4. **Log artifacts under mounts**: Instead of absolute paths, use URIs like `inputs://land_use.csv`.

**Example for a Research Team**

Suppose your team shares an ActivitySim project:

```python
from consist import Tracker
from pathlib import Path

# Shared setup (agreed upon by the team)
tracker = Tracker(
    run_dir="./runs",
    db_path="./provenance.duckdb",
    mounts={
        "inputs": "/shared/data/activitysim_inputs",      # Shared NFS mount
        "outputs": "/local/activitysim_outputs",          # Local SSD for speed
        "scratch": "/scratch/users/YOUR_USERNAME",        # Temporary workspace
    },
)
```

**On each team member's machine**, the paths differ but mount names stay the same:

```python
# Alice's setup
tracker = Tracker(
    run_dir="./runs",
    db_path="./provenance.duckdb",
    mounts={
        "inputs": "/mnt/nfs/activitysim_inputs",          # Alice's NFS mount point
        "outputs": "/home/alice/activitysim_outputs",
        "scratch": "/scratch/alice",
    },
)

# Bob's setup
tracker = Tracker(
    run_dir="./runs",
    db_path="./provenance.duckdb",
    mounts={
        "inputs": "/data/nfs/inputs",                     # Bob's mount point
        "outputs": "/var/cache/bob/outputs",
        "scratch": "/tmp/bob_scratch",
    },
)
```

When Alice runs a simulation and logs an output (inside a run context):

```python
with tracker.start_run("asim_baseline", model="activitysim"):
    consist.log_artifact(
        Path("/home/alice/activitysim_outputs/results.parquet"),
        key="results",
        direction="output",
    )
```

Consist detects the mount and stores a portable URI:

```
outputs://results.parquet
```

When Bob retrieves this artifact, Consist resolves it using *his* mount configuration:

```
outputs:// → /var/cache/bob/outputs/ → /var/cache/bob/outputs/results.parquet
```

**Benefits**

- **Portability**: Runs move between machines without breaking lineage.
- **Flexibility**: Each user can customize their local paths for their hardware (local SSD, NFS, cloud storage).
- **Shared provenance**: A single `provenance.duckdb` file shared across the team still works even if underlying filesystems differ.
- **Isolation**: Temporary outputs stay local; shared data stays on shared infrastructure.

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
./outputs/<run_id>/model.csv
```

Consist resolves these using the run's `_physical_run_dir` metadata field, which records the absolute run directory at execution time.

| Scenario | Behavior |
|----------|----------|
| Current run directory matches original | Files accessible |
| Run directory moved | Metadata-only cache hits work; file access fails |
| Run directory deleted | Metadata-only cache hits work; file access fails |

!!! note "`_physical_run_dir`"
    Stored in `run.meta["_physical_run_dir"]`. Used for historical path resolution when hydrating artifacts from prior runs.

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
