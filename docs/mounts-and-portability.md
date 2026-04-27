# Mounts & Portability

Consist stores portable URIs where it can, instead of baking in one machine's
absolute paths. Mounts let each machine map the same logical URI scheme to its
own filesystem.

!!! note "Recommended path"
    Use `tracker.run(...)`, `tracker.trace(...)`, or `scenario.run(...)` for
    normal workflows. This page focuses on path behavior and URI mechanics.

## Mounts at a Glance

Mounts map a short scheme to a local root:

```python
from consist import Tracker

tracker = Tracker(
    run_dir="./runs",
    db_path="./provenance.duckdb",
    mounts={
        "inputs": "/shared/inputs",
        "outputs": "/scratch/outputs",
    },
)
```

Artifacts under those roots are stored as URIs:

```text
inputs://land_use.csv
outputs://baseline/results.parquet
```

Another user can open the same provenance database with different physical
paths, as long as the mount names keep the same meaning.

## What Mounts Do Not Do

Mounts make paths portable; they do not replicate bytes.

| Situation | Result |
|---|---|
| Current machine can resolve the URI and the file exists | Artifact bytes are usable |
| Current machine can resolve the URI but the file is missing | Metadata remains, byte access fails or skips |
| Current machine lacks the mount | Consist cannot safely resolve the URI |
| Artifact has `recovery_roots` | Hydration may find archived bytes elsewhere |

For durable archived outputs, see [Historical Recovery](guides/historical-recovery.md).

## Container Volumes

For container runs, keep in-container paths stable and map host roots through
tracker mounts:

```python
from pathlib import Path

from consist.integrations.containers import run_container

inputs_root = Path(tracker.mounts["inputs"]).resolve()
runs_root = Path(tracker.run_dir).resolve()

container = run_container(
    tracker=tracker,
    run_id="model_step",
    image="my-model:1.0",
    command=["python", "/app/run.py", "--input", "/inputs/data.csv"],
    volumes={
        str(inputs_root): "/inputs",
        str(runs_root): "/outputs",
    },
    inputs=[inputs_root / "data.csv"],
    outputs=[runs_root / "model" / "summary.csv"],
    backend_type="docker",
)
```

Guidelines:

- Use stable container paths such as `/inputs` and `/outputs`.
- Use tracker mounts for machine-specific host paths.
- Keep `strict_mounts=True` unless an external path is intentional.
- Remember that container cache identity currently includes resolved host
  volume paths, so different host roots can miss cache even if mounted data is
  logically the same.

See [Container Guide](containers-guide.md) for runnable container examples.

## Run-Local Outputs

Paths under the run directory are usually stored relative to the active run:

```text
./outputs/<run_id>/model.csv
```

Historical resolution uses the producing run's `_physical_run_dir` metadata for
workspace-relative paths. If that directory moves or is deleted, metadata-only
cache hits can still work, but byte access needs a recovery path.

## Historical Path Resolution

When Consist needs bytes from a prior run, it resolves paths in this order:

1. Per-call `source_root=...` override.
2. Workspace-relative paths under the producing run's `_physical_run_dir`.
3. Mount-backed URIs through the historical mount snapshot or current mounts.
4. Ordered `artifact.meta["recovery_roots"]`.
5. DuckDB export fallback for ingested tabular artifacts.

Absolute paths and `file://...` URIs are treated conservatively. Consist avoids
silently reinterpreting them under unrelated roots.

## Sharing A Database

Sharing a DuckDB provenance file across machines is supported when the team
agrees on mount names and each user maps those names locally.

Recommended practice:

- Use shared mount names such as `inputs`, `outputs`, `scratch`, and `archive`.
- Keep the database in a shared location with write access controls.
- Archive or ingest outputs that must survive workspace cleanup.
- Record `recovery_roots` when archived bytes should remain discoverable.

## Cache-Hit File Behavior

Function runs and container runs differ:

| Surface | Default cache-hit bytes behavior |
|---|---|
| `tracker.run(...)` / `scenario.run(...)` | Hydrate metadata only unless output materialization is requested |
| `run_container(...)` | Materialize cached outputs to requested host output paths |
| `hydrate_run_outputs(...)` | Restore selected historical outputs into a target root |

Use `cache_hydration="outputs-requested"` when a function step needs selected
output files copied on cache hit. Use historical recovery APIs for restart or
workspace rebuild flows.

## Best Practices

- Prefer mount-backed paths for shared data and archived outputs.
- Treat run directories as local workspaces, not durable archives.
- Keep artifact identity canonical even when bytes are copied elsewhere.
- Record `recovery_roots` once instead of passing ad hoc source roots forever.
- Prefer `hydrate_run_outputs(...)` over legacy aggregate materialization for
  new restart or recovery code.
- Keep external target paths opt-in with `allow_external_paths=True`.

## Troubleshooting

- **Cache hit but file missing:** Check the original run directory, mount map,
  and `recovery_roots`.
- **URI does not resolve:** Add the missing mount or use a tracker configured
  with the same mount names used by the producer.
- **Moved run directory:** Metadata can still hydrate, but workspace-relative
  bytes need an archive, recovery root, or DB fallback.
- **Container cache miss across machines:** Compare resolved host volume roots;
  those paths are part of the container signature today.

## See Also

- [Container Guide](containers-guide.md)
- [Caching & Hydration](concepts/caching-and-hydration.md)
- [Historical Recovery](guides/historical-recovery.md)
- [Materialization API](api/materialize.md)
