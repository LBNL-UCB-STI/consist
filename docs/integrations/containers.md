# Containers

This page is the API surface for container execution. For architecture,
portability patterns, and practical examples, use:

- [Container Integration Guide](../containers-guide.md)
- [Mounts & Portability](../mounts-and-portability.md)
- [Caching & Hydration](../concepts/caching-and-hydration.md)

## Portable volume mapping quick example

Use tracker mount roots as host-side volumes and keep stable container paths:

```python
from pathlib import Path
from consist import Tracker
from consist.integrations.containers import run_container

tracker = Tracker(
    run_dir="/scratch/consist_runs",
    db_path="./provenance.duckdb",
    mounts={"inputs": "/shared/inputs", "runs": "/scratch/consist_runs"},
)

run_container(
    tracker=tracker,
    run_id="container_step",
    image="my-org/tool:v1",
    command=["python", "tool.py", "--in", "/inputs", "--out", "/outputs"],
    volumes={
        str(Path(tracker.mounts["inputs"]).resolve()): "/inputs",
        str(Path(tracker.mounts["runs"]).resolve()): "/outputs",
    },
    inputs=[Path(tracker.mounts["inputs"]) / "data.csv"],
    outputs=[Path(tracker.mounts["runs"]) / "container_step" / "result.csv"],
)
```

!!! note "Mount validation and defaults"
    `run_container(...)` validates host volume paths against
    `Tracker(mounts=...)` when `strict_mounts=True` (default).

On cache hits, `run_container(...)` materializes requested output paths on the
host (copy-only) and skips container execution.

For an end-to-end example and portability guidance, see
[Container Integration Guide](../containers-guide.md).

::: consist.integrations.containers.api
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - ContainerResult
        - run_container
      filters:
        - "!^_"

::: consist.integrations.containers.models.ContainerDefinition
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false
      members: true

## See Also

- [Container Guide](../containers-guide.md) — prose guide with examples and volume behavior
- [Mounts & Portability](../mounts-and-portability.md)
