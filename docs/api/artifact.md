# Artifact

`Artifact` represents a tracked input or output file with stable provenance
metadata (`run_id`, `hash`, `container_uri`, driver, and custom metadata).

Container-backed child artifacts, such as HDF5 tables discovered from a parent
file, are still ordinary `Artifact` rows. Consist records their canonical
parent-child relation on `artifact.parent_artifact_id`. Legacy metadata-based
links in `artifact.meta["parent_id"]` remain for compatibility and debugging,
but new code should treat `parent_artifact_id` as the source of truth.

## When to use `Artifact` directly

- You want to pass outputs from one step to another (`inputs={...}` mappings).
- You need a portable path via `artifact.path` rather than hard-coded file paths.
- You need metadata checks (`artifact.get_meta(...)`, `artifact.is_tabular`,
  `artifact.is_matrix`) before loading.
- You need to inspect or traverse container child-artifact relations after
  logging, hydration, or query retrieval.

## Container child artifacts

Use `Tracker.get_child_artifacts(...)` and `Tracker.get_parent_artifact(...)`
to navigate first-class parent-child artifact relations. This is the supported
query surface for HDF5 container artifacts and other container-child patterns.

## Minimal runnable example

```python
from pathlib import Path
import consist
from consist import Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

def write_output() -> Path:
    out = consist.output_path("report", ext="txt")
    out.write_text("hello\n")
    return out

with consist.use_tracker(tracker):
    result = consist.run(fn=write_output, outputs=["report"])

artifact = result.outputs["report"]
print(artifact.key)
print(artifact.path)
print(artifact.path.read_text().strip())
```

See [API Helpers](api_helpers.md) for helper functions that return and consume
artifacts (`consist.run`, `consist.ref`, `consist.refs`, `consist.load*`).
For query helpers that traverse child artifacts, see
[Tracker](tracker.md#tracker).

::: consist.models.artifact.Artifact
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - abs_path
        - path
        - is_matrix
        - is_tabular
        - created_at_iso
        - get_meta
