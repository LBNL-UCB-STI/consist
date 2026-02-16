# Artifact

`Artifact` represents a tracked input or output file with stable provenance
metadata (`run_id`, `hash`, `container_uri`, driver, and custom metadata).

## When to use `Artifact` directly

- You want to pass outputs from one step to another (`inputs={...}` mappings).
- You need a portable path via `artifact.path` rather than hard-coded file paths.
- You need metadata checks (`artifact.get_meta(...)`, `artifact.is_tabular`,
  `artifact.is_matrix`) before loading.

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
