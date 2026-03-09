---
icon: lucide/book-open
---

# API reference

The API reference is organized into two tiers:

- **Essentials**: the recommended path API for most users (`run`, `trace`, `scenario`)
- **Advanced**: lifecycle, decorators, and lower-level utilities for integrations

## Start Here

1. [API Essentials](essentials.md)
2. [API Advanced](advanced.md)
3. [Public API](public_api.md) for complete surface-area inventory

## Essentials entry points

| If you need | Start here | Why |
|---|---|---|
| One cache-aware function call | [`consist.run`](api_helpers.md#consist.api.run) | Smallest API surface; uses active/default tracker |
| Multi-step workflow with shared context | [`consist.scenario`](api_helpers.md#consist.api.scenario) | Groups steps and lineage under one scenario header |
| Always-execute run-scoped block | [`consist.trace`](api_helpers.md#consist.api.trace) | Records provenance but executes block every time |
| Multi-run grouping and pairwise comparison | [`RunSet`](runset.md#runset-and-alignment) / [`consist.run_set`](api_helpers.md#consist.api.run_set) | Fluent partitioning, recency selection, and alignment |

## Minimal first run

```python
from pathlib import Path
import consist
from consist import Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

def write_answer() -> Path:
    out = consist.output_path("answer", ext="txt")
    out.write_text("42\n")
    return out

with consist.use_tracker(tracker):
    result = consist.run(fn=write_answer, outputs=["answer"])

print(result.outputs["answer"].path)
```

## API pages (full)

- [API Essentials](essentials.md)
- [API Advanced](advanced.md)
- [Public API](public_api.md)
- [API Helpers](api_helpers.md)
- [Tracker](tracker.md)
- [Workflow Contexts](workflow.md)
- [Artifact](artifact.md)
- [Run](run.md)
- [RunSet and Alignment](runset.md)
- [Views](views.md)
- [Matrix Views](matrix.md)
- [Materialization](materialize.md)
- [Indexing](indexing.md)
- [Identity Manager](identity.md)
