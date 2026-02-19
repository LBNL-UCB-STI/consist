---
icon: lucide/book-open
---

# API reference

This section documents Consist's public API and key helpers. Start with the
public surface area, then drill into the core classes and supporting utilities.

## Recommended path

1. [Public API](public_api.md) for the stable surface and entry-point choices.
2. [API Helpers](api_helpers.md) for top-level `consist.*` convenience calls.
3. [Tracker](tracker.md), [Workflow Contexts](workflow.md), and
   [Artifact](artifact.md) for class-level behavior.

## Choose your entry point

| If you need | Start here | Why |
|---|---|---|
| One cache-aware function call | [`consist.run`](api_helpers.md#consist.api.run) | Smallest API surface; uses active/default tracker |
| Multi-step workflow with shared context | [`consist.scenario`](api_helpers.md#consist.api.scenario) | Groups steps and lineage under one scenario header |
| Explicit tracker wiring in library/app code | [`Tracker.run`](tracker.md#consist.core.tracker.Tracker.run) | No global context; dependencies are explicit |

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

## API pages

- [Public API](public_api.md)
- [API Helpers](api_helpers.md)
- [Tracker](tracker.md)
- [Workflow Contexts](workflow.md)
- [Artifact](artifact.md)
- [Run](run.md)
- [Views](views.md)
- [Matrix Views](matrix.md)
- [Materialization](materialize.md)
- [Indexing](indexing.md)
- [Identity Manager](identity.md)
