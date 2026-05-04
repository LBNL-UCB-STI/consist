<p align="center">
  <img src="docs/assets/logo.png" alt="Consist" width="320">
</p>

<p align="center">
  <a href="https://github.com/LBNL-UCB-STI/consist/actions/workflows/ci.yml">
    <img src="https://github.com/LBNL-UCB-STI/consist/actions/workflows/ci.yml/badge.svg" alt="CI">
  </a>
  <img src="https://img.shields.io/badge/python-3.11+-blue.svg" alt="Python 3.11+">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-BSD--3--Clause-blue.svg" alt="License BSD 3-Clause"></a>
</p>

**Consist** is a caching and provenance layer for scientific simulation
workflows. It records the code, configuration, input data, and output artifacts
behind each run so expensive steps can be skipped safely and results remain
queryable after the fact.

Consist is useful when a workflow has:

- long-running model steps that should cache-hit when inputs are unchanged;
- scenario variants that need explicit lineage and comparison;
- file-based tools that need stable local paths but still need canonical
  provenance;
- post-run questions like "which config produced this output?"

## Installation

```bash
pip install consist
```

Optional integrations are installed as extras:

```bash
pip install "consist[ingest]"
pip install "consist[docker]"
```

> [!NOTE]
> Consist is pre-`1.0`. It is ready for real workflows, but minor releases may
> still include breaking changes while the API settles.

## Quick Example

```python
from pathlib import Path

import pandas as pd

import consist
from consist import ExecutionOptions, Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")


def clean_data(raw: Path, threshold: float = 0.5) -> dict[str, Path]:
    df = pd.read_parquet(raw)
    out = Path("./cleaned.parquet")
    df[df["value"] > threshold].to_parquet(out)
    return {"cleaned": out}


first = tracker.run(
    fn=clean_data,
    inputs={"raw": Path("raw.parquet")},
    config={"threshold": 0.5},
    outputs=["cleaned"],
    execution_options=ExecutionOptions(input_binding="paths"),
)

second = tracker.run(
    fn=clean_data,
    inputs={"raw": Path("raw.parquet")},
    config={"threshold": 0.5},
    outputs=["cleaned"],
    execution_options=ExecutionOptions(input_binding="paths"),
)

print(first.cache_hit, second.cache_hit)  # False, True
cleaned = consist.load_df(second.outputs["cleaned"])
```

In this example, `input_binding="paths"` tells Consist to pass local `Path` objects 
into the callable instead of loading input files. Those same paths are still hashed 
and recorded for cache identity and lineage. For tools that need inputs copied to
specific local filenames, see [Usage Guide](docs/usage-guide.md#path-staging-example).

## Documentation

| Start here | Use it for |
|:--|:--|
| [Quickstart](docs/getting-started/quickstart.md) | First tracked run and cache hit |
| [First Workflow](docs/getting-started/first-workflow.md) | Two-step pipeline with explicit artifact links |
| [Usage Guide](docs/usage-guide.md) | Choosing between `run`, `trace`, and `scenario` |
| [Caching & Hydration](docs/concepts/caching-and-hydration.md) | Cache identity, hit behavior, and output recovery concepts |
| [Historical Recovery](docs/guides/historical-recovery.md) | Restoring archived outputs and staging inputs |
| [CLI Reference](docs/cli-reference.md) | Inspecting runs, artifacts, lineage, and schemas |
| [API Reference](docs/api/index.md) | Public Python API and generated signatures |

## Etymology

In railroad terminology, a **consist** is the lineup of locomotives and cars
that make up a train. In this library, a consist is the immutable record of the
code, config, inputs, and outputs coupled together to produce a result.
