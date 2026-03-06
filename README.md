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

**Consist** is a caching layer for scientific simulation workflows that makes provenance queryable. It automatically
records what code, configuration, and input data produced each output in your pipeline—eliminating redundant computation
and enabling post-hoc inspection of results via SQL.

### Why Consist?

Multi-run simulation workflows typically accumulate friction:

- **Provenance ambiguity**: "Which configuration produced those results in Figure 3?"
- **Redundant computation**: Re-running a 4-hour pipeline because you changed one unrelated parameter.
- **Scattered outputs**: Finding and comparing results across scenario variants manually.
- **Hidden wiring**: Tools with implicit dependencies (name-based injection, global state) are hard to debug and modify
  when something breaks.

Consist tracks lineage explicitly. Tasks are ordinary Python functions; dependencies flow through concrete values, not
framework magic. Your pipeline remains inspectable and testable.

---

## Installation

```bash
# We are currently preparing our initial PyPI release.
# Install from GitHub in the meantime:
pip install git+https://github.com/LBNL-UCB-STI/consist.git
```

> [!WARNING]
> Consist is in **Beta**. We are using it in production, but the API may still undergo breaking changes before v0.1.

---

## Quick Example

```python
import consist
from pathlib import Path
from consist import Tracker
import pandas as pd

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")


def clean_data(raw: Path, threshold: float = 0.5) -> dict[str, Path]:
    df = pd.read_parquet(raw)
    out = Path("./cleaned.parquet")
    df[df["value"] > threshold].to_parquet(out)
    return {"cleaned": out}


# Executes function and records inputs, config, and output artifact
result = tracker.run(
    fn=clean_data,
    inputs={"raw": Path("raw.parquet")},  # hashed for cache identity
    config={"threshold": 0.5},  # hashed for cache identity
)

# Second call with identical inputs: instant cache hit, no execution
result = tracker.run(
    fn=clean_data,
    inputs={"raw": Path("raw.parquet")},
    config={"threshold": 0.5},
)

# Artifact: the output file with provenance metadata attached
artifact = result.outputs["cleaned"]
print(artifact.path)  # -> PosixPath('./cleaned.parquet')

# Load as a DataFrame when needed
cleaned_df = consist.load_df(artifact)
```

**Summary**: Consist computes a deterministic fingerprint from your code version, config, and input files. If you change
anything upstream, only affected downstream steps will re-execute.

### Multi-Step Pipeline

Dependencies are explicit: the output of one step becomes the input of the next via a concrete reference, not name
matching or injection.

```python
def analyze_data(cleaned: Path) -> dict[str, Path]:
    df = pd.read_parquet(cleaned)
    out = Path("./analysis.parquet")
    summary = df.groupby("category")["value"].mean()
    summary.to_parquet(out)
    return {"analysis": out}


preprocess = tracker.run(
    fn=clean_data,
    inputs={"raw": Path("raw.parquet")},
    config={"threshold": 0.5},
)
analyze = tracker.run(
    fn=analyze_data,
    inputs={"cleaned": consist.ref(preprocess, key="cleaned")},  # explicit artifact reference
)
```

Use `output_paths` when a function returns `None` but writes files, or when you need explicit destination control.

---

## Key Features

- **Deterministic Caching**: Cache identity is based on an inspectable fingerprint of code, config, and inputs. Only
  affected downstream steps re-execute when any upstream piece changes.
- **Plain Python**: Tasks are ordinary Python functions—callable and testable without the tracker. The tracker is
  additive and does not restructure your code.
- **Complete Lineage**: Every result is tagged with the exact code and config that created it. Trace lineage from any
  output back to its sources.
- **SQL-Native Analysis**: All metadata is indexed in DuckDB. Query across runs, join tables, and compare variants using
  standard SQL.
- **HPC and Container Support**: Track Docker and Singularity containers as pure functions—image digests and mounted
  volumes become part of the cache signature. Ideal for long-running jobs on shared compute.
- **Queryable CLI**: Inspect history, trace lineage, and compare results from the command line after a job completes. No
  code required.

---

## Documentation Index

| Section                                                   | Description                                                  |
|:----------------------------------------------------------|:-------------------------------------------------------------|
| **[Getting Started](docs/getting-started/quickstart.md)** | 5-minute guide to your first tracked run.                    |
| **[Usage Guide](docs/usage-guide.md)**                    | Detailed patterns for scenarios and complex workflows.       |
| **[Architecture](docs/architecture.md)**                  | Deep dive into hashing, lineage, and the DuckDB core.        |
| **[CLI Reference](docs/cli-reference.md)**                | Guide to the `consist` command-line tools.                   |
| **[DB Maintenance](docs/db-maintenance.md)**              | Operational runbooks for inspect/doctor/purge/merge/rebuild. |
| **[Example Gallery](docs/examples.md)**                   | Interactive notebooks for Monte Carlo, Demand Modeling, etc. |

---

## Etymology

In railroad terminology, a **consist** (noun, pronounced *CON-sist*) is the specific lineup of locomotives and cars that
make up a train. In this library, a **consist** is the immutable record of exactly which components—code, config, and
inputs—were coupled together to produce a result.
