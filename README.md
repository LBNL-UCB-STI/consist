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

**Consist** is a provenance-first caching layer for scientific simulation workflows. It automatically records what code, configuration, and input data produced each output in your pipeline—eliminating redundant computation and making results searchable via SQL.

### Why Consist?
Multi-run simulation workflows typically accumulate friction:
- **Provenance ambiguity**: "Which configuration produced those results in Figure 3?"
- **Redundant computation**: Re-running a 4-hour pipeline because you changed one unrelated parameter.
- **Scattered outputs**: Finding and comparing results across scenario variants manually.

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

def clean_data(raw: pd.DataFrame, threshold: float = 0.5) -> pd.DataFrame:
    return raw[raw["value"] > threshold]

# Executes function and records inputs/config/outputs
result = tracker.run(
    fn=clean_data,
    inputs={"raw": Path("raw.csv")},   # Hashed for cache identity
    config={"threshold": 0.5},    # Hashed for cache identity
    outputs=["cleaned"],
)

# Second run with same inputs: instant cache hit, no execution
result = tracker.run(
    fn=clean_data,
    inputs={"raw": Path("raw.csv")},
    config={"threshold": 0.5},
    outputs=["cleaned"],
)

# Artifact: file with provenance metadata attached
artifact = result.outputs["cleaned"]
print(artifact.path)   # -> PosixPath('.../runs/outputs/.../cleaned.parquet')

# Load the data back as a DataFrame
cleaned_df = consist.load_df(artifact)
```

**Summary**: Consist computes a fingerprint from your code version, config, and input files. If you change anything upstream, only affected downstream steps will re-execute.

**Best practice for multi-step workflows**: Chain outputs explicitly with
`consist.ref(...)` instead of string key indirection.

```python
def analyze_data(cleaned: pd.DataFrame) -> pd.DataFrame:
    return cleaned.groupby("category", as_index=False)["value"].mean()

preprocess = tracker.run(
    fn=clean_data,
    inputs={"raw": Path("raw.csv")},
    outputs=["cleaned"],
)
analyze = tracker.run(
    fn=analyze_data,
    inputs={"cleaned": consist.ref(preprocess, key="cleaned")},
    outputs=["analysis"],
)
```

Pass `key=...` whenever an upstream run has multiple outputs. Legacy coupler-key
patterns (for example `inputs=["cleaned"]`) still work, but explicit refs are
recommended for new docs and code.

---

## Key Features

- **Intelligent Caching**: Instant cache hits for matching fingerprints. If you change one parameter, only affected downstream steps re-execute.
- **Complete Lineage**: Every result is tagged with the exact code and config that created it. Trace lineage from any output back to its sources.
- **SQL-Native Analysis**: All metadata is indexed in DuckDB. Query across runs, join tables, and compare variants using standard SQL.
- **Container Support**: Track Docker/Singularity containers as functions--image digests and mounts become part of the cache signature.
- **User Friendly CLI**: Inspect history (`consist runs`), trace lineage (`consist lineage`), and compare results without writing code.

---

## Documentation Index

| Section                                                   | Description                                                  |
|:----------------------------------------------------------|:-------------------------------------------------------------|
| **[Getting Started](docs/getting-started/quickstart.md)** | 5-minute guide to your first tracked run.                    |
| **[Usage Guide](docs/usage-guide.md)**                    | Detailed patterns for scenarios and complex workflows.       |
| **[Architecture](docs/architecture.md)**                  | Deep dive into hashing, lineage, and the DuckDB core.        |
| **[CLI Reference](docs/cli-reference.md)**                | Guide to the `consist` command-line tools.                   |
| **[Example Gallery](docs/examples.md)**                   | Interactive notebooks for Monte Carlo, Demand Modeling, etc. |

---

## Etymology

In railroad terminology, a **consist** (noun, pronounced *CON-sist*) is the specific lineup of locomotives and cars that make up a train. In this library, a **consist** is the immutable record of exactly which components—code, config, and inputs—were coupled together to produce a result.
