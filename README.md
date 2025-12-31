# Consist

[![CI](https://github.com/LBNL-UCB-STI/consist/actions/workflows/ci.yml/badge.svg)](https://github.com/LBNL-UCB-STI/consist/actions/workflows/ci.yml)

**Provenance tracking, intelligent caching, and data virtualization for scientific simulation workflows.**

Consist automatically records what code, configuration, and input data produced each output in your pipeline. It uses this information to skip redundant computation and to let you query results across many runs without manual bookkeeping.

---

## Etymology

In railroad terminology, a **consist** (noun, pronounced CON-sist) is the specific lineup of locomotives and cars that make up a train. In this library, a **consist** is the immutable record of exactly which components—code version, configuration, input artifacts—were coupled together to produce a particular run's outputs.

---

## Why Consist?

Multi-model simulation workflows accumulate friction over time:

- **Provenance ambiguity.** Which configuration produced the results in Figure 3?
- **Redundant computation.** Re-running an entire pipeline because one downstream parameter changed.
- **Scattered outputs.** Writing one-off scripts to compare results across scenario variants.

Consist addresses these by treating provenance as a first-class concern. Each run is identified by a cryptographic hash of its inputs (code version, configuration, input artifact signatures). Matching signatures return cached results; differing signatures trigger execution and record new lineage.

---

## Installation

```bash
pip install consist
```

Requires Python 3.11+.

---

## Quick Example

```python
from consist import Tracker
from pathlib import Path
import pandas as pd

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

def clean_data(raw_path):
    df = pd.read_csv(raw_path)
    df = df[df["value"] > 0.5]
    # Keep outputs under the run directory for easy cleanup and portability.
    out_path = tracker.run_dir / "cleaned.parquet"
    df.to_parquet(out_path)
    return out_path

result = tracker.run(
    fn=clean_data,
    inputs={"raw_path": "raw.csv"},
    outputs=["cleaned"],
)
result = tracker.run(
    fn=clean_data,
    inputs={"raw_path": "raw.csv"},
    outputs=["cleaned"],
)
# Second call with same inputs -> cache hit

cleaned = result["cleaned"]
cleaned.path  # Local path to the artifact file
```

Runs return `Artifact` objects that wrap output paths with provenance metadata. Use `consist.load(artifact)` to read the data or `artifact.path` to access the file directly.

---

## Key Features

- **Signature-based caching**: Runs are keyed by `SHA256(code_hash | config_hash | input_hash)`. Changing any component invalidates only affected downstream runs.

- **Scenario workflows**: Group multi-step simulations under a parent run for easy comparison across variants.

- **Data virtualization**: Query artifacts across runs as SQL views without loading data into memory.

- **Container support**: Track Docker/Singularity containers as functions—image digest and mounts become part of the signature.

- **Ghost Mode**: If an artifact was ingested into DuckDB, `consist.load(...)` can recover it even if the file is deleted (by default only when it’s a declared input to an uncached run).

- **CLI tools**: Inspect runs, trace lineage, and validate artifacts from the command line.

---

## Documentation

- **[Usage Guide](docs/usage-guide.md)**: Detailed patterns for runs, scenarios, and querying
- **[CLI Reference](docs/cli-reference.md)**: Command-line tools for inspecting provenance
- **[Architecture](docs/architecture.md)**: How caching and lineage tracking work

### Build and View Docs Locally

You can build and browse the documentation locally:

```bash
pip install -e ".[docs]"
mkdocs serve
```

Then open `http://127.0.0.1:8000/`.

---

## Current Status

Consist is under active development and used in production for the [PILATES](https://github.com/ual/PILATES) integrated land use and transportation modeling framework.

We welcome feedback from researchers working with multi-model simulation workflows.

---

## License

TODO: Open source permissive preferred
