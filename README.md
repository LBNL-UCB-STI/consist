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
import pandas as pd

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

def clean_data(raw_path):
    df = pd.read_csv(raw_path)
    df = df[df["value"] > 0.5]
    out_path = tracker.run_dir / "cleaned.parquet"
    df.to_parquet(out_path)
    return {"cleaned": out_path}  # Keys match `outputs` list

result = tracker.run(
    fn=clean_data,
    inputs={"raw_path": "raw.csv"},
    outputs=["cleaned"],
)

# Re-run with identical inputs: cache hit, no execution
result = tracker.run(
    fn=clean_data,
    inputs={"raw_path": "raw.csv"},
    outputs=["cleaned"],
)

artifact = result["cleaned"]
artifact.path   # -> PosixPath('./runs/<run_id>/cleaned.parquet')
```

Runs return `Artifact` objects that wrap output paths with provenance metadata. Use `consist.load(artifact)` to read the data or `artifact.path` to access the file directly.

---

## Key Features

- **Signature-based caching**: Runs are keyed by `SHA256(code_hash | config_hash | input_hash)`. Changing any component invalidates only affected downstream runs.

- **Scenario workflows**: Group multi-step simulations under a parent run for easy comparison across variants.

- **Data virtualization**: Query artifacts across runs as SQL views without loading data into memory.

- **Container support**: Track Docker/Singularity containers as functions—image digest and mounts become part of the signature.

- **Ghost Mode**: Artifacts ingested into DuckDB can be recovered via `consist.load(...)` even if the original file is deleted. This can be useful when re-running pipelines against archived inputs.

- **CLI tools**: Inspect runs, trace lineage, and validate artifacts from the command line.

---

## CLI

Inspect provenance from the command line:

```bash
consist runs --limit 5
```

```
                                    Recent Runs
┏━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┓
┃ ID                     ┃ Model              ┃ Status    ┃ Tags ┃ Created        ┃ Duration (s) ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━┩
│ ...summaries_57cb6369  │ summaries          │ completed │      │ 2025-12-31     │ 0.61         │
│ ...traffic_simula…     │ traffic_simulation │ completed │      │ 2025-12-31     │ 0.58         │
│ ...assignment_14       │ assignment         │ completed │      │ 2025-12-31     │ 0.33         │
│ ...mode_choice_14      │ mode_choice        │ completed │      │ 2025-12-31     │ 0.42         │
│ ...utilities_14        │ calculate_utilities│ completed │      │ 2025-12-31     │ 0.65         │
└────────────────────────┴────────────────────┴───────────┴──────┴────────────────┴──────────────┘
```

Trace how an artifact was produced:

```bash
consist lineage traffic_volumes
```
```
Lineage for Artifact: traffic_volumes (a1b2c3d4)
└── Run: traffic_simulation_14 (traffic_simulation)
    ├── Input: assigned_trips (assignment)
    │   └── Run: assignment_14 (assignment)
    │       └── Input: trip_tables (mode_choice)
    │           └── Run: mode_choice_14 (mode_choice)
    │               └── ...
    └── Input: network (parquet)
```

See [CLI Reference](docs/cli-reference.md) for `consist show`, `consist scenarios`, `consist preview`, and more.
---

## Documentation

- **[Usage Guide](docs/usage-guide.md)**: Detailed patterns for runs, scenarios, and querying
- **[CLI Reference](docs/cli-reference.md)**: Command-line tools for inspecting provenance
- **[Architecture](docs/architecture.md)**: How caching and lineage tracking work

### Build and View Docs Locally

We use [Zensical](https://zensical.org/):

```bash
pip install -e ".[docs]"
zensical serve
```

Then open `http://localhost:8000/`.

<details>
<summary>Alternative: build with MkDocs</summary>

```bash
mkdocs serve
```

A `mkdocs.yml` is included for compatibility.

</details>
---

## Current Status

Consist is under active development and used in production for the [PILATES](https://github.com/ual/PILATES) integrated land use and transportation modeling framework.

We welcome feedback from researchers working with multi-model simulation workflows.

---

## License

TODO: Open source permissive preferred
