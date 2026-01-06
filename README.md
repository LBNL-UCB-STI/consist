# Consist

[![CI](https://github.com/LBNL-UCB-STI/consist/actions/workflows/ci.yml/badge.svg)](https://github.com/LBNL-UCB-STI/consist/actions/workflows/ci.yml)

Consist automatically records what code, configuration, and input data produced each output in your pipeline. This makes it possible to skip redundant computation, answer "which configuration made Figure 3?", and query results across runs in SQL.

Multi-run simulation workflows typically accumulate friction:

- **Provenance ambiguity**: Which configuration produced those results?
- **Redundant computation**: Re-running an entire pipeline because you changed one parameter takes hours.
- **Scattered outputs**: Writing scripts to find and compare results across scenario variants.

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

# Initialize tracker: run_dir stores outputs, db_path stores metadata
tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

def clean_data(raw_path):
    df = pd.read_csv(raw_path)
    df = df[df["value"] > 0.5]
    out_path = tracker.run_dir / "cleaned.parquet"
    df.to_parquet(out_path)
    return {"cleaned": out_path}

# First run: executes function, records inputs/config/outputs
result = tracker.run(
    fn=clean_data,
    inputs={"raw_path": "raw.csv"},  # Files to hash (part of cache key)
    config={"threshold": 0.5},        # Config to hash (part of cache key)
    outputs=["cleaned"],               # Output artifact names
)

# Second run with identical inputs+config: instant cache hit, no execution
result = tracker.run(
    fn=clean_data,
    inputs={"raw_path": "raw.csv"},
    config={"threshold": 0.5},
    outputs=["cleaned"],
)

# Artifact: file with provenance metadata attached
artifact = result["cleaned"]
artifact.path   # -> PosixPath('./runs/<run_id>/cleaned.parquet')
```

**Key insight**: Consist computes a fingerprint from your code version, config, and input files. If you re-run with the same fingerprint, cached results return instantly. Different inputs/config? Full re-execution, with lineage recorded.

---

## Key Features

- **Intelligent caching**: Runs are identified by fingerprints of code version, config, and inputs. Rerun with matching fingerprints? Instant cache hit. Changing one parameter? Only affected downstream steps re-execute.

- **Complete provenance**: Every result is tagged with the exact code, config, and input data that created it. Query by tags, compare variants, and trace lineage from any output back to its sources.

- **SQL-native analysis**: All results indexed in DuckDB. Query across runs, join with your own tables, and export comparisons—all in standard SQL. No scripting or data movement required.

- **Multi-step orchestration**: Build complex workflows with caching, branching, and loops. Group related runs into scenarios for easy A/B comparison.

- **Container support**: Track Docker/Singularity containers as functions—image digest and mounts become part of the cache signature. Run legacy black-box models deterministically.

- **Command-line tools**: No code required. Use `consist lineage` to trace results, `consist runs` to inspect history, and `consist show` to compare variants.

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

## Where to Go Next

**I maintain or develop simulation tools (ActivitySim, SUMO, etc.):**
- Start with [Usage Guide](docs/usage-guide.md) for integration patterns. Container support lets you wrap existing tools without modification.

**I'm an MPO official or practitioner who runs models:**
- No coding required. See [CLI Reference](docs/cli-reference.md) to query and compare results from the command line.

**I'm a researcher building simulation workflows:**
- See [Concepts](docs/concepts.md) for mental models, then [Ingestion & Hybrid Views](docs/ingestion-and-hybrid-views.md) for SQL-native analytics and reproducibility.

**I want to understand how it works:**
- [Concepts](docs/concepts.md) explains the mental model. [Architecture](docs/architecture.md) goes deeper into caching and lineage tracking.

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

## Etymology

In railroad terminology, a **consist** (noun, pronounced CON-sist) is the specific lineup of locomotives and cars that make up a train. In this library, a **consist** is the immutable record of exactly which components—code version, configuration, input artifacts—were coupled together to produce a particular run's outputs.

---

## License

TODO: Open source permissive preferred
