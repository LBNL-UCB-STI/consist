<p align="center">
  <img src="docs/assets/logo.png" alt="Consist" width="320">
</p>

<p align="center">
  <a href="https://github.com/LBNL-UCB-STI/consist/actions/workflows/ci.yml">
    <img src="https://github.com/LBNL-UCB-STI/consist/actions/workflows/ci.yml/badge.svg" alt="CI">
  </a>
</p>

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

**Requires Python 3.11+**

---

## Quick Example

```python
import consist
from consist import Tracker
import pandas as pd

# Initialize tracker: run_dir stores outputs, db_path stores metadata
tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

def clean_data(raw: pd.DataFrame, threshold: float = 0.5) -> pd.DataFrame:
    df = raw[raw["value"] > threshold]
    return df

# First run: executes function, records inputs/config/outputs
result = tracker.run(
    fn=clean_data,
    inputs={"raw": "raw.csv"},       # Files to hash (part of cache key)
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
artifact = result.outputs["cleaned"]
artifact.path   # -> PosixPath('.../runs/outputs/.../cleaned.parquet')

# Load the data back as a DataFrame
cleaned_df = consist.load_df(artifact)
```

Note: when `inputs` is a mapping, Consist auto-loads common tabular formats (CSV/Parquet) into pandas by default.

**Key insight**: Consist computes a fingerprint from your code version, config, and input files. If you re-run with the same fingerprint, cached results return instantly—no re-execution, no data movement. Change anything upstream? Only affected downstream steps re-execute.

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

## Model-Specific Configuration Tracking

For complex, file-based model configurations (YAML hierarchies, CSV parameters), Consist provides **config adapters** that automatically discover, canonicalize, and make configurations queryable. See [Config Adapters Integration Guide](docs/integrations/config_adapters.md) for details.

<details>
<summary>ActivitySim example (config adapters + cached runs)</summary>

```python
from pathlib import Path
import argparse

from activitysim.cli.run import add_run_args, run as run_activitysim_cli
from consist import Tracker
from consist.integrations.activitysim import ActivitySimConfigAdapter

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")
adapter = ActivitySimConfigAdapter()

def run_activitysim(config_dir: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    parser = argparse.ArgumentParser()
    add_run_args(parser)
    args = parser.parse_args(["-c", str(config_dir), "-o", str(output_dir)])
    run_activitysim_cli(args)

def run_scenario(name: str, config_dir: Path) -> None:
    output_dir = tracker.run_dir / "activitysim_outputs" / name
    plan = tracker.prepare_config(adapter, [config_dir])
    tracker.run(
        fn=run_activitysim,
        name="activitysim",
        run_id=f"activitysim_{name}",
        config={"scenario": name},
        config_plan=plan,  # plan hash participates in cache identity
        runtime_kwargs={"config_dir": config_dir, "output_dir": output_dir},
        capture_dir=output_dir,
        cache_mode="reuse",
    )

run_scenario("baseline", Path("./configs/base"))
run_scenario("time_coeff_1.5", Path("./configs/adjusted"))

# Query: which runs used which coefficients?
time_coeff_runs = adapter.coefficients_by_run(
    coefficient="time",
    collapse="first",
    tracker=tracker,
)
for run_id, value in time_coeff_runs.items():
    print(f"Run: {run_id}, Time coefficient: {value}")
```
</details>

See [Config Adapters Integration Guide](docs/integrations/config_adapters.md) for ActivitySim details, best practices, and examples.

---

## Protocols and Noop Mode

Consist exposes protocol-based interfaces so integrations can type-check cleanly even
when tracking is disabled. Use `create_tracker` to return a real tracker or a noop
implementation based on configuration.

```python
from pathlib import Path
from consist import Tracker, TrackerLike, create_tracker

def build_tracker() -> Tracker:
    return Tracker(run_dir=Path("./runs"), db_path=Path("./provenance.duckdb"))

tracker: TrackerLike = create_tracker(
    enabled=settings.consist.enabled,
    tracker_factory=build_tracker,
)

with tracker.scenario("baseline") as sc:
    result = sc.run(
        fn=run_step,
        name="step",
        runtime_kwargs={"input_path": "inputs.csv"},
        output_paths={"out": "outputs.parquet"},
    )
    artifact = result.outputs["out"]
    print(artifact.path)
```

Protocols are available at `consist.protocols`:

- `ArtifactLike`
- `RunResultLike`
- `ScenarioLike`
- `TrackerLike`

---

## Where to Go Next

**I maintain or develop simulation tools:**
- Start with [Usage Guide](docs/usage-guide.md) for integration patterns
- See [Container Integration Guide](docs/containers-guide.md) to wrap existing tools with provenance and caching
- For ActivitySim users: See [Config Adapters](docs/integrations/config_adapters.md) to track and query calibration parameters

**I'm a practitioner who runs models:**
- No coding required. See [CLI Reference](docs/cli-reference.md) to query and compare results from the command line
- Need help? Check [Troubleshooting](docs/troubleshooting.md) for common issues and solutions

**I'm a researcher building simulation workflows:**
- See [Concepts](docs/concepts/overview.md) for mental models
- Then [Data Materialization](docs/concepts/data-materialization.md) for SQL-native analytics
- See [DLT Loader Guide](docs/dlt-loader-guide.md) for schema-validated data ingestion

**I want to understand how it works:**
- [Concepts](docs/concepts/overview.md) explains the mental model
- [Architecture](docs/architecture.md) goes deeper into caching and lineage tracking

**I'm debugging an issue:**
- See [Troubleshooting Guide](docs/troubleshooting.md) for cache issues, mount problems, data schema errors, and container failures

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

---

## Current Status

Consist is under active development and used in production for the [PILATES](https://github.com/ual/PILATES) integrated land use and transportation modeling framework.

We welcome feedback from researchers working with multi-model simulation workflows.

---

## Etymology

In railroad terminology, a **consist** (noun, pronounced CON-sist) is the specific lineup of locomotives and cars that make up a train. In this library, a **consist** is the immutable record of exactly which components—code version, configuration, input artifacts—were coupled together to produce a particular run's outputs.

---

## License

[BSD (modified)](LICENSE)
