# Consist

**Provenance tracking, intelligent caching, and data virtualization for scientific simulation workflows.**

Consist automatically records what code, configuration, and input data produced each output in your pipeline. It uses this information to skip redundant computation and to let you query results across many runs without manual bookkeeping.

---

## Etymology

In railroad terminology, a **consist** (noun, pronounced CON-sist) is the specific lineup of locomotives and cars that make up a train.

In this library, a **consist** is the immutable record of exactly which components—code version, configuration, input artifacts—were coupled together to produce a particular run's outputs.

---

## Motivation

Multi-model simulation workflows accumulate friction over time:

- **Provenance ambiguity.** Which configuration produced the results in Figure 3? Was that before or after the land use update?
- **Redundant computation.** Re-running an entire pipeline because one downstream parameter changed.
- **Scattered outputs.** Writing one-off scripts to compare results across scenario variants.
- **Costly data transfers.** Sharing results means copying large files, losing the context of how they were produced.

Consist addresses these problems by treating provenance as a first-class concern rather than an afterthought.

---

## How It Works

Consist maintains two synchronized records of your workflow:

1. **A JSON log** (`consist.json`) that serves as the portable, human-readable source of truth. This file can be version-controlled and survives database corruption.

2. **A DuckDB database** that enables fast queries across runs, artifacts, and lineage relationships.

Each run is identified by a cryptographic hash of its inputs: the code version (git SHA), configuration parameters, and the provenance IDs of input artifacts. If all three match the prior run, Consist returns cached results. If any differ, it executes the run and records the new lineage.

### Path Virtualization

Consist stores relative URIs rather than absolute paths, so provenance records remain valid when data moves between machines or storage systems. You define mount points (e.g., `inputs → /mnt/project/data`), and Consist translates paths automatically.

---

## Quickstart

### Installation

```bash
pip install consist
```

<!-- [TODO: Update once published to PyPI] -->

### Basic Usage

Consist exposes two complementary interfaces:
- **Tasks (`@task`)**: Best for pure-ish units where you want signature-based caching and automatic artifact registration. Call a task multiple times with the same inputs to get cache hits.
- **Scenarios/Steps (`scenario` / `scenario.step`)**: Best for hierarchical, multi-step workflows with a parent header run and child step runs (e.g., multi-year simulations). Steps capture lineage and grouping; you can still call cached tasks inside steps for reuse.

#### Example: Task

```python
from consist import Tracker
from pydantic import BaseModel
from pathlib import Path

# Schemas allow Consist to create virtual tables for your outputs
class CleanedData(BaseModel):
    id: int
    value: float

tracker = Tracker(
    root="./runs",                 # Where run directories are created
    db_path="./provenance.duckdb", # Queryable provenance database
    schemas=[CleanedData]          # Register schemas for querying
)

class CleaningConfig(BaseModel):
    threshold: float = 0.5
    remove_outliers: bool = True

@tracker.task()
def clean_data(raw_file: Path, config: CleaningConfig) -> Path:
    """Load raw data, apply cleaning rules, write output."""
    output_path = Path("./cleaned.parquet")
    # ... processing logic ...
    return output_path
```

When `clean_data` is called:

1. Consist computes a signature from the function's code, the config object, and the input file's hash.
2. If a prior run with the same signature exists, Consist returns the cached output immediately.
3. Otherwise, it executes the function, records the output artifact, and links it to the run.

```python
config = CleaningConfig(threshold=0.8)

# First call: executes the function
result = clean_data(Path("raw.csv"), config)

# Second call with same inputs: returns cached result
result = clean_data(Path("raw.csv"), config)

# Third call with different config: executes again
result = clean_data(Path("raw.csv"), CleaningConfig(threshold=0.9))
```

#### Example: Scenario/Step

```python
from consist import scenario, log_dataframe

with scenario("baseline", tracker=tracker, model="travel_demand") as sc:
    sc.add_input("population.csv", key="pop")
    with sc.step("year_2030", model="travel_demand"):
        # do work, log artifacts
        log_dataframe(df_result, key="persons_2030")
```

You can mix both: call cached `@task` functions inside `scenario.step` to reuse heavy computations.

### Wrapping Existing Code

Many scientific models read configuration from files and write outputs to directories rather than accepting arguments and returning paths. The `depends_on` and `capture_dir` parameters handle this pattern:

```python
@tracker.task(
    depends_on=["config.yaml", "parameters.json"],  # Include in signature
    capture_dir="./outputs",                         # Watch for new files
    capture_pattern="*.csv"
)
def run_simulation(upstream_artifact):
    """Wrapper around legacy simulation code."""
    import legacy_model
    legacy_model.run()  # Reads config.yaml, writes to ./outputs
    # Return None when using capture_dir; Consist logs outputs automatically
```

### Inspecting Provenance

Consist provides ergonomic tools to find specific runs and load their results without manually traversing directory paths.

```python
# Find a specific run using filters
# options: consist.find_run() for one, consist.find_runs() for many
run = consist.find_run(
    tracker=tracker,
    model_name="clean_data",
    tags=["production"],
    year=2030
)

# Access artifacts using structured keys
# No need to parse file paths manually
artifacts = tracker.get_artifacts_for_run(run.id)
cleaned_file = artifacts.outputs["cleaned_parquet"]

# Load data (handles path resolution automatically)
df = consist.load(cleaned_file)
```

### Scenario Workflows

For complex simulations involving multiple steps (e.g., Year 1 → Year 2 → Year 3), Consist offers the `scenario` context. This automatically links child steps to a parent "Header Run" and names them predictably.

```python
with consist.scenario("baseline_scenario", tracker=tracker) as scenario:
    # 1. Register exogenous inputs once for the whole scenario
    scenario.add_input("population_forecast.csv", key="pop_growth")
    
    # 2. Run steps (automatically linked to parent "baseline_scenario")
    with scenario.step("year_2020", model="travel_demand"):
        run_model(year=2020)
        
    with scenario.step("year_2030", model="travel_demand"):
        run_model(year=2030)
```

In the database, these runs will share a `consist_scenario_id="baseline_scenario"`, making it trivial to group results.

Tip: use `consist.log_dataframe(df, key="households", schema=Household)` inside a step to write, log, and ingest a DataFrame in one call, and `consist.run_query(query, tracker=tracker)` to execute SQLModel/SQLAlchemy queries without manually managing sessions.

---

## Features

### Intelligent Caching

Runs are identified by the hash of their inputs: code version, configuration, and upstream artifact provenance. This Merkle DAG structure means that:

- Changing a parameter invalidates only downstream runs that depend on it.
- Identical inputs always produce cache hits, even across machines (given the same code version).
- You can safely delete intermediate files; provenance validity depends on the lineage graph, not file existence.

The last point—which we call **Ghost Mode**—is particularly useful for long-running studies where intermediate outputs may be cleaned up to save storage, but you still want to query what happened.

### Data Virtualization

Consist can create SQL views over your artifacts without copying data into the database. This is useful when you want to query across many simulation runs (e.g., "compare household counts across all 2030 scenarios").

Because Consist knows your schemas (registered via `Tracker(schemas=[...])`) and the file locations, it generates **Smart Views** that automatically union data from disk.

```python
# 1. Access the auto-generated view for your schema
VHousehold = tracker.views.Household

# 2. Query using SQLModel (or raw SQL)
# Note: 'consist_scenario_id' is automatically injected for easy grouping
query = select(
    VHousehold.consist_scenario_id,
    VHousehold.income,
    VHousehold.zone_id
).where(VHousehold.consist_year == 2030)

results = consist.run_query(query, tracker=tracker)
```

These **hybrid views** combine data that has been explicitly ingested into DuckDB with data that remains in Parquet or CSV files on disk. DuckDB's vectorized reader handles the files directly, avoiding memory overhead.

#### Performance

Querying 10 million rows across multiple simulation runs:

| Operation      | Pandas         | Polars (lazy)  | Consist        |
|:---------------|:---------------|:---------------|:---------------|
| Aggregate      | 0.46s / 780 MB | 0.24s / 280 MB | 0.04s / 80 MB  |
| Join (5M rows) | 0.76s / 620 MB | 0.20s / 440 MB | 0.12s / 272 MB |

*Benchmark on Apple M3 Max. Consist queries Parquet files directly via DuckDB without loading into memory.*

### Container Support

For models that run inside Docker or Singularity containers, Consist treats the container as a function: the image digest and command are part of the configuration hash, and mounted volumes are tracked as inputs and outputs.

```python
result = tracker.run_container(
    image="model:v2.1",
    command=["python", "run.py", "--year", "2030"],
    mounts={
        "inputs": "/data/inputs",    # Read-only input directory
        "outputs": "/data/outputs",  # Captured as output artifacts
    },
)
```

This ensures that changing the container version invalidates the cache, while re-running the same version with the same inputs returns cached results.

---

## Comparison with Related Tools

| Tool                 | Primary Focus                            | Consist Difference                                                                        |
|:---------------------|:-----------------------------------------|:------------------------------------------------------------------------------------------|
| DVC                  | Data versioning, Git-like workflow       | Consist tracks execution provenance, not just file versions                               |
| MLflow               | ML experiment tracking                   | Consist is domain-agnostic and handles multi-model simulation workflows                   |
| Snakemake / Nextflow | Workflow orchestration and DAG execution | Consist focuses on provenance and caching; can complement an orchestrator                 |
| OpenLineage          | Lineage metadata standard                | Consist is a runtime library, not a metadata spec (though it can emit OpenLineage events) |

Consist is designed for scientific simulation workflows where: runs are expensive (minutes to hours), provenance questions arise after the fact ("what produced this?"), and comparing results across many scenario variants is a common analysis task.

---

## Current Status

Consist is under active development. It is currently used in production for the [PILATES](https://github.com/ual/PILATES) integrated land use and transportation modeling framework.

We welcome feedback from researchers working with similar multi-model simulation workflows.

---

## Citation

<!-- [TODO: Add citation once published] -->

If you use Consist in academic work, please cite:

```bibtex
@software{consist,
  author = {[Author Name]},
  title = {Consist: Provenance Tracking and Caching for Scientific Simulation Workflows},
  year = {2025},
  url = {https://github.com/[org]/consist}
}
```

---

## Contributing

Contributions are welcome. Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

**Requirements:**
- Python 3.10+
- DuckDB
- Docker or Singularity (optional, for container support)

---

## License

[MIT License](LICENSE)

<!-- [TODO: Confirm license choice] -->
