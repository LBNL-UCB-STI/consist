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

### Which Approach Should I Use?

Consist provides two complementary ways to structure your workflow:

**Use `@task` decorators if:**
- You have individual processing functions that should be cached independently
- Each function takes clear inputs and returns outputs
- You want automatic signature-based caching without manual run management
- Your workflow is a simple chain of function calls

**Use `scenario` + `step` contexts if:**
- You have multi-step workflows that naturally group together (e.g., multi-year simulations)
- You need a parent "header" run that summarizes the entire workflow
- You want to query and compare results across scenario variants
- Your steps involve imperative code blocks, not just pure functions

**Use both together if:**
- You have complex workflows where some steps are reusable functions (tasks) and others are larger orchestration blocks (scenarios/steps)

### Example 1: Single-Step Processing with Tasks

Tasks are best for functions that transform data in a cacheable way. Consist inspects the function signature to determine inputs and configuration automatically.

```python
from consist import Tracker
from pydantic import BaseModel
from pathlib import Path

# Define your configuration and output schemas
class CleaningConfig(BaseModel):
    threshold: float = 0.5
    remove_outliers: bool = True

class CleanedData(BaseModel):
    id: int
    value: float

# Initialize the tracker
tracker = Tracker(
    root="./runs",                 # Where run directories are created
    db_path="./provenance.duckdb", # Queryable provenance database
    schemas=[CleanedData]          # Register schemas for querying
)

# Decorate your function
@tracker.task()
def clean_data(raw_file: Path, config: CleaningConfig) -> Path:
    """Load raw data, apply cleaning rules, write output."""
    output_path = Path("./cleaned.parquet")
    # ... processing logic ...
    return output_path

# Use it like a normal function - caching happens automatically
config = CleaningConfig(threshold=0.8)
result = clean_data(Path("raw.csv"), config)  # First call: executes
result = clean_data(Path("raw.csv"), config)  # Second call: cached!

# Different config = different signature = new execution
result = clean_data(Path("raw.csv"), CleaningConfig(threshold=0.9))

# Control cache behavior with cache_mode
@tracker.task(cache_mode="overwrite")  # Always re-execute, update cache
def clean_data_fresh(raw_file: Path, config: CleaningConfig) -> Path:
    ...

@tracker.task(cache_mode="readonly")  # Use cache but don't persist changes
def clean_data_whatif(raw_file: Path, config: CleaningConfig) -> Path:
    ...
```

When `clean_data` is called, Consist computes a signature from the function's code, the config object, and the input file's hash. Matching signatures return cached results immediately; otherwise the function executes and the output is recorded.

### Example 2: Multi-Step Workflows with Scenarios

For complex simulations with multiple sequential steps, scenarios provide structure and grouping. Each scenario creates a header run that links all its steps together.

```python
from consist import scenario, log_dataframe

# The scenario context creates a parent "header" run
with scenario("baseline", tracker=tracker, model="travel_demand") as sc:
    # Register inputs that apply to the whole scenario
    sc.add_input("population.csv", key="pop")
    sc.add_input("network.geojson", key="road_network")
    
    # Each step is a child run, automatically linked to the parent
    with sc.step("year_2020", model="travel_demand"):
        df_2020 = run_model(year=2020)
        log_dataframe(df_2020, key="persons_2020")
    
    with sc.step("year_2030", model="travel_demand"):
        df_2030 = run_model(year=2030)
        log_dataframe(df_2030, key="persons_2030")
    
    with sc.step("year_2040", model="travel_demand"):
        df_2040 = run_model(year=2040)
        log_dataframe(df_2040, key="persons_2040")
```

All steps share the same `consist_scenario_id="baseline"` in the database, making it easy to query results across years or compare different scenario variants.

### Mixing Tasks and Scenarios

You can call cached `@task` functions inside scenario steps to reuse heavy computations:

```python
@tracker.task()
def preprocess_network(network_file: Path, year: int) -> Path:
    """Expensive preprocessing that should be cached."""
    # ... processing logic ...
    return output_path

with scenario("baseline", tracker=tracker) as sc:
    sc.add_input("raw_network.geojson", key="network")
    
    with sc.step("preprocess"):
        # This task is cached independently - running multiple scenarios
        # with the same network won't recompute preprocessing
        processed = preprocess_network(Path("raw_network.geojson"), year=2020)
    
    with sc.step("simulate"):
        run_simulation(processed)
```

### Wrapping Legacy Code

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

Consist will hash the configuration files, execute the function, capture any CSV files created in `./outputs`, and register them as output artifacts.

### Querying Results

Consist provides tools to find runs and load their artifacts without manually parsing directory paths:

```python
# Find a specific run using filters
run = consist.find_run(
    tracker=tracker,
    model_name="travel_demand",
    tags=["baseline"],
    year=2030
)

# Access artifacts using structured keys
artifacts = tracker.get_artifacts_for_run(run.id)
persons_file = artifacts.outputs["persons_2030"]

# Load data (handles path resolution automatically)
df = consist.load(persons_file)
```

For cross-run queries, use Consist's data virtualization to query artifacts as if they were database tables:

```python
from sqlmodel import select

# Access the auto-generated view for your schema
VCleanedData = tracker.views.CleanedData

# Query across all runs that produced this type of artifact
query = select(
    VCleanedData.consist_scenario_id,
    VCleanedData.value
).where(VCleanedData.consist_year == 2030)

results = consist.run_query(query, tracker=tracker)
```

These views combine ingested data and files on disk, allowing you to query millions of rows across multiple runs without loading everything into memory.

---

## Features

### Intelligent Caching

Consist identifies runs using a three-part signature: `SHA256(code_hash | config_hash | input_hash)`.

- **Code hash**: Captured from your Git commit SHA. If your working tree has uncommitted changes, Consist appends a `-dirty-<timestamp>` suffix to ensure edited code invalidates the cache. For `@task` decorators, you can choose to hash just the function or the entire module.
  
- **Config hash**: A canonical representation of your configuration dictionary, normalized to handle dict ordering and numeric type variations (e.g., `int` vs `float`).
  
- **Input hash**: For Consist-produced artifacts, this references the *signature of the run that produced them* (Merkle linking). For raw files, Consist hashes the file contents or metadata.

This Merkle DAG structure means that:

- Changing a parameter invalidates only downstream runs that depend on it.
- Identical inputs always produce cache hits, even across machines (given the same code version).
- You can safely delete intermediate files; provenance validity depends on the lineage graph, not file existence.

The last point—which we call **Ghost Mode**—is particularly useful for long-running studies where intermediate outputs may be cleaned up to save storage, but you still want to query what happened.

#### Cache Modes

Control caching behavior with the `cache_mode` parameter:

- **`reuse` (default)**: Look up runs with matching signatures. On a cache hit, return the cached outputs without re-execution. This is the standard behavior for reproducible workflows.

- **`overwrite`**: Always execute the run, but update the cache entry for that signature. Useful when you've fixed a bug and want to regenerate results without changing the signature.

- **`readonly`**: Use cached results when available, but don't persist any changes. Ideal for sandbox or what-if explorations where you don't want to pollute the provenance database.

```python
# Default: reuse existing results
@tracker.task()
def process_data(input_file: Path) -> Path:
    ...

# Always regenerate, update cache
@tracker.task(cache_mode="overwrite")
def process_data_fresh(input_file: Path) -> Path:
    ...

# Read from cache but don't save changes
with tracker.start_run(..., cache_mode="readonly"):
    ...
```

#### Automatic Lineage Tracking

When an input artifact was produced by Consist, the tracker automatically sets `parent_run_id` to link runs together. This creates an explicit provenance chain: if `run_B` consumes an artifact from `run_A`, querying `run_B`'s lineage will recursively show that it depends on `run_A`, along with `run_A`'s configuration and inputs. You can export this full DAG to understand exactly what produced any result.

### Data Virtualization

Consist can create SQL views over your artifacts without copying data into the database. This is useful when you want to query across many simulation runs (e.g., "compare household counts across all 2030 scenarios").

Because Consist knows your schemas (registered via `Tracker(schemas=[...])`) and the file locations, it generates **Smart Views** that automatically union data from disk.

```python
# Access the auto-generated view for your schema
VHousehold = tracker.views.Household

# Query using SQLModel (or raw SQL)
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
