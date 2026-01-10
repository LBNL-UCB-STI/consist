# Usage Guide

Consist provides flexible patterns for tracking provenance in scientific workflows. This guide walks you through the main usage patterns, from simple single-step runs to complex multi-year simulations.

**New to Consist?** Start with the [quickstart notebook](examples.md#quickstart), then work through the examples below in order.

---

## Choosing Your Pattern

Choose based on what you're building:

| Your Workflow | Pattern | Why |
|---|---|---|
| Single data processing step (clean, transform, aggregate) | **`run()`** | Simple, caches entire function call, lowest overhead. Use for self-contained operations. |
| Multi-step workflow (preprocessing → simulation → analysis) | **`scenario()`** | Groups related steps, shares state via coupler, per-step caching. Use when steps have dependencies or shared configuration. |
| Existing tool/model (subprocess, legacy code, container) | **`container` or `depends_on`** | Wraps external executables, tracks container digest as cache key. Use for black-box tools. |
| Parameter sweep / sensitivity analysis | **`scenario()` + loop** | Run the same step with different configs, compare results. |
| Multi-year simulation | **`scenario()` + loop** | Runs in years, each year caches independently, all years share scenario context. |

---

## Pattern 1: Single-Step Runs (`run()`)

Use `run()` when you have a self-contained operation: data cleaning, transformation, aggregation, or any callable that takes inputs and produces outputs. Consist caches the entire function call based on code version + config + input data.

**When to use:**
- Simple data transformations (filter, aggregate, merge)
- Expensive computations that don't depend on other runs
- One-off analyses
- You want the simplest mental model

**When NOT to use:**
- Multi-step workflows with dependencies between steps (use `scenario()` instead)
- Existing tools that write to directories (use `depends_on` or `container`)

### Simple Example: Data Cleaning

Here's the basic pattern:

```python
import consist
from consist import Tracker, use_tracker
from pathlib import Path
import pandas as pd

# 1. Create tracker
tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

# 2. Define your function
def clean_data(raw: pd.DataFrame, threshold: float = 0.5) -> pd.DataFrame:
    df = raw[raw["value"] > threshold]  # Filter
    return df

# 3. Run it with tracker context
with use_tracker(tracker):
    result = consist.run(
        fn=clean_data,
        inputs={"raw": Path("raw.csv")},
        config={"threshold": 0.5},  # Part of cache key
        outputs=["cleaned"],
    )

# 4. Access results
cleaned_artifact = result.outputs["cleaned"]
cleaned_df = consist.load(cleaned_artifact)
print(f"Output: {cleaned_artifact.path}")
```

Run it again with the same inputs and config → instant cache hit, no re-execution.

<details>
<summary>Alternative: keep raw file paths (no auto-load)</summary>

Use this when your function needs a `Path` and manages I/O directly.

```python
def clean_data(raw_file: Path, _consist_ctx) -> None:
    df = pd.read_csv(raw_file)
    out_path = _consist_ctx.run_dir / "cleaned.parquet"
    df.to_parquet(out_path)
    _consist_ctx.log_output(out_path, key="cleaned")

with use_tracker(tracker):
    result = consist.run(
        fn=clean_data,
        inputs={"raw_file": Path("raw.csv")},  # hashed input
        load_inputs=False,
        runtime_kwargs={"raw_file": Path("raw.csv")},
        inject_context=True,
    )
```
</details>

### Example with Config

Use Pydantic models for structured configs:

```python
import consist
from consist import Tracker, use_tracker
from pydantic import BaseModel
from pathlib import Path
import pandas as pd

class CleaningConfig(BaseModel):
    threshold: float = 0.5
    remove_outliers: bool = True

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

def clean_data(raw: pd.DataFrame, config: CleaningConfig) -> pd.DataFrame:
    df = raw
    df = df[df["value"] >= config.threshold]
    if config.remove_outliers:
        df = df[df["value"] < df["value"].quantile(0.95)]
    return df

with use_tracker(tracker):
    result = consist.run(
        fn=clean_data,
        inputs={"raw": Path("raw.csv")},
        config=CleaningConfig(threshold=0.5, remove_outliers=True),
        outputs=["cleaned"],
    )
```

Each distinct config → separate cache entries. Change the threshold? Only that run re-executes.

### Wrapping Legacy or Black-Box Tools

If you have existing code that writes files to a directory, use the injected run context to capture outputs:

```python
from pathlib import Path

def run_legacy_model(upstream, _consist_ctx) -> None:
    import legacy_model

    output_dir = _consist_ctx.run_dir / "legacy_outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    with _consist_ctx.capture_outputs(output_dir, pattern="*.csv"):
        legacy_model.run(upstream, output_dir=output_dir)

with use_tracker(tracker):
    result = consist.run(
        fn=run_legacy_model,
        inputs={"upstream": Path("input.csv")},
        depends_on=[Path("config.yaml"), Path("parameters.json")],  # Hash these files too
        inject_context=True,
    )
```

Captured outputs are keyed by filename stem (for example, `results.csv` -> `results`).

<details>
<summary>Alternative: capture a fixed output directory</summary>

If the tool always writes to a known folder and returns `None`, you can capture it directly:

```python
with use_tracker(tracker):
    def run_legacy_model(upstream) -> None:
        import legacy_model

        legacy_model.run(upstream, output_dir="outputs")

    result = consist.run(
        fn=run_legacy_model,
        inputs={"upstream": Path("input.csv")},  # auto-loaded into the upstream arg
        depends_on=[Path("config.yaml")],
        capture_dir=Path("outputs"),
        capture_pattern="*.csv",
    )
```
</details>

<details>
<summary>How input mappings become function arguments</summary>

When `inputs` is a mapping, Consist matches keys to function parameters and auto-loads
those artifacts (for example, a CSV becomes a DataFrame) into the call. This is why
`inputs={"upstream": ...}` is passed as the `upstream` argument above.

If you want raw paths instead of auto-loaded data, set `load_inputs=False` and pass
paths explicitly via `runtime_kwargs`.
</details>

The `depends_on` files are hashed as part of the cache key, so changing config.yaml invalidates the cache.

---

## Pattern 2: Multi-Step Workflows (`scenario()`)

Use `scenario()` when you have multiple interdependent steps that share state or configuration. Scenarios group steps into a coherent unit (a "run scenario"), while each step caches independently.

**When to use:**
- Multi-step pipelines (preprocess → simulate → analyze)
- Steps have data dependencies (output of step 1 is input to step 2)
- Multi-year simulations (year 2020 → 2030 → 2040)
- Parameter sweeps where you want to compare across variants
- Shared configuration across multiple steps

**Benefits over `run()`:**
- Steps cache independently—skip re-executing steps whose inputs haven't changed
- Use the coupler to pass data between steps with automatic provenance tracking
- Group runs into scenarios for easy cross-scenario queries

### Simple Example: Two-Step Workflow

```python
import consist
from consist import Tracker, use_tracker
from pathlib import Path
import pandas as pd

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

# Define steps as regular functions
def preprocess_data(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw[raw["value"] > 0.5]
    return df

def analyze_data(preprocessed: pd.DataFrame) -> pd.DataFrame:
    summary = preprocessed.groupby("category", as_index=False)["value"].mean()
    return summary

# Execute as a scenario
with use_tracker(tracker):
    with consist.scenario("my_analysis") as sc:
        sc.run(
            name="preprocess",
            fn=preprocess_data,
            inputs={"raw": Path("raw.csv")},
            outputs=["preprocessed"],
        )

        sc.run(
            name="analyze",
            fn=analyze_data,
            input_keys=["preprocessed"],
            outputs=["analysis"],
        )
```

All steps have `scenario_id="my_analysis"`, making it easy to query together. Change preprocess logic? The preprocess step re-runs; the analyze step re-runs only if the preprocessed artifact changes. Change raw.csv? Both re-execute.

<details>
<summary>Alternative: inline steps with sc.trace</summary>

Use this when you want inline code blocks (they always execute, even on cache hits).

```python
with use_tracker(tracker):
    with consist.scenario("my_analysis") as sc:
        with sc.trace(name="preprocess", inputs={"raw": Path("raw.csv")}):
            df = pd.read_csv("raw.csv")
            consist.log_dataframe(df, key="preprocessed")

        with sc.trace(name="analyze", input_keys=["preprocessed"]):
            df = consist.load(sc.coupler.require("preprocessed"))
            summary = df.groupby("category", as_index=False)["value"].mean()
            consist.log_dataframe(summary, key="analysis")
```
</details>

### Passing Data with the Coupler

The **coupler** tracks data flowing between steps and makes provenance explicit:

```python
with use_tracker(tracker):
    with consist.scenario("baseline", model="travel_demand") as sc:
        coupler = sc.coupler

        # Step 1: Load and prepare
        with sc.trace(name="initialize", run_id="baseline_init"):
            df_pop = load_population()
            pop_artifact = consist.log_dataframe(df_pop, key="population")
            coupler.set("population", pop_artifact)  # Store for later steps

        # Step 2: Simulate for each year
        for year in [2020, 2030, 2040]:
            with sc.trace(
                name="simulate",
                run_id=f"baseline_{year}",
                year=year,
                input_keys=["population"],  # Declare dependency
            ):
                # Get data from coupler (with automatic cache detection)
                df_pop = consist.load(coupler.require("population"))
                df_result = run_model(year, df_pop)

                # Log output and store for downstream steps
                result_artifact = consist.log_dataframe(df_result, key="persons")
                coupler.set("persons", result_artifact)
```

**What `input_keys` does:**
- Declares that this step depends on "population" artifact
- Consist tracks this as part of the cache key
- If the population artifact hasn't changed, this step's cache is still valid

**Optional: Declare outputs and use a typed coupler view**

Output validation and typed coupler access provide safety and ergonomics:

```python
from consist import coupler_schema

@coupler_schema
class WorkflowCoupler:
    """Schema for workflow outputs with IDE autocomplete support."""
    zarr_skims: Artifact
    synthetic_population: Artifact

with use_tracker(tracker):
    with consist.scenario("workflow") as sc:
        # Declare what outputs are required (optional, but catches missing data early)
        sc.declare_outputs(
            "zarr_skims", "synthetic_population",
            required={"zarr_skims": True, "synthetic_population": True}
        )

        # Get a typed view for attribute-style access
        typed = sc.coupler_schema(WorkflowCoupler)

        # Run a step and collect specific outputs
        compile_result = sc.run("compile", fn=asim_compile_runner.run)
        sc.collect_by_keys(compile_result.outputs, "zarr_skims", "synthetic_population")

        # Typed attribute access with IDE autocomplete (type-safe)
        skims_artifact = typed.zarr_skims
        sc.run("main", fn=asim_runner.run, input_keys="zarr_skims")
```

**How output validation works:**
- If a required output is not set before the scenario exits, a `RuntimeError` is raised
- This catches data flow bugs early rather than failing downstream
- Mix required and optional outputs with per-key granularity

**When a missing output error occurs:**
```
RuntimeError: Scenario missing declared outputs: synthetic_population, zarr_skims.
```

**Bulk logging with metadata:**
```python
with consist.scenario("outputs") as sc:
    # Log multiple files at once with explicit keys
    outputs = sc.log_artifacts(
        {
            "persons": "results/persons.parquet",
            "households": "results/households.parquet",
            "jobs": "results/jobs.parquet"
        },
        metadata_by_key={
            "households": {"role": "primary_unit"},
            "jobs": {"role": "employment_proxy"}
        },
        year=2030,
        scenario_name="base"
    )
    # All get year=2030, scenario_name="base"
    # households also gets role="primary_unit"
```

### Example: Parameter Sweep in a Scenario

```python
with use_tracker(tracker):
    with consist.scenario("sensitivity_analysis") as sc:
        coupler = sc.coupler

        # Load once, use for all variants
        with sc.trace(name="setup"):
            df = pd.read_csv("data.csv")
            artifact = consist.log_dataframe(df, key="data")
            coupler.set("shared_data", artifact)

        # Test different thresholds
        for threshold in [0.3, 0.5, 0.7]:
            with sc.trace(
                name="analyze",
                run_id=f"threshold_{threshold}",
                threshold=threshold,
                input_keys=["shared_data"],
            ):
                df = consist.load(coupler.require("shared_data"))
                filtered = df[df["value"] > threshold]
                consist.log_dataframe(filtered, key="filtered")
```

Each threshold creates a separate run with its own cache entry. Re-run later? Consist returns cached results for matching thresholds, skips setup (since input unchanged).

---

## Pattern 3: Container Integration

Use containers when you have existing tools, models, or legacy code that runs as a subprocess or Docker container. The image digest becomes part of the cache key.

**When to use:**
- Running ActivitySim, SUMO, BEAM, or other external models
- Legacy code you don't want to refactor
- Python/R/Java executables that you invoke as subprocesses
- Tools that expect specific file paths or output directories

```python
from consist.integrations.containers import run_container
from pathlib import Path

host_inputs = Path("data/inputs").resolve()
host_outputs = Path("data/outputs").resolve()

result = run_container(
    tracker=tracker,
    run_id="model_2030",
    image="travel-model:v2.1",  # Image digest becomes cache key
    command=["python", "run.py", "--year", "2030"],
    volumes={
        str(host_inputs): "/inputs",
        str(host_outputs): "/outputs",
    },
    inputs=[host_inputs / "input.csv"],
    outputs={"results": host_outputs / "results.parquet"},
)

output_artifact = result.artifacts["results"]
```

Change the image version? Cache invalidates. Same image + inputs? Returns cached results.

<details>
<summary>Alternative: use consist.run with executor="container"</summary>

If you prefer the same API as `consist.run`, you can use the container executor:

```python
import consist

with consist.use_tracker(tracker):
    result = consist.run(
        name="model_2030",
        executor="container",
        inputs=[host_inputs / "input.csv"],
        output_paths={"results": host_outputs / "results.parquet"},
        container={
            "image": "travel-model:v2.1",
            "command": ["python", "run.py", "--year", "2030"],
            "volumes": {str(host_inputs): "/inputs", str(host_outputs): "/outputs"},
        },
    )
```
</details>

---

## Advanced Patterns

### Cache Hydration

By default, Consist returns metadata-only cache hits (no file copies). You can opt in to materializing cached files when needed:

- **`outputs-requested`**: Copy only specific cached outputs to paths you provide
- **`outputs-all`**: Copy all cached outputs into a target directory
- **`inputs-missing`**: When a cache miss occurs, backfill missing inputs from prior runs before executing

Set per-run via `cache_hydration=...` or for scenario steps via `step_cache_hydration=...`:

```python
with use_tracker(tracker):
    with consist.scenario("baseline", step_cache_hydration="inputs-missing") as sc:
        coupler = sc.coupler
        # This step will backfill inputs if they're missing from disk
        with sc.trace(name="simulate", input_keys=["population"]):
            df_pop = consist.load(coupler.require("population"))
            # ... rest of simulation ...
```

### Mixing Runs and Scenarios

Call `consist.run(...)` inside a scenario when a step should cache independently:

```python
with use_tracker(tracker):
    with consist.scenario("baseline") as sc:
        coupler = sc.coupler
        # Run expensive preprocessing independently
        preprocess = consist.run(
            fn=expensive_preprocessing,
            inputs={"network_file": Path("network.geojson")},
            outputs=["processed"],
        )
        # Add outputs to coupler for downstream steps
        coupler.update(preprocess.outputs)

        # Later steps can use the preprocessed output
        with sc.trace(name="simulate", input_keys=["processed"]):
            network = consist.load(coupler.require("processed"))
            # ... simulation ...
```

### Function-Shaped Scenario Steps (Skip on Cache Hit)

`sc.trace(...)` is a context manager, so its block always executes even on cache hits. If you want Consist to skip calling expensive code on cache hits, use `sc.run(...)`:

```python
with use_tracker(tracker):
    with consist.scenario("baseline") as sc:
        def beam_preprocess(data):
            # Prepare inputs for the external model
            return data

        sc.run(
            name="beam_preprocess",
            fn=beam_preprocess,  # Only called on cache miss
            inputs={"data": Path("data.parquet")},
            outputs=["beam_inputs"],
        )
        beam_inputs = sc.coupler.require("beam_inputs")
```

If your step operates on large files, prefer path-based inputs and keep `load_inputs=False` to avoid eager reads; use `cache_hydration` to ensure cached outputs exist on disk when needed.

<details>
<summary>Alternative: log file outputs inside the step</summary>

If your function writes files, log them with the injected context (and read inputs yourself if needed):

```python
def beam_preprocess(data_file, _consist_ctx) -> None:
    out_path = _consist_ctx.run_dir / "beam_inputs.parquet"
    # ... write out_path ...
    _consist_ctx.log_output(out_path, key="beam_inputs")

with use_tracker(tracker):
    with consist.scenario("baseline") as sc:
        sc.run(
            name="beam_preprocess",
            fn=beam_preprocess,
            inputs={"data_file": Path("data.parquet")},
            load_inputs=False,
            runtime_kwargs={"data_file": Path("data.parquet")},
            inject_context=True,
        )
```
</details>

### Query Facets with `pivot_facets`

Log small, queryable config values (facets) and pivot them into a wide table for analysis:

```python
from sqlmodel import select
import consist

# Pivot config facets into columns
params = consist.pivot_facets(
    namespace="simulate",
    keys=["alpha", "beta", "mode"],
    value_columns={"mode": "value_str"},
)

rows = consist.run_query(
    select(params.c.run_id, params.c.alpha, params.c.beta, params.c.mode),
    tracker=tracker,
)

# Result: DataFrame with columns [run_id, alpha, beta, mode] for comparison
```

See [Concepts](concepts.md#the-config-vs-facet-distinction) for when to use `config` vs `facet`.

---

## Querying Results

### Finding Runs
See: [Example notebooks](examples.md#tutorial-series).

```python
import consist

# Find a specific run
run = consist.find_run(
    tracker=tracker,
    parent_id="baseline",  # Scenario ID
    year=2030,
    model="simulate"
)

# Get multiple runs indexed by a field
runs_by_year = consist.find_runs(
    tracker=tracker,
    parent_id="baseline",
    model="simulate",
    index_by="year"
)
result_2030 = runs_by_year[2030]
```

### Loading Artifacts
See: [Example notebooks](examples.md#tutorial-series).

```python
# Get artifacts for a run
artifacts = tracker.get_artifacts_for_run(run.id)
persons_artifact = artifacts.outputs["persons"]

# Load the data
df = consist.load(persons_artifact)
```

### Cross-Run Queries with Views
See: [Example notebooks](examples.md#tutorial-series).

Register schemas to enable SQL queries across all runs:

```python
import consist
from sqlmodel import SQLModel, Field, select, func
from consist import Tracker

class Person(SQLModel, table=True):
    person_id: int = Field(primary_key=True)
    age: int
    number_of_trips: int

tracker = Tracker(
    run_dir="./runs",
    db_path="./provenance.duckdb",
    schemas=[Person]
)

# After running scenarios...
VPerson = tracker.views.Person

query = (
    select(
        VPerson.consist_scenario_id,
        VPerson.consist_year,
        func.avg(VPerson.number_of_trips).label("avg_trips")
    )
    .where(VPerson.consist_scenario_id.in_(["baseline", "high_gas"]))
    .group_by(VPerson.consist_scenario_id, VPerson.consist_year)
)

results = consist.run_query(query, tracker=tracker)
```

Views automatically include `consist_scenario_id`, `consist_year`, and other metadata columns for filtering and grouping.
For more on ingestion and hybrid views, see [Ingestion & Hybrid Views](ingestion-and-hybrid-views.md).

### Generating Schemas from Captured Data

If you ingest tabular data into DuckDB, Consist can capture the observed schema and export an **editable SQLModel stub** so you can curate PK/FK constraints and then register the model for views.

See `docs/schema-export.md` for the full workflow (CLI + Python) and column-name/`__tablename__` guidelines.
See [Ingestion & Hybrid Views](ingestion-and-hybrid-views.md) for ingestion tradeoffs and DB fallback behavior.
