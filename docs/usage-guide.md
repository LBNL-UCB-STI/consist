# Usage Guide

Consist provides flexible patterns for tracking provenance in scientific workflows. This guide walks you through the main usage patterns, from simple single-step runs to complex multi-year simulations.

**New to Consist?** Start with the [quickstart notebook](examples.md#quickstart), then work through the examples below in order.

---

## Choosing Your Pattern

Choose based on what you're building:

| Your Workflow                                               | Pattern                         | Why                                                                                                                         |
|-------------------------------------------------------------|---------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| Single data processing step (clean, transform, aggregate)   | **`run`**                       | Simple: caches the entire function call with low overhead. Use for self-contained operations.                               |
| Multi-step workflow (preprocessing → simulation → analysis) | **`scenario`**                  | Groups related steps, shares state via coupler, per-step caching. Use when steps have dependencies or shared configuration. |
| Existing tool/model (subprocess, legacy code, container)    | **`container` or `depends_on`** | Wraps external executables, tracks container digest as cache key. Use for black-box tools.                                  |
| Parameter sweep / sensitivity analysis                      | **`scenario`&nbsp;+&nbsp;loop** | Run the same step with different configs, compare results.                                                                  |
| Multi-year simulation                                       | **`scenario`&nbsp;+&nbsp;loop** | Runs in years, each year caches independently, all years share scenario context.                                            |

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

If you run it again with the same inputs and config, you should get a cache hit and no re-execution.

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

### Understanding the Coupler

The **coupler** is your scenario-scoped artifact registry. When you log an artifact with a key, it's automatically stored in the coupler, making data flow between steps explicit and traceable.

**Etymology**: A coupler (like the library name "consist" from railroad terminology) is the mechanism that links train cars together. In Consist, the coupler links your workflow steps by storing and threading their outputs.

**Key behaviors:**
- When you log an artifact with a `key`, it's automatically synced to the coupler
- You retrieve artifacts with `coupler.require(key)` or via `inputs=` declarations
- The coupler persists across all steps in a scenario
- Each scenario has its own coupler; they don't share data
- On cache hits, cached outputs are pre-synced to the coupler before your step runs

**You interact with the coupler when:**
- Accessing inputs in trace blocks: `coupler.require("population")`
- Declaring inputs to `sc.run()`: `inputs=["population"]`
- Validating that outputs were produced: `sc.require_outputs(...)`

**Live-sync (automatic):** When you log an artifact, it's immediately available in the coupler—you don't need to manually call `coupler.set()`.

**For optional-Consist workflows:** If you're using Consist in optional mode (with fallback to Path objects or artifact-like objects), use `coupler.set_from_artifact(key, value)` instead of `coupler.set()`. It handles both real Artifacts and artifact-like objects (Paths, strings, noop artifacts) transparently.

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
            inputs=["preprocessed"],
            load_inputs=True,
            outputs=["analysis"],
        )
```

All steps have `scenario_id="my_analysis"`, making it easy to query together. Change preprocess logic? The preprocess step re-runs; the analyze step re-runs only if the preprocessed artifact changes. Change raw.csv? Both re-execute.

### Declaring Inputs: Mapping Form vs List Form

The `inputs=` parameter supports two different forms for different situations:

**Mapping form** (explicit paths from disk):
```python
sc.run(
    name="preprocess",
    fn=preprocess_data,
    inputs={"raw": Path("raw.csv")},  # Load from disk
    outputs=["preprocessed"],
)
```
Use this when you're loading data from disk for the first time.

**List form** (resolve from coupler):
```python
sc.run(
    name="analyze",
    fn=analyze_data,
    inputs=["preprocessed"],          # Resolve from coupler
    load_inputs=True,                 # Auto-load as function parameters
    outputs=["analysis"],
)
```
Use this when the artifact is already in the coupler from a prior step. With `load_inputs=True`, each key becomes a function parameter with the loaded data. For example, `inputs=["preprocessed"]` means your function receives a `preprocessed` parameter with the loaded DataFrame.

**Note**: If you have existing code using `input_keys=`, it continues to work identically. Both `inputs=` and `input_keys=` are supported and stable long-term. The `inputs=` parameter is simply the newer, preferred name.

<details>
<summary>Alternative: inline steps with sc.trace</summary>

Use this when you want inline code blocks (they always execute, even on cache hits).

```python
with use_tracker(tracker):
    with consist.scenario("my_analysis") as sc:
        with sc.trace(name="preprocess", inputs={"raw": Path("raw.csv")}):
            df = pd.read_csv("raw.csv")
            consist.log_dataframe(df, key="preprocessed")

        with sc.trace(name="analyze", inputs=["preprocessed"]):
            df = consist.load(sc.coupler.require("preprocessed"))
            summary = df.groupby("category", as_index=False)["value"].mean()
            consist.log_dataframe(summary, key="analysis")
```
</details>

### Passing Data Between Steps with the Coupler

```python
with use_tracker(tracker):
    with consist.scenario("baseline", model="travel_demand") as sc:
        coupler = sc.coupler

        # Step 1: Load and prepare
        with sc.trace(name="initialize", run_id="baseline_init"):
            df_pop = load_population()
            consist.log_dataframe(df_pop, key="population")

        # Step 2: Simulate for each year
        for year in [2020, 2030, 2040]:
            with sc.trace(
                name="simulate",
                run_id=f"baseline_{year}",
                year=year,
                inputs=["population"],  # Declare dependency
            ):
                # Get data from coupler (with automatic cache detection)
                df_pop = consist.load(coupler.require("population"))
                df_result = run_model(year, df_pop)

                # Log output and store for downstream steps
                consist.log_dataframe(df_result, key="persons")
```

**What `inputs` (list form) does:**
- Declares that this step depends on "population" artifact
- Consist tracks this as part of the cache key
- If the population artifact hasn't changed, this step's cache is still valid

**Simpler alternative (auto-load inputs with `sc.run`)**

If your step can be expressed as a function, `sc.run` can auto-load inputs into
function parameters so you don't need to call `coupler.require(...)` manually:

```python
def simulate_year(population: pd.DataFrame, config: dict) -> pd.DataFrame:
    year = config["year"]
    return run_model(year, population)

with use_tracker(tracker):
    with consist.scenario("baseline", model="travel_demand") as sc:
        with sc.trace(name="initialize", run_id="baseline_init"):
            df_pop = load_population()
            consist.log_dataframe(df_pop, key="population")

        for year in [2020, 2030, 2040]:
            sc.run(
                name=f"simulate_{year}",
                fn=simulate_year,
                inputs=["population"],
                outputs=["persons"],
                load_inputs=True,
                config={"year": year},
            )
```

### Output Validation

Consist can validate that your workflow produces expected outputs, catching typos or missing data early. You have three patterns to choose from based on your workflow. **Most users just need Pattern A.**

---

**Pattern A: Declare required outputs (Static workflows) — START HERE**

Use this when you know all your workflow outputs upfront.

```python
with use_tracker(tracker):
    with consist.scenario("workflow") as sc:
        sc.require_outputs(
            "zarr_skims",
            "synthetic_population",
        )

        # Run steps—at scenario exit, missing outputs raise RuntimeError
        compile_result = sc.run("compile", fn=asim_compile_runner.run)
```

**When to use:** Static workflows where steps always produce the same outputs (most common).

**Benefits:** Simple, clear contract. Missing outputs are caught at scenario exit. Typos are caught immediately.

**Optional: Add guardrails for typos**
```python
sc.require_outputs(
    "zarr_skims",
    "synthetic_population",
    warn_undocumented=True,  # Warn if you set other keys by mistake
    description={
        "zarr_skims": "Zone-to-zone travel times in Zarr format",
        "synthetic_population": "Synthetic population with activity schedules",
    }
)
```

**Shortcut:** Pass required outputs directly to `scenario()`:
```python
with consist.scenario(
    "workflow",
    require_outputs=["zarr_skims", "synthetic_population"],
) as sc:
    ...
```

---

**Pattern B: Runtime-declared validation (Dynamic/optional outputs)**

Use this when outputs are dynamic or optional (e.g., optional debug outputs, or conditional branching).

```python
with use_tracker(tracker):
    with consist.scenario("workflow") as sc:
        # Declare outputs as you build them
        sc.declare_outputs(
            "zarr_skims", "synthetic_population",
            required={"zarr_skims": True, "synthetic_population": True}
        )

        # Add more outputs later if needed
        if debugging:
            sc.declare_outputs("debug_report", required={"debug_report": False})

        # Run steps—missing required outputs raise RuntimeError at exit
        compile_result = sc.run("compile", fn=asim_compile_runner.run)
```

**When to use:** Workflows with per-key control over required vs optional, or conditional outputs.

**Benefits:** Granular per-key control. Mix required and optional outputs. Add outputs dynamically.

---

### Selective Output Collection with `collect_by_keys()`

By default, when a step produces multiple outputs, all are automatically synced to the coupler. Use `collect_by_keys()` when you need to:
- Select only specific outputs from many (ignore others)
- Namespace outputs by year or scenario (prefix them)

```python
# Select specific outputs
result = sc.run("step", fn=some_func)  # Produces: persons, households, jobs
sc.collect_by_keys(result.outputs, "persons", "households")
# coupler now has: "persons", "households" (jobs ignored)

# Namespace by year (useful in multi-year simulations)
for year in [2020, 2030, 2040]:
    result = sc.run(f"forecast_{year}", fn=forecast_fn)
    sc.collect_by_keys(result.outputs, "population", "skims", prefix=f"{year}_")

# coupler now has: "2020_population", "2020_skims", "2030_population", etc.
```

---

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
            consist.log_dataframe(df, key="data")

        # Test different thresholds
        for threshold in [0.3, 0.5, 0.7]:
            with sc.trace(
                name="analyze",
                run_id=f"threshold_{threshold}",
                threshold=threshold,
                inputs=["shared_data"],
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

### Cache Hits and the Coupler

When Consist detects a cache hit (same step, same code version, same config and inputs):

1. **In `sc.run()`**: Your function is skipped entirely. Cached outputs are returned and automatically synced to the coupler.
2. **In `sc.trace()`**: Cached outputs are pre-synced to the coupler BEFORE your trace body runs, so you can access them with `coupler.require()` immediately. Your code still executes (unlike `sc.run()`).

This means **your code doesn't need to handle cache hits differently**—the coupler is populated automatically in both cases.

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
        with sc.trace(name="simulate", inputs=["population"]):
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
        with sc.trace(name="simulate", inputs=["processed"]):
            network = consist.load(coupler.require("processed"))
            # ... simulation ...
```

## When Does Code Execute? Understanding `sc.run()` vs `sc.trace()`

When building multi-step scientific workflows, a critical question arises: **Does my Python code run every time, or only when inputs change?** This section clarifies the difference between `sc.run()` and `sc.trace()`, two fundamental Consist patterns that differ in execution behavior on cache hits.

### The Core Distinction

On a **cache hit** (when Consist finds previously-cached results for this step with the same inputs):

- **`sc.trace(...)`** — Your Python block *always executes*. Consist returns cached outputs, but your code still runs. Use this for logging, diagnostics, or steps that must track intermediate state every run.

- **`sc.run(...)`** — Your Python function *only executes on cache miss*. On a cache hit, Consist skips calling your function entirely and returns the cached output. Use this for expensive operations like scientific simulations, data processing, or model fitting.

### Why This Matters: Performance & Side Effects

Consider an expensive simulation that takes 2 hours. Running it 100 times with the same inputs would normally take 200 hours of compute. With Consist:

- If you use `sc.trace()`: code runs 100 times (200 hours) — caching provides metadata only
- If you use `sc.run()`: code runs once (2 hours), then 99 cache hits retrieve results instantly

**Side effects also differ.** If your step writes temporary files, updates external systems, or has other side effects, `sc.trace()` repeats them on every run, while `sc.run()` skips them on cache hits.

### Example: ActivitySim-Style Land Use Simulation

Here's a realistic example showing the difference:

#### Using `sc.trace()` (Always Runs)

```python
with use_tracker(tracker):
    with consist.scenario("baseline") as sc:
        # This block executes every time, even on cache hits
        with sc.trace(
            name="prepare_land_use",
            inputs={"geojson": Path("land_use.geojson")},
            year=2030
        ):
            # This code ALWAYS runs—useful if you log, print status, etc.
            print(f"Processing land use for year {year}")
            zones = load_zones("land_use.geojson")

            # But Consist returns cached output if it exists
            df_zones = pd.DataFrame(zones)
            consist.log_dataframe(df_zones, key="zones")
```

#### Using `sc.run()` (Skips on Cache Hit)

```python
with use_tracker(tracker):
    with consist.scenario("baseline") as sc:
        def prepare_land_use(geojson_path: Path) -> pd.DataFrame:
            # This function ONLY runs on cache miss
            # On cache hit, Consist returns cached output without calling it
            print(f"Processing land use")  # Only prints on first run
            zones = load_zones(geojson_path)
            return pd.DataFrame(zones)

        result = sc.run(
            name="prepare_land_use",
            fn=prepare_land_use,
            inputs={"geojson_path": Path("land_use.geojson")},
            outputs=["zones"],
            load_inputs=True,  # Auto-load Path → argument
        )
        # Outputs are synced to the coupler automatically
```

### Which Should You Use?

Choose based on your workflow needs:

| Scenario | Use | Why |
|----------|-----|-----|
| Expensive simulation, model fitting, or large data transformation | `sc.run()` | Skip re-execution on cache hits; critical for 2+ hour runtimes or iterative analysis |
| Steps that log, print diagnostics, or validate state on every run | `sc.trace()` | Need to see side effects repeated; cheaper operations that re-run quickly |
| Multi-year simulation where early years are cached, new years execute | `sc.run()` | Each year has independent cache entry; skip re-running 2020 when computing 2030 |
| Mixed: some expensive, some diagnostic | Both in same scenario | Use `sc.run()` for expensive steps, `sc.trace()` for cheap validation |

### Practical Guidance

**For most scientific workflows, prefer `sc.run()`** when:
- Your function is deterministic (no randomness unless seeded in config)
- It doesn't have important side effects outside the outputs you log
- You can structure it as a pure function (inputs → outputs)

**Use `sc.trace()`** when:
- You need to run initialization or setup code that triggers external systems
- You want explicit control over what happens every run vs. only on cache miss
- Your step is fast enough that re-execution overhead doesn't matter

**On large file inputs:** If your function receives multi-GB files, set `load_inputs=False` and use `cache_hydration="inputs-missing"` to ensure input files are available on cache misses without re-loading on every run.

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

You can also opt into lightweight file schema capture when logging CSV/Parquet artifacts by passing `profile_file_schema=True` (and optionally `file_schema_sample_rows=`) to `log_artifact`. These captured schemas are stored in the provenance DB and remain available even if the original files move or are deleted.
If you already have a content hash (e.g., after copying or moving a file), pass `content_hash=` to `log_artifact` to reuse it without re-hashing the file. For safety, Consist will not overwrite an existing, different hash unless you pass `force_hash_override=True`. To verify the hash against disk, use `validate_content_hash=True`.

See `docs/schema-export.md` for the full workflow (CLI + Python) and column-name/`__tablename__` guidelines.
See [Ingestion & Hybrid Views](ingestion-and-hybrid-views.md) for ingestion tradeoffs and DB fallback behavior.
