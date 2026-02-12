# Usage Guide

Consist provides flexible patterns for tracking provenance in scientific workflows. This guide walks you through the main usage patterns, from simple single-step runs to complex multi-year simulations. Each section is written to help:

- **Developers** integrating Consist into a simulation tool
- **Practitioners** running tools and wanting clearer inputs/outputs
- **Researchers** managing multi-stage pipelines and reproducibility

**New to Consist?** Start with the [quickstart notebook](examples.md#quickstart), then work through the examples below in order.

---

## Choosing Your Pattern

Choose based on what you're building and how much structure you need:

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

``` python
import consist
from consist import Tracker, use_tracker
from pathlib import Path
import pandas as pd

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")  # (1)!

def clean_data(raw: pd.DataFrame, threshold: float = 0.5) -> pd.DataFrame:  # (2)!
    df = raw[raw["value"] > threshold]  # (3)!
    return df

with use_tracker(tracker):  # (4)!
    result = consist.run(
        fn=clean_data,
        inputs={"raw": Path("raw.csv")},
        config={"threshold": 0.5},  # (5)!
        outputs=["cleaned"],
    )

cleaned_artifact = result.outputs["cleaned"]  # (6)!
cleaned_df = consist.load_df(cleaned_artifact)
print(f"Output: {cleaned_artifact.path}")
```

1. Create the tracker for run metadata and caching.
2. Define the function Consist will cache.
3. Filter rows above the threshold.
4. Run inside the tracker context so Consist can log provenance.
5. `config` participates in the cache key.
6. Access the cached output artifact and load data.

If you run it again with the same inputs and config, you should get a cache hit and no re-execution.

Preferred run configuration style (options objects):

``` python
from consist import CacheOptions, ExecutionOptions, OutputPolicyOptions

result = consist.run(
    fn=clean_data,
    inputs={"raw": Path("raw.csv")},
    config={"threshold": 0.5},
    outputs=["cleaned"],
    cache_options=CacheOptions(cache_mode="reuse", cache_hydration="inputs-missing"),
    output_policy=OutputPolicyOptions(output_missing="error"),
    execution_options=ExecutionOptions(load_inputs=True),
)
```

Legacy run-policy kwargs are no longer supported on `run(...)` APIs. Use
`cache_options=...`, `output_policy=...`, and `execution_options=...`.

<details>
<summary>Alternative: keep raw file paths (no auto-load)</summary>

Use this when your function needs a `Path` and manages I/O directly (common for legacy tools or file-based APIs).

``` python
from consist import ExecutionOptions

def clean_data(raw_file: Path, _consist_ctx) -> None:
    df = pd.read_csv(raw_file)
    out_path = _consist_ctx.run_dir / "cleaned.parquet"
    df.to_parquet(out_path)
    _consist_ctx.log_output(out_path, key="cleaned")

with use_tracker(tracker):
    result = consist.run(
        fn=clean_data,
        inputs={"raw_file": Path("raw.csv")},  # (1)!
        execution_options=ExecutionOptions(
            load_inputs=False,
            runtime_kwargs={"raw_file": Path("raw.csv")},
            inject_context=True,
        ),
    )
```

1. Hash the input file as part of the cache key.
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

``` python
from pathlib import Path
from consist import ExecutionOptions

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
        depends_on=[Path("config.yaml"), Path("parameters.json")],  # (1)!
        execution_options=ExecutionOptions(
            load_inputs=False,
            runtime_kwargs={"upstream": Path("input.csv")},  # (2)!
            inject_context=True,
        ),
    )
```

1. Hash these files too so config changes invalidate the cache.
2. Pass raw paths at runtime with
   `execution_options=ExecutionOptions(load_inputs=False, runtime_kwargs={...})`.

Captured outputs are keyed by filename stem (for example, `results.csv` -> `results`).

<details>
<summary>Alternative: capture a fixed output directory</summary>

If the tool always writes to a known folder and returns `None`, you can capture it directly:

``` python
from consist import ExecutionOptions

with use_tracker(tracker):
    def run_legacy_model(upstream) -> None:
        import legacy_model

        legacy_model.run(upstream, output_dir="outputs")

    result = consist.run(
        fn=run_legacy_model,
        inputs={"upstream": Path("input.csv")},  # (1)!
        depends_on=[Path("config.yaml")],
        execution_options=ExecutionOptions(
            load_inputs=False,
            runtime_kwargs={"upstream": Path("input.csv")},
        ),
        capture_dir=Path("outputs"),
        capture_pattern="*.csv",
    )
```

1. Hash the input file as part of the cache key.
</details>

<details>
<summary>How input mappings become function arguments</summary>

When `inputs` is a mapping, Consist matches keys to function parameters and auto-loads
those artifacts (for example, a CSV becomes a DataFrame) into the call by default.
If you keep auto-loading enabled (`execution_options=ExecutionOptions(load_inputs=True)`),
`inputs={"upstream": ...}` becomes the `upstream`
argument automatically.

If you want raw paths instead of auto-loaded data, use
`execution_options=ExecutionOptions(load_inputs=False, runtime_kwargs={...})`.
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

The **coupler** is your scenario-scoped artifact registry. When you log an artifact with a key, it's automatically stored in the coupler, making data flow between steps explicit and traceable. This is especially useful when:
- You need clean handoffs between tools (developer workflows)
- You want a clear list of outputs by name (practitioner workflows)
- You want auditable step-to-step lineage (research workflows)

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

**Optional: artifact key registries** help keep keys consistent across large workflows.

``` python
from consist import ExecutionOptions
from consist.utils import ArtifactKeyRegistry

class Keys(ArtifactKeyRegistry):
    RAW = "raw"
    PREPROCESSED = "preprocessed"
    ANALYSIS = "analysis"

# Use keys in calls
sc.run(fn=preprocess, inputs={Keys.RAW: "raw.csv"}, outputs=[Keys.PREPROCESSED])
sc.run(
    fn=analyze,
    inputs=[Keys.PREPROCESSED],
    execution_options=ExecutionOptions(load_inputs=True),
    outputs=[Keys.ANALYSIS],
)

# Validate ad-hoc key lists when needed
Keys.validate([Keys.RAW, Keys.PREPROCESSED])
```

**Live-sync (automatic):** When you log an artifact, it's immediately available in the coupler—you don't need to manually call `coupler.set()`.

**Optional: Namespace keys with a scoped view:** For larger workflows, you can
scope reads/writes while keeping fully-qualified keys globally accessible.

```python
beam = sc.coupler.view("beam")
beam.set("plans_in", artifact)            # writes key "beam/plans_in"
beam.require("plans_in")                  # namespace-local access
sc.coupler.require("beam/plans_in")       # global access still works
```

**For optional-Consist workflows:** If you're using Consist in optional mode (with fallback to Path objects or artifact-like objects), use `coupler.set_from_artifact(key, value)` instead of `coupler.set()`. It handles both real Artifacts and artifact-like objects (Paths, strings, noop artifacts) transparently.

### Simple Example: Two-Step Workflow

``` python
import consist
from consist import ExecutionOptions, Tracker, use_tracker
from pathlib import Path
import pandas as pd

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

def preprocess_data(raw: pd.DataFrame) -> pd.DataFrame:  # (1)!
    df = raw[raw["value"] > 0.5]
    return df

def analyze_data(preprocessed: pd.DataFrame) -> pd.DataFrame:
    summary = preprocessed.groupby("category", as_index=False)["value"].mean()
    return summary

with use_tracker(tracker):  # (2)!
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
            execution_options=ExecutionOptions(load_inputs=True),
            outputs=["analysis"],
        )
```

1. Define steps as regular functions for `sc.run`.
2. Execute the steps inside a scenario context.

All steps have `scenario_id="my_analysis"`, making it easy to query together. Change preprocess logic? The preprocess step re-runs; the analyze step re-runs only if the preprocessed artifact changes. Change raw.csv? Both re-execute.

### Declaring Inputs: Mapping Form vs List Form

The `inputs=` parameter supports two different forms for different situations:

**Mapping form** (explicit paths from disk):
``` python
sc.run(
    name="preprocess",
    fn=preprocess_data,
    inputs={"raw": Path("raw.csv")},  # (1)!
    outputs=["preprocessed"],
)
```

1. Load from disk to create the initial artifact.
Use this when you're loading data from disk for the first time.

**List form** (resolve from coupler):
``` python
from consist import ExecutionOptions

sc.run(
    name="analyze",
    fn=analyze_data,
    inputs=["preprocessed"],          # (1)!
    execution_options=ExecutionOptions(load_inputs=True),  # (2)!
    outputs=["analysis"],
)
```

1. Resolve inputs from the coupler.
2. Auto-load inputs as function parameters.
Use this when the artifact is already in the coupler from a prior step. With
`execution_options=ExecutionOptions(load_inputs=True)`, each key becomes a
function parameter with the loaded data. For example, `inputs=["preprocessed"]`
means your function receives a `preprocessed` parameter with the loaded
DataFrame.

**Note**: If you have existing code using `input_keys=`, it continues to work for backward compatibility. `inputs=` is the preferred name for new code.

<details>
<summary>Alternative: inline steps with sc.trace</summary>

Use this when you want inline code blocks (they always execute, even on cache hits). This can be useful for lightweight validation or logging.

```python
with use_tracker(tracker):
    with consist.scenario("my_analysis") as sc:
        with sc.trace(name="preprocess", inputs={"raw": Path("raw.csv")}):
            df = pd.read_csv("raw.csv")
            consist.log_dataframe(df, key="preprocessed")

        with sc.trace(name="analyze", inputs=["preprocessed"]):
            df = consist.load_df(sc.coupler.require("preprocessed"))
            summary = df.groupby("category", as_index=False)["value"].mean()
            consist.log_dataframe(summary, key="analysis")
```
</details>

### Decorator Defaults, Templates, and Schema Introspection

For the full guide to `@define_step` defaults, callable metadata, name templates,
schema introspection, and cache invalidation helpers, see
**[Decorators & Metadata](concepts/decorators-and-metadata.md)**.

### Passing Data Between Steps with the Coupler

``` python
with use_tracker(tracker):
    with consist.scenario("baseline", model="travel_demand") as sc:
        coupler = sc.coupler

        with sc.trace(name="initialize", run_id="baseline_init"):  # (1)!
            df_pop = load_population()
            consist.log_dataframe(df_pop, key="population")

        for year in [2020, 2030, 2040]:  # (2)!
            with sc.trace(
                name="simulate",
                run_id=f"baseline_{year}",
                year=year,
                inputs=["population"],  # (3)!
            ):
                df_pop = consist.load_df(coupler.require("population"))  # (4)!
                df_result = run_model(year, df_pop)

                consist.log_dataframe(df_result, key="persons")  # (5)!
```

1. Step 1: load and prepare the population artifact.
2. Step 2: simulate for each year.
3. Declare the dependency on the population artifact.
4. Get data from the coupler with automatic cache detection.
5. Log output and store it for downstream steps.

**What `inputs` (list form) does:**

- Declares that this step depends on "population" artifact
- Consist tracks this as part of the cache key
- If the population artifact hasn't changed, this step's cache is still valid

**Simpler alternative (auto-load inputs with `sc.run`)**

If your step can be expressed as a function, `sc.run` can auto-load inputs into
function parameters so you don't need to call `coupler.require(...)` manually:

```python
from consist import ExecutionOptions

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
                execution_options=ExecutionOptions(load_inputs=True),
                config={"year": year},
            )
```

### Output Validation

Consist can validate that your workflow produces expected outputs, catching typos or missing data early. You have three patterns to choose from based on your workflow. **Most users just need Pattern A.**

---

**Pattern A: Declare required outputs (Static workflows) — START HERE**

Use this when you know all your workflow outputs upfront.

``` python
with use_tracker(tracker):
    with consist.scenario("workflow") as sc:
        sc.require_outputs(
            "zarr_skims",
            "synthetic_population",
        )

        compile_result = sc.run("compile", fn=asim_compile_runner.run)  # (1)!
```

1. Run steps; at scenario exit, missing outputs raise `RuntimeError`.

**When to use:** Static workflows where steps always produce the same outputs (most common).

**Benefits:** Simple, clear contract. Missing outputs are caught at scenario exit. Typos are caught immediately.

**Optional: Add guardrails for typos**
``` python
sc.require_outputs(
    "zarr_skims",
    "synthetic_population",
    warn_undefined=True,  # (1)!
    description={
        "zarr_skims": "Zone-to-zone travel times in Zarr format",
        "synthetic_population": "Synthetic population with activity schedules",
    }
)
```

1. Warn if you set other keys by mistake.

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

``` python
with use_tracker(tracker):
    with consist.scenario("workflow") as sc:
        sc.declare_outputs(
            "zarr_skims", "synthetic_population",
            required={"zarr_skims": True, "synthetic_population": True}
        )

        if debugging:  # (1)!
            sc.declare_outputs("debug_report", required={"debug_report": False})  # (2)!

        compile_result = sc.run("compile", fn=asim_compile_runner.run)  # (3)!
```

1. Add optional outputs later if needed.
2. Declare an optional debug output.
3. Missing required outputs raise `RuntimeError` at exit.

**When to use:** Workflows with per-key control over required vs optional, or conditional outputs.

**Benefits:** Granular per-key control. Mix required and optional outputs. Add outputs dynamically.

---

### Selective Output Collection with `collect_by_keys()`

By default, when a step produces multiple outputs, all are automatically synced to the coupler. Use `collect_by_keys()` when you need to:

- Select only specific outputs from many (ignore others)
- Namespace outputs by year or scenario (prefix them)

``` python
result = sc.run("step", fn=some_func)  # (1)!
sc.collect_by_keys(result.outputs, "persons", "households")  # (2)!

for year in [2020, 2030, 2040]:
    result = sc.run(f"forecast_{year}", fn=forecast_fn)
    sc.collect_by_keys(result.outputs, "population", "skims", prefix=f"{year}_")  # (3)!
```

1. Produces `persons`, `households`, `jobs`.
2. Keep only selected outputs; others are ignored.
3. Namespace outputs by year (e.g., `2020_population`).

---

**Bulk logging with metadata:**
``` python
with consist.scenario("outputs") as sc:
    with sc.trace(name="export_outputs"):
        outputs = consist.log_artifacts(  # (1)!
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
            scenario_name="base"  # (2)!
        )
```

1. Log multiple files at once with explicit keys.
2. All outputs get `year=2030`, `scenario_name="base"`; `households` also gets `role="primary_unit"`.

### Example: Parameter Sweep in a Scenario

``` python
with use_tracker(tracker):
    with consist.scenario("sensitivity_analysis") as sc:
        coupler = sc.coupler

        with sc.trace(name="setup"):  # (1)!
            df = pd.read_csv("data.csv")
            consist.log_dataframe(df, key="data")

        for threshold in [0.3, 0.5, 0.7]:  # (2)!
            with sc.trace(
                name="analyze",
                run_id=f"threshold_{threshold}",
                threshold=threshold,
                inputs=["data"],
            ):
                df = consist.load_df(coupler.require("data"))
                filtered = df[df["value"] > threshold]
                consist.log_dataframe(filtered, key="filtered")
```

1. Load once, reuse for all variants.
2. Test different thresholds as separate runs.

Each threshold creates a separate run with its own cache entry. Re-run later? Consist returns cached results for matching thresholds, skips setup (since input unchanged).

---

## Pattern 3: Container Integration

Use containers when you have existing tools, models, or legacy code that runs as a subprocess or Docker container. The image digest becomes part of the cache key.

**When to use:**

- Running ActivitySim, SUMO, BEAM, or other external models
- Legacy code you don't want to refactor
- Python/R/Java executables that you invoke as subprocesses
- Tools that expect specific file paths or output directories

``` python
from consist.integrations.containers import run_container
from pathlib import Path

host_inputs = Path("data/inputs").resolve()
host_outputs = Path("data/outputs").resolve()

result = run_container(
    tracker=tracker,
    run_id="model_2030",
    image="travel-model:v2.1",  # (1)!
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

1. The image digest becomes part of the cache key.

Change the image version? Cache invalidates. Same image + inputs? Returns cached results.

<details>
<summary>Alternative: use consist.run with ExecutionOptions(executor="container")</summary>

If you prefer the same API as `consist.run`, you can use the container executor:

```python
import consist
from consist import ExecutionOptions

with consist.use_tracker(tracker):
    result = consist.run(
        name="model_2030",
        inputs=[host_inputs / "input.csv"],
        output_paths={"results": host_outputs / "results.parquet"},
        execution_options=ExecutionOptions(
            executor="container",
            container={
                "image": "travel-model:v2.1",
                "command": ["python", "run.py", "--year", "2030"],
                "volumes": {str(host_inputs): "/inputs", str(host_outputs): "/outputs"},
            },
        ),
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

**Note:** `outputs-requested` requires `output_paths=...` so Consist knows where to write the files.
`inputs-missing` only works for inputs that are tracked artifacts (not raw paths), so Consist can find
the prior run's files or reconstruct ingested tables.

Set per-run via `cache_options=CacheOptions(cache_hydration=...)` (for `run(...)`)
or for scenario defaults via `step_cache_hydration=...`:

``` python
with use_tracker(tracker):
    with consist.scenario("baseline", step_cache_hydration="inputs-missing") as sc:
        coupler = sc.coupler
        with sc.trace(name="simulate", inputs=["population"]):  # (1)!
            df_pop = consist.load_df(coupler.require("population"))
            ...
```

1. This step backfills missing inputs from prior runs before executing.

### Mixing Runs and Scenarios

Call `consist.run(...)` inside a scenario when a step should cache independently:

``` python
from consist import ExecutionOptions

with use_tracker(tracker):
    with consist.scenario("baseline") as sc:
        coupler = sc.coupler
        preprocess = consist.run(
            fn=expensive_preprocessing,
            inputs={"network_file": Path("network.geojson")},
            outputs=["processed"],
        )  # (1)!
        coupler.update(preprocess.outputs)  # (2)!

        with sc.trace(name="simulate", inputs=["processed"]):  # (3)!
            network = consist.load_df(coupler.require("processed"))
            ...
```

1. Run expensive preprocessing independently with its own cache key.
2. Add outputs to the coupler for downstream steps.
3. Later steps consume the preprocessed output.

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

``` python
with use_tracker(tracker):
    with consist.scenario("baseline") as sc:
        year = 2030
        with sc.trace(  # (1)!
            name="prepare_land_use",
            inputs={"geojson": Path("land_use.geojson")},
            year=year
        ):
            print(f"Processing land use for year {year}")  # (2)!
            zones = load_zones("land_use.geojson")

            df_zones = pd.DataFrame(zones)  # (3)!
            consist.log_dataframe(df_zones, key="zones")
```

1. This block executes every time, even on cache hits.
2. Code always runs—useful for logging or status.
3. Consist still returns cached outputs if they exist.

#### Using `sc.run()` (Skips on Cache Hit)

``` python
with use_tracker(tracker):
    with consist.scenario("baseline") as sc:
        def prepare_land_use(geojson_path: Path) -> pd.DataFrame:
            print(f"Processing land use")  # (1)!
            zones = load_zones(geojson_path)
            return pd.DataFrame(zones)

        result = sc.run(  # (3)!
            name="prepare_land_use",
            fn=prepare_land_use,
            inputs={"geojson_path": Path("land_use.geojson")},
            outputs=["zones"],
            execution_options=ExecutionOptions(load_inputs=True),  # (2)!
        )
```

1. This function only runs on cache miss; it prints on the first run.
2. Auto-load `Path` into the function argument.
3. Outputs are synced to the coupler automatically.

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

**On large file inputs:** If your function receives multi-GB files, use
`execution_options=ExecutionOptions(load_inputs=False, runtime_kwargs={...})`
and `cache_options=CacheOptions(cache_hydration="inputs-missing")` to ensure
input files are available on cache misses without re-loading on every run.

<details>
<summary>Alternative: log file outputs inside the step</summary>

If your function writes files, log them with the injected context (and read inputs yourself if needed):

```python
from consist import ExecutionOptions

def beam_preprocess(data_file, _consist_ctx) -> None:
    out_path = _consist_ctx.run_dir / "beam_inputs.parquet"
    ...
    _consist_ctx.log_output(out_path, key="beam_inputs")

with use_tracker(tracker):
    with consist.scenario("baseline") as sc:
        sc.run(
            name="beam_preprocess",
            fn=beam_preprocess,
            inputs={"data_file": Path("data.parquet")},
            execution_options=ExecutionOptions(
                load_inputs=False,
                runtime_kwargs={"data_file": Path("data.parquet")},
                inject_context=True,
            ),
        )
```
</details>

### Query Facets with `pivot_facets`

Log small, queryable config values (facets) and pivot them into a wide table for analysis. This is a simple way to compare many runs side‑by‑side (for example, a sensitivity analysis across parameters):

``` python
from sqlmodel import select
import consist

params = consist.pivot_facets(
    namespace="simulate",
    keys=["alpha", "beta", "mode"],
    value_columns={"mode": "value_str"},
)  # (1)!

rows = consist.run_query(
    select(params.c.run_id, params.c.alpha, params.c.beta, params.c.mode),
    tracker=tracker,
)
```

1. Pivot config facets into columns for analysis.

See [Concepts](concepts.md#the-config-vs-facet-distinction) for when to use `config` vs `facet`.

---

## Motivation: When Caching Saves Time

Caching is most valuable in workflows with many runs and expensive computation. Here are realistic scenarios from scientific domains:

**Example 1: Land-Use Model Sensitivity Analysis**

A sensitivity sweep tests 40 parameter combinations (toll levels, parking costs, transit subsidies).
- Each ActivitySim run: 30 minutes
- Without caching: 40 runs × 30 min = 20 hours
- With caching: Base population synthesis (30 min, once) + 39 parameter tweaks with cache hits (5 min each) = 3.75 hours
- **Time saved: 81% reduction in modeling time**

**Example 2: ActivitySim Calibration Iteration**

Mode choice coefficients need iterative calibration against observed transit ridership.
- Without caching: Repeat all 3 steps = 75 minutes per iteration × 5 iterations = 375 minutes total
- With caching: Step 1–2 are cache hits, only step 3 re-executes = 115 minutes
- **Time saved: 69% reduction; frees analyst time for interpretation**

**Example 3: Grid Dispatch Multi-Scenario Ensemble**

A baseline scenario and 8 future scenarios all share the same network preprocessing pipeline.
- Preprocessing: 3 hours; Each scenario dispatch: 20 minutes
- Without caching: 9 × (3 hours + 20 min) = 29.85 hours
- With caching: Preprocessing once (3 hours), then 8 scenario runs hit cache on preprocessing = 5.67 hours
- **Time saved: 81% reduction; enables rapid scenario exploration**

---

## Querying Results

### Finding Runs
See: [Example notebooks](examples.md#tutorial-series).

``` python
import consist

run = consist.find_run(
    tracker=tracker,
    parent_id="baseline",  # (1)!
    year=2030,
    model="simulate"
)

runs_by_year = consist.find_runs(
    tracker=tracker,
    parent_id="baseline",
    model="simulate",
    index_by="year"
)
result_2030 = runs_by_year[2030]
```

1. Filter by scenario ID.

### Loading Artifacts
See: [Example notebooks](examples.md#tutorial-series).

``` python
artifacts = tracker.get_artifacts_for_run(run.id)
persons_artifact = artifacts.outputs["persons"]

df = consist.load_df(persons_artifact)  # (1)!
```

1. Load the artifact data into a DataFrame.

### Cross-Run Queries with Views
See: [Example notebooks](examples.md#tutorial-series).

Register schemas to enable SQL queries across all runs:

``` python
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

VPerson = tracker.views.Person  # (1)!

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

1. Views are available after running scenarios and registering schemas.

Views automatically include `consist_scenario_id`, `consist_year`, and other metadata columns for filtering and grouping.
For more on ingestion and hybrid views, see [Data Materialization Strategy](concepts/data-materialization.md).

### Generating Schemas from Captured Data

If you ingest tabular data into DuckDB, Consist can capture the observed schema and export an **editable SQLModel stub** so you can curate PK/FK constraints and then register the model for views. This is useful when you want a stable, documented schema for downstream analysis or audits.

You can also opt into lightweight file schema capture when logging CSV/Parquet artifacts by passing `profile_file_schema=True` (and optionally `file_schema_sample_rows=`) to `log_artifact`. These captured schemas are stored in the provenance DB and remain available even if the original files move or are deleted.
If you already have a content hash (e.g., after copying or moving a file), pass `content_hash=` to `log_artifact` to reuse it without re-hashing the file. For safety, Consist will not overwrite an existing, different hash unless you pass `force_hash_override=True`. To verify the hash against disk, use `validate_content_hash=True`.

See `docs/schema-export.md` for the full workflow (CLI + Python) and column-name/`__tablename__` guidelines.
See [Data Materialization Strategy](concepts/data-materialization.md) for ingestion tradeoffs and DB fallback behavior.
