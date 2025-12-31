# Usage Guide

Consist provides two complementary patterns for tracking provenance: `run()` for single steps and **scenarios** for multi-step workflows. You can use either independently or combine them.

---

## Common Patterns

- **Parameter sweeps**: See [Predator-Prey §3](../examples/01_parameter_sweep_monte_carlo.ipynb#3-running-the-sweep-with-provenance-tracking).
- **Iterative/feedback loops**: See [Iterative Workflows §Baseline Run](../examples/02_iterative_workflows.ipynb#baseline-run-10-iterations).
- **Schema export**: See [Predator-Prey §4](../examples/01_parameter_sweep_monte_carlo.ipynb#4-schema-export-from-data-to-typed-queries).
- **Lineage queries**: See [Iterative Workflows §Querying Provenance](../examples/02_iterative_workflows.ipynb#querying-provenance).

---

## Runs

Runs execute a callable with explicit inputs, config, and outputs, and return a `RunResult`.
See: [Predator-Prey §1](../examples/01_parameter_sweep_monte_carlo.ipynb#1-single-simulation-preview).

```python
from consist import Tracker
from pydantic import BaseModel
from pathlib import Path
import pandas as pd

class CleaningConfig(BaseModel):
    threshold: float = 0.5
    remove_outliers: bool = True

tracker = Tracker(
    run_dir="./runs",
    db_path="./provenance.duckdb",
)

@tracker.define_step(outputs=["cleaned"])
def clean_data(raw_file: Path, config: CleaningConfig):
    df = pd.read_csv(raw_file)
    df = df[df["value"] >= config.threshold]
    return {"cleaned": df}

result = tracker.run(
    fn=clean_data,
    inputs={"raw_file": Path("raw.csv")},
    config=CleaningConfig(threshold=0.5),
    load_inputs=True,
)

cleaned_artifact = result.outputs["cleaned"]
```

### Wrapping legacy code

For legacy tools that write to directories, use the injected context helper:

```python
def run_legacy_model(upstream_artifact, ctx):
    import legacy_model
    legacy_model.run()
    ctx.capture_outputs(Path("outputs"), pattern="*.csv")

tracker.run(
    fn=run_legacy_model,
    inputs={"upstream_artifact": Path("input.csv")},
    depends_on=["config.yaml", "parameters.json"],
    inject_context="ctx",
)
```

---

## Scenarios

Scenarios group related steps under a parent run, useful for multi-year simulations or variant comparisons.
See: [Iterative Workflows §Baseline Run](../examples/02_iterative_workflows.ipynb#baseline-run-10-iterations).

```python
import consist
from consist import Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

with consist.scenario("baseline", tracker=tracker, model="travel_demand") as sc:

    with sc.trace(name="initialize", run_id="baseline_init"):
        df_pop = load_population()
        consist.log_dataframe(df_pop, key="population", schema=Population)
    
    for year in [2020, 2030, 2040]:
        with sc.trace(name="simulate", run_id=f"baseline_{year}", year=year):
            df_result = run_model(year)
            consist.log_dataframe(df_result, key="persons", schema=Person)
```

All steps share the same `scenario_id`, making cross-scenario queries straightforward.

### Passing Data Between Steps

Use the coupler to track artifacts flowing between steps:
See: [Iterative Workflows §Baseline Run](../examples/02_iterative_workflows.ipynb#baseline-run-10-iterations).

```python
with consist.scenario(
    "baseline",
    tracker=tracker,
    step_cache_hydration="inputs-missing",
) as sc:
    coupler = sc.coupler
    
    with sc.trace(name="preprocess"):
        df = preprocess_data()
        art = consist.log_dataframe(df, key="processed")
        coupler.set("data", art)
    
    # Declare upstream artifacts as step inputs so caching and provenance are correct.
    # `input_keys=[...]` avoids repeating `coupler.require(...)` in `inputs=[...]`.
    # Use `optional_input_keys=[...]` to include artifacts only if they already exist.
    with sc.trace(name="simulate", input_keys=["data"]):
        df = consist.load(coupler.require("data"))
        # ... simulation logic ...

## Query facets with `pivot_facets`

If you log small, queryable config facets on runs, you can pivot them into a wide table
and join them to other run metadata for analysis.
See: [Predator-Prey §5](../examples/01_predator_prey_end_to_end.ipynb#5-querying-across-runs-with-hybrid-views).
For when to use `config` vs `facet`, see [Concepts](concepts.md#the-config-vs-facet-distinction).

```python
from sqlmodel import select
import consist

params = consist.pivot_facets(
    namespace="simulate",
    keys=["alpha", "beta", "mode"],
    value_columns={"mode": "value_str"},
)

rows = consist.run_query(
    select(params.c.run_id, params.c.alpha, params.c.beta, params.c.mode),
    tracker=tracker,
)
```
```

### Mixing Runs and Scenarios

Call `tracker.run(...)` inside a scenario when a step should be cached independently:

```python
def expensive_preprocessing(network_file: Path):
    ...
    return {"processed": processed_df}

with consist.scenario("baseline", tracker=tracker) as sc:
    preprocess = tracker.run(
        fn=expensive_preprocessing,
        inputs={"network_file": Path("network.geojson")},
        outputs=["processed"],
        load_inputs=True,
    )
    sc.coupler.update(preprocess.outputs)
```

### Function-Shaped Scenario Steps (Skip on Cache Hit)

`sc.trace(...)` is a context manager, so its Python block always executes even on cache hits.
If you want Consist to *skip* calling an expensive function/bound method on cache hits (while
still hydrating cached outputs into the Coupler), use `sc.run(...)`:

```python
with consist.scenario("baseline", tracker=tracker) as sc:
    sc.run(
        name="beam_preprocess",
        fn=beamPreprocessor.run,  # imported function or bound method
        inputs={"data": "data"},
        output_paths={"beam_inputs": "beam_inputs.parquet"},
        load_inputs=False,
    )
    beam_inputs = sc.coupler.require("beam_inputs")
```

---

## Querying Results

### Finding Runs
See: [Iterative Workflows §Querying Provenance](../examples/02_iterative_workflows.ipynb#querying-provenance).

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
See: [Iterative Workflows §Querying Provenance](../examples/02_iterative_workflows.ipynb#querying-provenance).

```python
# Get artifacts for a run
artifacts = tracker.get_artifacts_for_run(run.id)
persons_artifact = artifacts.outputs["persons"]

# Load the data
df = consist.load(persons_artifact)
```

### Cross-Run Queries with Views
See: [Predator-Prey §5](../examples/01_parameter_sweep_monte_carlo.ipynb#5-querying-across-runs-with-hybrid-views).

Register schemas to enable SQL queries across all runs:

```python
from sqlmodel import SQLModel, Field, select, func

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

### Generating Schemas from Captured Data

If you ingest tabular data into DuckDB, Consist can capture the observed schema and export an **editable SQLModel stub** so you can curate PK/FK constraints and then register the model for views.

See `docs/schema-export.md` for the full workflow (CLI + Python) and column-name/`__tablename__` guidelines.

---

## Container Integration

Track containerized models where the image digest becomes part of the cache signature:

```python
from consist.integrations.containers import run_container

result = run_container(
    tracker=tracker,
    run_id="model_2030",
    image="travel-model:v2.1",
    command=["python", "run.py", "--year", "2030"],
    volumes={"/data/inputs": "/inputs", "/data/outputs": "/outputs"},
    inputs=[input_artifact],
    outputs={"results": Path("/data/outputs/results.parquet")},
)

output_artifact = result.artifacts["results"]
```

Changing the image version invalidates the cache; identical inputs with the same image return cached results.
