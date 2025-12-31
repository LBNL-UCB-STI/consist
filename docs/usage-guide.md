# Usage Guide

Consist provides two complementary patterns for tracking provenance: **tasks** for cacheable functions and **scenarios** for multi-step workflows. You can use either independently or combine them.

---

## Tasks

Tasks are decorated functions with automatic signature-based caching. Consist inspects the function's inputs to compute a cache key.

```python
from consist import Tracker
from pydantic import BaseModel
from pathlib import Path

class CleaningConfig(BaseModel):
    threshold: float = 0.5
    remove_outliers: bool = True

tracker = Tracker(
    run_dir="./runs",
    db_path="./provenance.duckdb",
)

@tracker.task()
def clean_data(raw_file: Path, config: CleaningConfig) -> Path:
    """Clean raw data according to config rules."""
    # Prefer writing outputs under `tracker.run_dir` (or a mounted outputs:// root)
    # so artifacts remain portable and easy to locate.
    output_path = tracker.run_dir / "cleaned.parquet"
    df = pd.read_csv(raw_file)
    # ... cleaning logic ...
    df.to_parquet(output_path)
    return output_path
```

### Task Return Values

Tasks return `Artifact` objects, not raw paths:

```python
result = clean_data(Path("raw.csv"), config)

# Access the file path
print(result.path)  # Path to the output file

# Load the data directly
import consist
df = consist.load(result)
```

### Chaining Tasks

Pass artifacts directly between tasks to build lineage chains:

```python
@tracker.task()
def analyze_data(cleaned_file: Path, multiplier: float) -> Path:
    output_path = tracker.run_dir / "analysis.parquet"
    df = pd.read_parquet(cleaned_file)
    df["scaled"] = df["value"] * multiplier
    df.to_parquet(output_path)
    return output_path

# Chain: clean_data -> analyze_data
cleaned = clean_data(Path("raw.csv"), config)
analyzed = analyze_data(cleaned, multiplier=2.0)  # Pass artifact directly
```

### Cache Modes

Control caching behavior per-task:

```python
# Default: reuse cached results when signature matches
@tracker.task()
def process(input_file: Path) -> Path: ...

# Always re-execute, update the cache
@tracker.task(cache_mode="overwrite")
def process_fresh(input_file: Path) -> Path: ...

# Use cache but don't persist new results (sandbox mode)
@tracker.task(cache_mode="readonly")
def process_whatif(input_file: Path) -> Path: ...
```

### Wrapping Legacy Code

For code that reads configuration files and writes to directories:

```python
@tracker.task(
    depends_on=["config.yaml", "parameters.json"],  # Include in signature
    capture_dir="./outputs",                         # Watch for new files
    capture_pattern="*.csv"
)
def run_legacy_model(upstream_artifact):
    """Wrapper around legacy simulation."""
    import legacy_model
    legacy_model.run()  # Reads config files, writes to ./outputs
    # Return None; Consist captures outputs automatically
```

The `depends_on` files are hashed into the signature. Any files matching `capture_pattern` created in `capture_dir` are registered as output artifacts.

---

## Scenarios

Scenarios group related steps under a parent run, useful for multi-year simulations or variant comparisons.

```python
import consist
from consist import Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

with consist.scenario("baseline", tracker=tracker, model="travel_demand") as sc:
    
    with sc.step(name="initialize", run_id="baseline_init"):
        df_pop = load_population()
        consist.log_dataframe(df_pop, key="population", schema=Population)
    
    for year in [2020, 2030, 2040]:
        with sc.step(name="simulate", run_id=f"baseline_{year}", year=year):
            df_result = run_model(year)
            consist.log_dataframe(df_result, key="persons", schema=Person)
```

All steps share the same `scenario_id`, making cross-scenario queries straightforward.

### Passing Data Between Steps

Use the coupler to track artifacts flowing between steps:

```python
with consist.scenario("baseline", tracker=tracker) as sc:
    coupler = sc.coupler
    
    with sc.step(name="preprocess"):
        df = preprocess_data()
        art = consist.log_dataframe(df, key="processed")
        coupler.set("data", art)
    
    # Declare upstream artifacts as step inputs so caching and provenance are correct.
    # `input_keys=[...]` avoids repeating `coupler.require(...)` in `inputs=[...]`.
    # Use `optional_input_keys=[...]` to include artifacts only if they already exist.
    with sc.step(name="simulate", input_keys=["data"]):
        df = consist.load(coupler.require("data"))
        # ... simulation logic ...
```

### Mixing Tasks and Scenarios

Call cached tasks inside scenario steps:

```python
@tracker.task()
def expensive_preprocessing(network_file: Path) -> Path:
    # Cached independently of scenarios
    ...

with consist.scenario("baseline", tracker=tracker) as sc:
    with sc.step(name="preprocess"):
        # Task cache is checked; won't re-run if inputs unchanged
        processed = expensive_preprocessing(Path("network.geojson"))
    
    # If you want this step itself to be cacheable, declare the task output as an input.
    with sc.step(name="simulate", inputs=[processed]):
        run_simulation(processed)
```

### Function-Shaped Scenario Steps (Skip on Cache Hit)

`sc.step(...)` is a context manager, so its Python block always executes even on cache hits.
If you want Consist to *skip* calling an expensive function/bound method on cache hits (while
still hydrating cached outputs into the Coupler), use `sc.run_step(...)`:

```python
with consist.scenario("baseline", tracker=tracker) as sc:
    sc.run_step(
        name="beam_preprocess",
        fn=beamPreprocessor.run,  # imported function or bound method
        input_keys=["data"],
        output_paths={"beam_inputs": "beam_inputs.parquet"},
    )
    beam_inputs = sc.coupler.require("beam_inputs")
```

---

## Querying Results

### Finding Runs

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

```python
# Get artifacts for a run
artifacts = tracker.get_artifacts_for_run(run.id)
persons_artifact = artifacts.outputs["persons"]

# Load the data
df = consist.load(persons_artifact)
```

### Cross-Run Queries with Views

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
