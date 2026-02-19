# First Workflow

This tutorial builds a two-step data pipeline demonstrating artifact passing, cache invalidation, and lineage tracking. Each step is a separate cached run; changing config or code in one step invalidates only that step and its dependents.

## Prerequisites

Install Consist first and ensure your environment can write Parquet files:

- Base installation instructions: [Installation](installation.md)
- Parquet engine required for this tutorial: `pip install "consist[parquet]"`

## Create Input Data

Save this as `setup_data.py` and run it once:

``` python
from pathlib import Path
import pandas as pd

Path("./data").mkdir(exist_ok=True)
df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "value": [10, 20, 30, 40, 50]})
df.to_csv("./data/raw.csv", index=False)
```

```bash
python setup_data.py
```

## Define the Pipeline

Save the following as `workflow.py`:

``` python
from pathlib import Path
import pandas as pd
import consist
from consist import ExecutionOptions, Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

def clean_data(raw_path: Path, threshold: float) -> Path:
    """Remove rows below threshold and write cleaned output."""
    df = pd.read_csv(raw_path)
    df_clean = df[df["value"] >= threshold]

    out_path = consist.output_path("cleaned")  # (1)!
    df_clean.to_parquet(out_path, index=False)
    return out_path  # (2)!


def summarize(cleaned) -> Path:  # (3)!
    """Compute summary statistics from cleaned data."""
    summary = {"mean": cleaned["value"].mean(), "count": len(cleaned)}

    out_path = consist.output_path("summary", ext="json")
    pd.Series(summary).to_json(out_path)
    return out_path


# --- Execute the pipeline ---

with consist.use_tracker(tracker):
    # Step 1: Clean
    clean_result = tracker.run(
        fn=clean_data,
        name="clean",
        config={"threshold": 15},  # (4)!
        inputs=[Path("./data/raw.csv")],  # (5)!
        outputs=["cleaned"],
        execution_options=ExecutionOptions(
            runtime_kwargs={"raw_path": Path("./data/raw.csv"), "threshold": 15}
        ),
    )
    print(f"Clean: {clean_result.run.status}")

    # Step 2: Summarize (consumes output from Step 1)
    summary_result = tracker.run(
        fn=summarize,
        name="summarize",
        inputs={"cleaned": consist.ref(clean_result, key="cleaned")},  # (6)!
        outputs=["summary"],
        execution_options=ExecutionOptions(load_inputs=True),
    )
    print(f"Summarize: {summary_result.run.status}")

    # Load final result
    final = pd.read_json(summary_result.outputs["summary"].path, typ="series")
    print(f"Result: {final.to_dict()}")
```

1. Use `consist.output_path(...)` to resolve a managed output path. This honors output policy and `artifact_dir` overrides while avoiding manual path bugs.
2. Return the output path. Consist registers it as the `cleaned` artifact.
3. Artifact inputs are auto-loaded when input keys match parameter names and `load_inputs=True`.
4. `config` is hashed into the run's signature. Changing `threshold` causes a cache miss.
5. `inputs` lists files or artifacts whose content hashes are included in the signature.
6. Use `consist.ref(...)` to select and link the upstream output artifact explicitly.

**Best practice:** Keep step-to-step links explicit with
`consist.ref(run_result, key="...")`. Passing a raw `RunResult` is only clear
when there is exactly one output.

## Run and Observe Caching

Execute the pipeline:

```bash
python workflow.py
```

Output:

```
Clean: completed
Summarize: completed
Result: {'mean': 35.0, 'count': 4}
```

Run again without changes:

```bash
python workflow.py
```

Both steps return cached results instantly. The function bodies do not execute.

## Invalidate with Config Change

Edit `workflow.py` to change the threshold:

``` python
config={"threshold": 25},  # Changed from 15
execution_options=ExecutionOptions(
    runtime_kwargs={"raw_path": Path("./data/raw.csv"), "threshold": 25},
),
```

Run again:

```bash
python workflow.py
```

Consist detects the config change and re-executes `clean_data`. Because its output artifact changes, `summarize` also re-executes—its input hash differs from the cached version.

```
Clean: completed
Summarize: completed
Result: {'mean': 40.0, 'count': 3}
```

This is the Merkle DAG at work: changes propagate downstream automatically.

## View Lineage

Query the provenance database:

```bash
python -m consist.cli runs --db-path ./runs/provenance.duckdb --limit 10
```

Each run shows its inputs, outputs, config hash, and status. Artifacts link to their producing runs, forming a complete lineage graph.

## Summary

This workflow demonstrated:

- **Cacheable function steps** with explicit `run(..., outputs=[...])`
- **Artifact passing** where one step's output becomes another's input
- **Cache invalidation** triggered by config changes
- **Lineage tracking** linking runs through their artifacts

## Next Steps

- **[Core Concepts](../concepts/overview.md)** — Mental model for runs, artifacts, and signatures
- **[Config Management](../concepts/config-management.md)** — Control which parameters affect caching
- **[Caching & Hydration](../concepts/caching-and-hydration.md)** — Advanced cache strategies and data recovery
- **[Data Materialization](../concepts/data-materialization.md)** — Ingest artifacts for SQL analysis
