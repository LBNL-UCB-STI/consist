# First Workflow

This tutorial builds a two-step data pipeline demonstrating artifact passing, cache invalidation, and lineage tracking. Each step is a separate cached run; changing config or code in one step invalidates only that step and its dependents.

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


@tracker.define_step(outputs=["cleaned"])  # (1)!
def clean_data(raw_path: Path, threshold: float) -> Path:
    """Remove rows below threshold and write cleaned output."""
    df = pd.read_csv(raw_path)
    df_clean = df[df["value"] >= threshold]

    out_path = tracker.run_dir / "cleaned.parquet"  # (2)!
    df_clean.to_parquet(out_path, index=False)
    return out_path  # (3)!


@tracker.define_step(outputs=["summary"])
def summarize(cleaned_artifact) -> Path:  # (4)!
    """Compute summary statistics from cleaned data."""
    df = pd.read_parquet(cleaned_artifact.path)
    summary = {"mean": df["value"].mean(), "count": len(df)}

    out_path = tracker.run_dir / "summary.json"
    pd.Series(summary).to_json(out_path)
    return out_path


# --- Execute the pipeline ---

with consist.use_tracker(tracker):
    # Step 1: Clean
    clean_result = tracker.run(
        fn=clean_data,
        name="clean",
        config={"threshold": 15},  # (5)!
        inputs=[Path("./data/raw.csv")],  # (6)!
        execution_options=ExecutionOptions(
            runtime_kwargs={"raw_path": Path("./data/raw.csv"), "threshold": 15}
        ),
    )
    print(f"Clean: {clean_result.run.status}")

    # Step 2: Summarize (consumes output from Step 1)
    cleaned_artifact = clean_result.outputs["cleaned"]  # (7)!
    summary_result = tracker.run(
        fn=summarize,
        name="summarize",
        inputs={"cleaned_artifact": cleaned_artifact},  # (8)!
    )
    print(f"Summarize: {summary_result.run.status}")

    # Load final result
    final = pd.read_json(summary_result.outputs["summary"].path, typ="series")
    print(f"Result: {final.to_dict()}")
```

1. `@tracker.define_step` declares which output keys this function produces. Consist creates artifacts for each key.
2. Write outputs under `tracker.run_dir`—Consist manages this directory per run.
3. Return the output path. Consist registers it as the `cleaned` artifact.
4. Artifact parameters are auto-loaded when input keys match parameter names.
5. `config` is hashed into the run's signature. Changing `threshold` causes a cache miss.
6. `inputs` lists files or artifacts whose content hashes are included in the signature.
7. Access outputs by key from the result object. Each output is a full `Artifact` with path, hash, and metadata.
8. Passing an artifact as input creates lineage: the summarize run depends on the clean run.

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

- **Step definition** with `@tracker.define_step` declaring outputs
- **Artifact passing** where one step's output becomes another's input
- **Cache invalidation** triggered by config changes
- **Lineage tracking** linking runs through their artifacts

## Next Steps

- **[Core Concepts](../concepts/overview.md)** — Mental model for runs, artifacts, and signatures
- **[Config Management](../concepts/config-management.md)** — Control which parameters affect caching
- **[Caching & Hydration](../concepts/caching-and-hydration.md)** — Advanced cache strategies and data recovery
- **[Data Materialization](../concepts/data-materialization.md)** — Ingest artifacts for SQL analysis
