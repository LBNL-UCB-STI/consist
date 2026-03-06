# First Workflow

This tutorial builds a two-step data pipeline demonstrating artifact passing, cache invalidation, and lineage tracking. Each step is a separate cached run; changing config or code in one step invalidates only that step and its dependents.

## Prerequisites

Install Consist first and ensure your environment can write Parquet files:

- Base installation instructions: [Installation](installation.md)
- Parquet engine required for this tutorial (from a local clone):
  `pip install -e ".[parquet]"`

## Create Input Data

Save this as `setup_data.py` and run it once:

``` python
from pathlib import Path
import pandas as pd
import json

Path("./data").mkdir(exist_ok=True)
df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "value": [10, 20, 30, 40, 50]})
df.to_csv("./data/raw.csv", index=False)

Path("./config.json").write_text(json.dumps({"threshold": 15}))
```

```bash
python setup_data.py
```

## Define the Pipeline

The function bodies are identical in both versions — Consist only changes how you call them.

=== "Without Consist"

    Save the following as `workflow.py`:

    ``` python
    from pathlib import Path
    import pandas as pd
    import json


    def clean_data(raw_path: Path, config_path: Path) -> dict[str, Path]:
        """Remove rows below threshold and write cleaned output."""
        threshold = json.loads(config_path.read_text())["threshold"]
        df = pd.read_csv(raw_path)
        out_path = Path("./cleaned.parquet")
        df[df["value"] >= threshold].to_parquet(out_path, index=False)
        return {"cleaned": out_path}


    def summarize(cleaned_path: Path) -> dict[str, Path]:
        """Compute summary statistics from cleaned data."""
        df = pd.read_parquet(cleaned_path)
        out_path = Path("./summary.json")
        pd.Series({"mean": df["value"].mean(), "count": len(df)}).to_json(out_path)
        return {"summary": out_path}


    # Direct calls — no tracking, no caching
    clean_outputs = clean_data(Path("./data/raw.csv"), Path("./config.json"))
    summarize(clean_outputs["cleaned"])

    final = pd.read_json(Path("./summary.json"), typ="series")
    print(f"Result: {final.to_dict()}")
    ```

    This runs correctly, but every execution re-runs both steps regardless of
    whether inputs changed. There is no record of which `config.json` produced
    which `summary.json`, no way to skip work on re-runs, and no lineage linking
    outputs back to their inputs.

=== "With Consist"

    Save the following as `workflow.py`:

    ``` python
    from pathlib import Path
    import pandas as pd
    import json
    import consist
    from consist import Tracker

    tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")


    def clean_data(raw_path: Path, config_path: Path) -> dict[str, Path]:  # (1)!
        """Remove rows below threshold and write cleaned output."""
        threshold = json.loads(config_path.read_text())["threshold"]
        df = pd.read_csv(raw_path)
        out_path = Path("./cleaned.parquet")
        df[df["value"] >= threshold].to_parquet(out_path, index=False)
        return {"cleaned": out_path}


    def summarize(cleaned_path: Path) -> dict[str, Path]:
        """Compute summary statistics from cleaned data."""
        df = pd.read_parquet(cleaned_path)
        out_path = Path("./summary.json")
        pd.Series({"mean": df["value"].mean(), "count": len(df)}).to_json(out_path)
        return {"summary": out_path}


    # Step 1: Clean
    clean_result = tracker.run(
        fn=clean_data,
        inputs={  # (2)!
            "raw_path": Path("./data/raw.csv"),
            "config_path": Path("./config.json"),
        },
    )
    print(f"Clean: {clean_result.run.status}")

    # Step 2: Summarize (consumes output from Step 1)
    summary_result = tracker.run(
        fn=summarize,
        inputs={"cleaned_path": consist.ref(clean_result, key="cleaned")},  # (3)!
    )
    print(f"Summarize: {summary_result.run.status}")

    # Load final result
    final = pd.read_json(summary_result.outputs["summary"].path, typ="series")
    print(f"Result: {final.to_dict()}")
    ```

    1. The function bodies are unchanged from the plain Python version.
    2. `inputs` is a dict mapping parameter names to paths or artifacts. Each file's content hash is included in the cache signature.
    3. `consist.ref(...)` links the upstream output artifact explicitly — the dependency is a concrete value, not an implicit name match.

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

## Invalidate with a Config Change

Edit `config.json` to change the threshold:

```json
{"threshold": 25}
```

Run again:

```bash
python workflow.py
```

Consist detects that `config.json`'s content hash changed and re-executes `clean_data`. Because its output artifact changes, `summarize` also re-executes — its input hash now differs from the cached version.

```
Clean: completed
Summarize: completed
Result: {'mean': 40.0, 'count': 3}
```

This is the Merkle DAG at work: a change to any input propagates downstream automatically. The `config.json` file is just another hashed input — no special configuration API required.

## View Lineage

Query the provenance database:

```bash
python -m consist.cli runs --db-path ./runs/provenance.duckdb --limit 10
```

Each run shows its inputs, outputs, config hash, and status. Artifacts link to their producing runs, forming a complete lineage graph.

## Summary

This workflow demonstrated:

- **Cacheable function steps** using `tracker.run(...)` with `dict[str, Path]` returns
- **Artifact passing** where one step's output becomes another's input
- **Cache invalidation** triggered by config changes
- **Lineage tracking** linking runs through their artifacts

## Next Steps

- **[Core Concepts](../concepts/overview.md)** — Mental model for runs, artifacts, and signatures
- **[Config Management](../concepts/config-management.md)** — Control which parameters affect caching
- **[Caching & Hydration](../concepts/caching-and-hydration.md)** — Advanced cache strategies and data recovery
- **[Data Materialization](../concepts/data-materialization.md)** — Ingest artifacts for SQL analysis
- **[Building a Domain Tracker](../building-domain-tracker.md)** — Wrap Consist behind domain verbs
