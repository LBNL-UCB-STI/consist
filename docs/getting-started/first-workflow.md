# First Workflow

This tutorial builds a two-step data pipeline demonstrating artifact passing, cache invalidation, and lineage tracking. Each step is a separate cached run; changing config or code in one step invalidates only that step and its dependents.

## Prerequisites

Install Consist first and ensure your environment can write Parquet files:

- Base installation instructions: [Installation](installation.md)
- Parquet engine required for this tutorial:
  `pip install "consist[parquet]"`

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

This tutorial uses `input_binding="paths"` throughout. That keeps the callable
boundary explicit and makes it obvious which files each step reads and writes.
If you prefer hydrated tables or objects instead, see the loaded-binding
examples in the quickstart and usage guide.

Adding Consist requires minimal changes to existing functions: return
`dict[str, Path]`, declare `outputs=[...]`, and bind named inputs as paths.

=== "Without Consist"

    Save the following as `workflow.py`:

    ``` python
    from pathlib import Path
    import pandas as pd
    import json

    CLEANED_PATH = Path("./cleaned.parquet")  # (1)!


    def clean_data(raw_path: Path, config_path: Path) -> None:
        """Remove rows below threshold and write cleaned output."""
        threshold = json.loads(config_path.read_text())["threshold"]
        df = pd.read_csv(raw_path)
        df[df["value"] >= threshold].to_parquet(CLEANED_PATH, index=False)


    def summarize(cleaned_path: Path) -> None:
        """Compute summary statistics from cleaned data."""
        df = pd.read_parquet(cleaned_path)
        pd.Series({"mean": df["value"].mean(), "count": len(df)}).to_json("./summary.json")


    clean_data(Path("./data/raw.csv"), Path("./config.json"))
    summarize(CLEANED_PATH)  # (2)!

    final = pd.read_json(Path("./summary.json"), typ="series")
    print(f"Result: {final.to_dict()}")
    ```

    1. The output path must be declared somewhere both functions can see. If
       `clean_data` ever writes to a different location, `summarize` breaks silently.
    2. The dependency between steps is implicit — this call only works because
       `clean_data` happened to run first and write to `CLEANED_PATH`.

    This runs correctly, but every execution re-runs both steps. There is no record
    of which `config.json` produced which `summary.json`, and no way to ask why
    results changed between runs.

=== "With Consist (Explicit Paths)"

    Save the following as `workflow.py`:

    ``` python
    from pathlib import Path
    import pandas as pd
    import json
    import consist
    from consist import ExecutionOptions, Tracker

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
        outputs=["cleaned"],
        execution_options=ExecutionOptions(input_binding="paths"),
    )
    print(f"Clean: {clean_result.run.status}")

    # Step 2: Summarize (consumes output from Step 1)
    summary_result = tracker.run(
        fn=summarize,
        inputs={"cleaned_path": consist.ref(clean_result, key="cleaned")},  # (3)!
        outputs=["summary"],
        execution_options=ExecutionOptions(input_binding="paths"),
    )
    print(f"Summarize: {summary_result.run.status}")

    # Load final result
    final = pd.read_json(summary_result.outputs["summary"].path, typ="series")
    print(f"Result: {final.to_dict()}")
    ```

    1. The function now returns `{"cleaned": out_path}` and `tracker.run(...)`
       declares `outputs=["cleaned"]`, so Consist knows which artifact to log.
    2. `inputs` declares the hashed dependencies, and
       `input_binding="paths"` passes those named inputs into the function body
       as local `Path` objects.
    3. `consist.ref(...)` passes the upstream artifact explicitly. With
       `input_binding="paths"`, Consist resolves that artifact to a local path
       before calling `summarize`.

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
