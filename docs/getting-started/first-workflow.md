# First Workflow

This tutorial builds a two-step data pipeline demonstrating artifact passing, cache invalidation, and lineage tracking. Each step is a separate cached run; changing config or code in one step invalidates only that step and its dependents.

This page uses `Tracker` instance methods for clarity. If you prefer the
shorter `consist.*` module-level helpers, the underlying behavior is the same.

## Prerequisites

Install Consist first. Base installation instructions: [Installation](installation.md)

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

This tutorial uses `: Path` parameter annotations with symmetric `inputs={...}`
and `outputs={...}` dictionaries throughout. That keeps the callable boundary
explicit and makes it obvious which files each step reads and writes. If you
prefer hydrated tables or objects instead, see the loaded-binding examples in
the [Usage Guide](../usage-guide.md#input-binding).

=== "Without Consist"

    Save the following as `workflow.py`:

    ``` python
    from pathlib import Path
    import pandas as pd
    import json

    CLEANED_PATH = Path("./cleaned.parquet")  # (1)!


    def clean_data(raw_path: Path, config_path: Path, cleaned: Path) -> None:
        """Remove rows below threshold and write cleaned output."""
        threshold = json.loads(config_path.read_text())["threshold"]
        df = pd.read_csv(raw_path)
        df[df["value"] >= threshold].to_parquet(cleaned, index=False)


    def summarize(cleaned_path: Path, summary: Path) -> None:
        """Compute summary statistics from cleaned data."""
        df = pd.read_parquet(cleaned_path)
        pd.Series({"mean": df["value"].mean(), "count": len(df)}).to_json(summary)


    clean_data(Path("./data/raw.csv"), Path("./config.json"), CLEANED_PATH)
    summarize(CLEANED_PATH, Path("./summary.json"))  # (2)!

    final = pd.read_json(Path("./summary.json"), typ="series")
    print(f"Result: {final.to_dict()}")
    ```

    1. The output path must be coordinated manually between steps. If
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
    from consist import Tracker

    tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")


    def clean_data(raw_path: Path, config_path: Path, cleaned: Path) -> None:  # (1)!
        """Remove rows below threshold and write cleaned output."""
        threshold = json.loads(config_path.read_text())["threshold"]
        df = pd.read_csv(raw_path)
        df[df["value"] >= threshold].to_parquet(cleaned, index=False)


    def summarize(cleaned_path: Path, summary: Path) -> None:
        """Compute summary statistics from cleaned data."""
        df = pd.read_parquet(cleaned_path)
        pd.Series({"mean": df["value"].mean(), "count": len(df)}).to_json(summary)


    # Step 1: Clean
    clean_result = tracker.run(
        fn=clean_data,
        inputs={  # (2)!
            "raw_path": "./data/raw.csv",
            "config_path": "./config.json",
        },
        outputs={"cleaned": "cleaned.parquet"},
    )
    print(f"Clean: {clean_result.run.status}")

    # Step 2: Summarize (consumes output from Step 1)
    summary_result = tracker.run(
        fn=summarize,
        inputs={"cleaned_path": clean_result.outputs["cleaned"]},  # (3)!
        outputs={"summary": "summary.json"},
    )
    print(f"Summarize: {summary_result.run.status}")

    # Inspect intermediate table and load final result
    cleaned_df = clean_result.outputs["cleaned"].as_df()
    final = pd.read_json(summary_result.outputs["summary"].path, typ="series")
    print(f"Cleaned rows: {len(cleaned_df)}, Result: {final.to_dict()}")
    ```

    1. Because `raw_path`, `config_path`, and `cleaned` are annotated with
       `: Path`, Consist passes local `Path` objects for both inputs and outputs
       directly into `clean_data`.
    2. `inputs` and `outputs` map parameter names to relative or absolute file
       paths, and Consist verifies that `cleaned.parquet` exists when the step
       completes.
    3. Passing `clean_result.outputs["cleaned"]` (or `consist.ref(clean_result, key="cleaned")`)
       wires the upstream artifact into `summarize` and resolves its local `.path`
       automatically.

## Run and Observe Caching

Execute the pipeline:

```bash
python workflow.py
```

Output:

```
Clean: completed
Summarize: completed
Cleaned rows: 4, Result: {'mean': 35.0, 'count': 4}
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

Consist detects that `config.json`'s content fingerprint changed and re-executes `clean_data`. Because its output artifact changes, `summarize` also re-executes — its input fingerprint now differs from the cached version.

```
Clean: completed
Summarize: completed
Cleaned rows: 3, Result: {'mean': 40.0, 'count': 3}
```

This is the Merkle DAG at work: a change to any input propagates downstream automatically. The `config.json` file is just another fingerprinted input — no special configuration API required.

## View Lineage

Query the provenance database:

```bash
python -m consist.cli runs --db-path ./provenance.duckdb --limit 10
```

Each run shows its inputs, outputs, config fingerprint, and status. Artifacts link to their producing runs, forming a complete lineage graph.

## Summary

This workflow demonstrated:

- **Cacheable function steps** using `tracker.run(...)` with symmetric `inputs={...}` and `outputs={...}`
- **Artifact passing** where one step's output handle (`clean_result.outputs["cleaned"]`) becomes another step's input
- **Cache invalidation** triggered by input file changes
- **Lineage tracking** linking runs through their artifacts

## Next Steps

- **[Usage Guide](../usage-guide.md)** — Broader pattern guide for `run(...)`, `scenario(...)`, and external tool wrappers
- **[Core Concepts](../concepts/overview.md)** — Mental model for runs, artifacts, and signatures
- **[Config Management](../concepts/config-management.md)** — Control which parameters affect caching
- **[Caching & Hydration](../concepts/caching-and-hydration.md)** — Advanced cache strategies and data recovery
- **[Data Storage and Ingestion](../concepts/data-materialization.md)** — Ingest artifacts for SQL analysis
- **[Building a Domain Tracker](../building-domain-tracker.md)** — Wrap Consist behind domain verbs
