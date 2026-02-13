# Quickstart

This example demonstrates Consist's core capability: caching a function so that repeated calls with the same inputs skip re-execution and return stored results.

## Create a Cached Run

Save the following as `quickstart.py`. This example uses `consist.run()`, which is the simplest way to track one-off functions. For building reusable pipeline libraries, you may prefer the `@tracker.define_step` decorator shown in the next tutorial.

``` python
from pathlib import Path
import consist
from consist import ExecutionOptions, Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")  # (1)!

def process_data(value: int) -> dict:
    print(f"Executing with value={value}")  # (2)!
    return {"squared": value ** 2}

with consist.use_tracker(tracker):  # (3)!
    result = consist.run(
        fn=process_data,
        name="square",
        config={"value": 5},
        execution_options=ExecutionOptions(runtime_kwargs={"value": 5}),  # (4)!
    )

print(result.run.id, result.outputs)
```

1. The **Tracker** manages the provenance database and run directory. All runs and artifacts are stored here.
2. This print statement appears only when the function executes—not on cache hits.
3. `use_tracker` sets the active tracker for the context. Functions inside this block use this tracker.
4. `runtime_kwargs` passes arguments to the function. `config` is hashed to determine cache validity.

## Run Twice, Observe Caching

Execute the script twice:

```bash
python quickstart.py
python quickstart.py
```

**First run**: The function executes, printing `Executing with value=5`. Consist records the run's signature—a hash of the code version, config, and inputs—and stores the output.

**Second run**: Consist finds a run with an identical signature (a **cache hit**) and returns the stored result. The function body does not execute; no print statement appears.

This is Consist's core value proposition: execute once, reuse forever—until code or config changes.

## Inspect Provenance

Query the provenance database to see recorded runs:

```bash
python -m consist.cli runs --db-path ./provenance.duckdb --limit 5
```

Output includes run ID, model name, status, and execution time.

## Next Steps

- **[First Workflow](first-workflow.md)** — Build a multi-step pipeline with data passing between tasks
- **[Core Concepts](../concepts/overview.md)** — Understand artifacts, signatures, and lineage
