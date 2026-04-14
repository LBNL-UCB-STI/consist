# Quickstart

Consist has one recommended onboarding path:

1. `run(...)` for cacheable function steps
2. `trace(...)` for always-execute blocks with provenance
3. `scenario(...)` to compose multi-step workflows from those two patterns

This tutorial uses a `Tracker` instance for explicitness. Elsewhere in the docs
you will also see the `consist.*` module-level helpers; they expose the same
operations through a more concise entry point.

## Pattern 1: Cacheable Step (`run`)

The same step can be expressed three ways. The recommended onboarding path is
`input_binding="paths"` because it keeps the function boundary explicit.

=== "Without Consist"

    Save this as `quickstart_run.py`:

    ``` python
    from pathlib import Path
    import pandas as pd


    def summarize_values(data_path: Path) -> Path:
        df = pd.read_csv(data_path)
        out = Path("./summary.txt")
        out.write_text(f"mean={df['value'].mean():.1f}\n", encoding="utf-8")
        print(f"executed summarize_values on {data_path}")
        return out


    summary_path = summarize_values(Path("input.csv"))
    print(summary_path.read_text().strip())
    ```

=== "With Consist (Explicit Paths)"

    Save this as `quickstart_run.py`:

    ``` python
    from pathlib import Path
    import pandas as pd
    from consist import ExecutionOptions, Tracker

    tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")


    def summarize_values(data_path: Path) -> dict[str, Path]:
        df = pd.read_csv(data_path)
        out = Path("./summary.txt")
        out.write_text(f"mean={df['value'].mean():.1f}\n", encoding="utf-8")
        print(f"executed summarize_values on {data_path}")
        return {"summary": out}


    first = tracker.run(
        fn=summarize_values,
        inputs={"data_path": Path("input.csv")},
        outputs=["summary"],
        execution_options=ExecutionOptions(input_binding="paths"),
    )
    second = tracker.run(
        fn=summarize_values,
        inputs={"data_path": Path("input.csv")},
        outputs=["summary"],
        execution_options=ExecutionOptions(input_binding="paths"),
    )

    print(first.cache_hit, second.cache_hit)  # False, True
    ```

=== "With Consist (Auto-Loaded Data)"

    Save this as `quickstart_run.py`:

    ``` python
    from pathlib import Path
    import pandas as pd
    from consist import ExecutionOptions, Tracker

    tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")


    def summarize_values(data: pd.DataFrame) -> dict[str, Path]:
        out = Path("./summary.txt")
        out.write_text(f"mean={data['value'].mean():.1f}\n", encoding="utf-8")
        print("executed summarize_values on loaded DataFrame")
        return {"summary": out}


    first = tracker.run(
        fn=summarize_values,
        inputs={"data": Path("input.csv")},
        outputs=["summary"],
        execution_options=ExecutionOptions(input_binding="loaded"),
    )
    second = tracker.run(
        fn=summarize_values,
        inputs={"data": Path("input.csv")},
        outputs=["summary"],
        execution_options=ExecutionOptions(input_binding="loaded"),
    )

    print(first.cache_hit, second.cache_hit)  # False, True
    ```

Create `input.csv` containing:

```bash
printf "value\n4\n9\n16\n" > input.csv
python quickstart_run.py
python quickstart_run.py
```

On the second call (and on the first call's second `tracker.run`), `run(...)`
returns a cache hit and skips execution.

`input_binding="paths"` means your function receives local `Path` objects.
`input_binding="loaded"` means Consist loads the artifact for you before calling
the function. Use `"loaded"` when you want hydrated tables or objects; use
`"paths"` when you want the callable to own its own I/O boundary.

If a path-bound step needs an input at a specific local filename, request that
staging directly in `ExecutionOptions`:

``` python
first = tracker.run(
    fn=summarize_values,
    inputs={"data_path": Path("input.csv")},
    outputs=["summary"],
    execution_options=ExecutionOptions(
        input_binding="paths",
        input_materialization="requested",
        input_paths={"data_path": Path("./workspace/input.csv")},
    ),
)
```

That is the recommended pattern for subprocesses or legacy tools that expect
fixed workspace-local input paths, and it still works on cache hits.

The main Consist-specific onboarding cost in these examples is small but real:
return `dict[str, Path]` instead of a bare `Path`, and declare
`outputs=[...]` so the tracker knows which artifact key to log.

## Pattern 2: Always-Execute Step (`trace`)

Use `trace(...)` for diagnostics or side effects that should always run:

``` python
with tracker.trace("inspect_summary", inputs={"data": Path("input.csv")}) as t:
    audit_path = Path("./audit.txt")
    audit_path.write_text("inspection complete\n", encoding="utf-8")
    t.log_output(audit_path, key="audit")
```

`trace(...)` still records identity/config/inputs, but the block always executes.

## Pattern 3: Compose with `scenario`

Combine `run(...)` and `trace(...)` in one workflow:

``` python
import consist

with tracker.scenario("demo_pipeline") as sc:
    step = sc.run(
        fn=summarize_values,
        inputs={"data_path": Path("input.csv")},
        outputs=["summary"],
        execution_options=ExecutionOptions(input_binding="paths"),
    )

    with sc.trace(
        "quality_check",
        inputs={"summary": consist.ref(step, key="summary")},
    ) as t:
        qc_path = Path("./qc.txt")
        qc_path.write_text("qc ok\n", encoding="utf-8")
        t.log_output(qc_path, key="qc")
```

## Next Steps

- **[Usage Guide](../usage-guide.md)** for choosing the right pattern for your real workflow
- **[First Workflow](first-workflow.md)** for a larger two-step pipeline
- **[API Essentials](../api/essentials.md)** for the recommended API surface
- **[Building a Domain Tracker](../building-domain-tracker.md)** for reusable wrapper design
