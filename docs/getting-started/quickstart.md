# Quickstart

Imagine you ran your model last week, tweaked a config file, and now the result has changed — but you can't tell which input or parameter actually moved the number. Or you re-ran a parameter sweep and watched 30 minutes of preprocessing repeat for inputs that hadn't changed. Consist exists to make both of those problems go away: it records exactly what code, config, and inputs went into each step, and skips re-execution when nothing relevant has changed.

> **You give inputs and outputs *names*. Consist gives your function *paths* to read and write. Cache hits depend on *what the files contain*, not where they are.**

Consist has one recommended onboarding path:

1. `run(...)` for cacheable function steps
2. `trace(...)` for always-execute blocks with provenance
3. `scenario(...)` to compose multi-step workflows from those two patterns

This tutorial uses a `Tracker` instance for explicitness. Elsewhere in the docs
you will also see the `consist.*` module-level helpers; they expose the same
operations through a more concise entry point.

`run(...)` receives a Python callable, so Consist can use that callable's module
as code identity outside a Git repository. `trace(...)` records an inline block
with no callable to inspect; therefore, trace examples (including the
trace-containing `scenario(...)` example) require GitPython and a Git repository
with at least one commit:

```bash
pip install "consist[git]"
```

Run those examples from an existing project checkout, or pass that repository as
`project_root` when creating `Tracker`. If you are following this tutorial in a
new folder, initialize it and make an initial commit before running patterns 2
and 3. If repository identity is unavailable, Consist fails closed rather than
using an unsafe placeholder code version.

## Pattern 1: Cacheable Step (`run`)

The same step can be expressed three ways. The recommended onboarding path is
annotating input and output parameters with `: Path` and passing symmetric
`inputs={...}` and `outputs={...}` dictionaries.

=== "Without Consist"

    Save this as `quickstart_run.py`:

    ``` python
    from pathlib import Path
    import pandas as pd


    def summarize_values(data_path: Path, summary: Path) -> None:
        df = pd.read_csv(data_path)
        summary.write_text(f"mean={df['value'].mean():.1f}\n", encoding="utf-8")
        print(f"executed summarize_values on {data_path}")


    summary_path = Path("./summary.txt")
    summarize_values(Path("input.csv"), summary_path)
    print(summary_path.read_text().strip())
    ```

=== "With Consist (Explicit Paths)"

    Save this as `quickstart_run.py`:

    ``` python
    from pathlib import Path
    import pandas as pd
    from consist import Tracker

    tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")


    def summarize_values(data_path: Path, summary: Path) -> None:
        df = pd.read_csv(data_path)
        summary.write_text(f"mean={df['value'].mean():.1f}\n", encoding="utf-8")
        print(f"executed summarize_values on {data_path}")


    first = tracker.run(
        fn=summarize_values,
        inputs={"data_path": "input.csv"},
        outputs={"summary": "summary.txt"},
    )
    second = tracker.run(
        fn=summarize_values,
        inputs={"data_path": "input.csv"},
        outputs={"summary": "summary.txt"},
    )

    print(first.cache_hit, second.cache_hit)  # False, True
    print(second.outputs["summary"].path.read_text(encoding="utf-8").strip())
    ```

=== "With Consist (Auto-Loaded Data)"

    Save this as `quickstart_run.py`:

    ``` python
    from pathlib import Path
    import pandas as pd
    from consist import ExecutionOptions, Tracker

    tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")


    def summarize_values(data: pd.DataFrame, summary: Path) -> None:
        summary.write_text(f"mean={data['value'].mean():.1f}\n", encoding="utf-8")
        print("executed summarize_values on loaded DataFrame")


    first = tracker.run(
        fn=summarize_values,
        inputs={"data": Path("input.csv")},
        outputs={"summary": "summary.txt"},
        execution_options=ExecutionOptions(input_binding="loaded"),
    )
    second = tracker.run(
        fn=summarize_values,
        inputs={"data": Path("input.csv")},
        outputs={"summary": "summary.txt"},
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
returns a cache hit and skips execution. For tabular outputs (such as Parquet or
CSV), you can also load the result directly with `second.outputs["summary"].as_df()`.

When input and output parameters are annotated with `: Path`, Consist passes
local `Path` objects into your function automatically. Setting
`input_binding="loaded"` tells Consist to load input artifacts into memory (for
example as a `pd.DataFrame`) before calling the function, while still injecting
any `: Path`-annotated output destinations.

If a path-bound step needs an input staged at a specific local filename, request
that staging directly in `ExecutionOptions`:

``` python
from consist import ExecutionOptions

first = tracker.run(
    fn=summarize_values,
    inputs={"data_path": Path("input.csv")},
    outputs={"summary": "summary.txt"},
    execution_options=ExecutionOptions(
        input_binding="paths",
        input_materialization="requested",
        input_paths={
            "data_path": tracker.run_dir / "workspace" / "input.csv"
        },
    ),
)
```

By default, requested staging is restricted to the tracker run directory and
configured mount roots. This example keeps a fixed path within the
tracker-managed workspace, so it works on both cache misses and cache hits
without weakening that safety boundary. If a subprocess genuinely requires a
durable workspace outside `tracker.run_dir`, prefer configuring that directory
as a mount root. For a one-off external destination, opt in on the tracker with
`Tracker(..., allow_external_paths=True)` or set
`CONSIST_ALLOW_EXTERNAL_PATHS=1`.

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
        inputs={"data_path": "input.csv"},
        outputs={"summary": "summary.txt"},
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
- **[Upgrading to 0.5](../migrating-to-0.5.md)** if you are upgrading an existing `0.4.x` project
- **[API Essentials](../api/essentials.md)** for the recommended API surface
- **[Building a Domain Tracker](../building-domain-tracker.md)** for reusable wrapper design
