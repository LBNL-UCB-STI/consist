# Quickstart

Consist has one recommended onboarding path:

1. `run(...)` for cacheable function steps
2. `trace(...)` for always-execute blocks with provenance
3. `scenario(...)` to compose multi-step workflows from those two patterns

## Pattern 1: Cacheable Step (`run`)

Save this as `quickstart_run.py`:

``` python
from pathlib import Path
import consist
from consist import ExecutionOptions, Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

def square_value(*, value: int) -> Path:
    out = consist.output_path("square", ext="txt")
    out.write_text(f"{value ** 2}\n", encoding="utf-8")
    print(f"executed square_value(value={value})")
    return out

with consist.use_tracker(tracker):
    first = consist.run(
        fn=square_value,
        name="square",
        config={"value": 5},
        outputs=["square"],
        execution_options=ExecutionOptions(runtime_kwargs={"value": 5}),
    )
    second = consist.run(
        fn=square_value,
        name="square",
        config={"value": 5},
        outputs=["square"],
        execution_options=ExecutionOptions(runtime_kwargs={"value": 5}),
    )

print(first.cache_hit, second.cache_hit)
```

Run it twice:

```bash
python quickstart_run.py
python quickstart_run.py
```

On the second call, `run(...)` returns a cache hit and skips function execution.

## Pattern 2: Always-Execute Step (`trace`)

Use `trace(...)` for diagnostics or side effects that should always run:

``` python
with consist.use_tracker(tracker):
    with consist.trace("inspect_square", config={"value": 5}) as t:
        audit_path = consist.output_path("audit", ext="txt")
        audit_path.write_text("inspection complete\n", encoding="utf-8")
        t.log_output(audit_path, key="audit")
```

`trace(...)` still records identity/config/inputs, but the block always executes.

## Pattern 3: Compose with `scenario`

Combine `run(...)` and `trace(...)` in one workflow:

``` python
with consist.scenario("demo_pipeline", tracker=tracker, config={"value": 5}) as sc:
    step = sc.run(
        fn=square_value,
        name="square",
        config={"value": 5},
        outputs=["square"],
        execution_options=ExecutionOptions(runtime_kwargs={"value": 5}),
    )

    with sc.trace(
        "quality_check",
        inputs={"square": consist.ref(step, key="square")},
    ) as t:
        qc_path = consist.output_path("qc", ext="txt")
        qc_path.write_text("qc ok\n", encoding="utf-8")
        t.log_output(qc_path, key="qc")
```

## Next Steps

- **[First Workflow](first-workflow.md)** for a larger two-step pipeline
- **[API Essentials](../api/essentials.md)** for the recommended API surface
