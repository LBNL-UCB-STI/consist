# Quickstart

Consist has one recommended onboarding path:

1. `run(...)` for cacheable function steps
2. `trace(...)` for always-execute blocks with provenance
3. `scenario(...)` to compose multi-step workflows from those two patterns

## Pattern 1: Cacheable Step (`run`)

Save this as `quickstart_run.py`:

``` python
from pathlib import Path
from consist import Tracker
import consist

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

def square_value(input_file: Path) -> dict[str, Path]:
    value = int(input_file.read_text().strip())
    out = Path("./square.txt")
    out.write_text(f"{value ** 2}\n", encoding="utf-8")
    print(f"executed square_value on {input_file}")
    return {"square": out}

first = tracker.run(fn=square_value, inputs={"input_file": Path("input.txt")})
second = tracker.run(fn=square_value, inputs={"input_file": Path("input.txt")})

print(first.cache_hit, second.cache_hit)  # False, True
```

Create `input.txt` containing `5`, then run the script twice:

```bash
echo "5" > input.txt
python quickstart_run.py
python quickstart_run.py
```

On the second call (and on the first call's second `tracker.run`), `run(...)` returns a cache hit and skips execution.

## Pattern 2: Always-Execute Step (`trace`)

Use `trace(...)` for diagnostics or side effects that should always run:

``` python
with tracker.trace("inspect_square", inputs={"input_file": Path("input.txt")}) as t:
    audit_path = Path("./audit.txt")
    audit_path.write_text("inspection complete\n", encoding="utf-8")
    t.log_output(audit_path, key="audit")
```

`trace(...)` still records identity/config/inputs, but the block always executes.

## Pattern 3: Compose with `scenario`

Combine `run(...)` and `trace(...)` in one workflow:

``` python
with tracker.scenario("demo_pipeline") as sc:
    step = sc.run(
        fn=square_value,
        inputs={"input_file": Path("input.txt")},
    )

    with sc.trace(
        "quality_check",
        inputs={"square": consist.ref(step, key="square")},
    ) as t:
        qc_path = Path("./qc.txt")
        qc_path.write_text("qc ok\n", encoding="utf-8")
        t.log_output(qc_path, key="qc")
```

## Next Steps

- **[First Workflow](first-workflow.md)** for a larger two-step pipeline
- **[API Essentials](../api/essentials.md)** for the recommended API surface
- **[Building a Domain Tracker](../building-domain-tracker.md)** for reusable wrapper design
