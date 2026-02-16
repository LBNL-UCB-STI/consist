# Workflow Contexts

Workflow contexts are the APIs that coordinate multi-step pipelines.

## When to use workflow contexts

- Use `Tracker.scenario(...)` or `consist.scenario(...)` to group related steps
  under one scenario header run.
- Use `ScenarioContext.run(...)` for cache-aware function-shaped steps.
- Use `ScenarioContext.trace(...)` when the inline block must execute every time
  (including cache hits).
- Use `RunContext` inside `inject_context=True` runs for managed output paths and
  run-aware logging helpers.

## Relationship to `consist.run` and `Tracker.run`

| API | Execution shape | Cache-hit behavior |
|---|---|---|
| `consist.run` / `Tracker.run` | Single function call | Skips function execution and reuses cached outputs |
| `ScenarioContext.run` | Function step inside scenario | Same cache behavior as `Tracker.run` |
| `ScenarioContext.trace` / `Tracker.trace` | Inline `with` block | Block executes each time; cache state is still recorded |

## Minimal runnable scenario example

```python
from pathlib import Path
import consist
from consist import Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

def prepare() -> Path:
    out = consist.output_path("prepared", ext="txt")
    out.write_text("prepared\n")
    return out

def analyze(prepared_artifact) -> Path:
    out = consist.output_path("analysis", ext="txt")
    out.write_text(prepared_artifact.path.read_text())
    return out

with tracker.scenario("baseline") as sc:
    prep = sc.run(fn=prepare, name="prepare", outputs=["prepared"])
    analysis = sc.run(
        fn=analyze,
        name="analyze",
        inputs={"prepared_artifact": consist.ref(prep, key="prepared")},
        outputs=["analysis"],
    )
```

## Scenario Context

::: consist.core.workflow.ScenarioContext
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - run_id
        - config
        - inputs
        - add_input
        - declare_outputs
        - require_outputs
        - collect_by_keys
        - run
        - trace

## Run Context

::: consist.core.workflow.RunContext
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - run_dir
        - output_dir
        - output_path
        - inputs
        - load
        - log_artifact
        - log_artifacts
        - log_input
        - log_output
        - log_meta
        - capture_outputs

## Coupler

::: consist.core.coupler.Coupler
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - set
        - set_from_artifact
        - update
        - view
        - get
        - require
        - keys
        - items
        - values
        - path
        - declare_outputs
        - missing_declared_outputs
        - require_outputs
        - collect_by_keys
        - describe_outputs
