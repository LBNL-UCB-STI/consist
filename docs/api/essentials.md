# API Essentials

Use this page for day-to-day Consist usage. These APIs are the recommended path 
for new projects and onboarding docs/examples.

## Core execution patterns

- **Cacheable function step**: [`consist.run`](api_helpers.md#consist.api.run)
- **Always-execute traced step**: [`consist.trace`](api_helpers.md#consist.api.trace)
- **Workflow composition**: [`consist.scenario`](api_helpers.md#consist.api.scenario),
  [`ScenarioContext.run`](workflow.md#consist.core.workflow.ScenarioContext.run),
  [`ScenarioContext.trace`](workflow.md#consist.core.workflow.ScenarioContext.trace)

If you already have a resolved binding plan from a planner or orchestrator,
pass `binding=BindingResult(...)` to `ScenarioContext.run(...)`. That envelope
is the preferred scenario-level path for complex or externally orchestrated
workflows. Keep using `consist.ref(...)` / `consist.refs(...)` for direct
step-to-step wiring and primitive `inputs=` kwargs for ordinary scenario code.

## Essential helpers

- Tracker setup and context:
  [`Tracker`](tracker.md#consist.core.tracker.Tracker),
  [`consist.use_tracker`](api_helpers.md#consist.api.use_tracker)
- Linking artifacts:
  [`consist.ref`](api_helpers.md#consist.api.ref),
  [`consist.refs`](api_helpers.md#consist.api.refs)
- Resolved scenario bindings:
  `consist.BindingResult`
- Managed output paths:
  [`consist.output_path`](api_helpers.md#consist.api.output_path),
  [`consist.output_dir`](api_helpers.md#consist.api.output_dir)
- Logging/loading:
  [`consist.log_output`](api_helpers.md#consist.api.log_output),
  [`consist.log_dataframe`](api_helpers.md#consist.api.log_dataframe),
  [`consist.load`](api_helpers.md#consist.api.load),
  [`consist.load_df`](api_helpers.md#consist.api.load_df)
- Run lookup:
  [`consist.find_run`](api_helpers.md#consist.api.find_run),
  [`consist.find_runs`](api_helpers.md#consist.api.find_runs),
  [`consist.run_set`](api_helpers.md#consist.api.run_set) for grouped/aligned
  multi-run analysis

## Identity kwargs (run/trace parity)

These run/trace entry points share the same public identity kwargs:

- `adapter=...`
- `identity_inputs=[...]`

This parity applies across:

- `consist.run(...)` and `consist.trace(...)`
- `Tracker.run(...)` and `Tracker.trace(...)`
- `ScenarioContext.run(...)` and `ScenarioContext.trace(...)`

## Minimal essentials example

```python
from pathlib import Path
import consist
from consist import ExecutionOptions, Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

def produce(*, value: int) -> Path:
    out = consist.output_path("value", ext="txt")
    out.write_text(f"{value}\n", encoding="utf-8")
    return out

with consist.use_tracker(tracker):
    result = consist.run(
        fn=produce,
        name="produce",
        config={"value": 42},
        outputs=["value"],
        execution_options=ExecutionOptions(runtime_kwargs={"value": 42}),
    )

    with consist.trace("inspect", inputs={"value": consist.ref(result, key="value")}):
        pass
```

For lower-level patterns (`start_run`, `begin_run/end_run`, `define_step`, and
manual lifecycle wiring), see [API Advanced](advanced.md).
