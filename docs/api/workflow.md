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

## Scenario step identity kwargs

`Tracker.scenario(...)` creates the header context. Identity kwargs are passed on
step surfaces (`ScenarioContext.run(...)` / `ScenarioContext.trace(...)`):

```python
with tracker.scenario("baseline") as sc:
    step = sc.run(
        fn=prepare,
        name="prepare",
        adapter=activitysim_adapter,
        identity_inputs=[("asim_config", asim_config_dir)],
        outputs=["prepared"],
    )

    with sc.trace(
        "analyze",
        adapter=activitysim_adapter,
        identity_inputs=[("asim_config", asim_config_dir)],
    ):
        analyze_inline(step.outputs["prepared"])
```

`config_plan` and `hash_inputs` are not accepted on scenario step run/trace
surfaces. Use `adapter` and `identity_inputs`.

## BindingResult execution envelopes

`ScenarioContext.run(...)` also accepts `binding=BindingResult(...)` for
orchestrator-facing code that has already resolved a step's explicit inputs,
required coupler keys, and optional coupler keys. Keep using primitive
`inputs`/`input_keys`/`optional_input_keys` kwargs for direct workflow code; the
`binding` envelope is the preferred path when the binding decision already
lives outside Consist.

`binding` is execution-only and mutually exclusive with the primitive input
kwargs. It does not change `ScenarioContext.trace(...)`.

```python
from pathlib import Path
import consist
from consist import BindingResult, ExecutionOptions, Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")


def prepare(raw: Path) -> dict[str, Path]:
    out = consist.output_path("prepared", ext="parquet")
    out.write_text("prepared\n")
    return {"prepared": out}


def analyze(prepared: Path) -> dict[str, Path]:
    out = consist.output_path("analysis", ext="parquet")
    out.write_text(prepared.read_text())
    return {"analysis": out}


with tracker.scenario("baseline") as sc:
    prep = sc.run(
        fn=prepare,
        inputs={"raw": Path("raw.csv")},
        execution_options=ExecutionOptions(input_binding="paths"),
        outputs=["prepared"],
    )

    sc.run(
        fn=analyze,
        binding=BindingResult(
            inputs={"prepared": consist.ref(prep, key="prepared")},
            input_keys=["prepared"],
        ),
        execution_options=ExecutionOptions(input_binding="loaded"),
        outputs=["analysis"],
    )
```

For direct step-to-step workflow code, `consist.ref(...)` and `consist.refs(...)`
remain the recommended default. Reach for `BindingResult` when a planner or
external orchestrator has already compiled the binding decision.

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
