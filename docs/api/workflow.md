# Workflow Contexts

Workflow contexts are the APIs that coordinate multi-step pipelines.

## When to use workflow contexts

- Use `Tracker.scenario(...)` or `consist.scenario(...)` to group related steps
  under one scenario header run.
- Use `ScenarioContext.run(...)` for cache-aware function-shaped steps.
- Use `ScenarioContext.map_runs(...)` for independent parameter sweeps where
  each row should become one child run under the scenario.
- Use `ScenarioContext.trace(...)` when the inline block must execute every time
  (including cache hits).
- Use `RunContext` inside `inject_context=True` runs for managed output paths and
  run-aware logging helpers.

## Relationship to `consist.run` and `Tracker.run`

| API | Execution shape | Cache-hit behavior |
|---|---|---|
| `consist.run` / `Tracker.run` | Single function call | Skips function execution and reuses cached outputs |
| `ScenarioContext.run` | Function step inside scenario | Same cache behavior as `Tracker.run` |
| `ScenarioContext.map_runs` | Many independent function runs inside scenario | Executes one child run per row and records a batch result |
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

`Tracker.scenario(...)` and `consist.scenario(...)` also accept
`step_tags=[...]` and `step_facet={...}` to apply default tags/facets to every
child step in the scenario. These defaults apply to `run(...)` and `trace(...)`
children only, not to the scenario header itself, and per-step values take
precedence on conflicts.

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

## Immutable resolved bindings (advanced)

`ResolvedBinding` is a stricter, opt-in contract for an external planner that
must replay one already-admitted artifact choice exactly. It is not the normal
Coupler workflow: use `inputs=`, `consist.ref(...)`, and `BindingResult` for
ordinary scenario composition.

A `ResolvedBinding` contains typed artifact identities, tracked-artifact
locators, the callable's step-contract identity, and optional admission
evidence. `ScenarioContext.run(...)` accepts it only with
`ExecutionOptions(input_binding="paths")`. Before invoking the callable,
Consist verifies the artifact identity and stages a fresh, run-owned copy at
each declared destination; it refuses an unsafe destination or a mismatched
callable. Every strict invocation is written to the `run_binding_invocation`
ledger. Cache hits keep their producer run immutable and record that producer
as `cache_source_run_id` on the new invocation record.

Build these bindings in orchestration code with `ResolvedBindingBuilder`; do
not construct them from unverified paths or use them to replace the normal
Coupler-based workflow.

For an artifact already admitted through Consist, use the tracked-artifact
bridge rather than reconstructing an identity or locator in downstream code:

```python
binding = (
    consist.ResolvedBindingBuilder(
        step_name="beam",
        step_contract_identity=step_contract_identity(run_beam, "beam"),
    )
    .bind_tracked_artifact(
        parameter="linkstats_warmstart",
        artifact=admitted_linkstats,
        destination=Path("inputs/linkstats.parquet"),
        source="external_admitted",
    )
    .with_diagnostics({"selection": {"reason": "approved warm start"}})
    .freeze()
)
```

The strict contract has two serializations. `identity_json()` contains the
step contract plus each parameter's typed identity and destination; its digest
partitions cache reuse. `evidence_json()` adds tracked locators, selection
details, metadata, admission evidence, and diagnostics. The latter is written
to `run_binding_invocation`, so explaining why a fallback was selected never
changes cache reuse.

### Snapshot and callable-path invariant

For `input_binding="paths"`, a strict input parameter receives the fresh
run-owned snapshot under `.resolved-bindings/<run-id>/<destination>`, not the
original admitted path. A model that reads a workspace path or a
container-mapped path must be configured so that its final visible path resolves
to those snapshot bytes. A separately staged workspace copy is not equivalent
merely because it has the same intended filename; it must resolve to the strict
snapshot (or be independently verified against its typed identity).

### V1 scope limitation

Every V1 strict input must correspond to a named callable parameter. Consist
does not yet model a strict `staged_artifact` that participates in identity and
staging while being accessed only through a workspace destination or
`RunContext`. Do not hide dynamic artifacts behind fake parameters or omit them
from strict identity; keep those restart closures on the ordinary workflow path
until that explicit artifact mode exists.

## Parallel sweep runs

`ScenarioContext.map_runs(...)` is the scenario-level fan-out helper for
embarrassingly parallel sweeps. It builds one `BatchRunSpec` per input row,
executes each spec in a worker process, and returns a `BatchResult` summary.

Use it when:

- Every row can run independently.
- The worker function is importable as `"module:function"`.
- The parent scenario should group all child runs for later queries.
- You want retry metadata for failed rows.

```python
rows = [
    {"case_id": 0, "seed": 10, "elasticity": -0.2},
    {"case_id": 1, "seed": 11, "elasticity": -0.4},
]

with tracker.scenario("elasticity_sweep") as sc:
    batch = sc.map_runs(
        rows=rows,
        fn="project.steps:run_elasticity_case",
        name_template="case_{case_id}",
        model="demand_model",
        config_from=lambda row: {
            "seed": row["seed"],
            "elasticity": row["elasticity"],
        },
        facet_from=lambda row: {
            "case_id": row["case_id"],
            "elasticity": row["elasticity"],
        },
        expected_outputs=["summary"],
    )
```

The returned `BatchResult` separates correlation metadata from persisted run
metadata:

- `BatchResult.results` contains `RunSpecResult` objects for observed outcomes.
- `RunSpecResult.run_id` is the requested child run ID for the spec.
- `RunSpecResult.persisted_run_id` is set only when a run record exists.
- `BatchResult.child_run_ids` includes only persisted child run IDs.
- `BatchResult.failed_specs` contains `BatchRunSpec` objects that can be retried.

The current backend is `backend="processes"`. It requires an importable callable
reference and does not support direct callables, lambdas, or notebook-local
functions. `cancel_pending_on_failure=True` makes a best-effort attempt to
cancel work that has not started after the first observed failure; running
workers may still complete.

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
        - map_runs
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
