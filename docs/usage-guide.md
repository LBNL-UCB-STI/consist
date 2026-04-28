# Usage Guide

Use this page as a workflow-pattern chooser. It is intentionally compact: pick
the pattern that matches your step shape, then follow the linked page when you
need deeper API or integration detail.

If you are new to Consist, start with the
[quickstart](getting-started/quickstart.md) and
[first workflow](getting-started/first-workflow.md) tutorials first.

## Choose a Pattern

| Need | Start with | Why |
|---|---|---|
| One Python callable with declared inputs and outputs | `Tracker.run(...)` | Smallest cache-aware execution surface. |
| Multi-step workflow with explicit handoffs | `tracker.scenario(...).run(...)` | Groups related steps and caches each child step independently. |
| Inline lifecycle block or custom logging | `tracker.trace(...)` or `sc.trace(...)` | Use when the body needs direct access to the active run context. |
| Step-to-step artifact handoff | `consist.ref(...)` / `consist.refs(...)` | Makes lineage explicit and avoids ambiguous coupler-key indirection. |
| External tool that expects local files | `input_binding="paths"` plus requested input staging | Keeps provenance identity separate from workspace-local file layout. |
| Container or black-box executable | Container integration | See [Containers Guide](containers-guide.md). |
| Historical output recovery or archive restore | `hydrate_run_outputs(...)` | See [Materialization](api/materialize.md). |

For cache semantics, hydration policies, and cache-miss diagnosis, see
[Caching and Hydration](concepts/caching-and-hydration.md).

## `Tracker.run(...)`: Single Cached Step

Use `Tracker.run(...)` when a step can be represented as a normal Python
function. Declare inputs that affect identity, declare expected outputs, and
let Consist decide whether to execute or return cached outputs.

```python
from pathlib import Path

import pandas as pd

from consist import ExecutionOptions, Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")


def summarize_trips(trips_path: Path, threshold: float) -> dict[str, Path]:
    trips = pd.read_parquet(trips_path)
    out = Path("./summary.parquet")
    trips.loc[trips["distance"] >= threshold].groupby("mode").size().to_frame(
        "count"
    ).to_parquet(out)
    return {"summary": out}


result = tracker.run(
    name="summarize_trips",
    fn=summarize_trips,
    inputs={"trips_path": Path("inputs/trips.parquet")},
    config={"threshold": 1.0},
    outputs=["summary"],
    execution_options=ExecutionOptions(input_binding="paths"),
)

summary_artifact = result.outputs["summary"]
```

Reach for this first when:

- The step has a clear callable boundary.
- Inputs and config fully describe the computation.
- Outputs can be returned as `dict[str, Path]` or declared with
  `output_paths={...}`.

Use options objects for policy controls:

```python
from consist import CacheOptions, ExecutionOptions, OutputPolicyOptions

result = tracker.run(
    fn=summarize_trips,
    inputs={"trips_path": Path("inputs/trips.parquet")},
    outputs=["summary"],
    cache_options=CacheOptions(
        cache_mode="reuse",
        cache_hydration="inputs-missing",
        code_identity="callable_module",
    ),
    output_policy=OutputPolicyOptions(output_missing="error"),
    execution_options=ExecutionOptions(input_binding="paths"),
)
```

For the full run API contract, see [Run](api/run.md).

## Input Binding

`input_binding` controls what Consist passes into your callable for named
`inputs={...}`. It is a runtime execution choice, not the whole cache identity
model.

| Mode | Callable receives | Use when |
|---|---|---|
| `"paths"` | Local `Path` objects | The function or wrapped tool owns its own file I/O. |
| `"loaded"` | Loaded Python objects, such as DataFrames | Short analysis code benefits from automatic loading. |
| `"none"` | Nothing is bound automatically | Inputs are identity-only or a custom wrapper handles binding. |

Prefer `"paths"` for production pipelines because the I/O boundary stays
visible. Use `"loaded"` when reducing boilerplate matters more than showing the
file boundary in the function signature.

`runtime_kwargs` is still useful for values that should be passed to the
callable but should not be treated as declared inputs.

## Path Staging Example

Requested input staging is for path-bound tools that need a declared input at a
specific local destination. `inputs={...}` remains the identity and lineage
source; `input_paths={...}` describes the workspace location the callable should
see.

```python
from pathlib import Path
import subprocess

from consist import ExecutionOptions, Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")


def run_legacy_tool(config_path: Path) -> dict[str, Path]:
    report = Path("./report.txt")
    subprocess.run(
        ["legacy-tool", "--config", str(config_path), "--out", str(report)],
        check=True,
    )
    return {"report": report}


result = tracker.run(
    name="legacy_report",
    fn=run_legacy_tool,
    inputs={"config_path": Path("./configs/baseline.yaml")},
    outputs=["report"],
    execution_options=ExecutionOptions(
        input_binding="paths",
        input_materialization="requested",
        input_materialization_mode="copy",
        input_paths={"config_path": Path("./workspace/tool-config.yaml")},
    ),
)
```

Use this when:

- A subprocess or model expects fixed filenames.
- The same local file contract must hold on cache hits and misses.
- You do not want manual copies to become hidden workflow state.

For lower-level staging helpers, see [Materialization](api/materialize.md).

## `scenario(...)`: Multi-Step Workflows

Use a scenario when steps belong to one workflow and should be queried together.
Each child step gets its own cache identity, so upstream hits can be reused
while downstream misses re-execute.

```python
from pathlib import Path

import pandas as pd

import consist
from consist import ExecutionOptions, Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")


def clean(raw_path: Path) -> dict[str, Path]:
    df = pd.read_csv(raw_path)
    out = Path("./cleaned.parquet")
    df[df["value"] > 0].to_parquet(out)
    return {"cleaned": out}


def analyze(cleaned_path: Path) -> dict[str, Path]:
    df = pd.read_parquet(cleaned_path)
    out = Path("./analysis.parquet")
    df.groupby("category").size().to_frame("count").to_parquet(out)
    return {"analysis": out}


with tracker.scenario("baseline") as sc:
    cleaned = sc.run(
        name="clean",
        fn=clean,
        inputs={"raw_path": Path("inputs/raw.csv")},
        outputs=["cleaned"],
        execution_options=ExecutionOptions(input_binding="paths"),
    )

    sc.run(
        name="analyze",
        fn=analyze,
        inputs={"cleaned_path": consist.ref(cleaned, key="cleaned")},
        outputs=["analysis"],
        execution_options=ExecutionOptions(input_binding="paths"),
    )
```

Use `scenario(...)` when:

- Steps have data dependencies.
- You need per-step cache reuse inside a larger workflow.
- A run should carry shared scenario metadata such as model, year, tags, or
  facets.
- You need scenario-level output validation.

For decorator defaults, templates, and step metadata, see
[Decorators and Metadata](concepts/decorators-and-metadata.md).

## `ref(...)` and `refs(...)`

Use explicit references for step-to-step data flow.

```python
# Select one output from a prior result.
single_input = {"cleaned_path": consist.ref(cleaned, key="cleaned")}

# Reuse output keys as downstream input names.
all_inputs = consist.refs(cleaned)

# Select a subset.
subset = consist.refs(cleaned, "cleaned")

# Rename outputs to match downstream function parameters.
aliased = consist.refs(cleaned, {"cleaned_path": "cleaned"})
```

Prefer `ref(...)` for one artifact and `refs(...)` for reusable mappings. Avoid
passing a multi-output `RunResult` directly as an input because the intended
output key is ambiguous.

## `trace(...)`: Inline Lifecycle Blocks

Use `trace(...)` when a step does not fit a single callable or when you need to
log artifacts directly inside a block.

```python
from pathlib import Path

with tracker.trace(
    name="manual_export",
    model="analysis",
    inputs={"source": Path("inputs/source.parquet")},
) as t:
    output = Path("./export.csv")
    build_export(output)
    t.log_artifact(output, key="export")
```

Inside a scenario, `sc.trace(...)` records a child run under the scenario:

```python
with tracker.scenario("baseline") as sc:
    with sc.trace(name="validate", inputs=consist.refs(cleaned, "cleaned")):
        artifact = consist.ref(cleaned, key="cleaned")
        df = consist.load_df(artifact, tracker=tracker)
        assert not df.empty
```

Important distinction:

- `run(...)` can skip callable execution on a cache hit.
- `trace(...)` enters your Python block; use it when explicit lifecycle control
  matters more than skip-on-hit function execution.

## External Tools and Containers

For existing tools, keep the wrapper thin:

1. Declare identity-bearing inputs with `inputs={...}` and `depends_on=[...]`.
2. Use `input_binding="paths"` so the wrapper receives paths.
3. Use requested input staging only when the tool needs exact local names.
4. Return `dict[str, Path]` for outputs when possible, or use `output_paths`.
5. Move container-specific details to the container integration layer.

For container setup, volume behavior, and image identity, see
[Containers Guide](containers-guide.md).

## Querying Results

Start from the tracker when you need to inspect what happened:

```python
run = tracker.get_run(result.run.id)
print(run.status)
print(run.identity_summary)
print(run.meta.get("cache_miss_explanation"))
```

For indexed metadata and query helpers, see:

- [Config Management](concepts/config-management.md) for config, facets, and
  queryable metadata.
- [Grouped Views](concepts/grouped-views.md) for cross-run views over matching
  artifact schemas.
- [CLI Reference](cli-reference.md) for command-line inspection.

## Where Deeper Detail Lives

- [Caching and Hydration](concepts/caching-and-hydration.md): cache identity,
  hit/miss behavior, hydration, and materialization policies.
- [Data Materialization](concepts/data-materialization.md): ingestion,
  database fallback, and hybrid views.
- [Materialization](api/materialize.md): `hydrate_run_outputs(...)`,
  `stage_inputs(...)`, and staging status meanings.
- [Run API](api/run.md): accepted run options and generated reference.
- [Building a Domain Tracker](building-domain-tracker.md): when to wrap direct
  Consist calls in a domain-specific interface.
