# Instrumentation Patterns

## Contents

- Choose the execution surface
- Recommended defaults
- Common integration shapes
- Legacy and external-tool wrapping
- Embedded end-to-end examples

## Choose The Execution Surface

- Use `consist.run(...)` or `Tracker.run(...)` for cacheable single-step work
  that should skip user code on cache hits.
- Use `consist.trace(...)` or `Tracker.trace(...)` when the body must still run
  on cache hits.
- Use `consist.scenario(...)` or `tracker.scenario(...)` for multi-step
  workflows, repeated sweeps, and explicit step-to-step artifacts.
- Use `binding=BindingResult(...)` on `sc.run(...)` when an external planner or
  orchestrator has already resolved the step contract.
- Use `use_tracker(tracker)` with top-level `consist.*` helpers in application,
  notebook, and test code.
- Use explicit `Tracker` ownership in reusable libraries and integration layers
  where hidden global tracker state would be brittle.
- Use `start_run(...)` or `begin_run(...)` / `end_run()` only when a framework
  truly splits lifecycle hooks across call sites.
- Use `ExecutionOptions(executor=\"container\", container=...)` or the
  containers integration when the wrapped step is fundamentally a container or
  subprocess boundary.

Rule of thumb: prefer the highest-level surface that preserves the real
workflow boundary.

## Recommended Defaults

- Construct `Tracker` explicitly with `run_dir`, `db_path`, and `mounts=...`
  when outputs may be inspected from multiple machines or directories.
- Default to `ExecutionOptions(input_binding=\"paths\")`.
- Keep domain functions plain and testable; let the wrapper express Consist
  identity and output capture.
- Put true cache identity in `inputs=...`, `config=...`, and
  `identity_inputs=[...]`.
- Use `depends_on=[...]` when extra files should be hashed like declared run
  inputs rather than hidden identity-only dependencies.
- Use `consist.ref(...)` or `consist.refs(...)` for upstream artifacts instead
  of passing filesystem paths manually between steps.
- Use `output_paths={...}` when a wrapped tool returns `None`, writes files on
  its own, or needs stable host destinations for cache reuse.
- When `output_paths={...}` declares named file outputs, do not also pass the
  same names through `outputs=[...]`. Use `outputs=[...]` for Python return
  values, and `output_paths={...}` for files written by wrapped code.
- Use `output_sets={...}` when one logical output is a directory of multiple
  files, such as yearly partitions, chunked simulation outputs, or diagnostic
  bundles.
- Repeat identity-relevant config on scenario steps. Scenario header config is
  useful metadata but is not a substitute for step identity.
- Inside a scenario, use `scenario.run(...)` or `scenario.trace(...)`; do not
  manually pass `parent_run_id` to `Tracker.run(...)` for child steps.

## Common Integration Shapes

### App Or Notebook Code With `use_tracker(...)`

```python
from pathlib import Path
import consist
from consist import ExecutionOptions, Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")


def clean_data(raw_path: Path) -> dict[str, Path]:
    out = Path("./cleaned.parquet")
    ...
    return {"cleaned": out}


with consist.use_tracker(tracker):
    result = consist.run(
        fn=clean_data,
        inputs={"raw_path": Path("raw.parquet")},
        outputs=["cleaned"],
        execution_options=ExecutionOptions(input_binding="paths"),
    )
```

Use this when the downstream project already owns the tracker at the app layer.

### Library-Style Integration With Explicit Tracker Ownership

```python
from pathlib import Path
from typing import Any

from consist import ExecutionOptions, Tracker


class SimulationTracker:
    def __init__(self, *, workspace: Path, config_dir: Path) -> None:
        self._tracker = Tracker(
            run_dir=workspace / "runs",
            db_path=workspace / "provenance.duckdb",
        )
        self._identity_inputs = [("model_config", config_dir)]

    def run_model(
        self,
        *,
        name: str,
        fn,
        inputs: dict[str, Any],
        config: dict[str, Any],
    ):
        return self._tracker.run(
            name=name,
            fn=fn,
            inputs=inputs,
            config=config,
            identity_inputs=self._identity_inputs,
            outputs=["results"],
            execution_options=ExecutionOptions(input_binding="paths"),
        )
```

Use this when the downstream project exposes domain verbs or needs stable
defaults in one place.

## Legacy And External-Tool Wrapping

### File Writer That Returns `None`

```python
def run_legacy(cleaned_file: Path, _consist_ctx) -> None:
    output_dir = _consist_ctx.output_dir("legacy")
    with _consist_ctx.capture_outputs(output_dir, pattern="*.csv"):
        legacy_main(cleaned_file, output_dir)


tracker.run(
    fn=run_legacy,
    inputs={"cleaned_file": cleaned_artifact},
    identity_inputs=[("tool_config", config_dir)],
    execution_options=ExecutionOptions(
        input_binding="paths",
        inject_context=True,
    ),
)
```

Use this when the wrapped code writes a directory tree or ad hoc output files.

### Declared File Output With Metadata

```python
from consist import ArtifactSpec, ExecutionOptions

result = scenario.run(
    name="generate_firms",
    fn=generate_firms,
    inputs={"cbp": cbp_file},
    output_paths={
        "synthetic_firms": ArtifactSpec(
            path=output_dir / "synthetic_firms.csv",
            schema=SyntheticFirms,
            profile_file_schema=True,
        )
    },
    profile_file_schema=True,
    execution_options=ExecutionOptions(input_binding="paths"),
)
```

Use `ArtifactSpec` when the output path itself should carry schema, driver,
facet, or profiling metadata. `schema=...` is a non-strict logical tag for
declared outputs; it does not validate file contents. These declarations apply
when Consist logs the artifact, so newly added schema or profiling metadata is
not backfilled onto existing cached artifacts. Force a rerun or change the cache
version/epoch when persisted metadata needs to change.

### Container-Backed Step With Stable Output Paths

```python
result = scenario.run(
    name="generate_trips",
    year=year,
    config={"year": year, "base_trips": base_trips},
    identity_inputs=[("generate_trips_config", external_cfg_dir)],
    inputs=[advanced_person_art],
    output_paths={"persons": output_path},
    execution_options=ExecutionOptions(
        executor="container",
        container=container_spec,
    ),
)
```

Use stable host `output_paths` when you expect identical runs to reuse cached
container outputs across scenarios or repeated executions.

### Logical Multi-File Output With `OutputSet`

```python
from consist import CacheOptions, ExecutionOptions, OutputSet


def run_forecast(_consist_ctx) -> None:
    annual_dir = _consist_ctx.run_dir / "annual"
    annual_dir.mkdir(parents=True, exist_ok=True)
    write_annual_outputs(annual_dir)


result = tracker.run(
    name="forecast",
    fn=run_forecast,
    config={"years": [2030, 2035]},
    output_sets={
        "annual_outputs": OutputSet(
            root="annual",
            include="annual_*.csv",
            expected_members=lambda config: [
                f"annual_{year}.csv" for year in config["years"]
            ],
        )
    },
    cache_options=CacheOptions(cache_hydration="outputs-requested"),
    execution_options=ExecutionOptions(inject_context="_consist_ctx"),
)
```

Use `OutputSet` when callers should see one logical output artifact even though
the tool wrote many member files. Only `root` and `include` are required;
`expected_members` and `expected_count` are optional completeness checks. Consist
logs one parent artifact with `driver="artifact_set"`, child file artifacts
linked by `parent_artifact_id`, and a JSON manifest artifact. Cache hydration
restores child files under the declared set root and returns the parent in
`RunResult.outputs`.

Inspect output sets with `consist artifacts RUN_ID --expand-sets` or, from
`consist shell`, with `members <artifact>` and `manifest <artifact>`.

Security boundary: output-set members must stay inside the set root. Symlinked
member files and cached member paths containing absolute paths or `..` are
rejected. Do not use output sets to intentionally collect arbitrary external
files; declare external dependencies with `inputs`, `depends_on`, or
`identity_inputs`.

### Compiled Bindings For Orchestrators

```python
from consist import BindingResult, ExecutionOptions

binding = BindingResult(
    inputs={"raw": Path("raw.csv")},
    input_keys=["prepared"],
    optional_input_keys=["maybe_aux"],
)

scenario.run(
    fn=consume,
    binding=binding,
    execution_options=ExecutionOptions(input_binding="paths"),
)
```

Use this when another layer already decided which upstream artifacts and direct
inputs belong to the step. Keep `consist.ref(...)` and `consist.refs(...)` as
the default for direct workflow code.

### Capture A Fixed Output Directory

```python
result = consist.run(
    fn=run_legacy_model,
    inputs={"upstream": Path("input.csv")},
    depends_on=[Path("config.yaml")],
    execution_options=ExecutionOptions(input_binding="paths"),
    capture_dir=Path("outputs"),
    capture_pattern="*.csv",
)
```

Use this when the tool always writes to a known folder and you do not need
context injection.

### Manual Lifecycle

```python
tracker.begin_run(run_id="request_123", model="external_framework")
try:
    tracker.log_input(config_path, key="config")
    ...
finally:
    tracker.end_run()
```

Use this only when the host framework truly splits start and finish hooks.

## Embedded End-To-End Examples

### Two-Step Pipeline With Explicit Artifact Handoff

```python
from pathlib import Path
import consist
from consist import ExecutionOptions, Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")


def clean_data(raw_path: Path) -> dict[str, Path]:
    out_path = Path("./cleaned.parquet")
    ...
    return {"cleaned": out_path}


def summarize(cleaned_path: Path) -> dict[str, Path]:
    out_path = Path("./summary.json")
    ...
    return {"summary": out_path}


clean_result = tracker.run(
    fn=clean_data,
    inputs={"raw_path": Path("./data/raw.csv")},
    outputs=["cleaned"],
    execution_options=ExecutionOptions(input_binding="paths"),
)

summary_result = tracker.run(
    fn=summarize,
    inputs={"cleaned_path": consist.ref(clean_result, key="cleaned")},
    outputs=["summary"],
    execution_options=ExecutionOptions(input_binding="paths"),
)
```

Use this when replacing implicit filesystem handoffs with explicit lineage.

### Scenario With Step Identity Repeated Explicitly

```python
with tracker.scenario(
    "baseline",
    config={"mode": "cold", "base_trips": 10},
    model="pilates_orchestrator",
) as scenario:
    with scenario.trace(
        name="pop_synth",
        config={"mode": "cold", "base_trips": 10},
    ):
        ...

    scenario.run(
        name="generate_trips",
        year=2030,
        config={"year": 2030, "base_trips": 10},
        identity_inputs=[("generate_trips_config", external_cfg_dir)],
        inputs=[advanced_person_art],
        output_paths={"persons": output_path},
        execution_options=ExecutionOptions(
            executor="container",
            container=container_spec,
        ),
    )
```

Use this shape when downstream scenario names are metadata, but cache identity
must still reflect the actual step inputs and config.

### Recursive Directory Capture

If a legacy tool writes a nested directory tree, prefer the top-level
`consist.capture_outputs(...)` helper or `tracker.capture_outputs(...)` with
`recursive=True`. `RunContext.capture_outputs(...)` only exposes `directory` and
`pattern`, so it is not the right surface when recursive capture matters.
