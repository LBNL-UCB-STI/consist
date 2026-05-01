# API Helpers

`consist.api` provides top-level helper functions that forward to the active
tracker. Use these when you want concise scripts and notebooks without passing
a tracker object to every call.

## When to use helpers vs class methods

| Prefer | When |
|---|---|
| `consist.*` helpers | Notebook/script workflows where `use_tracker(...)` is already set |
| `Tracker.*` methods | Library/application code where explicit object wiring is preferred |

## High-traffic helpers

- Run execution: `consist.run`, `consist.scenario`, `consist.trace`,
  `consist.start_run`
- Input wiring: `consist.ref`, `consist.refs`
- Context and output paths: `consist.use_tracker`, `consist.output_dir`,
  `consist.output_path`
- Artifact logging/loading: `consist.log_artifact`, `consist.log_dataframe`,
  `consist.load`, `consist.load_df`
- Recovery/staging/archive: `consist.hydrate_run_outputs`,
  `consist.materialize_run_outputs`, `consist.stage_artifact`,
  `consist.stage_inputs`, `consist.set_artifact_recovery_roots`,
  `consist.register_artifact_recovery_copy`,
  `consist.register_run_output_recovery_copies`, `consist.archive_artifact`,
  `consist.archive_run_outputs`, `consist.archive_current_run_outputs`
- Querying: `consist.find_run`, `consist.find_runs`,
  `consist.find_latest_run`, `consist.run_query`, `consist.run_set`,
  `consist.get_run_result`, `consist.config_run_query`,
  `consist.config_run_rows`

`consist.find_runs(...)`, `consist.find_run(...)`, and
`consist.find_latest_run(...)` forward standard run filters, including
`stage=` and `phase=` for workflow-level queries.

## Minimal runnable helper workflow

```python
from pathlib import Path
import consist
from consist import Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

def step() -> Path:
    out = consist.output_path("step", ext="txt")
    out.write_text("done\n")
    return out

with consist.use_tracker(tracker):
    result = consist.run(fn=step, outputs=["step"])
    latest = consist.find_run(run_id=result.run.id)

print(result.outputs["step"].path)
print(latest.id if latest else None)
```

For class-level equivalents, see [Tracker](tracker.md) and
[Workflow Contexts](workflow.md).

::: consist.api
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - view
        - use_tracker
        - run
        - ref
        - refs
        - trace
        - start_run
        - define_step
        - require_runtime_kwargs
        - scenario
        - single_step_scenario
        - current_tracker
        - current_run
        - current_consist
        - output_dir
        - output_path
        - cached_artifacts
        - cached_output
        - get_run_result
        - get_artifact
        - register_artifact_facet_parser
        - log_artifact
        - log_artifacts
        - log_input
        - log_output
        - log_dataframe
        - log_meta
        - ingest
        - register_views
        - find_run
        - find_runs
        - find_latest_run
        - run_set
        - db_session
        - run_query
        - config_run_query
        - config_run_rows
        - pivot_facets
        - capture_outputs
        - load
        - load_df
        - load_relation
        - hydrate_run_outputs
        - materialize_run_outputs
        - stage_artifact
        - stage_inputs
        - set_artifact_recovery_roots
        - register_artifact_recovery_copy
        - register_run_output_recovery_copies
        - archive_artifact
        - archive_run_outputs
        - archive_current_run_outputs
        - to_df
        - active_relation_count
        - set_current_tracker
        - noop_scenario
        - RelationConnectionLeakWarning
        - is_dataframe_artifact
        - is_tabular_artifact
        - is_json_artifact
        - is_zarr_artifact
        - is_hdf_artifact
        - is_spatial_artifact
      filters:
        - "!^_"
