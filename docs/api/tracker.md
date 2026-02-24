# Tracker

`Tracker` is the core class for provenance persistence, cache lookups, and query
access. If you want explicit control over where state is stored and how runs are
executed, start here.

## When to use `Tracker`

- You are building a reusable library or service and want explicit dependencies.
- You want direct control over lifecycle methods like `start_run`, `run`,
  `scenario`, and query methods (`find_runs`, `get_artifact`, lineage helpers).
- You want to avoid relying on global context (`consist.use_tracker(...)`).

## Minimal runnable example

```python
from pathlib import Path
import consist
from consist import Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

def write_summary() -> Path:
    out = consist.output_path("summary", ext="txt")
    out.write_text("summary\n")
    return out

result = tracker.run(fn=write_summary, outputs=["summary"])
latest = tracker.find_latest_run(model=result.run.model_name)

print(result.run.id)
print(latest.id if latest else None)
```

For top-level wrappers around these methods, see [API Helpers](api_helpers.md).
For grouped workflows, see [Workflow Contexts](workflow.md).

## Constructing with `TrackerConfig`

Use `Tracker.from_config(...)` when you want a typed configuration object for
tracker construction.

```python
from consist.core.tracker import Tracker
from consist.core.tracker_config import TrackerConfig

config = TrackerConfig(run_dir="./runs", db_path="./provenance.duckdb")
tracker = Tracker.from_config(config)
```

## Public identity kwargs (`run` / `trace`)

Use `adapter` and `identity_inputs` on `Tracker.run(...)` and
`Tracker.trace(...)`:

```python
result = tracker.run(
    fn=simulate,
    adapter=activitysim_adapter,
    identity_inputs=[("asim_config", asim_config_dir)],
)

with tracker.trace(
    "simulate_trace",
    adapter=activitysim_adapter,
    identity_inputs=[("asim_config", asim_config_dir)],
):
    simulate_inline()
```

`config_plan` and `hash_inputs` are not accepted on `Tracker.run(...)` and
`Tracker.trace(...)`. Use `adapter` and `identity_inputs`.

## Config Override Selectors

`Tracker.run_with_config_overrides(...)` now supports one-of base selectors:

- `base_run_id="existing_run_id"` for historical bundle/config artifacts
- `base_config_dirs=[Path("configs"), ...]` for first-run override execution

Use exactly one selector. Passing both raises a `ValueError`.

`base_primary_config=...` is optional and only applies with
`base_config_dirs` (for adapters that require/accept a primary config hint).

`run_with_config_overrides(...)` also accepts:

- `identity_inputs=[...]`: additive hash-only inputs. These are merged with the
  adapter-resolved config identity when enabled.
- `resolved_config_identity="auto" | "off"`:
  - `"auto"` (default) injects the adapter-selected resolved config root into
    identity hashing using `identity_label`.
  - `"off"` disables that auto injection and keeps only user-provided
    `identity_inputs`.

For override runs, Consist persists standardized run metadata:
`run.meta["resolved_config_identity"]` with `mode`, `adapter`, `label`, `path`,
and `digest`.

::: consist.core.tracker.Tracker
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - from_config
        # Core lifecycle
        - begin_run
        - start_run
        - run
        - run_with_config_overrides
        - trace
        - scenario
        - end_run
        - define_step
        - last_run
        - is_cached
        - cached_artifacts
        - cached_output
        - suspend_cache_options
        - restore_cache_options
        - capture_outputs
        - log_meta
        # Logging and loading
        - log_artifact
        - log_artifacts
        - log_input
        - log_output
        - log_dataframe
        - load
        - materialize
        - ingest
        # Querying and history
        - find_runs
        - find_run
        - find_latest_run
        - get_latest_run_id
        - find_artifacts
        - get_artifact
        - get_artifacts_for_run
        - get_run
        - get_run_config
        - get_run_inputs
        - get_run_outputs
        - get_config_bundle
        - get_artifact_lineage
        - print_lineage
        - history
        - diff_runs
        - get_config_facet
        - get_config_facets
        - get_run_config_kv
        - get_config_values
        - get_config_value
        - get_registered_schema
        - registered_schemas
        - find_runs_by_facet_kv
        # Views and matrices
        - view
        - create_view
        - create_grouped_view
        - load_matrix
        - export_schema_sqlmodel
        - netcdf_metadata
        - openmatrix_metadata
        - spatial_metadata
        # Config canonicalization
        - canonicalize_config
        - prepare_config
        - apply_config_plan
        - identity_from_config_plan
        # Format-specific logging
        - log_h5_container
        - log_h5_table
        - log_netcdf_file
        - log_openmatrix_file
        # Advanced / low-level
        - engine
        - set_run_subdir_fn
        - run_artifact_dir
        - resolve_uri
        - run_query
        - get_run_record
        - resolve_historical_path
        - load_input_bundle
        - get_artifact_by_uri
        - get_run_artifact
        - load_run_output
        - find_matching_run
        - on_run_start
        - on_run_complete
        - on_run_failed
