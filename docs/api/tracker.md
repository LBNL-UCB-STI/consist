# Tracker

`Tracker` is the core class for provenance persistence, cache lookups, and query
access. If you want explicit control over where state is stored and how runs are
executed, start here.

## When to use `Tracker`

- You are building a reusable library or service and want explicit dependencies.
- You want direct control over lifecycle methods like `start_run`, `run`,
  `scenario`, and query methods (`find_runs`, `run_set`, `get_artifact`,
  lineage helpers).
- You want to avoid relying on global context (`consist.use_tracker(...)`).

Run lookup helpers expose workflow-aware filters such as `stage=` and
`phase=`. Use those when you need to query by lifecycle step or pipeline stage
instead of treating those values as opaque metadata.

Tracker also exposes first-class container child-artifact queries.
`get_child_artifacts(...)` returns children ordered by creation time ascending,
and `get_parent_artifact(...)` resolves the canonical parent relation for a
child artifact. These helpers prefer `Artifact.parent_artifact_id` and fall
back to legacy `artifact.meta["parent_id"]` rows when older databases are
opened.

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

`find_runs(...)` and `find_latest_run(...)` accept `stage=` and `phase=` as
first-class workflow filters, alongside the existing run dimensions such as
`year`, `iteration`, `model`, and `status`. They also accept `facet=` for exact
matches against persisted facet values. Consist mirrors the workflow values
into `run.meta` for backward compatibility, but the canonical fields live on
`Run`.

For top-level wrappers around these methods, see [API Helpers](api_helpers.md).
For grouped workflows, see [Workflow Contexts](workflow.md).

## Container child artifacts and HDF5 helpers

Use `log_h5_container(...)` when you want to log an HDF5 file as one container
artifact plus optional child `h5_table` artifacts. The HDF5 logging surface now
supports:

- `child_specs={...}` keyed by HDF5 dataset path for per-child semantic
  customization
- `child_selection="all" | "include_only"` to either customize discovered
  children or restrict logging to the paths named in `child_specs`
- child-spec fields that mirror normal artifact ergonomics: `key`,
  `description`, `facet`, `facet_schema_version`, `facet_index`, and extra
  metadata

Use `log_h5_table(...)` when you want to register a single child artifact
without scanning the whole container. It accepts the same semantic metadata
surface (`description`, `facet`, `facet_schema_version`, `facet_index`) and can
link the child to a parent container artifact.

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
        - run_set
        - find_run
        - find_latest_run
        - get_latest_run_id
        - find_artifacts
        - get_artifact
        - get_child_artifacts
        - get_parent_artifact
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
