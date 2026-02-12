# Public API (v0.1)

This page defines Consist's **public API** for the `0.1.x` series. Items listed as
*Advanced* are still public but are primarily targeted at advanced users and may be
more verbose, lower-level, or easier to misuse.

## Stable (intended for external users)

- [`consist.Tracker`](tracker.md)
- [`consist.Run`](run.md)
- [`consist.Artifact`](artifact.md)
- `consist.CacheOptions`, `consist.OutputPolicyOptions`, `consist.ExecutionOptions`

### Scenario / workflow helpers

- [`consist.scenario`](api_helpers.md#consist.api.scenario)
- [`consist.single_step_scenario`](api_helpers.md#consist.api.single_step_scenario)
- [`consist.run`](api_helpers.md#consist.api.run)
- [`consist.trace`](api_helpers.md#consist.api.trace)
- [`consist.start_run`](api_helpers.md#consist.api.start_run)
- [`consist.define_step`](api_helpers.md#consist.api.define_step)
- [`consist.use_tracker`](api_helpers.md#consist.api.use_tracker)
- [`ScenarioContext`](workflow.md#consist.core.workflow.ScenarioContext) (returned by `consist.scenario(...)`)
  - `run_id`, `config`, `inputs`, `add_input`, `declare_outputs`, `require_outputs`, `collect_by_keys`, `run`, `trace`
- [`RunContext`](workflow.md#consist.core.workflow.RunContext) (injected via `inject_context=True`)
  - `run_dir`, `inputs`, `load`, `log_artifact`, `log_artifacts`, `log_input`, `log_output`, `log_meta`, `capture_outputs`
- [`Coupler`](workflow.md#consist.core.coupler.Coupler) (available at `scenario.coupler`)

Scenario defaults like `name_template` and `cache_epoch` are configured via `consist.scenario(...)` and flow into `ScenarioContext.run(...)`.

### Utilities and Introspection

- [`collect_step_schema`](../schema-export.md#introspection) (Extract outputs for Coupler schemas)
- [`ArtifactKeyRegistry`](../usage-guide.md#artifact-key-registries) (Manage consistent artifact keys)

### Artifact logging and loading

- [`consist.log_artifact`](api_helpers.md#consist.api.log_artifact)
- [`consist.log_dataframe`](api_helpers.md#consist.api.log_dataframe)
- [`consist.load`](api_helpers.md#consist.api.load)
- [`consist.capture_outputs`](api_helpers.md#consist.api.capture_outputs)
- [`consist.get_artifact`](api_helpers.md#consist.api.get_artifact)
- [`consist.register_artifact_facet_parser`](api_helpers.md#consist.api.register_artifact_facet_parser)
- [`consist.cached_output`](api_helpers.md#consist.api.cached_output)
- [`consist.cached_artifacts`](api_helpers.md#consist.api.cached_artifacts)
- [`consist.log_meta`](api_helpers.md#consist.api.log_meta)
- [`consist.current_run`](api_helpers.md#consist.api.current_run)
- [`consist.current_consist`](api_helpers.md#consist.api.current_consist)
- [`Tracker.define_step(...)`](tracker.md#consist.core.tracker.Tracker.define_step) (explicit tracker form)
- [`Tracker.materialize(...)`](tracker.md#consist.core.tracker.Tracker.materialize)

### Querying and views

- [`consist.view`](api_helpers.md#consist.api.view)
- [`consist.register_views`](api_helpers.md#consist.api.register_views)
- [`consist.find_run`](api_helpers.md#consist.api.find_run)
- [`consist.find_runs`](api_helpers.md#consist.api.find_runs)
- [`Tracker.find_latest_run(...)`](tracker.md#consist.core.tracker.Tracker.find_latest_run)
- [`Tracker.diff_runs(...)`](tracker.md#consist.core.tracker.Tracker.diff_runs)
- [`Tracker.get_run_inputs(...)`](tracker.md#consist.core.tracker.Tracker.get_run_inputs) / [`Tracker.get_run_outputs(...)`](tracker.md#consist.core.tracker.Tracker.get_run_outputs)
- [`Tracker.get_run_config(...)`](tracker.md#consist.core.tracker.Tracker.get_run_config)
- [`Tracker.print_lineage(...)`](tracker.md#consist.core.tracker.Tracker.print_lineage)
- [`consist.run_query`](api_helpers.md#consist.api.run_query)
- [`consist.db_session`](api_helpers.md#consist.api.db_session)
- [`consist.pivot_facets`](api_helpers.md#consist.api.pivot_facets)
- Indexing helpers: [`consist.index_by_field`](indexing.md#consist.core.indexing.index_by_field), [`consist.index_by_facet`](indexing.md#consist.core.indexing.index_by_facet), plus [`RunFieldIndex`](indexing.md#consist.core.indexing.RunFieldIndex) / [`FacetIndex`](indexing.md#consist.core.indexing.FacetIndex)
- Views registry: `tracker.views` ([`ViewRegistry`](views.md#consist.core.views.ViewRegistry))
- Matrix utilities: [`Tracker.load_matrix(...)`](tracker.md#consist.core.tracker.Tracker.load_matrix), [`MatrixViewFactory`](matrix.md#consist.core.matrix.MatrixViewFactory)
- Schema export: [`Tracker.export_schema_sqlmodel(...)`](tracker.md#consist.core.tracker.Tracker.export_schema_sqlmodel)

### Tracker methods (complete public surface)

This section enumerates all non-underscore `Tracker` methods. If you're new to Consist,
start with the **Core** and **Logging/Loading** groups and reach for **Advanced** only
as needed.

#### Core lifecycle

- `begin_run`, `start_run`, `run`, `trace`, `scenario`, `end_run`
- `define_step`, `last_run`, `is_cached`, `cached_artifacts`, `cached_output`
- `suspend_cache_options`, `restore_cache_options`, `capture_outputs`, `log_meta`

#### Logging and loading

- `log_artifact`, `log_artifacts`, `log_input`, `log_output`, `log_dataframe`
- `load`, `materialize`, `ingest`

#### Querying and history

- `find_runs`, `find_run`, `find_latest_run`, `get_latest_run_id`
- `find_artifacts`, `get_artifact`, `get_artifacts_for_run`
- `find_artifacts_by_params`, `get_artifact_kv`, `register_artifact_facet_parser`
- `get_run`, `get_run_config`, `get_run_inputs`, `get_run_outputs`
- `get_artifact_lineage`, `print_lineage`, `history`
- `diff_runs`, `get_config_facet`, `get_config_facets`, `get_run_config_kv`
- `get_config_values`, `get_config_value`, `find_runs_by_facet_kv`

#### Views and matrices

- `view`, `create_view`, `load_matrix`, `export_schema_sqlmodel`
- `netcdf_metadata`, `openmatrix_metadata`, `spatial_metadata`

#### Config canonicalization

- `canonicalize_config`, `prepare_config`, `apply_config_plan`, `identity_from_config_plan`

#### Format-specific logging

- `log_h5_container`, `log_h5_table`, `log_netcdf_file`, `log_openmatrix_file`

### Advanced (power-user / lower-level)

These methods are still public, but are more low-level or easier to misuse.

- `engine`, `set_run_subdir_fn`, `run_artifact_dir`, `resolve_uri`
- `run_query`, `get_run_record`, `resolve_historical_path`, `load_input_bundle`
- `get_artifact_by_uri`, `get_run_artifact`, `load_run_output`, `find_matching_run`
- `on_run_start`, `on_run_complete`, `on_run_failed`

## Stable, but optional extras

These APIs are part of the public surface, but require extra dependencies.

- Ingestion helpers: [`consist.ingest`](api_helpers.md#consist.api.ingest) (install with `consist[ingest]`)
- [`consist.integrations.containers`](../integrations/containers.md) (container execution + caching; requires Docker or Singularity)
- [`consist.integrations.dlt_loader`](../integrations/dlt_loader.md) (low-level ingestion integration; requires `consist[ingest]`)
