# Public API (v0.1)

This page defines what Consist considers its **public API** for the `0.1.x` series. Anything not listed here may change without notice.

## Stable (intended for external users)

- [`consist.Tracker`](tracker.md)
- [`consist.Run`](run.md)
- [`consist.Artifact`](artifact.md)

### Scenario / workflow helpers

- [`consist.scenario`](api_helpers.md#consist.api.scenario)
- [`consist.single_step_scenario`](api_helpers.md#consist.api.single_step_scenario)
- [`Tracker.define_step(...)`](tracker.md#consist.core.tracker.Tracker.define_step)
- [`ScenarioContext`](workflow.md#consist.core.workflow.ScenarioContext) (returned by `tracker.scenario(...)`) with `.run(...)` and `.trace(...)`
- [`RunContext`](workflow.md#consist.core.workflow.RunContext) (injected via `inject_context=True`)
- [`Coupler`](workflow.md#consist.core.coupler.Coupler) (available at `scenario.coupler`)

### Artifact logging and loading

- [`consist.log_artifact`](api_helpers.md#consist.api.log_artifact)
- [`consist.log_dataframe`](api_helpers.md#consist.api.log_dataframe)
- [`consist.load`](api_helpers.md#consist.api.load)
- [`consist.capture_outputs`](api_helpers.md#consist.api.capture_outputs)
- [`consist.get_artifact`](api_helpers.md#consist.api.get_artifact)
- [`consist.cached_output`](api_helpers.md#consist.api.cached_output)
- [`consist.cached_artifacts`](api_helpers.md#consist.api.cached_artifacts)
- [`consist.log_meta`](api_helpers.md#consist.api.log_meta)
- [`Tracker.define_step(...)`](tracker.md#consist.core.tracker.Tracker.define_step)
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

## Stable, but optional extras

These APIs are part of the public surface, but require extra dependencies.

- Ingestion helpers: [`consist.ingest`](api_helpers.md#consist.api.ingest) (install with `consist[ingest]`)

## Experimental (may change without notice)

These modules are useful, but are not part of the stable `0.1.x` contract yet:

- [`consist.integrations.containers`](../integrations/containers.md) (container execution + caching)
- [`consist.integrations.dlt_loader`](../integrations/dlt_loader.md) (low-level ingestion integration)
- `consist.web` (FastAPI service)
