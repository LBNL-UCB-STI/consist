# Public API (v0.1)

This page defines what Consist considers its **public API** for the `0.1.x` series. Anything not listed here may change without notice.

## Stable (intended for external users)

- `consist.Tracker`
- `consist.Run`
- `consist.Artifact`

### Scenario / workflow helpers

- `consist.scenario`
- `consist.single_step_scenario`
- `Tracker.task(...)`

### Artifact logging and loading

- `consist.log_artifact`
- `consist.log_dataframe`
- `consist.load`
- `consist.capture_outputs`
- `consist.get_artifact`
- `consist.cached_output`
- `consist.cached_artifacts`
- `consist.log_meta`

### Querying and views

- `consist.view`
- `consist.register_views`
- `consist.find_run`
- `consist.find_runs`
- `consist.run_query`
- `consist.db_session`
- Indexing helpers: `consist.index_by_field`, `consist.index_by_facet`, plus `RunFieldIndex` / `FacetIndex`

## Stable, but optional extras

These APIs are part of the public surface, but require extra dependencies.

- Ingestion helpers: `consist.ingest` (install with `consist[ingest]`)

## Experimental (may change without notice)

These modules are useful, but are not part of the stable `0.1.x` contract yet:

- `consist.integrations.containers` (container execution + caching)
- `consist.integrations.dlt_loader` (low-level ingestion integration)
- `consist.web` (FastAPI service)

