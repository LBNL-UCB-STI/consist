"""
Consist: Automatic provenance tracking and intelligent caching for scientific simulation workflows.

This package provides the main public API for Consist, allowing users to interact with
the Tracker for managing runs, logging artifacts, and ingesting data.
"""

# Models
from consist.models.run import Run, RunResult
from consist.models.artifact import Artifact

# Core
from consist.core.tracker import Tracker
from consist.core.indexing import (
    FacetIndex,
    RunFieldIndex,
    index_by_facet,
    index_by_field,
)

# API
from consist.api import (
    load,
    run,
    trace,
    start_run,
    log_artifact,
    log_artifacts,
    log_dataframe,
    define_step,
    coupler_schema,
    ingest,
    capture_outputs,
    use_tracker,
    current_tracker,
    current_run,
    current_consist,
    log_meta,
    view,
    cached_output,
    cached_artifacts,
    get_artifact,
    scenario,
    single_step_scenario,
    register_views,
    find_run,
    find_runs,
    db_session,
    run_query,
    config_run_query,
    config_run_rows,
    pivot_facets,
    # Type guards for artifact narrowing
    is_dataframe_artifact,
    is_tabular_artifact,
    is_json_artifact,
    is_zarr_artifact,
    is_hdf_artifact,
)

# Types
from consist.types import (
    DriverType,
)

__all__ = [
    # Core objects
    "Tracker",
    "Run",
    "RunResult",
    "Artifact",
    # Types
    "DriverType",
    # Indexing helpers
    "FacetIndex",
    "RunFieldIndex",
    "index_by_facet",
    "index_by_field",
    # Functional helpers
    "load",
    "run",
    "trace",
    "start_run",
    "log_artifact",
    "log_artifacts",
    "log_dataframe",
    "define_step",
    "coupler_schema",
    "ingest",
    "capture_outputs",
    "use_tracker",
    "current_tracker",
    "current_run",
    "current_consist",
    "log_meta",
    "view",
    "cached_output",
    "cached_artifacts",
    "get_artifact",
    "scenario",
    "single_step_scenario",
    "register_views",
    "find_run",
    "find_runs",
    "db_session",
    "run_query",
    "config_run_query",
    "config_run_rows",
    "pivot_facets",
    # Type guards for artifact narrowing
    "is_dataframe_artifact",
    "is_tabular_artifact",
    "is_json_artifact",
    "is_zarr_artifact",
    "is_hdf_artifact",
]
