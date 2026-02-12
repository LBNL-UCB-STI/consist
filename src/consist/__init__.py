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
from consist.core.coupler import Coupler
from consist.core.indexing import (
    FacetIndex,
    RunFieldIndex,
    index_by_facet,
    index_by_field,
)

# API
from consist.api import (
    load,
    load_df,
    load_relation,
    active_relation_count,
    RelationConnectionLeakWarning,
    to_df,
    run,
    trace,
    start_run,
    log_artifact,
    log_input,
    log_output,
    log_artifacts,
    log_dataframe,
    define_step,
    require_runtime_kwargs,
    ingest,
    capture_outputs,
    use_tracker,
    set_current_tracker,
    current_tracker,
    current_run,
    current_consist,
    output_dir,
    output_path,
    log_meta,
    view,
    cached_output,
    cached_artifacts,
    get_artifact,
    register_artifact_facet_parser,
    scenario,
    noop_scenario,
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
    is_spatial_artifact,
)

# Types
from consist.types import (
    CacheOptions,
    DriverType,
    ExecutionOptions,
    OutputPolicyOptions,
)
from consist.core.noop import (
    NoopArtifact,
    NoopCoupler,
    NoopRunResult,
    NoopScenarioContext,
    NoopTracker,
)
from consist.runtime import create_tracker
from consist.protocols import ArtifactLike, RunResultLike, ScenarioLike, TrackerLike

__all__ = [
    # Core objects
    "Tracker",
    "Coupler",
    "Run",
    "RunResult",
    "Artifact",
    # Types
    "DriverType",
    "CacheOptions",
    "OutputPolicyOptions",
    "ExecutionOptions",
    "NoopArtifact",
    "NoopCoupler",
    "NoopRunResult",
    "NoopScenarioContext",
    "NoopTracker",
    "create_tracker",
    "ArtifactLike",
    "RunResultLike",
    "ScenarioLike",
    "TrackerLike",
    # Indexing helpers
    "FacetIndex",
    "RunFieldIndex",
    "index_by_facet",
    "index_by_field",
    # Functional helpers
    "load",
    "load_df",
    "load_relation",
    "active_relation_count",
    "RelationConnectionLeakWarning",
    "to_df",
    "run",
    "trace",
    "start_run",
    "log_artifact",
    "log_input",
    "log_output",
    "log_artifacts",
    "log_dataframe",
    "define_step",
    "require_runtime_kwargs",
    "ingest",
    "capture_outputs",
    "use_tracker",
    "set_current_tracker",
    "current_tracker",
    "current_run",
    "current_consist",
    "output_dir",
    "output_path",
    "log_meta",
    "view",
    "cached_output",
    "cached_artifacts",
    "get_artifact",
    "register_artifact_facet_parser",
    "scenario",
    "noop_scenario",
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
    "is_spatial_artifact",
]
