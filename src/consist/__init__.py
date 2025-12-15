"""
Consist: Automatic provenance tracking and intelligent caching for scientific simulation workflows.

This package provides the main public API for Consist, allowing users to interact with
the Tracker for managing runs, logging artifacts, and ingesting data.
"""

# Models
from consist.models.run import Run
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
    log_artifact,
    log_dataframe,
    ingest,
    capture_outputs,
    current_tracker,
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
)

__all__ = [
    # Core objects
    "Tracker",
    "Run",
    "Artifact",
    # Indexing helpers
    "FacetIndex",
    "RunFieldIndex",
    "index_by_facet",
    "index_by_field",
    # Functional helpers
    "load",
    "log_artifact",
    "log_dataframe",
    "ingest",
    "capture_outputs",
    "current_tracker",
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
]
