"""
Consist: Automatic provenance tracking and intelligent caching for scientific simulation workflows.

This package provides the main public API for Consist, allowing users to interact with
the Tracker for managing runs, logging artifacts, and ingesting data.
"""

# Models
from consist.models.run import Run
from consist.models.artifact import Artifact
from sqlmodel import SQLModel

# Core
from consist.core.tracker import Tracker

# API
from consist.api import (
    load,
    log_artifact,
    ingest,
    capture_outputs,
    current_tracker,
    log_meta,
    view,
)
