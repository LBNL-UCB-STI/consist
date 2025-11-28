"""
Consist: Automatic provenance tracking and intelligent caching for scientific simulation workflows.

This package provides the main public API for Consist, allowing users to interact with
the Tracker for managing runs, logging artifacts, and ingesting data.
"""

from consist.models.run import Run


# src/consist/__init__.py

from typing import Optional, Dict, Any, Type, Union, Iterable, TYPE_CHECKING

if TYPE_CHECKING:
    from consist.core.tracker import Tracker

from consist.core.context import get_active_tracker
from consist.models.artifact import Artifact

# Import SQLModel for convenience so users don't need to install it separately
from sqlmodel import SQLModel


# --- Proxy Functions ---
# These allow users to call consist.log_artifact() directly


def log_artifact(
    path: Union[str, Artifact],
    key: Optional[str] = None,
    direction: str = "output",
    schema: Optional[Type[SQLModel]] = None,
    **meta,
) -> Artifact:
    """
    Logs an artifact (file or data reference) to the currently active run.

    This function is a convenient proxy to `consist.core.tracker.Tracker.log_artifact`.
    It automatically links the artifact to the current run context, handles path
    virtualization, and performs lineage discovery.

    Args:
        path (Union[str, Artifact]): The file path (str) or an existing `Artifact` object.
        key (Optional[str]): A semantic name for the artifact. Required if `path` is a string.
        direction (str): "input" or "output". Defaults to "output".
        schema (Optional[Type[SQLModel]]): Optional SQLModel class for schema metadata.
        **meta: Additional metadata for the artifact.

    Returns:
        Artifact: The created or updated `Artifact` object.
    """
    return get_active_tracker().log_artifact(
        path=path, key=key, direction=direction, schema=schema, **meta
    )


def ingest(
    artifact: Artifact,
    data: Optional[Union[Iterable[Dict[str, Any]], Any]] = None,
    schema: Optional[Type[SQLModel]] = None,
    run: Optional[Run] = None,
):
    """
    Ingests data associated with an `Artifact` into the active run's database.

    This function is a convenient proxy to `consist.core.tracker.Tracker.ingest`.
    It materializes data into the DuckDB, leveraging `dlt` for efficient loading
    and injecting provenance information.

    Args:
        artifact (Artifact): The artifact object representing the data being ingested.
        data (Optional[Union[Iterable[Dict[str, Any]], Any]]): The data to ingest. Can be a
                                                              file path, DataFrame, or iterable of dicts.
        schema (Optional[Type[SQLModel]]): An optional SQLModel class defining the schema.
        run (Optional[Run]): The run context for ingestion. Defaults to the active run.
    """
    return get_active_tracker().ingest(
        artifact=artifact, data=data, schema=schema, run=run
    )
