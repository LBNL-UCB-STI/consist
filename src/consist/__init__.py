def hello() -> str:
    """
    Returns a greeting string.

    Returns:
        str: A string containing the greeting "Hello from consist!".
    """
    return "Hello from consist!"


# src/consist/__init__.py

from typing import Optional, Dict, Any, Type, Union, Iterable

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
    Logs an artifact to the currently active run.
    See consist.core.tracker.Tracker.log_artifact for details.
    """
    return get_active_tracker().log_artifact(
        path=path, key=key, direction=direction, schema=schema, **meta
    )


def ingest(
    artifact: Artifact,
    data: Iterable[Dict[str, Any]],
    schema: Optional[Type[SQLModel]] = None,
):
    """
    Ingests data into the active run's database.
    See consist.core.tracker.Tracker.ingest for details.
    """
    return get_active_tracker().ingest(artifact=artifact, data=data, schema=schema)
