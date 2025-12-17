"""
Reusable database queries for inspecting Consist provenance data.
"""

from typing import List, Optional, Dict, Any
from sqlmodel import Session, select, func, col
import pandas as pd

from consist.models.run import Run
from consist.models.artifact import Artifact
from consist.core.tracker import Tracker


def get_runs(
    session: Session,
    limit: int = 10,
    model_name: Optional[str] = None,
    tags: Optional[List[str]] = None,
    status: Optional[str] = None,
) -> List[Run]:
    """Fetches a list of runs with optional filters."""
    statement = select(Run).order_by(Run.created_at.desc())

    if model_name:
        statement = statement.where(Run.model_name == model_name)
    if status:
        statement = statement.where(Run.status == status)
    if tags:
        # This is a simple "contains all" search for tags
        for tag in tags:
            # SQLModel doesn't have a clean way to query JSON arrays on all backends,
            # so we use a LIKE query as a good-enough solution for DuckDB.
            statement = statement.where(col(Run.tags).like(f'%"{tag}"%'))

    statement = statement.limit(limit)
    return session.exec(statement).all()


def get_summary(session: Session) -> Dict[str, Any]:
    """Returns a high-level summary of the database."""
    total_runs = session.exec(select(func.count(Run.id))).one()
    completed_runs = session.exec(
        select(func.count(Run.id)).where(Run.status == "completed")
    ).one()
    failed_runs = session.exec(
        select(func.count(Run.id)).where(Run.status == "failed")
    ).one()
    total_artifacts = session.exec(select(func.count(Artifact.id))).one()

    models_stmt = select(Run.model_name, func.count(Run.id)).group_by(Run.model_name)
    models_dist = session.exec(models_stmt).all()

    first_run_stmt = select(Run.created_at).order_by(Run.created_at.asc()).limit(1)
    first_run_date = session.exec(first_run_stmt).first()

    last_run_stmt = select(Run.created_at).order_by(Run.created_at.desc()).limit(1)
    last_run_date = session.exec(last_run_stmt).first()

    return {
        "total_runs": total_runs,
        "completed_runs": completed_runs,
        "failed_runs": failed_runs,
        "total_artifacts": total_artifacts,
        "models_distribution": models_dist,
        "first_run_at": first_run_date,
        "last_run_at": last_run_date,
    }


def get_artifact_preview(
    tracker: Tracker, artifact_key: str, limit: int = 5
) -> Optional[pd.DataFrame]:
    """
    Loads an artifact and returns the first few rows as a pandas DataFrame.
    Returns None if the artifact is not found or not a tabular format.
    """
    # Find the artifact by key or ID
    artifact = tracker.get_artifact(artifact_key)
    if not artifact:
        return None

    try:
        # Use the public API instead of assuming tracker.load exists
        import consist

        data = consist.load(artifact, tracker=tracker, db_fallback="always")
        if isinstance(data, pd.DataFrame):
            return data.head(limit)
    except FileNotFoundError:
        # Propagate so CLI can give a precise message
        raise
    except Exception:
        # Could fail if file is missing, etc.
        return None

    return None
