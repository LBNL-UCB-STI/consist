"""
Reusable database queries for inspecting Consist provenance data.
"""

from typing import Any, Dict, Iterable, List, Optional
from sqlmodel import Session, select, func, col
import duckdb
import pandas as pd

from consist.models.run import Run
from consist.models.artifact import Artifact
from consist.core.tracker import Tracker
import consist


def get_runs(
    session: Session,
    limit: int = 10,
    model_name: Optional[str] = None,
    tags: Optional[List[str]] = None,
    status: Optional[str] = None,
) -> List[Run]:
    """Fetches a list of runs with optional filters."""
    statement = select(Run).order_by(col(Run.created_at).desc())

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
    return list(session.exec(statement).all())


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

    first_run_stmt = select(Run.created_at).order_by(col(Run.created_at).asc()).limit(1)
    first_run_date = session.exec(first_run_stmt).first()

    last_run_stmt = select(Run.created_at).order_by(col(Run.created_at).desc()).limit(1)
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
        data = consist.load(
            artifact, tracker=tracker, db_fallback="always", nrows=limit
        )
        if isinstance(data, pd.DataFrame):
            return data.head(limit)
        if isinstance(data, duckdb.DuckDBPyRelation):
            return consist.to_df(data)
    except FileNotFoundError:
        # Propagate so CLI can give a precise message
        raise
    except Exception:
        # Could fail if file is missing, etc.
        return None

    return None


def find_artifacts_by_params(
    tracker: Tracker,
    *,
    params: Optional[Iterable[str]] = None,
    namespace: Optional[str] = None,
    key_prefix: Optional[str] = None,
    artifact_family_prefix: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """
    Query artifacts by indexed facet predicates and optional prefix filters.

    Parameters
    ----------
    tracker : Tracker
        Active tracker with a configured database.
    params : Optional[Iterable[str]], optional
        Predicate expressions of the form ``key=value``, ``key>=value``,
        or ``key<=value``. A dotted prefix can be used as namespace
        (e.g., ``beam.phys_sim_iteration=2``).
    namespace : Optional[str], optional
        Default namespace filter when predicates do not specify one.
    key_prefix : Optional[str], optional
        Prefix filter on ``artifact.key``.
    artifact_family_prefix : Optional[str], optional
        Prefix filter on indexed ``artifact_family`` facet values.
    limit : int, default 100
        Maximum number of results.

    Returns
    -------
    List[Dict[str, Any]]
        Result rows containing artifact metadata and persisted facet metadata.
    """
    artifacts = tracker.find_artifacts_by_params(
        params=list(params or []),
        namespace=namespace,
        key_prefix=key_prefix,
        artifact_family_prefix=artifact_family_prefix,
        limit=limit,
    )

    rows: List[Dict[str, Any]] = []
    for artifact in artifacts:
        meta = artifact.meta or {}
        rows.append(
            {
                "artifact": artifact,
                "id": artifact.id,
                "key": artifact.key,
                "container_uri": artifact.container_uri,
                "driver": artifact.driver,
                "run_id": artifact.run_id,
                "facet_id": meta.get("artifact_facet_id"),
                "facet_namespace": meta.get("artifact_facet_namespace"),
                "facet_schema": meta.get("artifact_facet_schema"),
                "facet_schema_version": meta.get("artifact_facet_schema_version"),
                "meta": meta,
            }
        )
    return rows
