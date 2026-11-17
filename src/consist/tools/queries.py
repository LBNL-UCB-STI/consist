"""
Reusable database queries for inspecting Consist provenance data.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional

import duckdb
import pandas as pd

import consist
from consist.core.persistence import DatabaseManager
from consist.models.run import Run

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


def _resolve_db(db_or_tracker: DatabaseManager | Tracker) -> DatabaseManager:
    if isinstance(db_or_tracker, DatabaseManager):
        return db_or_tracker
    if db_or_tracker.db is None:
        raise RuntimeError("Tracker does not have an initialized database.")
    return db_or_tracker.db


def _normalize_dt(val: object) -> Optional[datetime]:
    if isinstance(val, datetime):
        if val.tzinfo is not None:
            return val.astimezone(timezone.utc).replace(tzinfo=None)
        return val
    return None


def get_runs(
    db_or_tracker: DatabaseManager | Tracker,
    limit: int = 10,
    model_name: Optional[str] = None,
    tags: Optional[List[str]] = None,
    status: Optional[str] = None,
    run_ids: Optional[Iterable[str]] = None,
) -> List[Run]:
    """Fetches a list of runs with optional filters."""
    db = _resolve_db(db_or_tracker)
    return db.find_runs(
        tags=tags,
        model=model_name,
        status=status,
        limit=limit,
        run_ids=run_ids,
    )


def get_summary(db_or_tracker: DatabaseManager | Tracker) -> Dict[str, object]:
    """Returns a high-level summary of the database."""
    db = _resolve_db(db_or_tracker)
    counts_row = db.fetchone(
        "SELECT "
        "COUNT(*), "
        "COUNT(*) FILTER (WHERE status = 'completed'), "
        "COUNT(*) FILTER (WHERE status = 'failed'), "
        "MIN(created_at), "
        "MAX(created_at) "
        "FROM run"
    )
    total_runs = int(counts_row[0]) if counts_row else 0
    completed_runs = int(counts_row[1]) if counts_row else 0
    failed_runs = int(counts_row[2]) if counts_row else 0
    first_run_date = _normalize_dt(counts_row[3]) if counts_row else None
    last_run_date = _normalize_dt(counts_row[4]) if counts_row else None

    art_row = db.fetchone("SELECT COUNT(*) FROM artifact")
    total_artifacts = int(art_row[0]) if art_row else 0

    models_dist = [
        (str(row[0]), int(row[1]))
        for row in db.fetchall(
            "SELECT model_name, COUNT(*) FROM run GROUP BY model_name"
        )
    ]

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
            return consist.to_df(data, close=True)
    except FileNotFoundError:
        raise
    except Exception:
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
) -> List[Dict[str, object]]:
    """
    Query artifacts by indexed facet predicates and optional prefix filters.
    """
    artifacts = tracker.find_artifacts_by_params(
        params=list(params or []),
        namespace=namespace,
        key_prefix=key_prefix,
        artifact_family_prefix=artifact_family_prefix,
        limit=limit,
    )

    rows: List[Dict[str, object]] = []
    for artifact in artifacts:
        meta = dict(artifact.meta) if artifact.meta else {}
        rows.append(
            {
                "artifact": artifact,
                "id": artifact.id,
                "key": artifact.key,
                "uri": artifact.uri,
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
