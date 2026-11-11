"""
Reusable database queries for inspecting Consist provenance data over `StoreLedger`.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, TYPE_CHECKING

import duckdb
import pandas as pd

import consist
from consist.core.persistence import DatabaseManager, _RUN_COLUMNS_SQL, _hydrate_run_row
from consist.core.run_ordering import recent_run_order_by_sql
from consist.models.run import Run
from consist.store.ledger import StoreLedger

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


def _resolve_ledger(
    source: "Tracker | DatabaseManager | StoreLedger",
) -> StoreLedger:
    if isinstance(source, StoreLedger):
        return source
    if isinstance(source, DatabaseManager):
        return source.ledger
    ledger = source.ledger
    if ledger is None:
        raise RuntimeError("Database connection required.")
    return ledger


def get_runs(
    source: "Tracker | DatabaseManager | StoreLedger",
    limit: int = 10,
    model_name: Optional[str] = None,
    tags: Optional[List[str]] = None,
    status: Optional[str] = None,
    run_ids: Optional[Iterable[str]] = None,
) -> List[Run]:
    """Fetch a list of runs with optional filters from `StoreLedger`."""
    ledger = _resolve_ledger(source)
    where_parts: list[str] = []
    params: list[Any] = []

    if run_ids is not None:
        id_list = list(run_ids)
        if not id_list:
            return []
        where_parts.append("r.id IN (SELECT UNNEST(?))")
        params.append(id_list)
    if model_name:
        where_parts.append("r.model_name = ?")
        params.append(model_name)
    if status:
        where_parts.append("r.status = ?")
        params.append(status)
    if tags:
        for tag in tags:
            where_parts.append(
                "json_contains(COALESCE(r.tags, '[]'::JSON), to_json(?))"
            )
            params.append(tag)

    sql = f"SELECT {_RUN_COLUMNS_SQL} FROM main.run r"
    if where_parts:
        sql += " WHERE " + " AND ".join(where_parts)
    sql += f" ORDER BY {recent_run_order_by_sql('r')} LIMIT ?"
    params.append(int(limit))

    with ledger.exec_cursor() as cur:
        rows = cur.execute(sql, params).fetchall()
    return [_hydrate_run_row(row) for row in rows]


def get_summary(
    source: "Tracker | DatabaseManager | StoreLedger",
) -> Dict[str, Any]:
    """Return a high-level summary of the Schema v2 provenance database."""
    ledger = _resolve_ledger(source)
    with ledger.exec_cursor() as cur:
        counts_row = cur.execute(
            "SELECT "
            "COUNT(*), "
            "COALESCE(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END), 0), "
            "COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0), "
            "MIN(created_at), "
            "MAX(created_at) "
            "FROM main.run"
        ).fetchone()
        art_row = cur.execute("SELECT COUNT(*) FROM main.artifact").fetchone()
        models_rows = cur.execute(
            "SELECT model_name, COUNT(*) FROM main.run "
            "GROUP BY model_name ORDER BY model_name ASC"
        ).fetchall()

    total_runs = int(counts_row[0]) if counts_row is not None else 0
    completed_runs = int(counts_row[1]) if counts_row is not None else 0
    failed_runs = int(counts_row[2]) if counts_row is not None else 0
    first_run_date = counts_row[3] if counts_row is not None else None
    last_run_date = counts_row[4] if counts_row is not None else None
    total_artifacts = int(art_row[0]) if art_row is not None else 0

    return {
        "total_runs": total_runs,
        "completed_runs": completed_runs,
        "failed_runs": failed_runs,
        "total_artifacts": total_artifacts,
        "models_distribution": [(str(m), int(c)) for m, c in models_rows],
        "first_run_at": first_run_date,
        "last_run_at": last_run_date,
    }


def get_artifact_preview(
    tracker: "Tracker", artifact_key: str, limit: int = 5
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
            return consist.to_df(data)
    except FileNotFoundError:
        raise
    except Exception:
        return None

    return None


def find_artifacts_by_params(
    tracker: "Tracker",
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
        primary_loc = artifact.primary_location
        uri = primary_loc.uri if primary_loc is not None else ""
        rows.append(
            {
                "artifact": artifact,
                "id": artifact.id,
                "key": artifact.key,
                "container_uri": uri,
                "driver": artifact.driver,
                "run_id": artifact.run_id,
                "facet_id": meta.get("artifact_facet_id") or meta.get("facet_id"),
                "facet_namespace": (
                    meta.get("facet_namespace") or meta.get("artifact_facet_namespace")
                ),
                "facet_schema": (
                    meta.get("facet_schema") or meta.get("artifact_facet_schema")
                ),
                "facet_schema_version": (
                    meta.get("facet_schema_version")
                    or meta.get("artifact_facet_schema_version")
                ),
                "meta": meta,
            }
        )
    return rows
