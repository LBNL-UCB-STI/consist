"""
Consist Spatial Metadata View Module

Provides a lightweight view for querying spatial metadata ingested by Consist.
"""

from __future__ import annotations

import logging
from numbers import Integral
from typing import TYPE_CHECKING, Dict, List, Optional

import pandas as pd
from sqlalchemy import text

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


class SpatialMetadataView:
    """
    View factory for querying spatial metadata stored in the DuckDB catalog.

    Spatial metadata is ingested as a small catalog table per artifact key
    (e.g., bounds, CRS, geometry types, column names).
    """

    def __init__(self, tracker: Tracker) -> None:
        self.tracker = tracker

    def get_metadata(
        self,
        concept_key: str,
        run_ids: Optional[List[str]] = None,
        year: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Return spatial metadata rows for the given artifact key.

        Parameters
        ----------
        concept_key : str
            The semantic key identifying the spatial artifact.
        run_ids : Optional[List[str]], optional
            Optional list of run IDs to include.
        year : Optional[int], optional
            Optional year filter.
        """
        if not self.tracker.engine:
            raise RuntimeError("Database connection required.")

        query = f"""
            WITH bounds AS (
                SELECT
                    _dlt_parent_id,
                    list(value ORDER BY _dlt_list_idx) AS bounds
                FROM global_tables.{concept_key}__bounds
                GROUP BY _dlt_parent_id
            ),
            geometry_types AS (
                SELECT
                    _dlt_parent_id,
                    list(value ORDER BY _dlt_list_idx) AS geometry_types
                FROM global_tables.{concept_key}__geometry_types
                GROUP BY _dlt_parent_id
            ),
            column_names AS (
                SELECT
                    _dlt_parent_id,
                    list(value ORDER BY _dlt_list_idx) AS column_names
                FROM global_tables.{concept_key}__column_names
                GROUP BY _dlt_parent_id
            )
            SELECT
                b.bounds,
                sm.crs,
                sm.feature_count,
                gt.geometry_types,
                sm.geometry_column,
                cn.column_names,
                ANY_VALUE(COALESCE(r.id, a.run_id)) as run_id,
                r.year,
                r.iteration
            FROM global_tables.{concept_key} sm
            LEFT JOIN bounds b
                ON b._dlt_parent_id = sm._dlt_id
            LEFT JOIN geometry_types gt
                ON gt._dlt_parent_id = sm._dlt_id
            LEFT JOIN column_names cn
                ON cn._dlt_parent_id = sm._dlt_id
            JOIN artifact a ON sm.consist_artifact_id = CAST(a.id AS TEXT)
            LEFT JOIN run r ON a.run_id = r.id
            WHERE TRUE
        """

        params: Dict[str, str | int] = {}

        if run_ids:
            placeholders = ",".join([f":{i}" for i in range(len(run_ids))])
            query += f" AND r.id IN ({placeholders})"
            for i, rid in enumerate(run_ids):
                params[str(i)] = rid

        if year is not None:
            query += " AND r.year = :year"
            params["year"] = year

        query += """
            GROUP BY
                sm._dlt_id,
                b.bounds,
                sm.crs,
                sm.feature_count,
                gt.geometry_types,
                sm.geometry_column,
                cn.column_names,
                r.id,
                r.year,
                r.iteration
            ORDER BY r.year, r.iteration
        """

        try:
            return pd.read_sql(text(query), self.tracker.engine, params=params)
        except Exception as e:
            logging.warning(
                "[Consist] Failed to query spatial metadata for %s: %s",
                concept_key,
                e,
            )
            return pd.DataFrame()

    def get_bounds(
        self,
        concept_key: str,
        run_ids: Optional[List[str]] = None,
    ) -> Dict[str, object]:
        """
        Return bounds keyed by run ID.
        """
        df = self.get_metadata(concept_key, run_ids=run_ids)
        if df.empty:
            return {}
        results: Dict[str, object] = {}
        for row in df.itertuples():
            run_id = row.run_id
            if not isinstance(run_id, str) or not run_id:
                continue
            results[run_id] = row.bounds
        return results

    def get_feature_counts(
        self,
        concept_key: str,
        run_ids: Optional[List[str]] = None,
    ) -> Dict[str, int]:
        """
        Return feature counts keyed by run ID.
        """
        df = self.get_metadata(concept_key, run_ids=run_ids)
        if df.empty:
            return {}
        results: Dict[str, int] = {}
        for row in df.itertuples():
            run_id = row.run_id
            if not isinstance(run_id, str) or not run_id:
                continue
            count = row.feature_count
            if isinstance(count, Integral):
                results[run_id] = int(count)
                continue
            if isinstance(count, float):
                results[run_id] = int(count)
        return results

    def get_geometry_types(
        self,
        concept_key: str,
        run_ids: Optional[List[str]] = None,
    ) -> Dict[str, object]:
        """
        Return geometry types keyed by run ID.
        """
        df = self.get_metadata(concept_key, run_ids=run_ids)
        if df.empty:
            return {}
        results: Dict[str, object] = {}
        for row in df.itertuples():
            run_id = row.run_id
            if not isinstance(run_id, str) or not run_id:
                continue
            results[run_id] = row.geometry_types
        return results
