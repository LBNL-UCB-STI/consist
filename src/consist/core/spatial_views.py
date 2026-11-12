"""
Consist Spatial Metadata View Module

Provides a lightweight view for querying spatial metadata ingested by Consist.
"""

from __future__ import annotations

import logging
from numbers import Integral
from typing import TYPE_CHECKING, Dict, List, Optional

import duckdb
import pandas as pd

from consist.core.stores import get_hot_data_db
from consist.core.views import (
    _SAFE_IDENTIFIER_RE as _SAFE_IDENTIFIER_RE,
    _build_metadata_artifact_join,
    _dlt_child_list_cte,
    _quote_ident,
    _validate_identifier,
)

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


def _validate_concept_key(concept_key: str) -> None:
    _validate_identifier(concept_key, label="concept_key")


class SpatialMetadataView:
    """View factory for querying spatial metadata stored in the DuckDB catalog."""

    def __init__(self, tracker: Tracker) -> None:
        self.tracker = tracker

    def get_metadata(
        self,
        concept_key: str,
        run_ids: Optional[List[str]] = None,
        year: Optional[int] = None,
    ) -> pd.DataFrame:
        """Return spatial metadata rows for the given artifact key."""
        db = get_hot_data_db(self.tracker)
        if db is None:
            raise RuntimeError("Database connection required.")
        _validate_concept_key(concept_key)
        if not db.has_table(concept_key, table_schema="global_tables"):
            return pd.DataFrame()

        q_base = f"global_tables.{_quote_ident(concept_key)}"
        bounds_cte = _dlt_child_list_cte(
            db, f"{concept_key}__bounds", "bounds", "DOUBLE[]"
        )
        geom_cte = _dlt_child_list_cte(
            db, f"{concept_key}__geometry_types", "geometry_types", "VARCHAR[]"
        )
        cols_cte = _dlt_child_list_cte(
            db, f"{concept_key}__column_names", "column_names", "VARCHAR[]"
        )
        artifact_join, params = _build_metadata_artifact_join(db, concept_key, "sm")
        if artifact_join is None:
            return pd.DataFrame()

        query = f"""
            WITH bounds AS ({bounds_cte}),
            geometry_types AS ({geom_cte}),
            column_names AS ({cols_cte})
            SELECT
                COALESCE(b.bounds, CAST([] AS DOUBLE[])) AS bounds,
                sm.crs,
                sm.feature_count,
                COALESCE(gt.geometry_types, CAST([] AS VARCHAR[])) AS geometry_types,
                sm.geometry_column,
                COALESCE(cn.column_names, CAST([] AS VARCHAR[])) AS column_names,
                ANY_VALUE(COALESCE(r.id, a.run_id)) AS run_id,
                r.year,
                r.iteration
            FROM {q_base} sm
            LEFT JOIN bounds b
                ON b._dlt_parent_id = sm._dlt_id
            LEFT JOIN geometry_types gt
                ON gt._dlt_parent_id = sm._dlt_id
            LEFT JOIN column_names cn
                ON cn._dlt_parent_id = sm._dlt_id
            {artifact_join}
            LEFT JOIN main.run r ON a.run_id = r.id
            WHERE TRUE
        """

        if run_ids:
            placeholders = ", ".join("?" for _ in run_ids)
            query += f" AND COALESCE(r.id, a.run_id) IN ({placeholders})"
            params.extend(run_ids)

        if year is not None:
            query += " AND r.year = ?"
            params.append(int(year))

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
            return db.fetchdf(query, params)
        except (duckdb.Error, ValueError, RuntimeError) as e:
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
        """Return bounds keyed by run ID."""
        df = self.get_metadata(concept_key, run_ids=run_ids)
        if df.empty:
            return {}
        results: Dict[str, object] = {}
        for _, row in df.iterrows():
            run_id = row.get("run_id")
            if not isinstance(run_id, str) or not run_id:
                continue
            results[run_id] = row.get("bounds")
        return results

    def get_feature_counts(
        self,
        concept_key: str,
        run_ids: Optional[List[str]] = None,
    ) -> Dict[str, int]:
        """Return feature counts keyed by run ID."""
        df = self.get_metadata(concept_key, run_ids=run_ids)
        if df.empty:
            return {}
        results: Dict[str, int] = {}
        for _, row in df.iterrows():
            run_id = row.get("run_id")
            if not isinstance(run_id, str) or not run_id:
                continue
            count = row.get("feature_count")
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
        """Return geometry types keyed by run ID."""
        df = self.get_metadata(concept_key, run_ids=run_ids)
        if df.empty:
            return {}
        results: Dict[str, object] = {}
        for _, row in df.iterrows():
            run_id = row.get("run_id")
            if not isinstance(run_id, str) or not run_id:
                continue
            results[run_id] = row.get("geometry_types")
        return results
