"""
Consist OpenMatrix Metadata View Module

This module provides the `OpenMatrixMetadataView` class, which allows querying
OpenMatrix metadata stored in the DuckDB catalog.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pandas as pd

from consist.core.stores import get_hot_data_db
from consist.core.views import (
    _SAFE_IDENTIFIER_RE as _SAFE_IDENTIFIER_RE,
    _qualified_table,
    _validate_identifier,
)

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


class OpenMatrixMetadataView:
    """View factory for querying OpenMatrix metadata from Consist's DuckDB catalog."""

    def __init__(self, tracker: Tracker) -> None:
        self.tracker = tracker

    def get_matrices(
        self,
        concept_key: str,
        run_ids: Optional[List[str]] = None,
        year: Optional[int] = None,
        min_rows: Optional[int] = None,
        max_rows: Optional[int] = None,
    ) -> pd.DataFrame:
        """Returns a DataFrame of all matrices in OpenMatrix artifacts."""
        db = get_hot_data_db(self.tracker)
        if db is None:
            raise RuntimeError("Database connection required.")
        _validate_identifier(concept_key, label="concept_key")
        if not db.has_table(concept_key, table_schema="global_tables"):
            return pd.DataFrame()

        base_table = _qualified_table("global_tables", concept_key)
        shape_name = f"{concept_key}__shape"
        if db.has_table(shape_name, table_schema="global_tables"):
            shape_table = _qualified_table("global_tables", shape_name)
            shape_cte = f"""
                SELECT
                    _dlt_parent_id,
                    list(value ORDER BY _dlt_list_idx) AS shape
                FROM {shape_table}
                GROUP BY _dlt_parent_id
            """
        else:
            shape_cte = (
                "SELECT CAST(NULL AS VARCHAR) AS _dlt_parent_id, "
                "CAST([] AS BIGINT[]) AS shape WHERE FALSE"
            )

        has_art_col = db.has_table_column(
            table_name=concept_key,
            column_name="consist_artifact_id",
            table_schema="global_tables",
        )
        has_run_col = db.has_table_column(
            table_name=concept_key,
            column_name="consist_run_id",
            table_schema="global_tables",
        )
        params: list[Any] = []
        if has_art_col and has_run_col:
            artifact_join = (
                "JOIN main.artifact a ON ("
                "TRY_CAST(omx.consist_artifact_id AS UUID) = a.id "
                "OR (omx.consist_artifact_id IS NULL AND a.key = ? "
                "AND CAST(omx.consist_run_id AS VARCHAR) = a.run_id)"
                ")"
            )
            params.append(concept_key)
        elif has_art_col:
            artifact_join = "JOIN main.artifact a ON TRY_CAST(omx.consist_artifact_id AS UUID) = a.id"
        elif has_run_col:
            artifact_join = (
                "JOIN main.artifact a ON (a.key = ? "
                "AND CAST(omx.consist_run_id AS VARCHAR) = a.run_id)"
            )
            params.append(concept_key)
        else:
            return pd.DataFrame()

        query = f"""
            WITH shape AS ({shape_cte})
            SELECT
                omx.matrix_name,
                COALESCE(s.shape, CAST([] AS BIGINT[])) AS shape,
                omx.dtype,
                omx.n_rows,
                omx.n_cols,
                omx.attributes,
                COALESCE(r.id, a.run_id) AS run_id,
                r.year,
                r.iteration
            FROM {base_table} omx
            LEFT JOIN shape s ON s._dlt_parent_id = omx._dlt_id
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

        if min_rows is not None:
            query += " AND omx.n_rows >= ?"
            params.append(int(min_rows))

        if max_rows is not None:
            query += " AND omx.n_rows <= ?"
            params.append(int(max_rows))

        query += " ORDER BY r.year, r.iteration, omx.matrix_name"

        try:
            return db.fetchdf(query, params)
        except Exception as e:
            logging.warning(
                f"[Consist] Failed to query OpenMatrix metadata for {concept_key}: {e}"
            )
            return pd.DataFrame()

    def get_zone_counts(
        self,
        concept_key: str,
    ) -> Dict[str, int]:
        """Returns zone counts (n_rows) for the first matrix in each run."""
        matrices_df = self.get_matrices(concept_key)
        if matrices_df.empty:
            return {}

        zone_counts: Dict[str, int] = {}
        for run_id in matrices_df["run_id"].dropna().unique():
            run_matrices = matrices_df[matrices_df["run_id"] == run_id]
            if not run_matrices.empty:
                zone_counts[str(run_id)] = int(run_matrices.iloc[0]["n_rows"])
        return zone_counts

    def get_matrix_names(
        self,
        concept_key: str,
        run_id: Optional[str] = None,
    ) -> List[str]:
        """Returns a list of unique matrix names in the artifact(s)."""
        matrices_df = self.get_matrices(
            concept_key, run_ids=[run_id] if run_id else None
        )
        if matrices_df.empty:
            return []
        return sorted(matrices_df["matrix_name"].unique().tolist())

    def get_attributes(
        self,
        concept_key: str,
        matrix_name: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Returns attributes for a specific matrix or the first matrix in an artifact."""
        matrices_df = self.get_matrices(
            concept_key, run_ids=[run_id] if run_id else None
        )
        if matrices_df.empty:
            return None

        if matrix_name:
            matrix_row = matrices_df[matrices_df["matrix_name"] == matrix_name]
            if matrix_row.empty:
                return None
            matrix_row = matrix_row.iloc[0]
        else:
            matrix_row = matrices_df.iloc[0]

        try:
            attrs_str = matrix_row["attributes"]
            if isinstance(attrs_str, str):
                return json.loads(attrs_str.replace("'", '"'))
            return attrs_str
        except Exception as e:
            logging.debug(f"[Consist] Failed to parse attributes: {e}")
            return None

    def compare_runs(
        self,
        concept_key: str,
        run_ids: List[str],
    ) -> pd.DataFrame:
        """Returns a pivot-style comparison of matrices across runs."""
        matrices_df = self.get_matrices(concept_key, run_ids=run_ids)
        if matrices_df.empty:
            return pd.DataFrame()

        pivot_data = []
        for matrix_name in sorted(matrices_df["matrix_name"].unique()):
            row: Dict[str, Any] = {"matrix_name": matrix_name}
            for rid in run_ids:
                matrix_row = matrices_df[
                    (matrices_df["matrix_name"] == matrix_name)
                    & (matrices_df["run_id"] == rid)
                ]
                if not matrix_row.empty:
                    mr = matrix_row.iloc[0]
                    row[rid] = f"{mr['n_rows']}x{mr['n_cols']}"
                else:
                    row[rid] = "N/A"
            pivot_data.append(row)

        return pd.DataFrame(pivot_data)

    def summary(self, concept_key: str, run_id: Optional[str] = None) -> str:
        """Returns a human-readable summary of the OpenMatrix structure."""
        matrices_df = self.get_matrices(
            concept_key, run_ids=[run_id] if run_id else None
        )
        if matrices_df.empty:
            return f"OpenMatrix: {concept_key} (No data found)"

        lines = [f"OpenMatrix: {concept_key}"]
        matrix_names = sorted(matrices_df["matrix_name"].unique())
        lines.append(f"\nMatrices ({len(matrix_names)}):")
        for mname in matrix_names:
            mrow = matrices_df[matrices_df["matrix_name"] == mname].iloc[0]
            lines.append(
                f"  - {mname} [{mrow['n_rows']}x{mrow['n_cols']}]: {mrow['dtype']}"
            )

        zone_counts = self.get_zone_counts(concept_key)
        if zone_counts:
            zone_summary = ", ".join(
                [f"{rid}: {z}" for rid, z in sorted(zone_counts.items())]
            )
            lines.append(f"\nZones: {zone_summary}")

        return "\n".join(lines)
