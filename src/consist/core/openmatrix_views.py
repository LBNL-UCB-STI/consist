"""
Consist OpenMatrix Metadata View Module

This module provides the `OpenMatrixMetadataView` class, which allows querying
OpenMatrix metadata stored in the DuckDB catalog. It enables exploration of
matrix structures, zone information, and attributes across multiple runs.

Key functionalities include:
-   **Matrix Discovery**: List all matrices in OpenMatrix artifacts across runs
-   **Zone Exploration**: Query zone counts and matrix dimensions
-   **Attribute Access**: Retrieve metadata attributes for matrices
-   **Cross-Run Analysis**: Compare matrix structures across different runs
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pandas as pd

from consist.core.views import (
    _SAFE_IDENTIFIER_RE as _SAFE_IDENTIFIER_RE,
    _qualified_table,
)

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


class OpenMatrixMetadataView:
    """
    A view factory for querying OpenMatrix metadata from Consist's DuckDB catalog.
    """

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
        """
        Returns a DataFrame of all matrices in OpenMatrix artifacts.
        """
        ledger = self.tracker.ledger
        if ledger is None:
            raise RuntimeError("Database connection required.")
        base_table = _qualified_table("global_tables", concept_key)
        shape_table = _qualified_table("global_tables", f"{concept_key}__shape")

        query = f"""
            WITH shape AS (
                SELECT
                    _dlt_parent_id,
                    list(value ORDER BY _dlt_list_idx) AS shape
                FROM {shape_table}
                GROUP BY _dlt_parent_id
            )
            SELECT
                omx.matrix_name,
                s.shape,
                omx.dtype,
                omx.n_rows,
                omx.n_cols,
                omx.attributes,
                COALESCE(r.id, a.run_id) AS run_id,
                r.year,
                r.iteration
            FROM {base_table} omx
            LEFT JOIN shape s ON s._dlt_parent_id = omx._dlt_id
            JOIN main.artifact a ON TRY_CAST(omx.consist_artifact_id AS UUID) = a.id
            LEFT JOIN main.run r ON a.run_id = r.id
            WHERE TRUE
        """

        params: list[Any] = []

        if run_ids:
            query += " AND COALESCE(r.id, a.run_id) IN (SELECT UNNEST(?))"
            params.append(list(run_ids))

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
            return ledger.query_df(query, params)
        except Exception as e:
            logging.warning(
                f"[Consist] Failed to query OpenMatrix metadata for {concept_key}: {e}"
            )
            return pd.DataFrame()

    def get_zone_counts(
        self,
        concept_key: str,
    ) -> Dict[str, int]:
        """
        Returns zone counts (n_rows) for the first matrix in each run.
        """
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
        """
        Returns a list of unique matrix names in the artifact(s).
        """
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
    ) -> Optional[Dict]:
        """
        Returns attributes for a specific matrix or the first matrix in an artifact.
        """
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
            import json

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
        """
        Returns a pivot-style comparison of matrices across runs.
        """
        matrices_df = self.get_matrices(concept_key, run_ids=run_ids)

        if matrices_df.empty:
            return pd.DataFrame()

        pivot_data = []
        for matrix_name in sorted(matrices_df["matrix_name"].unique()):
            row = {"matrix_name": matrix_name}
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
        """
        Returns a human-readable summary of the OpenMatrix structure.
        """
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

        unique_runs = [str(r) for r in matrices_df["run_id"].dropna().unique()]
        lines.append(f"\nRuns: {len(unique_runs)} ({', '.join(sorted(unique_runs))})")

        return "\n".join(lines)
