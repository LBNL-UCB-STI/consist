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
from typing import TYPE_CHECKING, Dict, List, Optional

import pandas as pd
from sqlalchemy import text

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


class OpenMatrixMetadataView:
    """
    A view factory for querying OpenMatrix metadata from Consist's DuckDB catalog.

    This class allows users to explore the structure and attributes of OpenMatrix files
    that have been logged and ingested as artifacts. It queries the `openmatrix_catalog`
    table to retrieve information about matrices, their dimensions, zone counts, and
    attributes across different runs.

    Attributes
    ----------
    tracker : Tracker
        An instance of the Consist `Tracker`, providing access to the database
        engine for querying artifact and run metadata.

    Example
    -------
    ```python
    view = OpenMatrixMetadataView(tracker)

    # List all matrices in an OpenMatrix artifact
    matrices = view.get_matrices("demand", year=2024)
    print(matrices[["matrix_name", "n_rows", "n_cols", "run_id"]])

    # Get zone counts across runs
    zones = view.get_zone_counts("demand")
    print(zones)  # {"baseline": 5000, "scenario_a": 5100}

    # Find matrices by dimensions
    large = view.get_matrices("demand", min_rows=5000)
    print(large)
    ```
    """

    def __init__(self, tracker: Tracker) -> None:
        """
        Initializes the OpenMatrixMetadataView with a reference to the Consist Tracker.

        Parameters
        ----------
        tracker : Tracker
            An instance of the Consist `Tracker`, providing access to the database
            for querying OpenMatrix metadata.
        """
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

        This method queries the `openmatrix_catalog` table for all matrices
        associated with a given artifact key. Results can be filtered by
        run ID, year, and dimension ranges.

        Parameters
        ----------
        concept_key : str
            The semantic key (artifact key) identifying the OpenMatrix artifact.
        run_ids : Optional[List[str]], optional
            Optional list of run IDs to include. If None, all runs are included.
        year : Optional[int], optional
            Optional year to filter by. If None, all years are included.
        min_rows : Optional[int], optional
            Optional minimum number of rows (zones) to filter by.
        max_rows : Optional[int], optional
            Optional maximum number of rows (zones) to filter by.

        Returns
        -------
        pd.DataFrame
            A DataFrame with columns:
            - matrix_name: Name of the matrix
            - shape: Shape tuple as string (e.g., "[5000, 5000]")
            - dtype: Data type of the matrix
            - n_rows: Number of rows (first dimension)
            - n_cols: Number of columns (second dimension)
            - attributes: JSON attributes (as string)
            - run_id: ID of the run that logged the artifact
            - year: Year of the run
            - iteration: Iteration of the run

        Raises
        ------
        RuntimeError
            If the Tracker instance does not have a configured database connection.
        """
        if not self.tracker.engine:
            raise RuntimeError("Database connection required.")

        query = f"""
            WITH shape AS (
                SELECT
                    _dlt_parent_id,
                    list(value ORDER BY _dlt_list_idx) AS shape
                FROM global_tables.{concept_key}__shape
                GROUP BY _dlt_parent_id
            )
            SELECT
                omx.matrix_name,
                s.shape,
                omx.dtype,
                omx.n_rows,
                omx.n_cols,
                omx.attributes,
                r.id as run_id,
                r.year,
                r.iteration
            FROM global_tables.{concept_key} omx
            LEFT JOIN shape s ON s._dlt_parent_id = omx._dlt_id
            JOIN artifact a ON omx.consist_artifact_id = CAST(a.id AS TEXT)
            JOIN run r ON a.run_id = r.id
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

        if min_rows is not None:
            query += " AND omx.n_rows >= :min_rows"
            params["min_rows"] = min_rows

        if max_rows is not None:
            query += " AND omx.n_rows <= :max_rows"
            params["max_rows"] = max_rows

        query += " ORDER BY r.year, r.iteration, omx.matrix_name"

        try:
            return pd.read_sql(text(query), self.tracker.engine, params=params)
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

        This is useful for understanding matrix dimensions across different scenarios
        or model versions. For OMX files following the OMX convention, this typically
        represents the number of traffic analysis zones.

        Parameters
        ----------
        concept_key : str
            The semantic key identifying the OpenMatrix artifact.

        Returns
        -------
        Dict[str, int]
            A dictionary mapping run IDs to their zone counts.

        Example
        -------
        ```python
        zones = view.get_zone_counts("demand")
        print(zones)  # {"baseline": 5000, "scenario_a": 5100}
        ```
        """
        matrices_df = self.get_matrices(concept_key)

        if matrices_df.empty:
            return {}

        # Group by run_id and get the first zone count (assuming all matrices
        # in a run have the same zone count)
        zone_counts = {}
        for run_id in matrices_df["run_id"].unique():
            run_matrices = matrices_df[matrices_df["run_id"] == run_id]
            if not run_matrices.empty:
                zone_counts[run_id] = run_matrices.iloc[0]["n_rows"]

        return zone_counts

    def get_matrix_names(
        self,
        concept_key: str,
        run_id: Optional[str] = None,
    ) -> List[str]:
        """
        Returns a list of unique matrix names in the artifact(s).

        Parameters
        ----------
        concept_key : str
            The semantic key identifying the OpenMatrix artifact.
        run_id : Optional[str], optional
            Optional run ID to filter to. If None, returns all unique matrix names.

        Returns
        -------
        List[str]
            Unique matrix names found in the artifact(s).

        Example
        -------
        ```python
        names = view.get_matrix_names("demand")
        print(names)  # ["trips", "trucks", "walking"]
        ```
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

        Parameters
        ----------
        concept_key : str
            The semantic key identifying the OpenMatrix artifact.
        matrix_name : Optional[str], optional
            Optional matrix name to filter to. If None, returns attributes of first matrix.
        run_id : Optional[str], optional
            Optional run ID to filter to.

        Returns
        -------
        Optional[Dict]
            A dictionary of attributes, or None if matrix not found.

        Example
        -------
        ```python
        attrs = view.get_attributes("demand", matrix_name="trips")
        print(attrs)  # {"units": "vehicles", "period": "am_peak"}
        ```
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

        This is useful for comparing matrix counts, dimensions, and zone counts
        across different scenarios or model versions.

        Parameters
        ----------
        concept_key : str
            The semantic key identifying the OpenMatrix artifact.
        run_ids : List[str]
            List of run IDs to compare.

        Returns
        -------
        pd.DataFrame
            A DataFrame with rows for each matrix, columns for each run showing
            dimensions and zone counts.

        Example
        -------
        ```python
        compare = view.compare_runs("demand", ["baseline", "scenario_a", "scenario_b"])
        print(compare)
        #     matrix_name  baseline  scenario_a  scenario_b
        # 0        trips    5000x5000  5100x5100  5100x5100
        # 1       trucks    5000x5000  5100x5100  5100x5100
        ```
        """
        matrices_df = self.get_matrices(concept_key, run_ids=run_ids)

        if matrices_df.empty:
            return pd.DataFrame()

        # Create pivot table with matrix names as rows, run_ids as columns
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

        Parameters
        ----------
        concept_key : str
            The semantic key identifying the OpenMatrix artifact.
        run_id : Optional[str], optional
            Optional run ID to summarize.

        Returns
        -------
        str
            A formatted summary of the OpenMatrix structure.

        Example
        -------
        ```python
        print(view.summary("demand"))
        # Output:
        # OpenMatrix: demand
        # Matrices (3):
        #   - trips [5000x5000]: float32
        #   - trucks [5000x5000]: float32
        #   - walking [5000x5000]: float32
        # Zones: 5000
        # Runs: 1 (baseline)
        ```
        """
        matrices_df = self.get_matrices(
            concept_key, run_ids=[run_id] if run_id else None
        )

        if matrices_df.empty:
            return f"OpenMatrix: {concept_key} (No data found)"

        lines = [f"OpenMatrix: {concept_key}"]

        # Matrices
        matrix_names = sorted(matrices_df["matrix_name"].unique())
        lines.append(f"\nMatrices ({len(matrix_names)}):")
        for mname in matrix_names:
            mrow = matrices_df[matrices_df["matrix_name"] == mname].iloc[0]
            lines.append(
                f"  - {mname} [{mrow['n_rows']}x{mrow['n_cols']}]: {mrow['dtype']}"
            )

        # Zones
        zone_counts = self.get_zone_counts(concept_key)
        if zone_counts:
            zone_summary = ", ".join(
                [f"{rid}: {z}" for rid, z in sorted(zone_counts.items())]
            )
            lines.append(f"\nZones: {zone_summary}")

        # Runs
        unique_runs = matrices_df["run_id"].unique()
        lines.append(f"\nRuns: {len(unique_runs)} ({', '.join(sorted(unique_runs))})")

        return "\n".join(lines)
