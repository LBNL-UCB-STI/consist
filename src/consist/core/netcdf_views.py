"""
Consist NetCDF Metadata View Module

This module provides the `NetCdfMetadataView` class, which allows querying
NetCDF metadata stored in the DuckDB catalog. It enables exploration of
variable structures, dimensions, and attributes across multiple runs.

Key functionalities include:
-   **Variable Discovery**: List all variables in NetCDF artifacts across runs
-   **Dimension Exploration**: Query dimension sizes and coordinate information
-   **Attribute Access**: Retrieve metadata attributes for variables
-   **Cross-Run Analysis**: Compare variable structures across different runs
"""

from __future__ import annotations

from collections.abc import Iterable
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pandas as pd

from consist.core.views import (
    _SAFE_IDENTIFIER_RE as _SAFE_IDENTIFIER_RE,
    _qualified_table,
)

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


class NetCdfMetadataView:
    """
    A view factory for querying NetCDF metadata from Consist's DuckDB catalog.
    """

    def __init__(self, tracker: Tracker) -> None:
        self.tracker = tracker

    def get_variables(
        self,
        concept_key: str,
        run_ids: Optional[List[str]] = None,
        year: Optional[int] = None,
        variable_type: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Returns a DataFrame of all variables in NetCDF artifacts.
        """
        ledger = self.tracker.ledger
        if ledger is None:
            raise RuntimeError("Database connection required.")
        base_table = _qualified_table("global_tables", concept_key)
        dims_table = _qualified_table("global_tables", f"{concept_key}__dims")
        shape_table = _qualified_table("global_tables", f"{concept_key}__shape")

        query = f"""
            WITH dims AS (
                SELECT
                    _dlt_parent_id,
                    list(value ORDER BY _dlt_list_idx) AS dims
                FROM {dims_table}
                GROUP BY _dlt_parent_id
            ),
            shape AS (
                SELECT
                    _dlt_parent_id,
                    list(value ORDER BY _dlt_list_idx) AS shape
                FROM {shape_table}
                GROUP BY _dlt_parent_id
            )
            SELECT
                nc.variable_name,
                nc.variable_type,
                d.dims,
                s.shape,
                nc.dtype,
                nc.attributes,
                COALESCE(r.id, a.run_id) AS run_id,
                r.year,
                r.iteration
            FROM {base_table} nc
            LEFT JOIN dims d ON d._dlt_parent_id = nc._dlt_id
            LEFT JOIN shape s ON s._dlt_parent_id = nc._dlt_id
            JOIN main.artifact a ON TRY_CAST(nc.consist_artifact_id AS UUID) = a.id
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

        if variable_type:
            query += " AND nc.variable_type = ?"
            params.append(variable_type)

        query += " ORDER BY r.year, r.iteration, nc.variable_name"

        try:
            return ledger.query_df(query, params)
        except Exception as e:
            logging.warning(
                f"[Consist] Failed to query NetCDF metadata for {concept_key}: {e}"
            )
            return pd.DataFrame()

    def get_dimensions(
        self,
        concept_key: str,
        run_id: Optional[str] = None,
    ) -> Dict[str, int]:
        """
        Returns a dictionary of unique dimension names and their maximum sizes.
        """
        vars_df = self.get_variables(concept_key, run_ids=[run_id] if run_id else None)

        if vars_df.empty:
            return {}

        dimensions: Dict[str, int] = {}

        for _, row in vars_df.iterrows():
            try:
                dims_value = row["dims"]
                shape_value = row["shape"]

                if isinstance(dims_value, list) and isinstance(shape_value, list):
                    dims_list = dims_value
                    shape_list = shape_value
                elif (
                    isinstance(dims_value, Iterable)
                    and not isinstance(dims_value, (str, bytes))
                    and isinstance(shape_value, Iterable)
                    and not isinstance(shape_value, (str, bytes))
                ):
                    dims_list = list(dims_value)
                    shape_list = list(shape_value)
                else:
                    import json

                    dims_list = json.loads(str(dims_value).replace("'", '"'))
                    shape_list = json.loads(str(shape_value).replace("'", '"'))

                for dim_name, dim_size in zip(dims_list, shape_list):
                    if dim_name not in dimensions:
                        dimensions[str(dim_name)] = int(dim_size)
                    else:
                        dimensions[str(dim_name)] = max(
                            dimensions[str(dim_name)], int(dim_size)
                        )
            except Exception as e:
                logging.debug(f"[Consist] Failed to parse dims/shape: {e}")
                continue

        return dimensions

    def get_data_variables(
        self,
        concept_key: str,
        run_ids: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Returns only data variables (not coordinates).
        """
        return self.get_variables(concept_key, run_ids=run_ids, variable_type="data")

    def get_coordinates(
        self,
        concept_key: str,
        run_ids: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Returns only coordinate variables.
        """
        return self.get_variables(
            concept_key, run_ids=run_ids, variable_type="coordinate"
        )

    def get_attributes(
        self,
        concept_key: str,
        variable_name: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> Optional[Dict]:
        """
        Returns attributes for a specific variable or all variables in an artifact.
        """
        vars_df = self.get_variables(concept_key, run_ids=[run_id] if run_id else None)

        if vars_df.empty:
            return None

        if variable_name:
            var_row = vars_df[vars_df["variable_name"] == variable_name]
            if var_row.empty:
                return None
            var_row = var_row.iloc[0]
        else:
            var_row = vars_df.iloc[0]

        try:
            import json

            attrs_str = var_row["attributes"]
            if isinstance(attrs_str, str):
                return json.loads(attrs_str.replace("'", '"'))
            return attrs_str
        except Exception as e:
            logging.debug(f"[Consist] Failed to parse attributes: {e}")
            return None

    def summary(self, concept_key: str, run_id: Optional[str] = None) -> str:
        """
        Returns a human-readable summary of the NetCDF structure.
        """
        vars_df = self.get_variables(concept_key, run_ids=[run_id] if run_id else None)

        if vars_df.empty:
            return f"NetCDF: {concept_key} (No data found)"

        lines = [f"NetCDF: {concept_key}"]

        data_vars = vars_df[vars_df["variable_type"] == "data"]
        coords = vars_df[vars_df["variable_type"] == "coordinate"]

        if not data_vars.empty:
            lines.append(f"\nData Variables ({len(data_vars)}):")
            for _, row in data_vars.iterrows():
                dims_value = row["dims"]
                if isinstance(dims_value, (list, tuple)):
                    dims_str = ", ".join(map(str, dims_value))
                else:
                    dims_str = str(dims_value).strip("[]").replace("'", "")
                lines.append(f"  - {row['variable_name']} [{dims_str}]: {row['dtype']}")

        if not coords.empty:
            lines.append(f"\nCoordinates ({len(coords)}):")
            for _, row in coords.iterrows():
                dims_value = row["dims"]
                if isinstance(dims_value, (list, tuple)):
                    dims_str = ", ".join(map(str, dims_value))
                else:
                    dims_str = str(dims_value).strip("[]").replace("'", "")

                shape_value = row["shape"]
                if isinstance(shape_value, (list, tuple)):
                    shape_str = ", ".join(map(str, shape_value))
                else:
                    shape_str = str(shape_value).strip("[]")
                lines.append(
                    f"  - {row['variable_name']} [{dims_str}]: {row['dtype']} ({shape_str})"
                )

        dims = self.get_dimensions(concept_key, run_id=run_id)
        if dims:
            lines.append("\nDimensions:")
            for dim_name, dim_size in sorted(dims.items()):
                lines.append(f"  - {dim_name}: {dim_size}")

        return "\n".join(lines)
