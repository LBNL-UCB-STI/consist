"""
Consist NetCDF Metadata View Module

This module provides the `NetCdfMetadataView` class, which allows querying
NetCDF metadata stored in the DuckDB catalog. It enables exploration of
variable structures, dimensions, and attributes across multiple runs.
"""

from __future__ import annotations

from collections.abc import Iterable
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


class NetCdfMetadataView:
    """View factory for querying NetCDF metadata from Consist's DuckDB catalog."""

    def __init__(self, tracker: Tracker) -> None:
        self.tracker = tracker

    def get_variables(
        self,
        concept_key: str,
        run_ids: Optional[List[str]] = None,
        year: Optional[int] = None,
        variable_type: Optional[str] = None,
    ) -> pd.DataFrame:
        """Returns a DataFrame of all variables in NetCDF artifacts."""
        db = get_hot_data_db(self.tracker)
        if db is None:
            raise RuntimeError("Database connection required.")
        _validate_identifier(concept_key, label="concept_key")
        if not db.has_table(concept_key, table_schema="global_tables"):
            return pd.DataFrame()

        base_table = _qualified_table("global_tables", concept_key)
        dims_name = f"{concept_key}__dims"
        shape_name = f"{concept_key}__shape"
        has_dims = db.has_table(dims_name, table_schema="global_tables")
        has_shape = db.has_table(shape_name, table_schema="global_tables")

        if has_dims:
            dims_table = _qualified_table("global_tables", dims_name)
            dims_cte = f"""
                SELECT
                    _dlt_parent_id,
                    list(value ORDER BY _dlt_list_idx) AS dims
                FROM {dims_table}
                GROUP BY _dlt_parent_id
            """
        else:
            dims_cte = (
                "SELECT CAST(NULL AS VARCHAR) AS _dlt_parent_id, "
                "CAST([] AS VARCHAR[]) AS dims WHERE FALSE"
            )

        if has_shape:
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
                "TRY_CAST(nc.consist_artifact_id AS UUID) = a.id "
                "OR (nc.consist_artifact_id IS NULL AND a.key = ? "
                "AND CAST(nc.consist_run_id AS VARCHAR) = a.run_id)"
                ")"
            )
            params.append(concept_key)
        elif has_art_col:
            artifact_join = "JOIN main.artifact a ON TRY_CAST(nc.consist_artifact_id AS UUID) = a.id"
        elif has_run_col:
            artifact_join = (
                "JOIN main.artifact a ON (a.key = ? "
                "AND CAST(nc.consist_run_id AS VARCHAR) = a.run_id)"
            )
            params.append(concept_key)
        else:
            return pd.DataFrame()

        query = f"""
            WITH dims AS ({dims_cte}),
            shape AS ({shape_cte})
            SELECT
                nc.variable_name,
                nc.variable_type,
                COALESCE(d.dims, CAST([] AS VARCHAR[])) AS dims,
                COALESCE(s.shape, CAST([] AS BIGINT[])) AS shape,
                nc.dtype,
                nc.attributes,
                COALESCE(r.id, a.run_id) AS run_id,
                r.year,
                r.iteration
            FROM {base_table} nc
            LEFT JOIN dims d ON d._dlt_parent_id = nc._dlt_id
            LEFT JOIN shape s ON s._dlt_parent_id = nc._dlt_id
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

        if variable_type:
            query += " AND nc.variable_type = ?"
            params.append(variable_type)

        query += " ORDER BY r.year, r.iteration, nc.variable_name"

        try:
            return db.fetchdf(query, params)
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
        """Returns a dictionary of unique dimension names and their maximum sizes."""
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
                    dims_list = json.loads(str(dims_value).replace("'", '"'))
                    shape_list = json.loads(str(shape_value).replace("'", '"'))

                for dim_name, dim_size in zip(dims_list, shape_list):
                    key = str(dim_name)
                    size = int(dim_size)
                    if key not in dimensions:
                        dimensions[key] = size
                    else:
                        dimensions[key] = max(dimensions[key], size)
            except Exception as e:
                logging.debug(f"[Consist] Failed to parse dims/shape: {e}")
                continue

        return dimensions

    def get_data_variables(
        self,
        concept_key: str,
        run_ids: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Returns only data variables (not coordinates)."""
        return self.get_variables(concept_key, run_ids=run_ids, variable_type="data")

    def get_coordinates(
        self,
        concept_key: str,
        run_ids: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Returns only coordinate variables."""
        return self.get_variables(
            concept_key, run_ids=run_ids, variable_type="coordinate"
        )

    def get_attributes(
        self,
        concept_key: str,
        variable_name: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Returns attributes for a specific variable or the first variable in an artifact."""
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
            attrs_str = var_row["attributes"]
            if isinstance(attrs_str, str):
                return json.loads(attrs_str.replace("'", '"'))
            return attrs_str
        except Exception as e:
            logging.debug(f"[Consist] Failed to parse attributes: {e}")
            return None

    def summary(self, concept_key: str, run_id: Optional[str] = None) -> str:
        """Returns a human-readable summary of the NetCDF structure."""
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
                if isinstance(dims_value, Iterable) and not isinstance(
                    dims_value, (str, bytes)
                ):
                    dims_str = ", ".join(map(str, dims_value))
                else:
                    dims_str = str(dims_value).strip("[]").replace("'", "")
                lines.append(f"  - {row['variable_name']} [{dims_str}]: {row['dtype']}")

        if not coords.empty:
            lines.append(f"\nCoordinates ({len(coords)}):")
            for _, row in coords.iterrows():
                dims_value = row["dims"]
                if isinstance(dims_value, Iterable) and not isinstance(
                    dims_value, (str, bytes)
                ):
                    dims_str = ", ".join(map(str, dims_value))
                else:
                    dims_str = str(dims_value).strip("[]").replace("'", "")

                shape_value = row["shape"]
                if isinstance(shape_value, Iterable) and not isinstance(
                    shape_value, (str, bytes)
                ):
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
