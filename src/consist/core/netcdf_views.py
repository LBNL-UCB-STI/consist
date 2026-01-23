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

import logging
from typing import TYPE_CHECKING, Dict, List, Optional

import pandas as pd
from sqlalchemy import text

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


class NetCdfMetadataView:
    """
    A view factory for querying NetCDF metadata from Consist's DuckDB catalog.

    This class allows users to explore the structure and attributes of NetCDF files
    that have been logged and ingested as artifacts. It queries the `netcdf_catalog`
    table to retrieve information about variables, coordinates, dimensions, and
    their attributes across different runs.

    Attributes
    ----------
    tracker : Tracker
        An instance of the Consist `Tracker`, providing access to the database
        engine for querying artifact and run metadata.

    Example
    -------
    ```python
    view = NetCdfMetadataView(tracker)

    # List all variables in a NetCDF artifact
    variables = view.get_variables("climate", year=2024)
    print(variables[["variable_name", "dims", "shape", "run_id"]])

    # Get unique dimensions
    dims = view.get_dimensions("climate")
    print(dims)  # {"lat": 180, "lon": 360, "time": 100}

    # Find variables with specific attributes
    attrs = view.get_attributes("climate", variable_name="temperature")
    print(attrs)
    ```
    """

    def __init__(self, tracker: Tracker) -> None:
        """
        Initializes the NetCdfMetadataView with a reference to the Consist Tracker.

        Parameters
        ----------
        tracker : Tracker
            An instance of the Consist `Tracker`, providing access to the database
            for querying NetCDF metadata.
        """
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

        This method queries the `netcdf_catalog` table for all variables
        associated with a given artifact key. Results can be filtered by
        run ID, year, and variable type (data or coordinate).

        Parameters
        ----------
        concept_key : str
            The semantic key (artifact key) identifying the NetCDF artifact.
        run_ids : Optional[List[str]], optional
            Optional list of run IDs to include. If None, all runs are included.
        year : Optional[int], optional
            Optional year to filter by. If None, all years are included.
        variable_type : Optional[str], optional
            Optional filter for variable type: "data" or "coordinate".
            If None, both types are returned.

        Returns
        -------
        pd.DataFrame
            A DataFrame with columns:
            - variable_name: Name of the variable or coordinate
            - variable_type: "data" or "coordinate"
            - dims: List of dimension names (as string)
            - shape: List of dimension sizes (as string)
            - dtype: Data type of the variable
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
            WITH dims AS (
                SELECT
                    _dlt_parent_id,
                    list(value ORDER BY _dlt_list_idx) AS dims
                FROM global_tables.{concept_key}__dims
                GROUP BY _dlt_parent_id
            ),
            shape AS (
                SELECT
                    _dlt_parent_id,
                    list(value ORDER BY _dlt_list_idx) AS shape
                FROM global_tables.{concept_key}__shape
                GROUP BY _dlt_parent_id
            )
            SELECT
                nc.variable_name,
                nc.variable_type,
                d.dims,
                s.shape,
                nc.dtype,
                nc.attributes,
                r.id as run_id,
                r.year,
                r.iteration
            FROM global_tables.{concept_key} nc
            LEFT JOIN dims d ON d._dlt_parent_id = nc._dlt_id
            LEFT JOIN shape s ON s._dlt_parent_id = nc._dlt_id
            JOIN artifact a ON nc.consist_artifact_id = CAST(a.id AS TEXT)
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

        if variable_type:
            query += " AND nc.variable_type = :var_type"
            params["var_type"] = variable_type

        query += " ORDER BY r.year, r.iteration, nc.variable_name"

        try:
            return pd.read_sql(text(query), self.tracker.engine, params=params)
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

        This method extracts dimension information from the NetCDF metadata catalog
        and aggregates them across variables (or within a specific run).

        Parameters
        ----------
        concept_key : str
            The semantic key identifying the NetCDF artifact.
        run_id : Optional[str], optional
            Optional run ID to filter to. If None, returns max dimensions across all runs.

        Returns
        -------
        Dict[str, int]
            A dictionary mapping dimension names (e.g., "lat", "lon", "time")
            to their maximum sizes across all variables.

        Example
        -------
        ```python
        dims = view.get_dimensions("climate")
        print(dims)  # {"lat": 180, "lon": 360, "time": 100}
        ```
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
                else:
                    # Parse dims and shape from string representation
                    # Example: "['lat', 'lon']" -> ['lat', 'lon']
                    # Example: "[180, 360]" -> [180, 360]
                    import json

                    dims_list = json.loads(str(dims_value).replace("'", '"'))
                    shape_list = json.loads(str(shape_value).replace("'", '"'))

                for dim_name, dim_size in zip(dims_list, shape_list):
                    if dim_name not in dimensions:
                        dimensions[dim_name] = dim_size
                    else:
                        dimensions[dim_name] = max(dimensions[dim_name], dim_size)
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

        Parameters
        ----------
        concept_key : str
            The semantic key identifying the NetCDF artifact.
        run_ids : Optional[List[str]], optional
            Optional list of run IDs to include.

        Returns
        -------
        pd.DataFrame
            Variables filtered to type == "data".
        """
        return self.get_variables(concept_key, run_ids=run_ids, variable_type="data")

    def get_coordinates(
        self,
        concept_key: str,
        run_ids: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Returns only coordinate variables.

        Parameters
        ----------
        concept_key : str
            The semantic key identifying the NetCDF artifact.
        run_ids : Optional[List[str]], optional
            Optional list of run IDs to include.

        Returns
        -------
        pd.DataFrame
            Variables filtered to type == "coordinate".
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

        Parameters
        ----------
        concept_key : str
            The semantic key identifying the NetCDF artifact.
        variable_name : Optional[str], optional
            Optional variable name to filter to. If None, returns all variables.
        run_id : Optional[str], optional
            Optional run ID to filter to.

        Returns
        -------
        Optional[Dict]
            A dictionary of attributes, or None if variable not found.

        Example
        -------
        ```python
        attrs = view.get_attributes("climate", variable_name="temperature")
        print(attrs)  # {"units": "K", "long_name": "Average Temperature"}
        ```
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

        Parameters
        ----------
        concept_key : str
            The semantic key identifying the NetCDF artifact.
        run_id : Optional[str], optional
            Optional run ID to summarize.

        Returns
        -------
        str
            A formatted summary of the NetCDF structure.

        Example
        -------
        ```python
        print(view.summary("climate"))
        # Output:
        # NetCDF: climate
        # Data Variables (3):
        #   - temperature [lat, lon, time]: float32
        #   - humidity [lat, lon, time]: float32
        #   - pressure [lat, lon]: float32
        # Coordinates (3):
        #   - lat [lat]: float64 (180)
        #   - lon [lon]: float64 (360)
        #   - time [time]: float64 (100)
        ```
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
                if isinstance(dims_value, list):
                    dims_str = ", ".join(map(str, dims_value))
                else:
                    dims_str = str(dims_value).strip("[]").replace("'", "")
                lines.append(f"  - {row['variable_name']} [{dims_str}]: {row['dtype']}")

        if not coords.empty:
            lines.append(f"\nCoordinates ({len(coords)}):")
            for _, row in coords.iterrows():
                dims_value = row["dims"]
                if isinstance(dims_value, list):
                    dims_str = ", ".join(map(str, dims_value))
                else:
                    dims_str = str(dims_value).strip("[]").replace("'", "")

                shape_value = row["shape"]
                if isinstance(shape_value, list):
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
