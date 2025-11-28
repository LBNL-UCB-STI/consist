"""
This module provides the integration layer between Consist and the dlt (Data Load Tool) library.
It is responsible for materializing artifact data into the DuckDB database, handling various
data formats (Pandas DataFrames, Parquet, CSV, Zarr) and ensuring that Consist's system
columns (e.g., run_id, artifact_id) are correctly injected for provenance.

Key functionalities include:
- Dynamic extension of user-defined SQLModel schemas with Consist system columns.
- Handling of different data ingestion strategies: vectorized (Pandas, PyArrow) and streaming.
- Specialized handlers for common file formats (Parquet, CSV, Zarr metadata).
- Integration with dlt pipeline for robust data loading, schema inference, and validation.
"""

import dlt
import os
import warnings
from typing import Type, Iterable, Dict, Any, Optional, Union, Tuple
from pathlib import Path
from sqlmodel import SQLModel, Field
from consist.models.artifact import Artifact
from consist.models.run import Run
from dlt.common.libs.pydantic import pydantic_to_table_schema_columns

# Robust imports
try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = None
    pq = None

try:
    import zarr
    import xarray as xr
except ImportError:
    zarr = None
    xr = None


def _extend_schema_with_system_columns(base_model: Type[SQLModel]) -> Type[SQLModel]:
    """
    Dynamically creates a new SQLModel class that extends a given base model
    by adding Consist-specific system columns (e.g., `consist_run_id`, `consist_artifact_id`,
    `consist_year`, `consist_iteration`).

    This function plays a crucial role in Consist's **"Ingestion & Schema Evolution"** strategy.
    It injects provenance information directly into the schema of ingested data,
    allowing `dlt` to automatically load these system columns. This avoids modifying
    the user's original SQLModel definition while ensuring every row in the
    global database is traceable to its origin.
    The dynamically created model does not set `table=True` as it's primarily for
    `dlt` schema inference and validation.

    Args:
        base_model (Type[SQLModel]): The original SQLModel class provided by the user,
                                      defining the structure of their data.

    Returns:
        Type[SQLModel]: A new SQLModel class that inherits from `base_model` and
                        includes the additional Consist system columns.
    """
    # 1. Define Annotations (The Types)
    new_annotations = {
        "consist_run_id": Optional[str],
        "consist_artifact_id": Optional[str],
        "consist_year": Optional[int],
        "consist_iteration": Optional[int],
    }

    # 2. Define Attributes (The Defaults/FieldInfos)
    new_attributes = {
        "consist_run_id": Field(default=None),
        "consist_artifact_id": Field(default=None),
        "consist_year": Field(default=None),
        "consist_iteration": Field(default=None),
        "__annotations__": new_annotations,
    }

    # 3. Preserve the table name
    # dlt uses this to determine the destination table in DuckDB
    if hasattr(base_model, "__tablename__"):
        new_attributes["__tablename__"] = base_model.__tablename__

    # 4. Create the class dynamically
    # We inherit from base_model so validation rules for user data are preserved.
    # Note: We do NOT set table=True. This is just a schema for dlt to read.
    extended_model = type(
        f"Extended_{base_model.__name__}", (base_model,), new_attributes
    )

    return extended_model


def _handle_zarr_metadata(path: str) -> Iterable[Dict[str, Any]]:
    """
    Handler for Zarr/NetCDF-like arrays.
    Yields structural metadata (variables, dims, attrs) instead of raw pixel data.
    """
    if not xr:
        raise ImportError("xarray and zarr are required for Zarr ingestion.")

    try:
        # Open consolidated=False to be safe with basic zarr stores
        ds = xr.open_zarr(path, consolidated=False)

        # 1. Yield Data Variables
        for var_name, da in ds.data_vars.items():
            yield {
                "variable_name": var_name,
                "variable_type": "data",
                "dims": list(da.dims),
                "shape": list(da.shape),
                "dtype": str(da.dtype),
                "attributes": da.attrs,  # dlt handles JSON dumping automatically
            }

        # 2. Yield Coordinates
        for coord_name, da in ds.coords.items():
            yield {
                "variable_name": coord_name,
                "variable_type": "coordinate",
                "dims": list(da.dims),
                "shape": list(da.shape),
                "dtype": str(da.dtype),
                "attributes": da.attrs,
            }
    except Exception as e:
        raise ValueError(f"Failed to extract Zarr metadata from {path}: {e}")


def _handle_parquet_path(path: str, ctx: Dict[str, Any]) -> Tuple[Any, bool]:
    """
    Handles ingestion of Parquet files, providing a streaming or vectorized data source
    suitable for `dlt` ingestion.

    Args:
        path (str): The file system path to the Parquet file.
        ctx (Dict[str, Any]): A dictionary of Consist system context values (e.g.,
                               `consist_run_id`) to be injected into the data.

    Returns:
        Tuple[Any, bool]: A tuple containing:
                          - The data source (either a Pandas DataFrame or a generator
                            yielding Pandas DataFrames/batches).
                          - A boolean indicating if the source is vectorized (True for DF,
                            False for generator).

    Raises:
        ImportError: If neither Pandas nor PyArrow is available.
    """
    if pa and pq:

        def parquet_stream():
            pf = pq.ParquetFile(path)
            for batch in pf.iter_batches():
                df_batch = batch.to_pandas()
                for k, v in ctx.items():
                    if v is not None:
                        df_batch[k] = v
                yield df_batch

        return parquet_stream(), True
    elif pd:
        df = pd.read_parquet(path)
        for k, v in ctx.items():
            if v is not None:
                df[k] = v
        return df, True
    else:
        raise ImportError(f"Pandas or PyArrow required for Parquet: {path}")


def _handle_csv_path(path: str, ctx: Dict[str, Any]) -> Tuple[Any, bool]:
    """
    Handles ingestion of CSV files, providing a streaming data source
    suitable for `dlt` ingestion.

    Args:
        path (str): The file system path to the CSV file.
        ctx (Dict[str, Any]): A dictionary of Consist system context values (e.g.,
                               `consist_run_id`) to be injected into the data.

    Returns:
        Tuple[Any, bool]: A tuple containing:
                          - The data source (a generator yielding Pandas DataFrames/chunks).
                          - A boolean indicating if the source is vectorized (True for generator
                            with chunks, effectively).

    Raises:
        ImportError: If Pandas is not available.
    """
    if pd:

        def csv_stream():
            for df_batch in pd.read_csv(path, chunksize=100_000):
                for k, v in ctx.items():
                    if v is not None:
                        df_batch[k] = v
                yield df_batch

        return csv_stream(), True
    else:
        raise ImportError(f"Pandas required for CSV: {path}")


def ingest_artifact(
    artifact: Artifact,
    run_context: Run,
    db_path: str,
    data_iterable: Union[Iterable[Dict[str, Any]], Any],
    schema_model: Optional[Type[SQLModel]] = None,
):
    """
    Ingests artifact data into DuckDB.
    Supports Vectorized loading for Tables and Metadata loading for Matrices.
    """
    # System Context
    ctx = {
        "consist_run_id": run_context.id,
        "consist_artifact_id": str(artifact.id),
        "consist_year": run_context.year,
        "consist_iteration": run_context.iteration,
    }

    is_vectorized = False
    resource_name = artifact.key  # Default name

    # --- 1. Data Source Resolution ---
    if isinstance(data_iterable, str):
        file_path = data_iterable
        driver = artifact.driver or Path(file_path).suffix.lstrip(".").lower()

        if driver == "zarr":
            # Matrix Mode: Ingest Metadata Only
            data_iterable = _handle_zarr_metadata(file_path)
            resource_name = "zarr_catalog"  # Force common catalog table
            is_vectorized = False  # It's a generator of dicts

        elif driver == "parquet" or file_path.endswith(".parquet"):
            data_iterable, is_vectorized = _handle_parquet_path(file_path, ctx)

        elif driver == "csv" or file_path.endswith(".csv"):
            data_iterable, is_vectorized = _handle_csv_path(file_path, ctx)

        else:
            raise ValueError(f"Unsupported ingestion driver: {driver}")

    # --- 2. Schema Handling ---
    dlt_columns = None

    # If we are in Matrix Mode (Zarr), we might want a fixed internal schema for the catalog,
    # but dlt infers it well enough (var_name, dims, shape, etc).

    extended_model = None

    if schema_model:
        resource_name = schema_model.__tablename__
        extended_model = _extend_schema_with_system_columns(schema_model)
        # Default to model for standard dict ingestion
        dlt_columns = extended_model

        # Direct DataFrame Injection Check
    if pd and isinstance(data_iterable, pd.DataFrame):
        is_vectorized = True
        for k, v in ctx.items():
            if v is not None:
                data_iterable[k] = v

        # --- FIX 2: Restore Schema Audit Warning ---
        if schema_model:
            # Get user-defined schema columns
            schema_cols = set(getattr(schema_model, "model_fields", {}).keys()) or set(
                getattr(schema_model, "__fields__", {}).keys()
            )
            data_cols = set(data_iterable.columns)
            ghost_cols = data_cols - schema_cols

            # Filter out system columns from ghost check
            ghost_cols = {c for c in ghost_cols if not c.startswith("consist_")}

            if ghost_cols:
                warnings.warn(
                    f"\n[Consist Schema Warning] Data columns {ghost_cols} present in input "
                    f"but missing from provided schema '{schema_model.__name__}'. "
                    "These columns will be dropped during ingestion in strict mode."
                )

    if is_vectorized and extended_model:
        try:
            dlt_columns = pydantic_to_table_schema_columns(extended_model)
        except Exception:
            # If conversion fails, fallback (but this might crash for DataFrames as seen)
            dlt_columns = extended_model

    # --- 3. Pipeline Execution ---
    pipeline_working_dir = os.path.dirname(os.path.abspath(db_path))

    pipeline = dlt.pipeline(
        pipeline_name="consist_materializer",
        pipelines_dir=pipeline_working_dir,
        destination=dlt.destinations.duckdb(f"duckdb:///{db_path}"),
        dataset_name="global_tables",
    )

    # --- Resource Creation (dlt's Data Source Definition) ---
    if is_vectorized:
        # Fast Path (Tables)
        resource = dlt.resource(
            data_iterable,
            name=resource_name,
            write_disposition="append",
            columns=dlt_columns,
        )
    else:
        # Slow/Metadata Path (Matrices or Lists)
        # We must manually inject context for every record
        def add_context(record: Dict[str, Any]):
            record.update({k: v for k, v in ctx.items() if v is not None})
            return record

        resource = dlt.resource(
            data_iterable,
            name=resource_name,
            write_disposition="append",
            columns=dlt_columns,
        ).add_map(add_context)

    return pipeline.run(resource)
