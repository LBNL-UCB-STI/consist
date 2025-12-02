"""
Consist dlt (Data Load Tool) Integration Module

This module provides the integration layer between Consist and the `dlt` library,
facilitating the robust and efficient ingestion of artifact data into the DuckDB database.
It is responsible for materializing various data formats (e.g., Pandas DataFrames,
Parquet, CSV, Zarr metadata) and ensuring that Consist's system-level provenance columns
(such as `consist_run_id`, `consist_artifact_id`) are correctly injected into the data.

Key functionalities include:
-   **Dynamic Schema Extension**: User-defined `SQLModel` schemas are dynamically extended
    with Consist's provenance-tracking system columns.
-   **Flexible Ingestion Strategies**: Supports different data ingestion mechanisms,
    including vectorized loading (for Pandas DataFrames, PyArrow tables) and streaming
    for large datasets.
-   **Format-Specific Handlers**: Contains specialized functions for processing and
    preparing data from common file formats like Parquet, CSV, and extracting
    structural metadata from Zarr archives.
-   **dlt Pipeline Integration**: Leverages the `dlt` pipeline for robust data loading,
    automatic schema inference, and optional strict validation, ensuring data quality
    and consistency.
"""

import dlt
import uuid
import pandas as pd
from typing import Optional, Any, Iterable, Union, Type, Set, Dict, Tuple
from sqlmodel import SQLModel
from consist.models.artifact import Artifact
from consist.models.run import Run

# Robust imports
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


def _handle_zarr_metadata(path: str) -> Iterable[Dict[str, Any]]:
    """
    Extracts and yields structural metadata from a Zarr or NetCDF-like array store.

    Instead of yielding raw pixel or array data, this handler focuses on providing
    metadata such as variable names, dimensions, shapes, data types, and attributes.
    This is particularly useful for cataloging and understanding the structure of
    large multi-dimensional datasets without ingesting their entire contents.

    Parameters
    ----------
    path : str
        The file system path to the Zarr store (directory).

    Yields
    ------
    Dict[str, Any]
        A dictionary representing the metadata for each data variable and coordinate
        within the Zarr store. Each dictionary includes keys like 'variable_name',
        'variable_type' (data or coordinate), 'dims', 'shape', 'dtype', and 'attributes'.

    Raises
    ------
    ImportError
        If `xarray` or `zarr` libraries are not installed.
    ValueError
        If an error occurs during the extraction of Zarr metadata from the specified path.
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
                "attributes": da.attrs,
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
    suitable for `dlt` ingestion, with Consist system context injection.

    This function attempts to use `pyarrow` for efficient batch-wise streaming if available,
    falling back to `pandas` for full file loading. It injects Consist's provenance-related
    columns into each data batch or DataFrame before yielding it.

    Parameters
    ----------
    path : str
        The file system path to the Parquet file.
    ctx : Dict[str, Any]
        A dictionary containing Consist system context values (e.g., `consist_run_id`,
        `consist_artifact_id`) to be injected as new columns into the data.

    Returns
    -------
    Tuple[Any, bool]
        A tuple where:
        - The first element is the data source: either a Pandas DataFrame (for full load)
          or a generator yielding Pandas DataFrames (for streaming batches).
        - The second element is a boolean: `True` if the source is vectorized (DataFrame
          or batch generator), `False` otherwise (not applicable here).

    Raises
    ------
    ImportError
        If neither `Pandas` nor `PyArrow` is available in the environment, which are
        required for processing Parquet files.
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
    suitable for `dlt` ingestion, with Consist system context injection.

    This function reads CSV files in chunks using `pandas.read_csv` and injects
    Consist's provenance-related columns into each DataFrame chunk before yielding it.

    Parameters
    ----------
    path : str
        The file system path to the CSV file.
    ctx : Dict[str, Any]
        A dictionary containing Consist system context values (e.g., `consist_run_id`,
        `consist_artifact_id`) to be injected as new columns into the data.

    Returns
    -------
    Tuple[Any, bool]
        A tuple where:
        - The first element is the data source: a generator yielding Pandas DataFrames (chunks).
        - The second element is a boolean: `True` indicating that the source
          effectively operates in a vectorized, chunked manner.

    Raises
    ------
    ImportError
        If `Pandas` is not available in the environment, which is required for
        processing CSV files.
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
    artifact: "Artifact",
    run_context: "Run",
    db_path: str,
    data_iterable: Optional[Union[Iterable[Any], str, pd.DataFrame]] = None,
    schema_model: Optional[Type[SQLModel]] = None,
) -> Tuple[Any, str]:
    """
    Ingests artifact data into a DuckDB database using the `dlt` (Data Load Tool) library.

    This function supports various data sources (file paths, Pandas DataFrames, iterables of dicts)
    and automatically injects Consist's provenance system columns (`consist_run_id`,
    `consist_artifact_id`, `consist_year`, `consist_iteration`) into the data. It leverages
    `dlt` for robust schema handling, including inference and optional strict validation
    based on a provided `SQLModel`.

    Parameters
    ----------
    artifact : Artifact
        The Consist `Artifact` object representing the data to be ingested. Its driver
        information is used to determine the appropriate data handler.
    run_context : Run
        The `Run` object providing the context (ID, year, iteration) for provenance tracking.
    db_path : str
        The file system path to the DuckDB database where the data will be loaded.
    data_iterable : Optional[Union[Iterable[Any], str, pd.DataFrame]], optional
        The data to ingest. Can be:
        - A file path (str) to a Parquet, CSV, HDF5, JSON, or Zarr file.
        - A Pandas DataFrame (will be treated as a single batch).
        - An iterable (e.g., list of dicts, generator) where each item represents a row.
        If `None`, it implies the data should be read directly from the `artifact`'s URI.
    schema_model : Optional[Type[SQLModel]], optional
        An optional `SQLModel` class that defines the expected schema for the data.
        If provided, `dlt` will use this for strict validation and schema management.
        If `None`, `dlt` will infer the schema.

    Returns
    -------
    Tuple[dlt.LoadInfo, str]
        A tuple containing:
        - `dlt.LoadInfo`: An object providing detailed information about the data loading process.
        - `str`: The actual normalized table name where the data was loaded in the database.

    Raises
    ------
    ValueError
        If no data is provided for ingestion, if the artifact driver is unsupported,
        or if a `schema_model` is provided but a schema contract violation occurs
        (e.g., new columns found in strict mode).
    ImportError
        If a required library for a specific driver (e.g., `pyarrow` for Parquet,
        `tables` for HDF5, `xarray`/`zarr` for Zarr) is not installed.
    """

    # 1. Resolve Data Source (Streaming Batches)
    if isinstance(data_iterable, str):
        file_path = data_iterable

        if artifact.driver == "parquet":
            data_source = _yield_parquet_batches(file_path)

        elif artifact.driver == "csv":
            data_source = _yield_csv_batches(file_path)

        elif artifact.driver == "h5_table":
            table_path = artifact.meta.get("table_path") or artifact.meta.get(
                "sub_path"
            )
            if not table_path:
                raise ValueError(f"Artifact '{artifact.key}' missing 'table_path'.")
            data_source = _yield_h5_batches(file_path, table_path)

        elif artifact.driver == "json":
            data_source = pd.read_json(file_path).to_dict(orient="records")

        elif artifact.driver == "zarr":
            data_source = _handle_zarr_metadata(file_path)

        else:
            raise ValueError(f"Ingestion not supported for driver: {artifact.driver}")

    elif data_iterable is not None:
        # User passed explicit data object
        if isinstance(data_iterable, pd.DataFrame):
            # Treat single DataFrame as a list containing one batch for vectorization
            data_source = [data_iterable]
        else:
            # Assume list of dicts or generator
            data_source = data_iterable
    else:
        raise ValueError("No data provided for ingestion.")

    # 2. Configure dlt Pipeline
    if schema_model and hasattr(schema_model, "__tablename__"):
        desired_table_name = schema_model.__tablename__
    else:
        desired_table_name = artifact.key

    pipeline_uid = uuid.uuid4().hex[:8]
    pipeline = dlt.pipeline(
        pipeline_name=f"consist_ingest_{pipeline_uid}",
        destination=dlt.destinations.duckdb(f"{db_path}"),
        dataset_name="global_tables",
    )

    # 3. Validation Setup
    allowed_keys: Optional[Set[str]] = None
    if schema_model:
        allowed_keys = set(schema_model.model_fields.keys())
        allowed_keys.update(
            {
                "consist_run_id",
                "consist_artifact_id",
                "consist_year",
                "consist_iteration",
            }
        )

    # 4. Enrich Rows (Vectorized & Scalar support)
    def _enrich_rows(source):
        for item in source:
            # PATH A: Vectorized (Pandas DataFrame)
            if isinstance(item, pd.DataFrame):
                batch = item.copy(deep=False)

                # Vectorized Assignment
                batch["consist_run_id"] = run_context.id
                batch["consist_artifact_id"] = str(artifact.id)
                batch["consist_year"] = run_context.year
                batch["consist_iteration"] = run_context.iteration

                # Vectorized Strict Mode Check
                if allowed_keys:
                    current_cols = set(batch.columns)
                    extra_cols = current_cols - allowed_keys
                    if extra_cols:
                        raise ValueError(
                            f"Schema Contract Violation: Found undefined columns {extra_cols} "
                            f"in artifact '{artifact.key}'. Strict Schema '{schema_model.__name__}' "
                            "does not allow new columns."
                        )
                yield batch

            # PATH B: Scalar (Dictionary)
            elif isinstance(item, dict):
                item["consist_run_id"] = run_context.id
                item["consist_artifact_id"] = str(artifact.id)
                item["consist_year"] = run_context.year
                item["consist_iteration"] = run_context.iteration

                if allowed_keys:
                    current_keys = set(item.keys())
                    extra_cols = current_keys - allowed_keys
                    if extra_cols:
                        raise ValueError(
                            f"Schema Contract Violation: Found undefined columns {extra_cols} "
                            f"in artifact '{artifact.key}'."
                        )
                yield item

            else:
                yield item

    # 5. Define Resource
    resource_kwargs = {
        "name": desired_table_name,
        "write_disposition": "append",
    }

    # Always hint system columns so dlt creates them even if values are NULL
    system_columns = {
        "consist_run_id": {"data_type": "text", "nullable": True},
        "consist_artifact_id": {"data_type": "text", "nullable": True},
        "consist_year": {"data_type": "bigint", "nullable": True},
        "consist_iteration": {"data_type": "bigint", "nullable": True},
    }

    if schema_model:
        resource_kwargs["schema_contract"] = {
            "tables": "evolve",
            "columns": "freeze",
            "data_type": "freeze",
        }
        columns = _sqlmodel_to_dlt_columns(schema_model)
        columns.update(system_columns)
        resource_kwargs["columns"] = columns
    else:
        # Loose Mode: Hint system columns, allow everything else to evolve
        resource_kwargs["columns"] = system_columns

    resource = dlt.resource(_enrich_rows(data_source), **resource_kwargs)

    # 6. Run
    info = pipeline.run(resource)

    real_table_name = pipeline.default_schema.naming.normalize_table_identifier(
        desired_table_name
    )
    return info, real_table_name


def _sqlmodel_to_dlt_columns(model: Type[SQLModel]) -> Dict[str, Dict[str, Any]]:
    """
    Converts a SQLModel class definition into a `dlt` columns dictionary.

    This function inspects the fields of a given `SQLModel` class and translates
    them into a format suitable for `dlt`'s schema definition. It infers `dlt`
    data types based on Python types and determines nullability.

    Parameters
    ----------
    model : Type[SQLModel]
        The `SQLModel` class to convert.

    Returns
    -------
    Dict[str, Dict[str, Any]]
        A dictionary where keys are column names and values are dictionaries
        containing `dlt` column properties (e.g., `data_type`, `nullable`).
    """
    columns = {}
    for name, field in model.model_fields.items():
        py_type = field.annotation
        dlt_type = "text"
        if py_type is int or py_type is Optional[int]:
            dlt_type = "bigint"
        elif py_type is float or py_type is Optional[float]:
            dlt_type = "double"
        elif py_type is bool or py_type is Optional[bool]:
            dlt_type = "bool"
        columns[name] = {"data_type": dlt_type, "nullable": not field.is_required()}
    return columns


# --- Generators ---


def _yield_h5_batches(path: str, key: str) -> Iterable[pd.DataFrame]:
    """
    Generator that yields Pandas DataFrames in chunks from an HDF5 table.

    This function provides a memory-efficient way to process large HDF5 tables
    by reading them in smaller batches, which can then be ingested by `dlt`.

    Parameters
    ----------
    path : str
        The file system path to the HDF5 file.
    key : str
        The HDF5 key (path within the HDF5 file) to the table to be read.

    Yields
    ------
    pd.DataFrame
        A chunk of the HDF5 table as a Pandas DataFrame.

    Raises
    ------
    ImportError
        If `PyTables` (the `tables` library) is not installed.
    KeyError
        If the specified `key` (table path) is not found within the HDF5 file.
    """
    try:
        import tables
    except ImportError:
        raise ImportError("PyTables is required.")
    with pd.HDFStore(path, mode="r") as store:
        if key not in store:
            raise KeyError(f"Key '{key}' not found in HDF5.")
        iterator = pd.read_hdf(path, key=key, chunksize=50000, iterator=True)
        for chunk in iterator:
            yield chunk


def _yield_parquet_batches(path: str) -> Iterable[pd.DataFrame]:
    """
    Generator that yields Pandas DataFrames in batches from a Parquet file.

    This function provides a memory-efficient way to process large Parquet files
    by reading them in smaller batches using `pyarrow`, which are then converted
    to Pandas DataFrames for ingestion by `dlt`.

    Parameters
    ----------
    path : str
        The file system path to the Parquet file.

    Yields
    ------
    pd.DataFrame
        A batch of the Parquet file as a Pandas DataFrame.

    Raises
    ------
    ImportError
        If `pyarrow` library is not installed.
    """
    import pyarrow.parquet as pq

    parquet_file = pq.ParquetFile(path)
    for batch in parquet_file.iter_batches():
        yield batch.to_pandas()


def _yield_csv_batches(path: str) -> Iterable[pd.DataFrame]:
    """
    Generator that yields Pandas DataFrames in chunks from a CSV file.

    This function provides a memory-efficient way to process large CSV files
    by reading them in smaller chunks using `pandas.read_csv`'s `chunksize`
    parameter, which can then be ingested by `dlt`.

    Parameters
    ----------
    path : str
        The file system path to the CSV file.

    Yields
    ------
    pd.DataFrame
        A chunk of the CSV file as a Pandas DataFrame.
    """
    for chunk in pd.read_csv(path, chunksize=50000):
        yield chunk
