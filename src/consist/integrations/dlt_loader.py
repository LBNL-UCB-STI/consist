import dlt
import os
import warnings
from typing import Type, Iterable, Dict, Any, Optional, Union
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


def ingest_artifact(
    artifact: Artifact,
    run_context: Run,
    db_path: str,
    data_iterable: Union[Iterable[Dict[str, Any]], Any],
    schema_model: Optional[Type[SQLModel]] = None,
):
    """
    Ingests data associated with an `Artifact` into the Consist DuckDB database using `dlt`.

    This function acts as **"The Materializer"**, integrating with `dlt` to load data
    from various sources (file paths, DataFrames, iterables of dicts) into a structured
    table within the `global_tables` dataset in DuckDB.

    Key features:
    -   **Vectorized Ingestion**: Optimizes for performance by supporting direct ingestion
        from file paths (Parquet, CSV) or DataFrames, leveraging `dlt`'s fast loading paths.
    -   **Schema Evolution**: Supports `dlt`'s automatic schema detection and evolution.
        When `schema_model` is provided, it enables a **"Strict Mode"** for validation
        against a predefined SQLModel schema, while also allowing flexible schema inference.
    -   **Provenance Injection**: Dynamically injects Consist-specific system columns
        (`consist_run_id`, `consist_artifact_id`, `consist_year`, `consist_iteration`)
        into the ingested data, linking every record back to its provenance.

    Args:
        artifact (Artifact): The artifact object representing the data being ingested.
                             Its metadata might include schema information.
        run_context (Run): The `Run` object providing context (e.g., run_id, year, iteration)
                           for provenance injection.
        db_path (str): The file path to the DuckDB database.
        data_iterable (Union[Iterable[Dict[str, Any]], Any]): The data to ingest. Can be:
                                                              - A file path (str) for Parquet/CSV.
                                                              - A Pandas DataFrame or PyArrow Table.
                                                              - An iterable of dictionaries (row-by-row).
        schema_model (Optional[Type[SQLModel]]): An optional SQLModel class that defines the
                                                    expected schema for the ingested data. If provided,
                                                    it enables strict validation and defines the table structure.

    Raises:
        ImportError: If required libraries (Pandas, PyArrow) are not installed for certain file types.
        ValueError: If an unsupported file extension is provided or other data issues.
        Exception: Any exception raised by the underlying `dlt` ingestion process.
    """
    # --- Context Helper Values for Provenance Injection ---
    ctx_run_id = run_context.id
    ctx_art_id = str(artifact.id)
    ctx_year = run_context.year
    ctx_iter = run_context.iteration

    # Flag to track if data is being processed in a vectorized manner
    is_vectorized = False

    # NEW: Handle File Paths (Streaming or Loading for Vectorized Ingestion)
    if isinstance(data_iterable, str):
        file_path = data_iterable

        # 1. PARQUET HANDLING
        if file_path.endswith(".parquet"):
            if pa and pq:
                # Streaming Path (Preferred: Low Memory for large files)
                def parquet_stream():
                    pf = pq.ParquetFile(file_path)
                    for batch in pf.iter_batches():
                        df_batch = batch.to_pandas()
                        # Vectorized Injection of System Columns
                        df_batch["consist_run_id"] = ctx_run_id
                        df_batch["consist_artifact_id"] = ctx_art_id
                        if ctx_year is not None:
                            df_batch["consist_year"] = ctx_year
                        if ctx_iter is not None:
                            df_batch["consist_iteration"] = ctx_iter
                        yield df_batch

                data_iterable = parquet_stream()
                is_vectorized = True

            elif pd:
                # Fallback Path (High Memory, but works without PyArrow/Stream, might use fastparquet)
                df_loaded = pd.read_parquet(file_path)
                data_iterable = df_loaded
                is_vectorized = True

            else:
                raise ImportError(
                    f"Cannot ingest '{file_path}': Pandas or PyArrow required for Parquet files."
                )

        # 2. CSV HANDLING
        elif file_path.endswith(".csv"):
            if pd:
                # Streaming CSV with Pandas chunking
                def csv_stream():
                    for df_batch in pd.read_csv(file_path, chunksize=100_000):
                        # Vectorized Injection of System Columns
                        df_batch["consist_run_id"] = ctx_run_id
                        df_batch["consist_artifact_id"] = ctx_art_id
                        if ctx_year is not None:
                            df_batch["consist_year"] = ctx_year
                        if ctx_iter is not None:
                            df_batch["consist_iteration"] = ctx_iter
                        yield df_batch

                data_iterable = csv_stream()
                is_vectorized = True
            else:
                raise ImportError(
                    f"Cannot ingest '{file_path}': Pandas required for CSV files."
                )

        else:
            raise ValueError(
                f"Cannot ingest '{file_path}': Unsupported extension or logic fell through. "
                f"Supported vectorized file types: .parquet, .csv"
            )

    # --- Standardize Data Source and Schema Handling ---

    dlt_columns = None
    resource_name = artifact.key  # Default resource name for dlt

    if schema_model:
        # If a schema model is provided, use its tablename for the dlt resource
        resource_name = schema_model.__tablename__
        # Create an extended model which includes Consist system columns
        extended_model = _extend_schema_with_system_columns(schema_model)

        # --- "Strict Mode" for Schema Validation & Evolution ---
        # By default, passing the class enables validation.
        dlt_columns = extended_model

    # Check for DataFrame/Arrow (Direct Memory Object) and apply system columns
    # We check 'is_vectorized' first in case we converted a file path to a DataFrame/generator above.
    if is_vectorized or (pd and isinstance(data_iterable, pd.DataFrame)):
        is_vectorized = True  # Confirm vectorized path

        # If it's a direct DataFrame (not our generator from a file), we inject cols here.
        # Generators already handle injection within their stream.
        if pd and isinstance(data_iterable, pd.DataFrame):
            # Vectorized Injection of System Columns for direct DataFrames
            data_iterable["consist_run_id"] = ctx_run_id
            data_iterable["consist_artifact_id"] = ctx_art_id
            if ctx_year is not None:
                data_iterable["consist_year"] = ctx_year
            if ctx_iter is not None:
                data_iterable["consist_iteration"] = ctx_iter

        # Schema Audit & dlt's Strict Mode (part of "Ingestion & Schema Evolution")
        if schema_model:
            # Get user-defined schema columns
            schema_cols = set(getattr(schema_model, "model_fields", {}).keys()) or set(
                getattr(schema_model, "__fields__", {}).keys()
            )

            # Only audit if we have the DataFrame handy and not a generator
            if pd and isinstance(data_iterable, pd.DataFrame):
                data_cols = set(data_iterable.columns)
                ghost_cols = data_cols - schema_cols
                if ghost_cols:
                    warnings.warn(
                        f"\n[Consist Schema Warning] Data columns {ghost_cols} present in input "
                        f"but missing from provided schema '{schema_model.__name__}'. "
                        "These columns will be dropped during ingestion in strict mode."
                    )

            # --- Critical: Disable Pydantic Validator for DataFrames in dlt ---
            # If dlt is given a Pydantic CLASS with a DataFrame, it attempts row-by-row
            # validation which is extremely slow and can crash for large datasets.
            # Instead, we convert the Pydantic model to a dlt table schema dictionary.
            # This allows dlt to use the schema for DB table creation/evolution,
            # but skips Python-level row validation.
            try:
                # Use the extended_model to generate the dlt schema including system columns
                dlt_columns = pydantic_to_table_schema_columns(extended_model)
            except Exception as e:
                warnings.warn(
                    f"[Consist Warning] Failed to convert SQLModel to dlt schema columns: {e}. "
                    "Falling back to passing extended model directly, which might be slower "
                    "for DataFrame ingestion if dlt tries to validate rows."
                )
                dlt_columns = extended_model  # Fallback if conversion fails

    # --- dlt Pipeline Configuration (Part of "The Materializer") ---
    # dlt will create/manage the tables in the specified DuckDB and dataset.
    pipeline_working_dir = os.path.dirname(os.path.abspath(db_path))

    pipeline = dlt.pipeline(
        pipeline_name="consist_materializer",
        pipelines_dir=pipeline_working_dir,  # dlt stores temporary files/state here
        destination=dlt.destinations.duckdb(f"duckdb:///{db_path}"),
        dataset_name="global_tables",  # All Consist ingested data goes into 'global_tables'
    )

    # --- Resource Creation (dlt's Data Source Definition) ---
    if is_vectorized:
        # Fast Path: DataFrames or generators yielding DataFrames/PyArrow Tables
        resource = dlt.resource(
            data_iterable,
            name=resource_name,
            write_disposition="append",  # Always append new data to existing tables
            columns=dlt_columns,  # Use our generated/extended schema
        )
    else:
        # Slow Path: Iterables of Python dictionaries (row-by-row processing)
        # We need to manually add context to each record in this path.
        def add_context(record: Dict[str, Any]):
            record["consist_run_id"] = ctx_run_id
            record["consist_artifact_id"] = ctx_art_id
            if ctx_year is not None:
                record["consist_year"] = ctx_year
            if ctx_iter is not None:
                record["consist_iteration"] = ctx_iter
            return record

        resource = dlt.resource(
            data_iterable,
            name=resource_name,
            write_disposition="append",
            columns=dlt_columns,
        ).add_map(
            add_context
        )  # Apply the context injection map

    # --- Execution ---
    # This runs the dlt pipeline, loading the data into DuckDB.
    info = pipeline.run(resource)

    return info
