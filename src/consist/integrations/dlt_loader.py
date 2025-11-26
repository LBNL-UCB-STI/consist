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
    by adding Consist-specific system columns (e.g., run_id, artifact_id, year, iteration).

    This is used to inject provenance information directly into the schema of
    ingested data without modifying the user's original SQLModel definition.
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
    # --- Context Helper Values ---
    ctx_run_id = run_context.id
    ctx_art_id = str(artifact.id)
    ctx_year = run_context.year
    ctx_iter = run_context.iteration

    # --- Strategy Selector ---
    is_vectorized = False

    # NEW: Handle File Paths (Streaming or Loading)
    if isinstance(data_iterable, str):
        file_path = data_iterable

        # 1. PARQUET HANDLING
        if file_path.endswith(".parquet"):
            if pa and pq:
                # Streaming Path (Preferred: Low Memory)
                def parquet_stream():
                    pf = pq.ParquetFile(file_path)
                    for batch in pf.iter_batches():
                        df_batch = batch.to_pandas()
                        # Vectorized Injection
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
                # Fallback Path (High Memory, but works without PyArrow/Stream)
                # Pandas might be using fastparquet engine
                df_loaded = pd.read_parquet(file_path)
                data_iterable = df_loaded
                is_vectorized = True

            else:
                raise ImportError(
                    f"Cannot ingest '{file_path}': Pandas/PyArrow required."
                )

        # 2. CSV HANDLING
        elif file_path.endswith(".csv"):
            if pd:

                def csv_stream():
                    for df_batch in pd.read_csv(file_path, chunksize=100_000):
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
                raise ImportError(f"Cannot ingest '{file_path}': Pandas required.")

        else:
            raise ValueError(
                f"Cannot ingest '{file_path}': Unsupported extension or logic fell through. "
                f"Supported: .parquet, .csv"
            )

    # --- Standardize ---

    dlt_columns = None
    resource_name = artifact.key

    if schema_model:
        resource_name = schema_model.__tablename__
        # Create the extended model (with system cols)
        extended_model = _extend_schema_with_system_columns(schema_model)

        # Default: Pass the class (Enables Validation)
        dlt_columns = extended_model

    # Check for DataFrame/Arrow (Direct Memory Object)
    # Note: We check 'is_vectorized' first in case we converted it above
    if is_vectorized or (pd and isinstance(data_iterable, pd.DataFrame)):
        is_vectorized = True

        # If it's a direct DataFrame (not our generator), we inject cols here
        if pd and isinstance(data_iterable, pd.DataFrame):
            data_iterable["consist_run_id"] = ctx_run_id
            data_iterable["consist_artifact_id"] = ctx_art_id
            if ctx_year is not None:
                data_iterable["consist_year"] = ctx_year
            if ctx_iter is not None:
                data_iterable["consist_iteration"] = ctx_iter

        # Schema Audit & Strict Mode Fix
        if schema_model:
            # Check fields
            if hasattr(schema_model, "model_fields"):
                schema_cols = set(schema_model.model_fields.keys())
            else:
                schema_cols = set(schema_model.__fields__.keys())

            # Audit only possible if we have the DataFrame handy
            if pd and isinstance(data_iterable, pd.DataFrame):
                data_cols = set(data_iterable.columns)
                ghost_cols = data_cols - schema_cols
                if ghost_cols:
                    warnings.warn(
                        f"\n[Consist Schema Warning] Data columns {ghost_cols} missing from schema '{schema_model.__name__}'."
                    )

            # --- Disable Pydantic Validator for DataFrames ---
            # If we pass the Pydantic CLASS, dlt tries to validate the DataFrame row-by-row (slow/crashes).
            # If we pass a DICT, dlt uses it for the DB Schema but skips Python validation.
            try:
                dlt_columns = pydantic_to_table_schema_columns(extended_model)
            except Exception as e:
                print(
                    f"[Consist Warning] Failed to convert SQLModel to dlt schema: {e}"
                )
                dlt_columns = extended_model

    # --- Pipeline Configuration ---
    pipeline_working_dir = os.path.dirname(os.path.abspath(db_path))

    pipeline = dlt.pipeline(
        pipeline_name="consist_materializer",
        pipelines_dir=pipeline_working_dir,
        destination=dlt.destinations.duckdb(f"duckdb:///{db_path}"),
        dataset_name="global_tables",
    )

    # --- Resource Creation ---
    if is_vectorized:
        # Fast Path (Generators of DFs or DFs)
        resource = dlt.resource(
            data_iterable,
            name=resource_name,
            write_disposition="append",
            columns=dlt_columns,
        )
    else:
        # Slow Path (Dicts)
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
        ).add_map(add_context)

    # --- Execution ---
    info = pipeline.run(resource)

    return info
