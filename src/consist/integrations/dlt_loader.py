# src/consist/integrations/dlt_loader.py

import dlt
from typing import Type, Iterable, Dict, Any, Optional
from sqlmodel import SQLModel, Field
from consist.models.artifact import Artifact
from consist.models.run import Run


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
        "__annotations__": new_annotations,  # Crucial: Pass annotations here
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
    data_iterable: Iterable[Dict[str, Any]],
    schema_model: Optional[Type[SQLModel]] = None,
):
    """
    Ingests data associated with an artifact into the DuckDB database using `dlt`.

    This function acts as a bridge, taking an iterable of data records and loading
    them into a destination table, dynamically extending the schema with Consist's
    provenance columns. It supports both strict schema validation (via `schema_model`)
    and automatic schema inference.

    Args:
        artifact (Artifact): The `Artifact` object representing the data being ingested.
                             Its key is used as the resource name if no `schema_model` is provided.
        run_context (Run): The current `Run` object, providing context (run_id, year, iteration)
                           for the provenance columns.
        db_path (str): The file path to the DuckDB database where data will be loaded.
        data_iterable (Iterable[Dict[str, Any]]): An iterable of dictionaries, where each
                                                  dictionary represents a record to be ingested.
        schema_model (Optional[Type[SQLModel]]): An optional SQLModel class that defines the
                                                  expected schema of the ingested data. If provided,
                                                  `dlt` uses this for strict schema enforcement.
                                                  If None, `dlt` attempts to infer the schema.

    Returns:
        dlt.pipeline.LoadInfo: An object containing information about the dlt load operation.
    """

    # 1. Define the Context Injection Logic
    def add_context(record: Dict[str, Any]):
        record["consist_run_id"] = run_context.id
        record["consist_artifact_id"] = str(artifact.id)

        if run_context.year is not None:
            record["consist_year"] = run_context.year
        if run_context.iteration is not None:
            record["consist_iteration"] = run_context.iteration

        return record

    # 2. Configure the Pipeline
    pipeline = dlt.pipeline(
        pipeline_name="consist_materializer",
        destination=dlt.destinations.duckdb(f"duckdb:///{db_path}"),
        dataset_name="global_tables",
    )

    if schema_model:
        # Story 1: Strict Mode
        resource_name = schema_model.__tablename__
        columns = _extend_schema_with_system_columns(schema_model)
    else:
        # Story 3: Quick Mode / Auto-Inference
        resource_name = artifact.key
        columns = None

    # 3. Create the dlt Resource
    resource = dlt.resource(
        data_iterable,
        name=resource_name,
        write_disposition="append",
        columns=columns,
    ).add_map(add_context)

    # 4. Run the Pipeline
    info = pipeline.run(resource)

    return info
