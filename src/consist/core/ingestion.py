import logging
from typing import Any, Iterable, Optional, TYPE_CHECKING, Union

from sqlmodel import SQLModel

from consist.models.artifact import Artifact
from consist.models.run import Run

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


def resolve_ingest_run(
    *,
    tracker: "Tracker",
    artifact: Artifact,
    run: Optional[Run],
) -> Run:
    """
    Resolve the run context to attribute an ingestion to.

    Parameters
    ----------
    tracker : Tracker
        Tracker instance used for run lookup and current context.
    artifact : Artifact
        Artifact being ingested.
    run : Optional[Run]
        Optional explicit run to use; if None, the tracker will attempt to
        resolve a run from the artifact or active context.

    Returns
    -------
    Run
        The resolved run to associate with the ingestion.

    Raises
    ------
    RuntimeError
        If no run context can be determined.
    """
    target_run = run

    if not target_run and not tracker.current_consist:
        if artifact.run_id:
            target_run = tracker.get_run(artifact.run_id)
            if target_run:
                logging.info(
                    "[Consist] Ingesting in Analysis Mode. Attributing to Run: %s",
                    target_run.id,
                )

    if not target_run and tracker.current_consist:
        target_run = tracker.current_consist.run

    if not target_run:
        raise RuntimeError("Cannot ingest: Could not determine associated Run context.")

    return target_run


def resolve_ingest_data(
    *,
    tracker: "Tracker",
    artifact: Artifact,
    data: Optional[Union[Iterable[dict[str, Any]], Any]],
) -> Any:
    """
    Resolve the data payload passed to ingestion.

    Parameters
    ----------
    tracker : Tracker
        Tracker instance used for URI resolution.
    artifact : Artifact
        Artifact being ingested.
    data : Optional[Union[Iterable[dict[str, Any]], Any]]
        Explicit data payload. If None, the artifact URI is resolved and used
        as the ingestion source.

    Returns
    -------
    Any
        Data payload suitable for ingestion.
    """
    if data is not None:
        return data
    return tracker.resolve_uri(artifact.container_uri)


def ingest_artifact(
    *,
    tracker: "Tracker",
    artifact: Artifact,
    data: Optional[Union[Iterable[dict[str, Any]], Any]],
    schema: Optional[type[SQLModel]],
    run: Optional[Run],
    profile_schema: bool,
) -> Any:
    """
    Ingest artifact data into the Consist database and update metadata.

    Parameters
    ----------
    tracker : Tracker
        Tracker instance used for context, DB updates, and schema profiling.
    artifact : Artifact
        Artifact representing the data being ingested.
    data : Optional[Union[Iterable[dict[str, Any]], Any]]
        Data payload; if None, the artifact URI is used as the source.
    schema : Optional[type[SQLModel]]
        Optional SQLModel schema for validation and ingestion.
    run : Optional[Run]
        Optional run to attribute ingestion to.
    profile_schema : bool
        If True, profile and persist schema metadata for the ingested table.

    Returns
    -------
    Any
        Result information from the ingestion backend.

    Raises
    ------
    RuntimeError
        If no database is configured.
    """
    if tracker.db_path is None:
        raise RuntimeError("Cannot ingest: db_path is not configured.")

    target_run = resolve_ingest_run(tracker=tracker, artifact=artifact, run=run)
    data_to_pass = resolve_ingest_data(tracker=tracker, artifact=artifact, data=data)

    if tracker.engine:
        tracker.engine.dispose()

    from consist.integrations.dlt_loader import ingest_artifact as ingest_with_dlt

    info, resource_name = ingest_with_dlt(
        artifact=artifact,
        run_context=target_run,
        db_path=tracker.db_path,
        data_iterable=data_to_pass,
        schema_model=schema,
        lock_retries=tracker._dlt_lock_retries,
        lock_base_sleep_seconds=tracker._dlt_lock_base_sleep_seconds,
        lock_max_sleep_seconds=tracker._dlt_lock_max_sleep_seconds,
    )

    if tracker.db:
        tracker.db.update_artifact_meta(
            artifact, {"is_ingested": True, "dlt_table_name": resource_name}
        )

        if profile_schema:
            tracker.artifact_schemas.profile_ingested_table(
                artifact=artifact,
                run=target_run,
                table_schema="global_tables",
                table_name=resource_name,
            )

    return info
