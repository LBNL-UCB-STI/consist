"""Optional Ibis bridge for Consist DuckDB views."""

from __future__ import annotations

from contextlib import contextmanager
import importlib
import uuid
from types import ModuleType
from typing import TYPE_CHECKING, Any, Iterable, Iterator, Literal, cast

import duckdb
from sqlmodel import SQLModel

from consist.core.context import get_active_tracker, get_default_tracker
from consist.core.stores import get_hot_data_db_path

if TYPE_CHECKING:
    from ibis.backends.duckdb import Backend as IbisDuckDBBackend
    from ibis.expr.types import Table as IbisTable
    from consist.core.tracker import Tracker

_IBIS_INSTALL_HINT = (
    "Ibis support is optional. Install it with `pip install -e '.[ibis]'` or "
    "`pip install 'consist[ibis]'`."
)
_TRACKER_HINT = (
    "No active Consist run and no default tracker configured. "
    "Use consist.set_current_tracker(tracker) or "
    "with consist.use_tracker(tracker): ..."
)
_DB_HINT = (
    "Consist tracker has no DuckDB database configured. "
    "Create the tracker with db_path=... or pass a tracker that already has one."
)
_OPEN_CONNECTION_HINT = (
    "A Consist DuckDB connection is already open for this tracker. "
    "Close the active SQLAlchemy session or exit the current run before using "
    "Ibis, because ibis.connect() opens its own DuckDB handle."
)


def _import_ibis() -> ModuleType:
    """Import the optional :mod:`ibis` package.

    Returns
    -------
    ModuleType
        The imported Ibis module.

    Raises
    ------
    ImportError
        Raised with an install hint if the ``consist[ibis]`` extra is missing.
    """
    try:
        return importlib.import_module("ibis")
    except ImportError as exc:
        raise ImportError(_IBIS_INSTALL_HINT) from exc


def _resolve_tracker(tracker: "Tracker | None") -> "Tracker":
    """Resolve a tracker from an explicit argument or process state.

    Parameters
    ----------
    tracker
        Explicit tracker to use. When ``None``, the active tracker is used
        first, then the default tracker if no active tracker is available.

    Returns
    -------
    Tracker
        The resolved tracker.

    Raises
    ------
    RuntimeError
        Raised when neither an active nor a default tracker exists.
    """
    if tracker is not None:
        return tracker
    try:
        return get_active_tracker()
    except RuntimeError:
        default = get_default_tracker()
        if default is None:
            raise RuntimeError(_TRACKER_HINT) from None
        return default


def _require_db_path(tracker: "Tracker") -> str:
    """Return the tracker DuckDB path or fail with a targeted error.

    Parameters
    ----------
    tracker
        Tracker expected to have a configured hot-data DuckDB database.

    Returns
    -------
    str
        Path to the tracker DuckDB database.

    Raises
    ------
    RuntimeError
        Raised when the tracker has no configured DuckDB database.
    """
    db_path = get_hot_data_db_path(tracker)
    if not db_path:
        raise RuntimeError(_DB_HINT)
    return db_path


def _connect_backend(db_path: str) -> "IbisDuckDBBackend":
    """Connect Ibis to a Consist DuckDB database.

    Parameters
    ----------
    db_path
        Path to the DuckDB database backing the Consist tracker.

    Returns
    -------
    IbisDuckDBBackend
        Connected Ibis DuckDB backend.

    Raises
    ------
    ImportError
        Raised when the optional Ibis dependency is unavailable.
    RuntimeError
        Raised when DuckDB refuses the connection because another Consist
        connection is already open for the same database.
    """
    ibis = _import_ibis()
    connect = cast(Any, ibis).connect
    try:
        return cast("IbisDuckDBBackend", connect(db_path))
    except duckdb.ConnectionException as exc:
        raise RuntimeError(_OPEN_CONNECTION_HINT) from exc


def ibis_connection(
    tracker: "Tracker | None" = None,
) -> "IbisDuckDBBackend":
    """Return an Ibis backend connected to the tracker DuckDB database.

    Parameters
    ----------
    tracker
        Tracker to resolve explicitly. When omitted, the active tracker is used
        first and the default tracker is used as a fallback.

    Returns
    -------
    IbisDuckDBBackend
        Native Ibis DuckDB backend connected to the tracker database.

    Raises
    ------
    ImportError
        Raised when the optional ``consist[ibis]`` dependency is not
        installed.
    RuntimeError
        Raised when no tracker can be resolved, no DuckDB database is
        configured, or the database is already open through another Consist
        connection.

    Notes
    -----
    This opens a separate DuckDB handle for analysis-time queries. Do not call
    it while a Consist SQLAlchemy session is still open for the same tracker.
    """
    resolved_tracker = _resolve_tracker(tracker)
    db_path = _require_db_path(resolved_tracker)
    return _connect_backend(db_path)


def ibis_view(
    tracker: "Tracker | None" = None,
    *,
    model: type[SQLModel],
    key: str | None = None,
) -> "IbisTable":
    """Return a native Ibis table for a Consist typed view.

    Parameters
    ----------
    tracker
        Tracker to resolve explicitly. When omitted, the active tracker is used
        first and the default tracker is used as a fallback.
    model
        SQLModel type used to build the typed Consist view.
    key
        Optional typed-view key passed through to :meth:`Tracker.view`.

    Returns
    -------
    IbisTable
        Native Ibis table expression for the generated view name.

    Raises
    ------
    ImportError
        Raised when the optional ``consist[ibis]`` dependency is not
        installed.
    RuntimeError
        Raised when no tracker can be resolved, no DuckDB database is
        configured, the generated view cannot be created, or the DuckDB file is
        already open through another Consist connection.

    Notes
    -----
    The tracker view is created or refreshed first through the existing Consist
    view path, then the resulting DuckDB view name is opened through Ibis. The
    returned object is a native Ibis table expression, not a Consist wrapper.
    """
    resolved_tracker = _resolve_tracker(tracker)
    _require_db_path(resolved_tracker)
    view_cls = resolved_tracker.view(model, key)
    view_name = cast(str, view_cls.__tablename__)
    return ibis_connection(resolved_tracker).table(view_name)


def _resolve_grouped_view_schema_id(
    resolved_tracker: "Tracker",
    *,
    artifact_id: uuid.UUID,
) -> str:
    """Resolve the stored schema id for a tracked artifact.

    Parameters
    ----------
    resolved_tracker
        Tracker whose database should be queried.
    artifact_id
        Artifact identifier to resolve.

    Returns
    -------
    str
        Stored schema id linked to the artifact.

    Raises
    ------
    RuntimeError
        Raised when the artifact has not been profiled or no schema observation
        can be resolved.
    """
    db = resolved_tracker.db
    if db is None:
        raise RuntimeError(_DB_HINT)
    fetched = db.get_artifact_schema_for_artifact(artifact_id=artifact_id)
    if fetched is None:
        raise RuntimeError(
            "No stored schema was found for artifact_id="
            f"{artifact_id}. Ingest or profile the artifact first, then call "
            "ibis_grouped_view(...) again."
        )
    schema, _fields = fetched
    return schema.id


@contextmanager
def ibis_grouped_view(
    tracker: "Tracker | None" = None,
    *,
    view_name: str,
    artifact_id: uuid.UUID,
    namespace: str | None = None,
    params: Iterable[str] | None = None,
    drivers: list[str] | None = None,
    attach_facets: list[str] | None = None,
    include_system_columns: bool = True,
    mode: Literal["hybrid", "hot_only", "cold_only"] = "hybrid",
    if_exists: Literal["replace", "error"] = "replace",
    missing_files: Literal["warn", "error", "skip_silent"] = "warn",
    run_id: str | None = None,
    parent_run_id: str | None = None,
    model: str | None = None,
    status: str | None = None,
    year: int | None = None,
    iteration: int | None = None,
    schema_compatible: bool = False,
) -> Iterator["IbisTable"]:
    """Create a grouped Consist view and expose it as a native Ibis table.

    Parameters
    ----------
    tracker
        Tracker to resolve explicitly. When omitted, the active tracker is used
        first and the default tracker is used as a fallback.
    view_name
        Name of the DuckDB view to create and query.
    artifact_id
        Artifact whose stored schema defines the grouped view.
    namespace
        Default facet namespace applied to unqualified predicate expressions.
    params
        Facet predicate expressions passed through to
        :meth:`Tracker.create_grouped_view`.
    drivers
        Optional artifact-driver filter, e.g. ``["parquet"]``.
    attach_facets
        Facet key paths to project into the view as typed ``facet_<key>``
        columns.
    include_system_columns
        Whether to include Consist system columns in the view.
    mode
        Which storage tier(s) to include in the view.
    if_exists
        Behavior when ``view_name`` already exists.
    missing_files
        Behavior when a selected cold file is missing.
    run_id
        Optional exact run-id filter.
    parent_run_id
        Optional parent/scenario run-id filter.
    model
        Optional run model-name filter.
    status
        Optional run status filter.
    year
        Optional run year filter.
    iteration
        Optional run iteration filter.
    schema_compatible
        If True, allow schema-compatible subset/superset variants by field
        names in addition to exact schema matches.

    Yields
    ------
    IbisTable
        Native Ibis table expression for the generated DuckDB view.

    Raises
    ------
    ImportError
        Raised when the optional ``consist[ibis]`` dependency is not
        installed.
    RuntimeError
        Raised when no tracker can be resolved, no DuckDB database is
        configured, no schema is linked to the artifact, the generated view
        cannot be created, the view name cannot be opened, or another Consist
        connection already has the DuckDB file open.
    """
    resolved_tracker = _resolve_tracker(tracker)
    _require_db_path(resolved_tracker)
    schema_id = _resolve_grouped_view_schema_id(
        resolved_tracker, artifact_id=artifact_id
    )
    resolved_tracker.create_grouped_view(
        view_name=view_name,
        schema_id=schema_id,
        namespace=namespace,
        params=params,
        drivers=drivers,
        attach_facets=attach_facets,
        include_system_columns=include_system_columns,
        mode=mode,
        if_exists=if_exists,
        missing_files=missing_files,
        run_id=run_id,
        parent_run_id=parent_run_id,
        model=model,
        status=status,
        year=year,
        iteration=iteration,
        schema_compatible=schema_compatible,
    )

    backend = ibis_connection(resolved_tracker)
    ibis = _import_ibis()
    ibis_exceptions = cast(Any, ibis).common.exceptions
    try:
        try:
            table = backend.table(view_name)
        except ibis_exceptions.TableNotFound as exc:
            raise RuntimeError(
                f"Grouped view {view_name!r} could not be opened through Ibis. "
                "Ensure the view was created successfully before querying it."
            ) from exc
        yield table
    finally:
        backend.disconnect()
