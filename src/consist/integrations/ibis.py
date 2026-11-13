"""Optional Ibis bridge for Consist DuckDB views over ``DatabaseManager``."""

from __future__ import annotations

from contextlib import contextmanager
import importlib
from types import ModuleType
from typing import TYPE_CHECKING, Any, Iterable, Iterator, Literal, cast
import uuid
import weakref

import duckdb
from pydantic import BaseModel

from consist.core.context import get_active_tracker, get_default_tracker
from consist.core.stores import get_hot_data_db
from consist.core.views import ViewFactory, _model_tablename

if TYPE_CHECKING:
    from ibis.backends.duckdb import Backend as IbisDuckDBBackend
    from ibis.expr.types import Table as IbisTable

    from consist.core.persistence import DatabaseManager
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

_CACHED_BACKENDS: weakref.WeakKeyDictionary[
    Tracker,
    tuple[str, DatabaseManager, duckdb.DuckDBPyConnection, IbisDuckDBBackend],
] = weakref.WeakKeyDictionary()


def _is_backend_connected(backend: IbisDuckDBBackend) -> bool:
    try:
        con = backend.con
    except AttributeError:
        return False
    if con is None:
        return False
    try:
        con.execute("SELECT 1")
    except duckdb.Error:
        return False
    return True


def _disconnect_backend_and_cursor(
    db: DatabaseManager,
    cursor: duckdb.DuckDBPyConnection,
    backend: IbisDuckDBBackend,
) -> None:
    try:
        backend.disconnect()
    except Exception:
        pass
    db.close_cursor(cursor)


def close_tracker_ibis_connection(tracker: Tracker) -> None:
    """Disconnect (closing and untracking the cloned cursor) and evict any cached Ibis backend."""
    cached = _CACHED_BACKENDS.pop(tracker, None)
    if cached is None:
        return
    _instance_key, cached_db, cached_cursor, backend = cached
    _disconnect_backend_and_cursor(cached_db, cached_cursor, backend)


def _import_ibis() -> ModuleType:
    """Import the optional :mod:`ibis` package."""
    try:
        return importlib.import_module("ibis")
    except ImportError as exc:
        raise ImportError(_IBIS_INSTALL_HINT) from exc


def _resolve_tracker(tracker: Tracker | None) -> Tracker:
    """Resolve a tracker from an explicit argument or process state."""
    if tracker is not None:
        return tracker
    try:
        return get_active_tracker()
    except RuntimeError:
        default = get_default_tracker()
        if default is None:
            raise RuntimeError(_TRACKER_HINT) from None
        return default


def _require_db(tracker: Tracker) -> DatabaseManager:
    """Return the tracker's ``DatabaseManager`` or fail with a targeted error."""
    db = get_hot_data_db(tracker) or tracker.db
    if db is None:
        raise RuntimeError(_DB_HINT)
    return db


def _connect_backend(
    tracker: Tracker, db: DatabaseManager
) -> tuple[duckdb.DuckDBPyConnection, IbisDuckDBBackend]:
    """Connect Ibis via a cloned ``DatabaseManager`` cursor."""
    ibis = _import_ibis()
    duckdb_backend_mod = cast(Any, ibis).duckdb
    cursor = db.cursor()
    tracker_ref = weakref.ref(tracker)

    def _on_suspend() -> None:
        t = tracker_ref()
        if t is not None:
            close_tracker_ibis_connection(t)

    db.register_suspend_hook(_on_suspend)
    try:
        backend = cast("IbisDuckDBBackend", duckdb_backend_mod.from_connection(cursor))
    except Exception:
        db.close_cursor(cursor)
        raise
    return cursor, backend


def ibis_connection(
    tracker: Tracker | None = None,
) -> IbisDuckDBBackend:
    """Return an Ibis backend connected via a cloned ``DatabaseManager`` cursor."""
    resolved_tracker = _resolve_tracker(tracker)
    db = _require_db(resolved_tracker)
    cached = _CACHED_BACKENDS.get(resolved_tracker)
    if cached is not None:
        cached_key, cached_db, _cached_cursor, cached_backend = cached
        if (
            cached_db is db
            and cached_key == db.instance_key
            and _is_backend_connected(cached_backend)
        ):
            return cached_backend
        close_tracker_ibis_connection(resolved_tracker)
    cursor, backend = _connect_backend(resolved_tracker, db)
    _CACHED_BACKENDS[resolved_tracker] = (db.instance_key, db, cursor, backend)
    return backend


def ibis_view(
    tracker: Tracker | None = None,
    *,
    model: type[BaseModel],
    key: str | None = None,
) -> IbisTable:
    """Create/refresh a hybrid view directly via ``ViewFactory`` and open it in Ibis."""
    resolved_tracker = _resolve_tracker(tracker)
    _require_db(resolved_tracker)
    concept_key = key or _model_tablename(model)
    view_name = f"v_{concept_key}"
    ViewFactory(resolved_tracker).create_hybrid_view(
        view_name,
        concept_key,
        schema_model=model,
    )
    return ibis_connection(resolved_tracker).table(view_name)


def _resolve_grouped_view_schema_id(
    resolved_tracker: Tracker,
    *,
    artifact_id: uuid.UUID,
) -> str:
    """Resolve the stored schema id for a tracked artifact."""
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
    tracker: Tracker | None = None,
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
) -> Iterator[IbisTable]:
    """Create a grouped Consist view and expose it as a native Ibis table."""
    resolved_tracker = _resolve_tracker(tracker)
    db = _require_db(resolved_tracker)
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
    entry = _CACHED_BACKENDS.get(resolved_tracker)
    entry_cursor = entry[2] if entry is not None and entry[3] is backend else None
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
        cached = _CACHED_BACKENDS.get(resolved_tracker)
        if cached is None or cached[3] is not backend:
            if entry_cursor is not None:
                _disconnect_backend_and_cursor(db, entry_cursor, backend)
            else:
                backend.disconnect()
