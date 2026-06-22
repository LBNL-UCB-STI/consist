"""Optional Ibis bridge for Consist DuckDB views."""

from __future__ import annotations

import importlib
from types import ModuleType
from typing import TYPE_CHECKING, Any, cast

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
