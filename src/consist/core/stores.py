from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import re
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import duckdb

from consist.core.persistence import DatabaseManager

if TYPE_CHECKING:
    from consist.core.tracker import Tracker
    from consist.store.ledger import StoreLedger

_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _is_safe_identifier(identifier: str) -> bool:
    return bool(_SAFE_IDENTIFIER_RE.fullmatch(identifier))


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


@runtime_checkable
class _HasHotDataStore(Protocol):
    hot_data_store: HotDataStore | None


@runtime_checkable
class _HasDbPath(Protocol):
    db_path: str | os.PathLike[str] | None


@runtime_checkable
class _HasLedger(Protocol):
    ledger: StoreLedger | None


@dataclass(slots=True)
class MetadataStore:
    """
    Metadata-store facade for single-store mode backed by ``StoreLedger``.
    """

    db: DatabaseManager

    @property
    def ledger(self) -> StoreLedger:
        return self.db.ledger

    @property
    def db_path(self) -> str:
        return self.db.db_path


@dataclass(slots=True)
class HotDataStore:
    """
    Hot-data-store facade for single-store mode backed by ``StoreLedger``.

    Hot data (``global_tables.*``) shares the same local DuckDB ``StoreLedger``
    connection as metadata.
    """

    db_path: str
    metadata_store: MetadataStore | None = None

    def __post_init__(self) -> None:
        raw = str(self.db_path).strip()
        if raw and raw != ":memory:":
            self.db_path = str(Path(raw).expanduser().resolve())
        self._validate_single_store_alignment()

    def _validate_single_store_alignment(self) -> None:
        if self.metadata_store is None:
            return
        metadata_db_path = self.metadata_store.db_path
        if metadata_db_path != self.db_path:
            raise ValueError(
                "HotDataStore single-store invariant violated: hot-data db_path "
                f"{self.db_path!r} does not match metadata db_path "
                f"{metadata_db_path!r}."
            )

    @property
    def ledger(self) -> StoreLedger | None:
        self._validate_single_store_alignment()
        if self.metadata_store is None:
            return None
        return self.metadata_store.ledger

    def ingest_cache_hit(self, table_name: str, content_hash: str) -> bool:
        self._validate_single_store_alignment()
        ledger = self.ledger
        if ledger is None:
            return False
        if not _is_safe_identifier(table_name):
            return False

        table_ref = f"{_quote_ident('global_tables')}.{_quote_ident(table_name)}"
        try:
            with ledger.exec_cursor() as cur:
                result = cur.execute(
                    f"SELECT 1 FROM {table_ref} WHERE content_hash = ? LIMIT 1",
                    [content_hash],
                ).fetchone()
            return result is not None
        except duckdb.Error:
            return False


def get_hot_data_store(tracker: Tracker | Any | None) -> HotDataStore | None:
    if tracker is None or not isinstance(tracker, _HasHotDataStore):
        return None
    return tracker.hot_data_store


def get_hot_data_db_path(tracker: Tracker | Any | None) -> str | None:
    store = get_hot_data_store(tracker)
    if store is not None:
        return os.fspath(store.db_path)
    if tracker is None or not isinstance(tracker, _HasDbPath):
        return None
    db_path = tracker.db_path
    if db_path is None:
        return None
    return os.fspath(db_path)


def get_hot_data_ledger(tracker: Tracker | Any | None) -> StoreLedger | None:
    store = get_hot_data_store(tracker)
    if store is not None:
        return store.ledger
    if tracker is None or not isinstance(tracker, _HasLedger):
        return None
    return tracker.ledger
