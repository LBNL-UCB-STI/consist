from __future__ import annotations

from dataclasses import dataclass
import os
import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from consist.core.persistence import DatabaseManager
    from consist.core.tracker import Tracker

_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _is_safe_identifier(identifier: str) -> bool:
    return bool(_SAFE_IDENTIFIER_RE.fullmatch(identifier))


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


@dataclass(slots=True)
class MetadataStore:
    """Metadata-store facade for single-store mode backed by ``DatabaseManager``."""

    db: DatabaseManager

    @property
    def db_path(self) -> str:
        return self.db.db_path

    def close(self) -> None:
        self.db.close()


@dataclass(slots=True)
class HotDataStore:
    """
    Hot-data-store facade for single-store mode backed by ``DatabaseManager``.

    Hot data (``global_tables.*``) shares the same local DuckDB connection
    managed by ``DatabaseManager`` as metadata.
    """

    db_path: str
    metadata_store: MetadataStore | None = None

    def __post_init__(self) -> None:
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
    def db(self) -> DatabaseManager | None:
        self._validate_single_store_alignment()
        if self.metadata_store is None:
            return None
        return self.metadata_store.db

    def close(self) -> None:
        db = self.db
        if db is not None:
            db.close()

    def ingest_cache_hit(self, table_name: str, content_hash: str) -> bool:
        self._validate_single_store_alignment()
        db = self.db
        if db is None:
            return False
        if not _is_safe_identifier(table_name):
            return False

        if not db.has_table_column(
            table_schema="global_tables",
            table_name=table_name,
            column_name="content_hash",
        ):
            return False

        table_ref = f"{_quote_ident('global_tables')}.{_quote_ident(table_name)}"
        row = db.fetchone(
            f"SELECT 1 FROM {table_ref} WHERE content_hash = ? LIMIT 1",
            [content_hash],
        )
        return row is not None


def build_metadata_store(db: DatabaseManager) -> MetadataStore:
    return MetadataStore(db=db)


def build_hot_data_store(
    db: DatabaseManager,
    metadata_store: MetadataStore | None = None,
) -> HotDataStore:
    resolved_meta = metadata_store or MetadataStore(db=db)
    return HotDataStore(db_path=db.db_path, metadata_store=resolved_meta)


def get_hot_data_store(tracker: Tracker | Any | None) -> HotDataStore | None:
    if tracker is None:
        return None
    try:
        return tracker.hot_data_store
    except AttributeError:
        return None


def get_hot_data_db_path(tracker: Tracker | Any | None) -> str | None:
    store = get_hot_data_store(tracker)
    if store is not None:
        try:
            store_path = store.db_path
        except AttributeError:
            store_path = None
        if store_path is not None:
            return os.fspath(store_path)
    if tracker is None:
        return None
    try:
        db_path = tracker.db_path
    except AttributeError:
        return None
    if db_path is None:
        return None
    return os.fspath(db_path)


def get_hot_data_db(tracker: Tracker | Any | None) -> DatabaseManager | None:
    store = get_hot_data_store(tracker)
    if store is not None:
        try:
            return store.db
        except AttributeError:
            return None
    if tracker is None:
        return None
    try:
        return tracker.db
    except AttributeError:
        return None
