from __future__ import annotations

import re
from dataclasses import dataclass

from sqlalchemy.engine import Engine

from consist.core.persistence import DatabaseManager

_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _is_safe_identifier(identifier: str) -> bool:
    return bool(_SAFE_IDENTIFIER_RE.fullmatch(identifier))


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


@dataclass(slots=True)
class MetadataStore:
    """
    Metadata-store facade for single-store mode.

    In this phase, metadata persistence is still backed by the existing
    ``DatabaseManager`` over the configured local DuckDB file.
    """

    db: DatabaseManager

    @property
    def engine(self) -> Engine:
        return self.db.engine

    @property
    def db_path(self) -> str:
        return self.db.db_path


@dataclass(slots=True)
class HotDataStore:
    """
    Hot-data-store facade for single-store mode.

    In this phase, hot data (``global_tables.*``) still lives in the same local
    DuckDB file as metadata, but ownership is made explicit at the API boundary.
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
    def engine(self) -> Engine | None:
        self._validate_single_store_alignment()
        if self.metadata_store is None:
            return None
        return self.metadata_store.engine

    def dispose_engine(self) -> None:
        self._validate_single_store_alignment()
        engine = self.engine
        if engine is not None:
            engine.dispose()

    def ingest_cache_hit(self, table_name: str, content_hash: str) -> bool:
        self._validate_single_store_alignment()
        engine = self.engine
        if engine is None:
            return False
        if not _is_safe_identifier(table_name):
            return False

        table_ref = f"{_quote_ident('global_tables')}.{_quote_ident(table_name)}"
        try:
            with engine.begin() as connection:
                result = connection.exec_driver_sql(
                    f"SELECT 1 FROM {table_ref} WHERE content_hash = ? LIMIT 1",
                    (content_hash,),
                ).fetchone()
            return result is not None
        except Exception:
            return False
