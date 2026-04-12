from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from consist.core.persistence import DatabaseManager


class _DatabaseOpsBase:
    def __init__(self, db: "DatabaseManager") -> None:
        self._db = db

    def __getattr__(self, name: str) -> Any:
        return getattr(self._db, name)
