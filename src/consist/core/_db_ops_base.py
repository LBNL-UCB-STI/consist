from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from consist.core.persistence import DatabaseManager


class _DatabaseOpsBase:
    """Internal base for helpers delegating to an owning ``DatabaseManager``."""

    def __init__(self, db: "DatabaseManager") -> None:
        self._db = db

    @property
    def db(self) -> "DatabaseManager":
        """Return the concrete owning ``DatabaseManager`` instance."""
        return self._db
