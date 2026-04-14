from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from consist.core.persistence import DatabaseManager


class _DatabaseOpsBase:
    """
    Internal transitional base for helpers extracted from ``DatabaseManager``.

    Like ``_TrackerServiceBase``, this class keeps extracted database helpers
    operating against the concrete owner while the persistence refactor settles.
    It intentionally favors low-churn extraction over a finalized dependency
    boundary.

    New code should prefer explicit ``self.db`` / ``self._db`` access where
    practical. The forwarding shim is here to preserve behavior during the
    extraction, not to hide long-term contracts.
    """

    def __init__(self, db: "DatabaseManager") -> None:
        self._db = db

    @property
    def db(self) -> "DatabaseManager":
        """Return the concrete owning ``DatabaseManager`` instance."""
        return self._db

    def __getattr__(self, name: str) -> Any:
        # Transitional compatibility shim for extracted DB helper code.
        return getattr(self._db, name)
