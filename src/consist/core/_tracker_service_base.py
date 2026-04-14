from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


class _TrackerServiceBase:
    """
    Internal transitional base for services extracted from ``Tracker``.

    These service classes were split out of ``Tracker`` to reduce file size and
    localize related behavior, but they still operate over the concrete owning
    ``Tracker`` instance. The forwarding behavior here is intentionally a
    compatibility shim so extracted methods can move with minimal mechanical
    rewrite during the refactor.

    This is not intended to be a strong abstraction boundary. Dependencies
    remain Tracker-shaped, and implicit forwarding can blur ownership if it is
    overused. Prefer explicit ``self.tracker`` / ``self._tracker`` access in new
    code when practical so service dependencies stay visible over time.
    """

    def __init__(self, tracker: "Tracker") -> None:
        self._tracker = tracker

    @property
    def tracker(self) -> "Tracker":
        """Return the concrete owning ``Tracker`` instance."""
        return self._tracker

    def __getattr__(self, name: str) -> Any:
        # Transitional compatibility shim for extracted service code. This keeps
        # method bodies stable while logic moves out of Tracker, but it should
        # not be mistaken for a hard service contract.
        return getattr(self._tracker, name)
