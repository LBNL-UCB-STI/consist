from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


class _TrackerServiceBase:
    def __init__(self, tracker: "Tracker") -> None:
        self._tracker = tracker

    def __getattr__(self, name: str) -> Any:
        return getattr(self._tracker, name)
