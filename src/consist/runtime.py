from __future__ import annotations

import logging
from typing import Callable, Optional

from consist.core.noop import NoopTracker
from consist.protocols import TrackerLike


def create_tracker(
    *,
    enabled: bool = True,
    tracker_factory: Optional[Callable[[], TrackerLike]] = None,
) -> TrackerLike:
    """
    Create a tracker instance or a NoopTracker based on configuration.

    Parameters
    ----------
    enabled : bool, default True
        If False, always return a NoopTracker.
    tracker_factory : Optional[Callable[[], TrackerLike]], optional
        Factory that returns a real tracker instance. Required when enabled=True.

    Returns
    -------
    TrackerLike
        A real tracker from the factory, or a NoopTracker when disabled or on error.
    """
    if not enabled:
        logging.info("[Consist] Tracking disabled; using NoopTracker.")
        return NoopTracker()

    if tracker_factory is None:
        raise ValueError("create_tracker requires tracker_factory when enabled=True.")

    try:
        return tracker_factory()
    except Exception as exc:
        logging.error(
            "[Consist] Tracker initialization failed (%s); falling back to NoopTracker.",
            exc,
        )
        return NoopTracker()
