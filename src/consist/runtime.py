from __future__ import annotations

from typing import Callable, Optional
import warnings

from consist.protocols import TrackerLike


def create_tracker(
    *,
    enabled: bool = True,
    tracker_factory: Optional[Callable[[], TrackerLike]] = None,
) -> TrackerLike:
    """
    Create a tracker instance via ``tracker_factory``.

    .. deprecated:: 0.5.0
        Instantiate :class:`consist.Tracker` directly instead. ``NoopTracker`` and
        ``enabled=False`` have been removed; use ``Tracker()`` (Ephemeral Mode) or
        ``Tracker(run_dir=..., db_path=None)`` (JSON-Only Mode) for database-free
        execution.

    Parameters
    ----------
    enabled : bool, default True
        Must be ``True``. Passing ``False`` raises ``ValueError``.
    tracker_factory : Optional[Callable[[], TrackerLike]], optional
        Factory that returns a tracker instance. Required.

    Returns
    -------
    TrackerLike
        The tracker instance created by ``tracker_factory``.
    """
    warnings.warn(
        "consist.create_tracker is deprecated; instantiate consist.Tracker(...) directly.",
        DeprecationWarning,
        stacklevel=2,
    )
    if not enabled:
        raise ValueError(
            "consist.create_tracker with enabled=False is no longer supported; "
            "NoopTracker was removed in Consist v0.5. Use Tracker() (Ephemeral Mode) "
            "or Tracker(run_dir=..., db_path=None) (JSON-Only Mode) for database-free "
            "execution."
        )

    if tracker_factory is None:
        raise ValueError("create_tracker requires tracker_factory when enabled=True.")

    return tracker_factory()
