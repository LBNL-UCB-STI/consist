"""
Context-local management for the active and default Consist ``Tracker``.

Synchronizes directly with ``consist.protocols``'s ``ArtifactResolver`` contextvars
so ``Artifact`` convenience methods automatically see the active or default tracker
without upward imports from Layer 1/2 into ``consist.core``.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator, Optional, TYPE_CHECKING, cast

from consist.protocols import (
    _ACTIVE_RESOLVER_STACK,
    _DEFAULT_RESOLVER,
    ArtifactResolver,
    get_active_resolver,
    get_default_resolver,
    pop_active_resolver,
    push_active_resolver,
    set_default_resolver,
)

if TYPE_CHECKING:
    from consist.core.tracker import Tracker

_TRACKER_STACK = _ACTIVE_RESOLVER_STACK
_DEFAULT_TRACKER = _DEFAULT_RESOLVER


def get_active_tracker() -> "Tracker":
    """Return the innermost active ``Tracker`` or raise ``RuntimeError``."""
    active = get_active_resolver()
    if active is None:
        raise RuntimeError(
            "No active Consist run found. "
            "Ensure you are within a 'with tracker.start_run()' or "
            "'with consist.start_run(...)' block, or use tracker.run/trace "
            "or consist.run/trace."
        )
    return cast("Tracker", active)


def push_tracker(tracker: "Tracker") -> None:
    """Push ``tracker`` onto the active tracker/resolver stack."""
    push_active_resolver(cast(ArtifactResolver, tracker))


def pop_tracker() -> Optional["Tracker"]:
    """Pop and return the top ``Tracker`` from the active stack, or ``None``."""
    return cast("Optional[Tracker]", pop_active_resolver())


def get_default_tracker() -> Optional["Tracker"]:
    """Return the default ``Tracker`` for the current context, if set."""
    return cast("Optional[Tracker]", get_default_resolver())


def set_default_tracker(tracker: Optional["Tracker"]) -> Optional["Tracker"]:
    """Set the default ``Tracker`` for the current context and return the previous one."""
    return cast(
        "Optional[Tracker]",
        set_default_resolver(cast("Optional[ArtifactResolver]", tracker)),
    )


@contextmanager
def use_tracker(tracker: "Tracker") -> Iterator["Tracker"]:
    """Temporarily set the default ``Tracker`` within a scoped context."""
    previous = set_default_tracker(tracker)
    try:
        yield tracker
    finally:
        set_default_tracker(previous)
