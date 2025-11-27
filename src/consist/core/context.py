"""
This module provides a mechanism for managing the active Consist `Tracker` instance
globally within a thread.

It uses a thread-safe stack (`_TRACKER_STACK`) to keep track of nested `start_run`
contexts, ensuring that `log_artifact` and other context-dependent operations
always refer to the correct `Tracker` instance.
"""

from typing import Optional, List, TYPE_CHECKING

if TYPE_CHECKING:
    from consist.core.tracker import Tracker

# Thread-safe storage for the active tracker stack
_TRACKER_STACK: List["Tracker"] = []


def get_active_tracker() -> "Tracker":
    """
    Returns the currently active Tracker instance.
    Raises RuntimeError if no run is currently in progress.
    """
    if not _TRACKER_STACK:
        raise RuntimeError(
            "No active Consist run found. "
            "Ensure you are within a 'with tracker.start_run():' block "
            "or have manually set the active tracker."
        )
    return _TRACKER_STACK[-1]


def push_tracker(tracker: "Tracker"):
    """
    Pushes a Tracker instance onto the active tracker stack.
    This makes the provided tracker the currently active one for the current thread.
    """
    _TRACKER_STACK.append(tracker)


def pop_tracker() -> Optional["Tracker"]:
    """
    Removes and returns the current active Tracker instance from the stack.
    Returns None if the stack is empty.
    """
    if _TRACKER_STACK:
        return _TRACKER_STACK.pop()
    return None
