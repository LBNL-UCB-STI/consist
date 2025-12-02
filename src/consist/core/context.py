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
    Retrieves the currently active `Tracker` instance from the thread-local stack.

    This function is essential for allowing Consist's API functions (e.g., `log_artifact`,
    `ingest`) to access the correct `Tracker` instance without it needing to be
    passed explicitly through every function call. It ensures that operations are
    always associated with the currently active `start_run` context.

    Returns
    -------
    Tracker
        The `Tracker` instance that is at the top of the active tracker stack
        for the current thread.

    Raises
    ------
    RuntimeError
        If no `Tracker` instance is currently active (i.e., the stack is empty).
        This typically means Consist API functions are being called outside
        a `with tracker.start_run():` block or a `@consist.task` decorated function.
    """
    if not _TRACKER_STACK:
        raise RuntimeError(
            "No active Consist run found. "
            "Ensure you are within a 'with tracker.start_run():' block "
            "or have manually set the active tracker."
        )
    return _TRACKER_STACK[-1]


def push_tracker(tracker: "Tracker") -> None:
    """
    Pushes a `Tracker` instance onto the top of the thread-local active tracker stack.

    This action makes the provided `tracker` the currently active one for the
    current thread, enabling subsequent Consist API calls to automatically
    interact with this `Tracker` instance.

    Parameters
    ----------
    tracker : Tracker
        The `Tracker` instance to be made active.
    """
    _TRACKER_STACK.append(tracker)


def pop_tracker() -> Optional["Tracker"]:
    """
    Removes and returns the current active `Tracker` instance from the top of the stack.

    This function is typically called when a `start_run` context exits, effectively
    deactivating the `Tracker` that was previously managing the run.

    Returns
    -------
    Optional[Tracker]
        The `Tracker` instance that was removed from the stack, or `None` if the
        stack was already empty.
    """
    if _TRACKER_STACK:
        return _TRACKER_STACK.pop()
    return None
