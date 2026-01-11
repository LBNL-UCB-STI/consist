"""
This module provides a mechanism for managing the active Consist `Tracker` instance
within an async-safe context.

It uses a context-local stack (`_TRACKER_STACK`) to keep track of nested `start_run`
contexts, ensuring that `log_artifact` and other context-dependent operations
always refer to the correct `Tracker` instance.
"""

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator, Optional, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from consist.core.tracker import Tracker

# Context-local storage for the active tracker stack
_TRACKER_STACK: ContextVar[Sequence["Tracker"]] = ContextVar(
    "consist_tracker_stack", default=()
)
_DEFAULT_TRACKER: ContextVar[Optional["Tracker"]] = ContextVar(
    "consist_default_tracker", default=None
)


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
        a `with tracker.start_run():` block or a `tracker.run`/`tracker.trace`
        (or `consist.start_run`/`consist.run`/`consist.trace`) call.
    """
    stack = _TRACKER_STACK.get()
    if not stack:
        raise RuntimeError(
            "No active Consist run found. "
            "Ensure you are within a 'with tracker.start_run()' or "
            "'with consist.start_run(...)' block, or use tracker.run/trace "
            "or consist.run/trace."
        )
    return stack[-1]


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
    stack = _TRACKER_STACK.get()
    _TRACKER_STACK.set((*stack, tracker))


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
    stack = _TRACKER_STACK.get()
    if not stack:
        return None
    tracker = stack[-1]
    _TRACKER_STACK.set(stack[:-1])
    return tracker


def get_default_tracker() -> Optional["Tracker"]:
    """
    Returns the default `Tracker` for the current context, if set.
    """
    return _DEFAULT_TRACKER.get()


def set_default_tracker(tracker: Optional["Tracker"]) -> Optional["Tracker"]:
    """
    Set the default `Tracker` for the current context.

    Returns the previous default tracker, if any.
    """
    previous = _DEFAULT_TRACKER.get()
    _DEFAULT_TRACKER.set(tracker)
    return previous


@contextmanager
def use_tracker(tracker: "Tracker") -> Iterator["Tracker"]:
    """
    Temporarily set the default `Tracker` within a scoped context.
    """
    token = _DEFAULT_TRACKER.set(tracker)
    try:
        yield tracker
    finally:
        _DEFAULT_TRACKER.reset(token)
