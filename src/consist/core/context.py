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
    _TRACKER_STACK.append(tracker)


def pop_tracker() -> Optional["Tracker"]:
    if _TRACKER_STACK:
        return _TRACKER_STACK.pop()
    return None
