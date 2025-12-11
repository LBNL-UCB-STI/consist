from typing import Optional, Dict, TYPE_CHECKING

from consist.models.artifact import Artifact

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


class Coupler:
    """
    Lightweight helper to thread named artifacts between steps.

    Keeps the latest artifact per key and can optionally hydrate absolute paths
    using the provided tracker.
    """

    def __init__(self, tracker: Optional["Tracker"] = None) -> None:
        self.tracker = tracker
        self._artifacts: Dict[str, Artifact] = {}

    def set(self, key: str, artifact: Artifact) -> Artifact:
        self._artifacts[key] = artifact
        return artifact

    def get(self, key: str) -> Optional[Artifact]:
        art = self._artifacts.get(key)
        if art and self.tracker and not art.abs_path:
            try:
                art.abs_path = self.tracker.resolve_uri(art.uri)
            except Exception:
                pass
        return art

    def get_cached(self, key: Optional[str] = None) -> Optional[Artifact]:
        """Deprecated alias for get_cached_output."""
        return self.get_cached_output(key=key)

    def get_cached_output(self, key: Optional[str] = None) -> Optional[Artifact]:
        """
        Return a hydrated cached output for the *current step run* if this step is a cache hit.

        This checks tracker.cached_output(), which is populated only when the active run
        matched a prior run and its outputs were hydrated at start_run/begin_run time.
        """
        if not self.tracker:
            return None
        art = self.tracker.cached_output(key=key)
        if art:
            self.set(key or art.key, art)
        return art
