from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, Optional, TYPE_CHECKING

from consist.models.artifact import Artifact

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


class Coupler:
    """
    Scenario-local helper to thread named artifacts between steps.

    Coupler is intentionally small:
    - It stores the "latest Artifact for a semantic key" in-memory.
    - It can "adopt" a cached output from the current step (when a cache hit occurred).

    It does not log artifacts, infer inputs/outputs, or mutate Artifacts as a side effect
    of reads. Keep provenance operations on the Tracker.
    """

    def __init__(self, tracker: Optional["Tracker"] = None) -> None:
        self.tracker = tracker
        self._artifacts: Dict[str, Artifact] = {}

    def set(self, key: str, artifact: Artifact) -> Artifact:
        self._artifacts[key] = artifact
        return artifact

    def update(self, artifacts: Optional[Dict[str, Artifact]] = None, /, **kwargs: Artifact) -> None:
        """
        Bulk-update the coupler mapping.

        Examples
        --------
        `coupler.update({"persons": art})` or `coupler.update(persons=art)`
        """
        if artifacts:
            self._artifacts.update(artifacts)
        if kwargs:
            self._artifacts.update(kwargs)

    def get(self, key: str) -> Optional[Artifact]:
        """Return the current artifact for `key`, or None if unset."""
        return self._artifacts.get(key)

    def require(self, key: str) -> Artifact:
        """Return the artifact for `key`, raising a clear error if unset."""
        artifact = self.get(key)
        if artifact is None:
            available = ", ".join(sorted(self._artifacts.keys())) or "<none>"
            raise KeyError(
                f"Coupler missing key={key!r}. Available keys: {available}. "
                f"Did you forget to call coupler.set({key!r}, ...)?"
            )
        return artifact

    def pop(self, key: str, default: Optional[Artifact] = None) -> Optional[Artifact]:
        return self._artifacts.pop(key, default)

    def keys(self) -> Iterable[str]:
        return self._artifacts.keys()

    def items(self) -> Iterable[tuple[str, Artifact]]:
        return self._artifacts.items()

    def values(self) -> Iterable[Artifact]:
        return self._artifacts.values()

    def __contains__(self, key: object) -> bool:
        return key in self._artifacts

    def __getitem__(self, key: str) -> Artifact:
        return self.require(key)

    def __setitem__(self, key: str, artifact: Artifact) -> None:
        self.set(key, artifact)

    def path(self, key: str, *, required: bool = True) -> Optional[Path]:
        """
        Resolve an artifact's URI to an absolute host path.

        This does not mutate the Artifact; it only returns a resolved Path.
        """
        artifact = self.require(key) if required else self.get(key)
        if not artifact:
            return None
        if not self.tracker:
            raise RuntimeError(
                f"Cannot resolve path for {key!r}: no tracker attached to Coupler."
            )
        return Path(self.tracker.resolve_uri(artifact.uri))

    def adopt_cached_output(self, key: Optional[str] = None) -> Optional[Artifact]:
        """
        If the *current step run* is a cache hit, adopt a hydrated cached output.

        This is a thin wrapper around `tracker.cached_output()`. When an artifact is found,
        it is stored into the Coupler under:
        - `key` if provided
        - otherwise `artifact.key`
        """
        if not self.tracker:
            return None
        artifact = self.tracker.cached_output(key=key)
        if artifact:
            self.set(key or artifact.key, artifact)
        return artifact

    def get_cached(self, key: Optional[str] = None) -> Optional[Artifact]:
        """Alias for `adopt_cached_output()` (kept for backwards compatibility)."""
        return self.adopt_cached_output(key=key)

    def get_cached_output(self, key: Optional[str] = None) -> Optional[Artifact]:
        """Alias for `adopt_cached_output()` (kept for backwards compatibility)."""
        return self.adopt_cached_output(key=key)
