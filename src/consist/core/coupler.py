from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, TYPE_CHECKING

from consist.core._coupler_shared import CouplerMapMixin
from consist.models.artifact import Artifact
from consist.core.validation import validate_artifact_key

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


def _normalize_namespace(namespace: str) -> str:
    if not isinstance(namespace, str):
        raise TypeError("Coupler namespace must be a string.")
    normalized = namespace.strip().strip("/")
    if not normalized:
        raise ValueError("Coupler namespace cannot be empty.")
    validate_artifact_key(normalized)
    return normalized


class CouplerView:
    """
    Namespace-scoped view over a coupler keyspace.

    Values written through the view are stored in the parent coupler under
    `<namespace>/<key>`, preserving global queryability while providing a
    narrower local API for step code.
    """

    def __init__(self, coupler: Any, namespace: str) -> None:
        self._coupler = coupler
        self.namespace = _normalize_namespace(namespace)
        self._prefix = f"{self.namespace}/"

    def _qualify(self, key: str) -> str:
        validate_artifact_key(key)
        return f"{self._prefix}{key}"

    def _strip_prefix(self, key: str) -> Optional[str]:
        if not key.startswith(self._prefix):
            return None
        return key[len(self._prefix) :]

    def qualify(self, key: str) -> str:
        """Return the fully-qualified coupler key for a namespace-local key."""
        return self._qualify(key)

    def view(self, namespace: str) -> "CouplerView":
        """Create a nested namespace view rooted under this view."""
        child = _normalize_namespace(namespace)
        return CouplerView(self._coupler, f"{self.namespace}/{child}")

    def set(self, key: str, artifact: Any) -> Any:
        return self._coupler.set(self._qualify(key), artifact)

    def set_from_artifact(self, key: str, value: Any) -> Any:
        return self._coupler.set_from_artifact(self._qualify(key), value)

    def update(
        self, artifacts: Optional[Mapping[str, Any]] = None, /, **kwargs: Any
    ) -> None:
        if artifacts:
            for key, artifact in artifacts.items():
                self.set(key, artifact)
        if kwargs:
            for key, artifact in kwargs.items():
                self.set(key, artifact)

    def get(self, key: str) -> Optional[Any]:
        return self._coupler.get(self._qualify(key))

    def require(self, key: str) -> Any:
        return self._coupler.require(self._qualify(key))

    def path(self, key: str, *, required: bool = True) -> Optional[Path]:
        return self._coupler.path(self._qualify(key), required=required)

    def keys(self) -> Iterable[str]:
        return [
            local_key
            for local_key in (self._strip_prefix(key) for key in self._coupler.keys())
            if local_key is not None
        ]

    def items(self) -> Iterable[tuple[str, Any]]:
        return [
            (local_key, artifact)
            for key, artifact in self._coupler.items()
            for local_key in [self._strip_prefix(key)]
            if local_key is not None
        ]

    def values(self) -> Iterable[Any]:
        return [artifact for _, artifact in self.items()]

    def declare_outputs(
        self,
        *names: str,
        required: bool | Mapping[str, bool] = False,
        warn_undefined: bool = False,
        description: Optional[Mapping[str, str]] = None,
    ) -> None:
        if isinstance(required, Mapping):
            required = {
                self._qualify(str(key)): bool(value) for key, value in required.items()
            }
        qualified_names = [self._qualify(name) for name in names]
        qualified_description = (
            {self._qualify(str(key)): value for key, value in description.items()}
            if description is not None
            else None
        )
        self._coupler.declare_outputs(
            *qualified_names,
            required=required,
            warn_undefined=warn_undefined,
            description=qualified_description,
        )

    def require_outputs(
        self,
        *names: str,
        required: bool | Mapping[str, bool] = True,
        warn_undefined: bool = False,
        description: Optional[Mapping[str, str]] = None,
    ) -> None:
        self.declare_outputs(
            *names,
            required=required,
            warn_undefined=warn_undefined,
            description=description,
        )

    def missing_declared_outputs(self) -> list[str]:
        return sorted(
            [
                local_key
                for local_key in (
                    self._strip_prefix(key)
                    for key in self._coupler.missing_declared_outputs()
                )
                if local_key is not None
            ]
        )

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        try:
            return self._qualify(key) in self._coupler
        except (TypeError, ValueError):
            return False

    def __getitem__(self, key: str) -> Any:
        return self.require(key)

    def __setitem__(self, key: str, artifact: Any) -> None:
        self.set(key, artifact)


@dataclass
class DeclaredOutput:
    required: bool = False
    description: Optional[str] = None


class Coupler(CouplerMapMixin[Artifact, DeclaredOutput]):
    """
    Scenario-local helper to thread named artifacts between steps.

    Coupler is intentionally small:
    - It stores the "latest Artifact for a semantic key" in-memory.

    It does not log artifacts, infer inputs/outputs, or mutate Artifacts as a side effect
    of reads. Keep provenance operations on the Tracker.
    """

    def __init__(self, tracker: Optional["Tracker"] = None) -> None:
        self.tracker = tracker
        self._artifacts: Dict[str, Artifact] = {}
        self._declared_outputs: Dict[str, DeclaredOutput] = {}
        self._warn_undefined = False
        self._undefined_keys: set[str] = set()
        self._output_descriptions: Dict[str, str] = {}

    def _validate_artifact_key(self, key: str) -> None:
        validate_artifact_key(key)

    def _create_declared_output(self) -> DeclaredOutput:
        return DeclaredOutput()

    def set(self, key: str, artifact: Artifact) -> Artifact:
        """Store an artifact under a validated key."""
        return self._store_artifact(key, artifact)

    def set_from_artifact(self, key: str, value: Any) -> Any:
        """
        Set an artifact, accepting both Artifact objects and artifact-like values.

        This method is useful when integrating with optional dependencies (like noop mode)
        where you may receive either:
        - A real Artifact (when tracking is enabled)
        - An artifact-like object with .path and .container_uri properties (noop mode)
        - A Path or string (fallback)

        All three forms are stored in the coupler and can be retrieved with get() or require().

        Parameters
        ----------
        key : str
            The coupler key (e.g., "persons", "skims"). Must follow artifact-key rules.
        value : Artifact or artifact-like or Path or str
            The value to store. Can be a real Artifact, artifact-like object with
            .path/.container_uri properties, a Path, or a string path.

        Returns
        -------
        Any
            The value that was stored.

        Examples
        --------
        Using with optional Consist dependency:
        ```python
        # Works whether log_output returns Artifact or NoopArtifact
        artifact = tracker.log_output(path, key="persons")
        coupler.set_from_artifact("persons", artifact)
        ```

        Mixed real and fallback artifacts:
        ```python
        artifact = log_output(path) or path  # Fallback to path string
        coupler.set_from_artifact("key", artifact)  # Handles both
        ```
        """
        return self._store_artifact(key, value)

    def keys(self) -> Iterable[str]:
        return self._artifacts.keys()

    def items(self) -> Iterable[tuple[str, Artifact]]:
        return self._artifacts.items()

    def values(self) -> Iterable[Artifact]:
        return self._artifacts.values()

    def view(self, namespace: str) -> CouplerView:
        """
        Return a namespace-scoped coupler view.

        The returned view writes keys as `<namespace>/<key>` into this coupler
        while allowing reads and writes using namespace-local key names.
        """
        return CouplerView(self, namespace)

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
        return Path(self.tracker.resolve_uri(artifact.container_uri))
