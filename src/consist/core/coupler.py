from __future__ import annotations

from dataclasses import dataclass, replace
import os
from pathlib import Path
import stat
from typing import (
    Any,
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
    Optional,
    Protocol,
    TYPE_CHECKING,
    Union,
    runtime_checkable,
)
import uuid

from consist.artifact import Artifact as V2Artifact
from consist.core._coupler_shared import CouplerMapMixin
from consist.core.validation import validate_artifact_key
from consist.engine import observe
from consist.models.artifact import Artifact as LegacyArtifact
from consist.protocols import ArtifactLike

if TYPE_CHECKING:
    from consist.core.tracker import Tracker

Artifact = LegacyArtifact
CouplerArtifact = Union[V2Artifact, LegacyArtifact]


@runtime_checkable
class _UriResolverProtocol(Protocol):
    def resolve_uri(self, uri: str) -> str: ...


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

    def pop(self, key: str, default: Optional[Any] = None) -> Optional[Any]:
        return self._coupler.pop(self._qualify(key), default)

    def require(self, key: str) -> Any:
        return self._coupler.require(self._qualify(key))

    def history(self, key: str) -> tuple[Any, ...]:
        """Return the chronological history of artifacts stored for `key` in this view."""
        return self._coupler.history(self._qualify(key))

    def previous(self, key: str) -> Optional[Any]:
        """Return the previous artifact stored for `key` before the current one, or `None`."""
        return self._coupler.previous(self._qualify(key))

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

    def clear(self) -> None:
        """Clear all stored artifacts in this view namespace."""
        for key in list(self.keys()):
            self.pop(key, None)
        coupler_history = self._coupler.__dict__.get("_history")
        if isinstance(coupler_history, MutableMapping):
            for qualified_key in list(coupler_history.keys()):
                if isinstance(qualified_key, str) and qualified_key.startswith(
                    self._prefix
                ):
                    coupler_history.pop(qualified_key, None)

    def __len__(self) -> int:
        return len(list(self.keys()))

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())


@dataclass
class DeclaredOutput:
    required: bool = False
    description: Optional[str] = None


class Coupler(CouplerMapMixin[Any, DeclaredOutput]):
    """
    Scenario-local helper to thread named artifacts between steps.

    Coupler is intentionally small:
    - It stores the "latest Artifact for a semantic key" in-memory.

    It does not log artifacts, infer inputs/outputs, or mutate Artifacts as a side effect
    of reads. Keep provenance operations on the Tracker.
    """

    def __init__(self, tracker: Optional["Tracker"] = None) -> None:
        self._tracker: Optional["Tracker"] = None
        self._artifacts: MutableMapping[str, Any] = {}
        self._history: dict[str, list[CouplerArtifact]] = {}
        self._stat_cache: dict[tuple[str, int, int, int], V2Artifact] = {}
        self._declared_outputs: MutableMapping[str, DeclaredOutput] = {}
        self._warn_undefined = False
        self._undefined_keys: set[str] = set()
        self._output_descriptions: MutableMapping[str, str] = {}
        self.tracker = tracker

    @property
    def tracker(self) -> Optional["Tracker"]:
        return self._tracker

    @tracker.setter
    def tracker(self, value: Optional["Tracker"]) -> None:
        self._tracker = value
        if value is not None:
            couplers = value.__dict__.get("_couplers")
            if couplers is not None:
                couplers.add(self)

    def _validate_artifact_key(self, key: str) -> None:
        validate_artifact_key(key)

    def _create_declared_output(self) -> DeclaredOutput:
        return DeclaredOutput()

    def _extract_tracker_mounts(self) -> Optional[Mapping[str, Any]]:
        if self._tracker is None:
            return None
        mounts = self._tracker.__dict__.get("mounts")
        if isinstance(mounts, Mapping):
            return mounts
        return None

    def _coerce_artifact(self, key: str, value: Any) -> Optional[CouplerArtifact]:
        if value is None:
            return None
        if isinstance(value, (V2Artifact, LegacyArtifact)):
            return value
        if isinstance(value, (str, Path, os.PathLike)):
            mounts = self._extract_tracker_mounts()
            if (
                isinstance(value, str)
                and "://" in value
                and isinstance(self._tracker, _UriResolverProtocol)
            ):
                raw_path = Path(self._tracker.resolve_uri(value))
            else:
                raw_path = Path(value)
            resolved_path = raw_path.expanduser().resolve()
            try:
                st = resolved_path.stat()
            except OSError:
                st = None
            if st is not None and stat.S_ISREG(st.st_mode):
                cache_key = (
                    str(resolved_path),
                    st.st_size,
                    st.st_mtime_ns,
                    st.st_ino,
                )
                cached = self._stat_cache.get(cache_key)
                if cached is not None:
                    return (
                        cached
                        if cached.key == key
                        else replace(cached, id=uuid.uuid4(), key=key)
                    )
                observed = observe(resolved_path, key=key, mounts=mounts)
                self._stat_cache[cache_key] = observed
                return observed
            return observe(resolved_path, key=key, mounts=mounts)
        raise TypeError(
            f"Coupler.set({key!r}, ...) expected an Artifact or path, "
            f"got {type(value).__name__}: {value!r}"
        )

    def set(self, key: str, artifact: Any) -> Any:
        """Store an artifact (or auto-observed path) under a validated key."""
        self._validate_artifact_key(key)
        coerced = self._coerce_artifact(key, artifact)
        stored = self._store_artifact(key, coerced, validate_key=False)
        if isinstance(stored, (V2Artifact, LegacyArtifact)):
            self._history.setdefault(key, []).append(stored)
        return stored

    def set_from_artifact(self, key: str, value: Any) -> Any:
        """
        Set an artifact, accepting both Artifact objects and artifact-like values.

        This method is useful when integrating with optional dependencies (like noop mode)
        where you may receive either:
        - A real Artifact (when tracking is enabled)
        - An artifact-like object with .path and .container_uri properties (noop mode)
        - A Path or string (fallback)

        All three forms are stored in the coupler and can be retrieved with get() or require().
        """
        stored = self._store_artifact(key, value)
        if isinstance(stored, (V2Artifact, LegacyArtifact)):
            self._history.setdefault(key, []).append(stored)
        return stored

    def clear(self) -> None:
        """Clear all stored artifacts, history, and stat cache."""
        super().clear()
        self._stat_cache.clear()

    def view(self, namespace: str) -> CouplerView:
        """
        Return a namespace-scoped coupler view.

        The returned view writes keys as `<namespace>/<key>` into this coupler
        while allowing reads and writes using namespace-local key names.
        """
        return CouplerView(self, namespace)

    def path(self, key: str, *, required: bool = True) -> Optional[Path]:
        """
        Resolve an artifact's URI to an absolute host path.

        This does not mutate the Artifact; it only returns a resolved Path.
        """
        artifact = self.require(key) if required else self.get(key)
        if not artifact:
            return None
        if isinstance(artifact, V2Artifact):
            if self.tracker is not None:
                mounts = self._extract_tracker_mounts()
                located = artifact.locate(mounts=mounts)
                if located is not None:
                    return located
                prim = artifact.primary_location
                if prim is not None and isinstance(self.tracker, _UriResolverProtocol):
                    return Path(self.tracker.resolve_uri(prim.uri))
            return artifact.path
        if isinstance(artifact, LegacyArtifact):
            if not self.tracker:
                raise RuntimeError(
                    f"Cannot resolve path for {key!r}: no tracker attached to Coupler."
                )
            if isinstance(self.tracker, _UriResolverProtocol):
                return Path(self.tracker.resolve_uri(artifact.container_uri))
            return Path(artifact.container_uri)
        if isinstance(artifact, (Path, str)):
            if (
                self.tracker is not None
                and "://" in str(artifact)
                and isinstance(self.tracker, _UriResolverProtocol)
            ):
                return Path(self.tracker.resolve_uri(str(artifact)))
            return Path(artifact)
        if isinstance(artifact, ArtifactLike):
            if artifact.path is not None:
                return Path(artifact.path)
            if artifact.container_uri:
                if self.tracker is not None and isinstance(
                    self.tracker, _UriResolverProtocol
                ):
                    return Path(self.tracker.resolve_uri(artifact.container_uri))
                return Path(artifact.container_uri)
        return None
