from __future__ import annotations

import builtins
from dataclasses import dataclass
import os
from pathlib import Path
import time
from typing import (
    Any,
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
    Optional,
    TYPE_CHECKING,
)
import warnings

from consist.core.validation import validate_artifact_key
from consist.location import FileStat
from consist.models.artifact import Artifact
from consist.protocols import ArtifactLike

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

    def __init__(self, coupler: "Coupler", namespace: str) -> None:
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

    def __len__(self) -> int:
        return len(list(self.keys()))

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())


@dataclass
class DeclaredOutput:
    required: bool = False
    description: Optional[str] = None


class Coupler:
    """
    Scenario-local helper to thread named artifacts between steps.

    Coupler stores the latest ``Artifact`` (or lazy path reference) for a
    semantic key in memory. When a raw filesystem path or URI is stored via
    ``set()``, hashing and verification are deferred until consumption (or
    scenario snapshot commit), keeping ``set()`` O(1) while detecting post-``set()``
    disk edits and keeping downstream runs cacheable.
    """

    def __init__(self, tracker: Optional["Tracker"] = None) -> None:
        self._tracker: Optional["Tracker"] = None
        self._artifacts: MutableMapping[str, Any] = {}
        self._stat_cache: dict[tuple[Any, ...], Artifact] = {}
        self._declared_outputs: MutableMapping[str, DeclaredOutput] = {}
        self._warn_undefined = False
        self._undefined_keys: builtins.set[str] = builtins.set()
        self._output_descriptions: MutableMapping[str, str] = {}
        self.tracker = tracker

    @property
    def tracker(self) -> Optional["Tracker"]:
        return self._tracker

    @tracker.setter
    def tracker(self, value: Optional["Tracker"]) -> None:
        self._tracker = value
        if value is not None:
            try:
                couplers = value._couplers
            except AttributeError:
                couplers = None
            if couplers is not None:
                couplers.add(self)

    def _validate_artifact_key(self, key: str) -> None:
        validate_artifact_key(key)

    def _maybe_warn_undefined(self, key: str) -> None:
        if not self._warn_undefined:
            return
        if key in self._declared_outputs or key in self._undefined_keys:
            return
        self._undefined_keys.add(key)
        warnings.warn(
            f"Setting undefined coupler key '{key}'. "
            f"Declared outputs: {sorted(self._declared_outputs)}",
            UserWarning,
            stacklevel=2,
        )

    def _resolve_lazy_entry(self, key: str, value: Any) -> Any:
        if not isinstance(value, (Path, str, os.PathLike)):
            return value
        tracker = self._tracker
        if tracker is None:
            return value
        raw_str = os.fspath(value) if isinstance(value, os.PathLike) else value
        if isinstance(raw_str, str) and "://" in raw_str:
            resolved_path = Path(tracker.resolve_uri(raw_str))
        else:
            resolved_path = Path(raw_str)
            if not resolved_path.is_absolute():
                resolved_path = (tracker.run_dir / resolved_path).resolve()
            else:
                resolved_path = resolved_path.resolve()

        if not resolved_path.exists():
            return value

        if resolved_path.is_dir():
            return tracker.artifacts.create_artifact(
                resolved_path,
                run_id=None,
                key=key,
                direction="input",
                reuse_if_unchanged=True,
            )

        now_ns = time.time_ns()
        fstat = FileStat.capture(resolved_path, observed_at_ns=now_ns)
        is_racy = (
            fstat.racy or (now_ns - max(fstat.mtime_ns, fstat.ctime_ns)) < 2_000_000_000
        )
        stat_key: tuple[Any, ...] = (
            "file",
            key,
            str(resolved_path),
            fstat._stat_tuple(),
        )

        if not is_racy:
            cached = self._stat_cache.get(stat_key)
            if cached is not None:
                return cached

        artifact = tracker.artifacts.create_artifact(
            resolved_path,
            run_id=None,
            key=key,
            direction="input",
            reuse_if_unchanged=True,
        )
        if not is_racy:
            self._stat_cache[stat_key] = artifact
        return artifact

    def set(self, key: str, artifact: Any) -> Any:
        """Store an artifact or lazy path reference under a validated key."""
        self._validate_artifact_key(key)
        self._maybe_warn_undefined(key)
        self._artifacts[key] = artifact
        return artifact

    def set_from_artifact(self, key: str, value: Any) -> Any:
        """Store an ``Artifact``, ``ArtifactLike``, ``Path``, or string path."""
        return self.set(key, value)

    def clear(self) -> None:
        """Clear all stored artifacts."""
        self._artifacts.clear()
        self._stat_cache.clear()

    def __len__(self) -> int:
        return len(self._artifacts)

    def __iter__(self) -> Iterator[str]:
        return iter(self._artifacts)

    def keys(self) -> Iterable[str]:
        return self._artifacts.keys()

    def items(self) -> Iterable[tuple[str, Any]]:
        return [
            (key, self._resolve_lazy_entry(key, value))
            for key, value in self._artifacts.items()
        ]

    def values(self) -> Iterable[Any]:
        return [
            self._resolve_lazy_entry(key, value)
            for key, value in self._artifacts.items()
        ]

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
        self._validate_artifact_key(key)
        if key not in self._artifacts:
            return None
        return self._resolve_lazy_entry(key, self._artifacts[key])

    def pop(self, key: str, default: Optional[Any] = None) -> Optional[Any]:
        self._validate_artifact_key(key)
        if key not in self._artifacts:
            return default
        value = self._artifacts.pop(key)
        return self._resolve_lazy_entry(key, value)

    def require(self, key: str) -> Any:
        artifact = self.get(key)
        if artifact is None:
            available = ", ".join(sorted(self._artifacts.keys())) or "<none>"
            raise KeyError(
                f"Coupler missing key={key!r}. Available keys: {available}. "
                f"Did you forget to call coupler.set({key!r}, ...)?"
            )
        return artifact

    def declare_outputs(
        self,
        *names: str,
        required: bool | Mapping[str, bool] = False,
        warn_undefined: bool = False,
        description: Optional[Mapping[str, str]] = None,
    ) -> None:
        if warn_undefined:
            self._warn_undefined = True
        if not names:
            return
        if isinstance(required, Mapping):
            required_map = {str(k): bool(v) for k, v in required.items()}
            default_required = False
        else:
            required_map = {}
            default_required = bool(required)
        description_map = {str(k): v for k, v in (description or {}).items()}

        for name in names:
            if not isinstance(name, str):
                raise TypeError("Coupler output names must be strings.")
            key = name.strip()
            if not key:
                raise ValueError("Coupler output names cannot be empty.")
            self._validate_artifact_key(key)
            entry = self._declared_outputs.get(key, DeclaredOutput())
            entry.required = entry.required or required_map.get(key, default_required)
            if key in description_map:
                entry.description = description_map[key]
                self._output_descriptions[key] = description_map[key]
            self._declared_outputs[key] = entry

    def missing_declared_outputs(self) -> list[str]:
        return sorted(
            [
                key
                for key, entry in self._declared_outputs.items()
                if entry.required
                and (key not in self._artifacts or self._artifacts[key] is None)
            ]
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

    def collect_by_keys(
        self, artifacts: Mapping[str, Any], *keys: str, prefix: str = ""
    ) -> dict[str, Any]:
        if not isinstance(artifacts, Mapping):
            raise TypeError("collect_by_keys expects a mapping of artifacts.")
        collected: dict[str, Any] = {}
        for key in keys:
            if not isinstance(key, str):
                raise TypeError("collect_by_keys keys must be strings.")
            self._validate_artifact_key(key)
            if key not in artifacts:
                raise KeyError(f"Missing artifact for key {key!r}.")
            coupler_key = f"{prefix}{key}"
            artifact = artifacts[key]
            self.set(coupler_key, artifact)
            collected[coupler_key] = artifact
        return collected

    def describe_outputs(self) -> dict[str, str]:
        return dict(self._output_descriptions)

    def view(self, namespace: str) -> CouplerView:
        """
        Return a namespace-scoped coupler view.

        The returned view writes keys as `<namespace>/<key>` into this coupler
        while allowing reads and writes using namespace-local key names.
        """
        return CouplerView(self, namespace)

    def __contains__(self, key: object) -> bool:
        return key in self._artifacts

    def __getitem__(self, key: str) -> Any:
        return self.require(key)

    def __setitem__(self, key: str, artifact: Any) -> None:
        self.set(key, artifact)

    def path(self, key: str, *, required: bool = True) -> Optional[Path]:
        """
        Resolve an artifact's URI to an absolute host path.

        This does not mutate the Artifact; it only returns a resolved Path.
        """
        artifact = self.require(key) if required else self.get(key)
        if artifact is None:
            return None
        if isinstance(artifact, Artifact):
            if not self.tracker:
                raise RuntimeError(
                    f"Cannot resolve path for {key!r}: no tracker attached to Coupler."
                )
            return Path(self.tracker.resolve_uri(artifact.uri))
        if isinstance(artifact, (Path, str, os.PathLike)):
            raw_str = (
                os.fspath(artifact) if isinstance(artifact, os.PathLike) else artifact
            )
            if self.tracker is not None and "://" in str(raw_str):
                return Path(self.tracker.resolve_uri(str(raw_str)))
            resolved = Path(raw_str)
            if not resolved.is_absolute() and self.tracker is not None:
                return (self.tracker.run_dir / resolved).resolve()
            return resolved
        if isinstance(artifact, ArtifactLike):
            if artifact.path is not None:
                return Path(artifact.path)
            if artifact.uri:
                if self.tracker is not None:
                    return Path(self.tracker.resolve_uri(artifact.uri))
                return Path(artifact.uri)
        return None
