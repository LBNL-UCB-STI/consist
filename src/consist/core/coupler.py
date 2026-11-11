from __future__ import annotations

import builtins
from dataclasses import dataclass, replace
import hashlib
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
    runtime_checkable,
)
from urllib.parse import unquote, urlparse
import uuid
import warnings

from consist.artifact import Artifact
from consist.core.validation import validate_artifact_key
from consist.digest import Digest, Identity
from consist.engine import observe, resolve_uri_to_path
from consist.location import (
    _SUFFIX_DRIVERS,
    Location,
    Observation,
    SiteContext,
    normalize_mounts,
)
from consist.protocols import ArtifactLike

if TYPE_CHECKING:
    from consist.core.tracker import Tracker

CouplerArtifact = Artifact


@runtime_checkable
class _UriResolverProtocol(Protocol):
    def resolve_uri(self, uri: str) -> str: ...


@runtime_checkable
class _ArtifactSyncDbProtocol(Protocol):
    @property
    def ledger(self) -> Any: ...

    def sync_artifact(
        self,
        artifact: Artifact,
        run_id: Optional[str] = None,
    ) -> None: ...


@runtime_checkable
class _WritableTrackerProtocol(Protocol):
    @property
    def db(self) -> Any: ...

    @property
    def read_only(self) -> bool: ...

    @property
    def access_mode(self) -> str: ...


@runtime_checkable
class _PathAndUriLike(Protocol):
    @property
    def key(self) -> str: ...

    @property
    def path(self) -> Optional[Path]: ...

    @property
    def container_uri(self) -> str: ...


def _normalize_namespace(namespace: str) -> str:
    if not isinstance(namespace, str):
        raise TypeError("Coupler namespace must be a string.")
    normalized = namespace.strip().strip("/")
    if not normalized:
        raise ValueError("Coupler namespace cannot be empty.")
    validate_artifact_key(normalized)
    return normalized


def _infer_lexical_driver(path: Path, *, is_dir: bool) -> str:
    """Infer an artifact driver purely from path syntax without touching disk."""
    suffix = path.suffix.lower()
    if is_dir or suffix == ".zarr":
        return "zarr" if suffix == ".zarr" else "directory"
    if suffix in (".h5", ".hdf5", ".hdf"):
        return "hdf5"
    return _SUFFIX_DRIVERS.get(suffix, "file")


def _lexical_uri_for_synthetic_path(
    raw_str: str,
    mounts: Optional[Mapping[str, Any]],
) -> tuple[str, Path, bool]:
    """Construct `(uri, lexical_path, is_dir_hint)` without filesystem stat/realpath."""
    is_dir_hint = raw_str.endswith(("/", os.sep))
    has_scheme = "://" in raw_str and not (len(raw_str) >= 2 and raw_str[1] == ":")
    if has_scheme:
        if raw_str.startswith("file://"):
            lexical_path = Path(unquote(urlparse(raw_str).path))
        else:
            lexical_path = Path(raw_str.split("://", 1)[1])
        return (raw_str, lexical_path, is_dir_hint)

    trimmed = (
        raw_str.rstrip("/\\")
        if is_dir_hint and len(raw_str.rstrip("/\\")) > 0
        else raw_str
    )
    lexical_path = Path(trimmed)
    norm_mounts = normalize_mounts(mounts)
    if norm_mounts and lexical_path.is_absolute():
        abs_str = os.path.normpath(str(lexical_path))
        best_mount_name: str | None = None
        best_rel: str | None = None
        best_len = -1
        for mount in norm_mounts.values():
            root_str = os.path.normpath(os.path.expanduser(str(mount.root)))
            prefix = root_str if root_str.endswith(os.sep) else f"{root_str}{os.sep}"
            if abs_str == root_str:
                if len(root_str) > best_len:
                    best_len = len(root_str)
                    best_mount_name = mount.name
                    best_rel = ""
            elif abs_str.startswith(prefix):
                if len(root_str) > best_len:
                    best_len = len(root_str)
                    best_mount_name = mount.name
                    best_rel = abs_str[len(prefix) :].replace(os.sep, "/")
        if best_mount_name is not None and best_rel is not None:
            uri = (
                f"{best_mount_name}://{best_rel}"
                if best_rel
                else f"{best_mount_name}://"
            )
            return (uri, lexical_path, is_dir_hint)

    return (str(lexical_path), lexical_path, is_dir_hint)


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


class Coupler:
    """
    Scenario-local helper to thread named artifacts between steps.

    Coupler is intentionally small:
    - It stores the "latest Artifact for a semantic key" in-memory.

    When bound to a writable ``Tracker``, ``set()`` observes filesystem paths via
    ``consist.engine.observe`` and immediately persists the resulting ``Artifact``
    and ``Observation`` into ``StoreLedger``.
    """

    def __init__(self, tracker: Optional["Tracker"] = None) -> None:
        self._tracker: Optional["Tracker"] = None
        self._artifacts: MutableMapping[str, Any] = {}
        self._history: MutableMapping[str, list[Any]] = {}
        self._stat_cache: dict[tuple[str, int, int, int], Artifact] = {}
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
            couplers = value.__dict__.get("_couplers")
            if couplers is not None:
                couplers.add(self)
            for key, existing in list(self._artifacts.items()):
                if isinstance(existing, Artifact):
                    updated = self._persist_or_bind(existing)
                    self._artifacts[key] = updated
                    hist = self._history.get(key)
                    if hist and hist[-1] is existing:
                        hist[-1] = updated

    def _writable_db(self) -> Optional[_ArtifactSyncDbProtocol]:
        trk = self._tracker
        if trk is None or not isinstance(trk, _WritableTrackerProtocol):
            return None
        if trk.read_only or trk.access_mode != "standard":
            return None
        db = trk.db
        if isinstance(db, _ArtifactSyncDbProtocol):
            return db
        return None

    def _persist_or_bind(self, artifact: Artifact) -> Artifact:
        db = self._writable_db()
        if db is not None:
            ledger_ref = db.ledger.ref
            if artifact.origin != ledger_ref:
                db.sync_artifact(artifact, run_id=None)
                artifact = replace(artifact, origin=ledger_ref)
            return artifact
        if self._tracker is not None:
            artifact.bind_tracker(self._tracker)
        return artifact

    def _validate_artifact_key(self, key: str) -> None:
        validate_artifact_key(key)

    def _store_artifact(
        self, key: str, artifact: Any, *, validate_key: bool = True
    ) -> Any:
        """Shared storage pipeline: optional validation + undefined-key warning."""
        if validate_key:
            self._validate_artifact_key(key)
        self._maybe_warn_undefined(key)
        self._artifacts[key] = artifact
        return artifact

    def _maybe_warn_undefined(self, key: str) -> None:
        """Emit a one-time undefined-key warning when warnings are enabled."""
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

    def _extract_tracker_mounts(self) -> Optional[Mapping[str, Any]]:
        if self._tracker is None:
            return None
        merged: dict[str, Any] = {}
        run_dir = self._tracker.__dict__.get("run_dir")
        if run_dir:
            merged["workspace"] = run_dir
        archive_mounts = self._tracker.__dict__.get("archive_mounts")
        if isinstance(archive_mounts, Mapping) and archive_mounts:
            merged.update(archive_mounts)
        mounts = self._tracker.__dict__.get("mounts")
        if isinstance(mounts, Mapping) and mounts:
            merged.update(mounts)
        return merged or None

    def _extract_tracker_site_context(self) -> Optional[SiteContext]:
        if self._tracker is None:
            return None
        site_ctx = self._tracker.__dict__.get("site_context")
        if isinstance(site_ctx, SiteContext):
            return site_ctx
        return None

    def _coerce_artifact(self, key: str, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, Artifact):
            return self._persist_or_bind(value)
        if isinstance(value, (str, Path, os.PathLike)):
            mounts = self._extract_tracker_mounts()
            site_ctx = self._extract_tracker_site_context()
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
                    if cached.key == key:
                        persisted_cached = self._persist_or_bind(cached)
                        self._stat_cache[cache_key] = persisted_cached
                        return persisted_cached
                    rekeyed = replace(
                        cached,
                        id=uuid.uuid4(),
                        key=key,
                        origin=None,
                        _tracker_ref=None,
                    )
                    return self._persist_or_bind(rekeyed)
                observed = observe(
                    resolved_path,
                    key=key,
                    mounts=mounts,
                    site_context=site_ctx,
                )
                persisted = self._persist_or_bind(observed)
                self._stat_cache[cache_key] = persisted
                return persisted
            observed = observe(
                resolved_path,
                key=key,
                mounts=mounts,
                site_context=site_ctx,
            )
            return self._persist_or_bind(observed)
        if isinstance(value, (ArtifactLike, _PathAndUriLike)):
            return value
        raise TypeError(
            f"Coupler.set({key!r}, ...) expected an Artifact or path, "
            f"got {type(value).__name__}: {value!r}"
        )

    def set(self, key: str, artifact: Any) -> Any:
        """Store an artifact (or auto-observed path) under a validated key."""
        self._validate_artifact_key(key)
        coerced = self._coerce_artifact(key, artifact)
        stored = self._store_artifact(key, coerced, validate_key=False)
        if stored is not None:
            self._history.setdefault(key, []).append(stored)
        return stored

    def replace_artifact_by_id(self, updated: Artifact) -> bool:
        """Replace in-memory entries whose `id` matches `updated.id` without appending duplicate history."""
        changed = False
        for k, cur in list(self._artifacts.items()):
            if isinstance(cur, Artifact) and cur.id == updated.id:
                self._artifacts[k] = updated
                changed = True
        for k, hist in self._history.items():
            for idx, cur in enumerate(hist):
                if isinstance(cur, Artifact) and cur.id == updated.id:
                    hist[idx] = updated
                    changed = True
        return changed

    def set_from_artifact(self, key: str, value: Any) -> Any:
        """
        Store an artifact, artifact-like object, or synthetic/unmaterialized path under `key`.

        Unlike `set()`, `str | Path` inputs are wrapped in a `Kind.HISTORICAL`
        (`basis="declared"`) `Artifact` without disk I/O.
        """
        self._validate_artifact_key(key)
        if value is None:
            return self._store_artifact(key, None, validate_key=False)
        if isinstance(value, Artifact):
            coerced: Any = self._persist_or_bind(value)
        elif isinstance(value, (str, Path, os.PathLike)):
            raw_str = os.fspath(value)
            mounts = self._extract_tracker_mounts()
            site_ctx = self._extract_tracker_site_context()
            uri, lexical_path, is_dir_hint = _lexical_uri_for_synthetic_path(
                raw_str, mounts
            )
            driver = _infer_lexical_driver(lexical_path, is_dir=is_dir_hint)
            anchor = Digest(
                "legacy-v1:unverified",
                hashlib.sha256(uri.encode("utf-8")).hexdigest(),
            )
            identity = Identity(anchor=anchor)
            obs = Observation(
                location=Location(uri=uri),
                identity=identity,
                access_driver=driver,
                witness=None,
                basis="declared",
                site=site_ctx.site if site_ctx is not None else "local",
            )
            synthetic = Artifact(
                id=uuid.uuid4(),
                key=key,
                driver=driver,
                identity=identity,
                hints=(obs,),
            )
            coerced = self._persist_or_bind(synthetic)
        else:
            coerced = value
        stored = self._store_artifact(key, coerced, validate_key=False)
        if stored is not None:
            self._history.setdefault(key, []).append(stored)
        return stored

    def clear(self) -> None:
        """Clear all stored artifacts, history, and stat cache."""
        self._artifacts.clear()
        self._history.clear()
        self._stat_cache.clear()

    def history(self, key: str) -> tuple[Any, ...]:
        """Return the chronological history of artifacts stored for ``key``."""
        self._validate_artifact_key(key)
        return tuple(self._history.get(key, ()))

    def previous(self, key: str) -> Optional[Any]:
        """Return the previous artifact stored for ``key`` before the current one, or ``None``."""
        self._validate_artifact_key(key)
        hist = self._history.get(key, ())
        return hist[-2] if len(hist) >= 2 else None

    def __contains__(self, key: object) -> bool:
        return key in self._artifacts

    def __getitem__(self, key: str) -> Any:
        return self.require(key)

    def __setitem__(self, key: str, artifact: Any) -> None:
        self.set(key, artifact)

    def __len__(self) -> int:
        """Return the number of stored artifacts."""
        return len(self._artifacts)

    def __iter__(self) -> Iterator[str]:
        """Iterate over stored artifact keys."""
        return iter(self._artifacts)

    def keys(self) -> Iterable[str]:
        """Return an iterable of stored artifact keys."""
        return self._artifacts.keys()

    def items(self) -> Iterable[tuple[str, Any]]:
        """Return an iterable of (key, artifact) pairs."""
        return self._artifacts.items()

    def values(self) -> Iterable[Any]:
        """Return an iterable of stored artifacts."""
        return self._artifacts.values()

    def update(
        self, artifacts: Optional[Mapping[str, Any]] = None, /, **kwargs: Any
    ) -> None:
        """Bulk-store artifacts from a mapping and/or keyword arguments."""
        if artifacts:
            for key, artifact in artifacts.items():
                self.set(key, artifact)
        if kwargs:
            for key, artifact in kwargs.items():
                self.set(key, artifact)

    def get(self, key: str) -> Optional[Any]:
        """Return artifact for ``key`` or ``None`` if unset."""
        self._validate_artifact_key(key)
        return self._artifacts.get(key)

    def pop(self, key: str, default: Optional[Any] = None) -> Optional[Any]:
        """Remove and return artifact for ``key``, or ``default`` if unset."""
        self._validate_artifact_key(key)
        return self._artifacts.pop(key, default)

    def require(self, key: str) -> Any:
        """Return artifact for ``key`` or raise a helpful ``KeyError``."""
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
        """Declare expected output keys and optional metadata."""
        if not names:
            return
        if warn_undefined:
            self._warn_undefined = True
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
        """Return required declared outputs that are still unset."""
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
        """Convenience wrapper for declaring outputs as required."""
        self.declare_outputs(
            *names,
            required=required,
            warn_undefined=warn_undefined,
            description=description,
        )

    def collect_by_keys(
        self, artifacts: Mapping[str, Any], *keys: str, prefix: str = ""
    ) -> dict[str, Any]:
        """Copy selected artifact keys into this coupler with an optional prefix."""
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
            stored = self.set(coupler_key, artifact)
            collected[coupler_key] = stored
        return collected

    def describe_outputs(self) -> dict[str, str]:
        """Return a copy of declared-output descriptions."""
        return dict(self._output_descriptions)

    def view(self, namespace: str) -> CouplerView:
        """
        Return a namespace-scoped coupler view.

        The returned view writes keys as `<namespace>/<key>` into this coupler
        while allowing reads and writes using namespace-local key names.
        """
        return CouplerView(self, namespace)

    def path(self, key: str, *, required: bool = True) -> Optional[Path]:
        """
        Resolve an artifact's URI to an absolute or declared host path.

        This does not mutate the Artifact; it only returns a resolved Path.
        """
        artifact = self.require(key) if required else self.get(key)
        if not artifact:
            return None
        if isinstance(artifact, Artifact):
            mounts = self._extract_tracker_mounts()
            located = artifact.locate(mounts=mounts)
            if located is not None:
                return located
            prim = artifact.primary_location
            if prim is not None:
                if prim.uri.startswith("file://"):
                    return Path(unquote(urlparse(prim.uri).path))
                if "://" in prim.uri and not (
                    len(prim.uri) >= 2 and prim.uri[1] == ":"
                ):
                    resolved_p, _ = resolve_uri_to_path(prim.uri, mounts)
                    if resolved_p is not None:
                        return resolved_p
                    if self.tracker is not None and isinstance(
                        self.tracker, _UriResolverProtocol
                    ):
                        return Path(self.tracker.resolve_uri(prim.uri))
                    if self.tracker is None:
                        raise RuntimeError(
                            f"Cannot resolve URI {prim.uri!r}: no tracker is attached to Coupler."
                        )
                else:
                    return Path(prim.uri)
            return artifact.path
        if isinstance(artifact, (Path, str)):
            if (
                self.tracker is not None
                and "://" in str(artifact)
                and isinstance(self.tracker, _UriResolverProtocol)
            ):
                return Path(self.tracker.resolve_uri(str(artifact)))
            return Path(artifact)
        if isinstance(artifact, _PathAndUriLike):
            if artifact.container_uri:
                if self.tracker is not None and isinstance(
                    self.tracker, _UriResolverProtocol
                ):
                    return Path(self.tracker.resolve_uri(artifact.container_uri))
                if artifact.path is not None:
                    return Path(artifact.path)
                return Path(artifact.container_uri)
            if artifact.path is not None:
                return Path(artifact.path)
        if isinstance(artifact, ArtifactLike):
            if artifact.primary_location is not None:
                uri = artifact.primary_location.uri
                if self.tracker is not None and isinstance(
                    self.tracker, _UriResolverProtocol
                ):
                    return Path(self.tracker.resolve_uri(uri))
            return Path(artifact.path)
        return None
