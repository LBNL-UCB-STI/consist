"""
Immutable non-ORM ``Artifact`` value model for Consist provenance and caching.

This module belongs to Layer 2 (``consist.models``):
- Imports only stdlib, Pydantic core schema hooks, and Layer-1 leaves
  (``consist.digest``, ``consist.protocols``, ``consist.types``).
- Zero imports from ``consist.core.*``, ``consist.api``, ``sqlalchemy``, or ``sqlmodel``.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, replace
from collections.abc import Iterator, Mapping as MappingABC, Sequence
from datetime import datetime, timezone
import logging
from pathlib import Path
from types import MappingProxyType
import uuid
import weakref
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Literal,
    Optional,
    cast,
    overload,
)

from pydantic_core import core_schema

from consist.digest import Digest, _HEX64_RE
from consist.protocols import (
    ArtifactResolver,
    resolve_active_or_default_resolver,
)
from consist.types import DriverType

UTC = timezone.utc
_UNSET: Any = object()

if TYPE_CHECKING:
    import pandas as pd


def _deep_copy_meta_value(value: Any) -> Any:
    """Return a plain JSON/pickle-safe mutable copy unwrapping any ``MappingProxyType``."""
    if value is None or isinstance(value, (bool, int, float, str, Digest)):
        return value
    if isinstance(value, MappingABC):
        return {str(k): _deep_copy_meta_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_deep_copy_meta_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_deep_copy_meta_value(item) for item in value)
    return copy.deepcopy(value)


_EMPTY_META: MappingABC[str, Any] = MappingProxyType({})


def _freeze_meta_value(value: Any) -> Any:
    """Deep-freeze nested mappings into ``MappingProxyType`` while defensive-copying sequences."""
    if value is None or isinstance(value, (bool, int, float, str, Digest)):
        return value
    if isinstance(value, MappingABC):
        if not value:
            return _EMPTY_META
        return MappingProxyType(
            {str(k): _freeze_meta_value(v) for k, v in value.items()}
        )
    if isinstance(value, list):
        return [_freeze_meta_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_freeze_meta_value(item) for item in value)
    return copy.deepcopy(value)


def _freeze_meta_mapping(raw_meta: MappingABC[str, Any] | None) -> MappingABC[str, Any]:
    if not raw_meta:
        return _EMPTY_META
    return MappingProxyType(
        {str(k): _freeze_meta_value(v) for k, v in raw_meta.items()}
    )


def _wrap_owned_meta_value(value: Any) -> Any:
    """Wrap an already-owned ``json.loads`` tree in ``MappingProxyType`` without re-copying."""
    if isinstance(value, dict):
        if not value:
            return _EMPTY_META
        for k, v in value.items():
            if isinstance(v, (dict, list)):
                value[k] = _wrap_owned_meta_value(v)
        return MappingProxyType(value)
    if isinstance(value, list):
        for idx, item in enumerate(value):
            if isinstance(item, (dict, list)):
                value[idx] = _wrap_owned_meta_value(item)
        return value
    return value


def _wrap_owned_meta_dict(raw_meta: Optional[dict[str, Any]]) -> MappingABC[str, Any]:
    if not raw_meta:
        return _EMPTY_META
    for k, v in raw_meta.items():
        if isinstance(v, (dict, list)):
            raw_meta[k] = _wrap_owned_meta_value(v)
    return MappingProxyType(raw_meta)


@overload
def _coerce_uuid(
    value: uuid.UUID | str | None, *, default_new: Literal[True]
) -> uuid.UUID: ...


@overload
def _coerce_uuid(
    value: uuid.UUID | str | None, *, default_new: Literal[False]
) -> Optional[uuid.UUID]: ...


def _coerce_uuid(
    value: uuid.UUID | str | None, *, default_new: bool
) -> Optional[uuid.UUID]:
    if value is None:
        return uuid.uuid4() if default_new else None
    if isinstance(value, uuid.UUID):
        return value
    if isinstance(value, str):
        return uuid.UUID(value)
    raise TypeError(f"Expected UUID or str, got {type(value).__name__}")


def _coerce_utc_datetime(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    elif isinstance(value, datetime):
        parsed = value
    else:
        raise TypeError(
            f"Expected datetime or ISO-8601 str, got {type(value).__name__}"
        )
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _coerce_resolver_ref(
    resolver: weakref.ReferenceType[ArtifactResolver] | ArtifactResolver | Any | None,
) -> Optional[weakref.ReferenceType[ArtifactResolver]]:
    if resolver is None:
        return None
    if isinstance(resolver, weakref.ReferenceType):
        return resolver
    try:
        return weakref.ref(cast(ArtifactResolver, resolver))
    except TypeError:
        return None


def _coerce_legacy_v04_hash(
    raw_hash: Any,
    *,
    driver: str,
    member: str | None,
    meta: dict[str, Any],
) -> Optional[Digest]:
    if raw_hash is None:
        return None
    if isinstance(raw_hash, Digest):
        return raw_hash
    if not isinstance(raw_hash, str):
        raise TypeError(f"Expected Digest, str, or None, got {type(raw_hash).__name__}")
    cleaned = raw_hash.strip()
    if not cleaned:
        return None
    if cleaned.lower().startswith("sha256:logical_matrices:v1:"):
        _, _, hex_part = cleaned.rpartition(":")
        if _HEX64_RE.fullmatch(hex_part):
            profile = meta.get("logical_profile")
            scheme = (
                "logical_matrices_f32_v1:sha256"
                if profile == "float32_matrices_v1"
                else "logical_matrices_canonical_v1:sha256"
            )
            return Digest(scheme, hex_part.lower())
    bare_candidate: Optional[str] = None
    if cleaned.lower().startswith("sha256:") and _HEX64_RE.fullmatch(cleaned[7:]):
        bare_candidate = cleaned[7:].lower()
    elif ":" in cleaned:
        scheme_part, _, hex_part = cleaned.rpartition(":")
        normalized_candidate = (
            f"{scheme_part}:{hex_part.lower()}"
            if _HEX64_RE.fullmatch(hex_part)
            else cleaned
        )
        try:
            return Digest.parse(normalized_candidate)
        except ValueError:
            meta.setdefault("legacy_v04_hash", cleaned)
            return None
    elif _HEX64_RE.fullmatch(cleaned):
        bare_candidate = cleaned.lower()

    if bare_candidate is not None:
        if (
            meta.get("directory_artifact") is True
            or meta.get("file_bundle_artifact") is True
            or driver in {"zarr", "shapefile", "directory"}
        ):
            return Digest("dir_manifest_v1:sha256", bare_candidate)
        if meta.get("artifact_set") in {True, "output_set"} or driver in {
            "artifact_set",
            "output_set",
        }:
            return Digest("output_set_v1:sha256", bare_candidate)
        if driver == "h5_table":
            return Digest("h5_fixed_v1:sha256", bare_candidate)
        if driver in {"h5", "hdf5"} and member is not None:
            return Digest("h5_dataset_v1:sha256", bare_candidate)
        return Digest("sha256:file", bare_candidate)
    meta.setdefault("legacy_v04_hash", cleaned)
    return None


@dataclass(frozen=True, slots=True)
class ArtifactLocationStatus:
    """Reachability status for one artifact location."""

    label: str
    path: Path
    exists: bool


@dataclass(frozen=True, slots=True)
class ArtifactAccessibility:
    """Structured accessibility report for an artifact."""

    locations: tuple[ArtifactLocationStatus, ...]

    @property
    def status(self) -> str:
        """Return a concise accessibility status string."""
        if self.locations and self.locations[0].exists:
            return "primary"
        for location in self.locations[1:]:
            if location.exists:
                return "recovery"
        return "missing"

    @property
    def accessible(self) -> bool:
        """Return whether any checked location is reachable."""
        return any(location.exists for location in self.locations)

    @property
    def best_location(self) -> Optional[ArtifactLocationStatus]:
        """Return the first reachable location, if any."""
        for location in self.locations:
            if location.exists:
                return location
        return None


class _ArtifactMeta(type):
    """Fast constructor normalizer keeping ``Artifact`` frozen without post-init mutation."""

    def __call__(
        cls,
        *,
        key: str,
        uri: str,
        driver: str,
        id: uuid.UUID | str | None = None,
        digest: Digest | str | None = None,
        member: Optional[str] = None,
        run_id: Optional[str] = None,
        parent_artifact_id: uuid.UUID | str | None = None,
        facet_id: Optional[str] = None,
        created_at: datetime | str | None = None,
        meta: MappingABC[str, Any] | None = None,
        _abs_path: str | Path | None = None,
        _resolver_ref: (
            weakref.ReferenceType[ArtifactResolver] | ArtifactResolver | Any | None
        ) = None,
    ) -> Any:
        if type(key) is not str and not isinstance(key, str):
            raise TypeError(f"Artifact.key must be str, got {type(key).__name__}")
        if type(uri) is not str and not isinstance(uri, str):
            raise TypeError(f"Artifact.uri must be str, got {type(uri).__name__}")
        if type(driver) is not str and not isinstance(driver, str):
            raise TypeError(f"Artifact.driver must be str, got {type(driver).__name__}")

        frozen_meta = _EMPTY_META if not meta else _freeze_meta_mapping(meta)
        parsed_digest = (
            digest
            if digest is None or type(digest) is Digest or isinstance(digest, Digest)
            else Digest.parse(digest)
        )
        resolved_id = (
            id if type(id) is uuid.UUID else _coerce_uuid(id, default_new=True)
        )
        resolved_parent_id = (
            None
            if parent_artifact_id is None
            else (
                parent_artifact_id
                if type(parent_artifact_id) is uuid.UUID
                else _coerce_uuid(parent_artifact_id, default_new=False)
            )
        )
        resolved_created_at = (
            created_at
            if type(created_at) is datetime and created_at.tzinfo is UTC
            else _coerce_utc_datetime(created_at)
        )
        resolved_abs = (
            None
            if _abs_path is None
            else (_abs_path if type(_abs_path) is str else str(_abs_path))
        )
        resolved_ref = (
            None if _resolver_ref is None else _coerce_resolver_ref(_resolver_ref)
        )

        artifact_cls = cast("type[Artifact]", cls)
        obj = object.__new__(artifact_cls)
        Artifact.__init__(
            obj,
            id=resolved_id,
            key=key,
            uri=uri,
            driver=driver,
            digest=parsed_digest,
            member=member,
            run_id=run_id,
            parent_artifact_id=resolved_parent_id,
            facet_id=facet_id,
            created_at=resolved_created_at,
            meta=frozen_meta,
            _abs_path=resolved_abs,
            _resolver_ref=resolved_ref,
        )
        return obj


@dataclass(frozen=True, kw_only=True, repr=False, eq=False)
class Artifact(metaclass=_ArtifactMeta):
    """
    Immutable value representation of a tracked Consist artifact.

    Attributes
    ----------
    id : uuid.UUID
        Unique artifact row identifier.
    key : str
        Semantic name for the artifact (for example ``"households"``).
    uri : str
        Portable virtualized URI (for example ``"inputs://land_use.csv"``).
    driver : str
        Format handler name (for example ``"parquet"``, ``"csv"``, ``"zarr"``).
    digest : Digest | None
        Self-describing cryptographic content or observation digest.
    member : str | None
        Optional child table or dataset path inside a multi-member container.
    run_id : str | None
        Producing run ID, or ``None`` for external inputs.
    parent_artifact_id : uuid.UUID | None
        Parent container/set artifact ID for child member artifacts.
    facet_id : str | None
        Optional persisted ``ArtifactFacet`` identifier.
    created_at : datetime
        UTC timestamp when the artifact was created.
    meta : Mapping[str, Any]
        Read-only ``MappingProxyType`` view over defensive-copied metadata.
    """

    __slots__ = (
        "id",
        "key",
        "uri",
        "driver",
        "digest",
        "member",
        "run_id",
        "parent_artifact_id",
        "facet_id",
        "created_at",
        "meta",
        "_abs_path",
        "_resolver_ref",
        "__weakref__",
    )

    id: uuid.UUID
    key: str
    uri: str
    driver: str
    digest: Optional[Digest]
    member: Optional[str]
    run_id: Optional[str]
    parent_artifact_id: Optional[uuid.UUID]
    facet_id: Optional[str]
    created_at: datetime
    meta: MappingABC[str, Any]
    _abs_path: Optional[str]
    _resolver_ref: Optional[weakref.ReferenceType[ArtifactResolver]]

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, Artifact):
            return NotImplemented
        return (
            self.id == other.id
            and self.key == other.key
            and self.uri == other.uri
            and self.driver == other.driver
            and self.digest == other.digest
            and self.member == other.member
            and self.run_id == other.run_id
            and self.parent_artifact_id == other.parent_artifact_id
            and self.facet_id == other.facet_id
            and self.created_at == other.created_at
            and self.meta == other.meta
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.id,
                self.key,
                self.uri,
                self.driver,
                self.member,
                self.digest,
                self.run_id,
                self.parent_artifact_id,
                self.facet_id,
                self.created_at,
            )
        )

    def _fast_clone(
        self,
        *,
        uri: Any = _UNSET,
        digest: Any = _UNSET,
        member: Any = _UNSET,
        run_id: Any = _UNSET,
        facet_id: Any = _UNSET,
        meta: Any = _UNSET,
        _abs_path: Any = _UNSET,
        _resolver_ref: Any = _UNSET,
    ) -> "Artifact":
        """O(1) internal clone bypassing metaclass signature/meta re-freezing."""
        cls = type(self)
        obj = object.__new__(cls)
        cls.__init__(
            obj,
            id=self.id,
            key=self.key,
            uri=self.uri if uri is _UNSET else uri,
            driver=self.driver,
            digest=self.digest if digest is _UNSET else digest,
            member=self.member if member is _UNSET else member,
            run_id=self.run_id if run_id is _UNSET else run_id,
            parent_artifact_id=self.parent_artifact_id,
            facet_id=self.facet_id if facet_id is _UNSET else facet_id,
            created_at=self.created_at,
            meta=self.meta if meta is _UNSET else meta,
            _abs_path=self._abs_path if _abs_path is _UNSET else _abs_path,
            _resolver_ref=(
                self._resolver_ref if _resolver_ref is _UNSET else _resolver_ref
            ),
        )
        return obj

    @classmethod
    def _from_trusted_db_row(
        cls,
        *,
        id: uuid.UUID,
        key: str,
        uri: str,
        driver: str,
        digest: Optional[Digest],
        member: Optional[str],
        run_id: Optional[str],
        parent_artifact_id: Optional[uuid.UUID],
        facet_id: Optional[str],
        created_at: datetime,
        owned_meta: Optional[dict[str, Any]],
        resolver_ref: Optional[weakref.ReferenceType[ArtifactResolver]] = None,
    ) -> "Artifact":
        """Hydrate directly from a validated DB row without deep-copying ``owned_meta``."""
        obj = object.__new__(cls)
        cls.__init__(
            obj,
            id=id,
            key=key,
            uri=uri,
            driver=driver,
            digest=digest,
            member=member,
            run_id=run_id,
            parent_artifact_id=parent_artifact_id,
            facet_id=facet_id,
            created_at=created_at,
            meta=_wrap_owned_meta_dict(owned_meta),
            _abs_path=None,
            _resolver_ref=resolver_ref,
        )
        return obj

    # --- Immutable Copy / Update Builders ---

    def with_meta(
        self,
        meta: MappingABC[str, Any] | None = None,
        *,
        merge: bool = False,
        **updates: Any,
    ) -> "Artifact":
        """Return a new ``Artifact`` with replaced or merged metadata."""
        if merge:
            merged: dict[str, Any] = dict(self.meta)
            if meta is not None:
                merged.update(meta)
            if updates:
                merged.update(updates)
            return self._fast_clone(meta=_freeze_meta_mapping(merged))
        base: dict[str, Any] = (
            dict(meta) if meta is not None else (dict(self.meta) if updates else {})
        )
        if updates:
            base.update(updates)
        return self._fast_clone(meta=_freeze_meta_mapping(base))

    def with_digest(self, digest: Digest | str | None) -> "Artifact":
        """Return a new ``Artifact`` with an updated ``Digest``."""
        parsed = (
            digest
            if isinstance(digest, Digest) or digest is None
            else Digest.parse(digest)
        )
        return self._fast_clone(digest=parsed)

    def with_uri(self, uri: str) -> "Artifact":
        """Return a new ``Artifact`` with an updated ``uri``."""
        if type(uri) is not str and not isinstance(uri, str):
            raise TypeError(f"Artifact.uri must be str, got {type(uri).__name__}")
        return self._fast_clone(uri=uri)

    def with_member(self, member: Optional[str]) -> "Artifact":
        """Return a new ``Artifact`` with an updated ``member``."""
        return self._fast_clone(member=member)

    def with_run_id(self, run_id: Optional[str]) -> "Artifact":
        """Return a new ``Artifact`` with an updated ``run_id``."""
        return self._fast_clone(run_id=run_id)

    def with_facet_id(self, facet_id: Optional[str]) -> "Artifact":
        """Return a new ``Artifact`` with an updated ``facet_id``."""
        return self._fast_clone(facet_id=facet_id)

    def with_abs_path(self, abs_path: str | Path | None) -> "Artifact":
        """Return a new ``Artifact`` carrying a resolved local ``_abs_path`` hint."""
        resolved_abs = (
            None
            if abs_path is None
            else (abs_path if type(abs_path) is str else str(abs_path))
        )
        return self._fast_clone(_abs_path=resolved_abs)

    def with_resolver(self, resolver: ArtifactResolver | Any | None) -> "Artifact":
        """Return a new ``Artifact`` weakly bound to ``resolver``."""
        return self._fast_clone(_resolver_ref=_coerce_resolver_ref(resolver))

    # --- Runtime Resolver & Path Delegation ---

    @property
    def abs_path(self) -> Optional[str]:
        """Runtime-only resolved absolute filesystem path, if attached."""
        bound = self._resolver_ref() if self._resolver_ref is not None else None
        resolver_obj = (
            bound if bound is not None else resolve_active_or_default_resolver()
        )
        if resolver_obj is not None and isinstance(resolver_obj, ArtifactResolver):
            override = resolver_obj.artifact_abs_path_override(self)
            if override is not None:
                recorded_from, override_val = override
                if self._abs_path == recorded_from:
                    return override_val
        return self._abs_path

    @property
    def resolver(self) -> Optional[ArtifactResolver]:
        """Return the attached ``ArtifactResolver`` if alive, else the active/default resolver."""
        if self._resolver_ref is not None:
            bound = self._resolver_ref()
            if bound is not None:
                return bound
        return resolve_active_or_default_resolver()

    @property
    def tracker(self) -> Optional[Any]:
        """Return the attached or active runtime tracker resolver, if any."""
        return self.resolver

    @property
    def path(self) -> Path:
        """Resolve this artifact to a filesystem ``Path`` via ``ArtifactResolver``."""
        return self.as_path(None)

    def as_path(self, tracker: Optional[Any] = None) -> Path:
        """Resolve this artifact to a filesystem ``Path`` using ``tracker`` or attached resolver."""
        resolved_abs = self.abs_path
        attached = self._resolver_ref() if self._resolver_ref is not None else None
        if tracker is not None:
            target_resolver: Optional[Any] = tracker
            override = (
                tracker.artifact_abs_path_override(self)
                if isinstance(tracker, ArtifactResolver)
                else None
            )
            has_tracker_override = (
                override is not None and override[0] == self._abs_path
            )
            effective_abs = override[1] if has_tracker_override else resolved_abs
        else:
            target_resolver = (
                attached
                if attached is not None
                else resolve_active_or_default_resolver()
            )
            has_tracker_override = False
            effective_abs = resolved_abs

        if effective_abs and (
            tracker is None or attached is tracker or has_tracker_override
        ):
            abs_candidate = Path(effective_abs)
            try:
                if abs_candidate.exists():
                    return abs_candidate
            except OSError:
                pass
        elif tracker is not None and target_resolver is not None:
            try:
                resolved = Path(target_resolver.resolve_uri(self.uri))
                if resolved.exists() or not effective_abs:
                    return resolved
            except (ValueError, RuntimeError, OSError, TypeError):
                pass

        if target_resolver is not None:
            if isinstance(target_resolver, ArtifactResolver):
                try:
                    return target_resolver.resolve_artifact_path(self)
                except (ValueError, RuntimeError, OSError, TypeError):
                    pass
            try:
                resolved = Path(target_resolver.resolve_uri(self.uri))
                if resolved.exists() or not effective_abs:
                    return resolved
            except (ValueError, RuntimeError, OSError, TypeError):
                pass

        if effective_abs:
            return Path(effective_abs)
        return Path(self.uri)

    def ensure_local_path(
        self,
        *,
        tracker: Optional[Any] = None,
        target_root: Optional[Path | str] = None,
        source_root: Optional[Path | str] = None,
        on_missing: Literal["warn", "raise"] = "warn",
    ) -> Optional[Path]:
        """Ensure this artifact exists on the local filesystem via ``ArtifactResolver``."""
        resolved_resolver = tracker if tracker is not None else self.resolver
        if target_root is None:
            candidate = self.as_path(resolved_resolver)
            try:
                if candidate.exists():
                    return candidate
            except OSError:
                pass

        if resolved_resolver is None:
            msg = (
                f"[Consist] Cannot materialize artifact {self.key!r}: "
                "no Tracker is attached or active in context."
            )
            if on_missing == "raise":
                raise FileNotFoundError(msg)
            logging.warning(msg)
            return None

        return resolved_resolver.ensure_local_artifact_path(
            self,
            target_root=target_root,
            source_root=source_root,
            on_missing=on_missing,
        )

    @property
    def recovery_roots(self) -> list[str]:
        """Ordered advisory recovery roots recorded on this artifact (consulting live resolver DB when available)."""
        active = self.resolver
        if active is not None and isinstance(active, ArtifactResolver):
            try:
                return active.artifact_recovery_roots(self)
            except (OSError, RuntimeError, ValueError, TypeError, LookupError):
                pass
        return extract_recovery_roots_from_meta(self.meta)

    def accessibility(self, tracker: Optional[Any] = None) -> ArtifactAccessibility:
        """Inspect primary and advisory recovery locations for this artifact."""
        resolver_obj = tracker if tracker is not None else self.resolver
        try:
            primary_path = (
                Path(resolver_obj.resolve_uri(self.uri))
                if resolver_obj is not None
                else self.path
            )
        except (OSError, RuntimeError, ValueError, TypeError):
            primary_path = self.path

        locations = [
            ArtifactLocationStatus(
                label="primary",
                path=primary_path,
                exists=primary_path.exists(),
            )
        ]
        if resolver_obj is None:
            return ArtifactAccessibility(locations=tuple(locations))

        try:
            fs = resolver_obj.fs
            relative_path = fs.get_remappable_relative_path(self.uri)
            if relative_path is None:
                return ArtifactAccessibility(locations=tuple(locations))
            raw_roots = (
                resolver_obj.artifact_recovery_roots(self)
                if isinstance(resolver_obj, ArtifactResolver)
                else extract_recovery_roots_from_meta(self.meta)
            )
            roots = fs.normalize_recovery_roots(raw_roots)
        except (AttributeError, OSError, RuntimeError, ValueError, TypeError):
            return ArtifactAccessibility(locations=tuple(locations))

        for index, root in enumerate(roots, start=1):
            candidate = (Path(root) / relative_path).resolve()
            locations.append(
                ArtifactLocationStatus(
                    label=f"recovery_root #{index}",
                    path=candidate,
                    exists=candidate.exists(),
                )
            )
        return ArtifactAccessibility(locations=tuple(locations))

    @property
    def locations(self) -> tuple[ArtifactLocationStatus, ...]:
        """Return the currently checked locations for this artifact."""
        return self.accessibility().locations

    @property
    def is_accessible(self) -> bool:
        """Return whether any checked location for this artifact exists locally."""
        return self.accessibility().accessible

    def as_df(
        self,
        tracker: Optional[Any] = None,
        *,
        db_fallback: Literal["inputs-only", "always", "never"] = "inputs-only",
        columns: Optional[Sequence[str]] = None,
        close: bool = True,
        **kwargs: Any,
    ) -> "pd.DataFrame":
        """Load this artifact as a pandas DataFrame through the active ``ArtifactResolver``."""
        resolver_obj = tracker if tracker is not None else self.resolver
        if resolver_obj is None:
            raise RuntimeError(
                f"Cannot load DataFrame for artifact {self.key!r}: "
                "as_df() requires a Tracker instance (or active ArtifactResolver)."
            )
        return cast(
            "pd.DataFrame",
            resolver_obj.load_artifact_df(
                self,
                db_fallback=db_fallback,
                columns=columns,
                close=close,
                **kwargs,
            ),
        )

    # --- Format & Metadata Helpers ---

    @property
    def is_matrix(self) -> bool:
        """Return True if the artifact represents array or matrix-like data."""
        return self.driver in ("zarr", "h5", "netcdf", "openmatrix")

    @property
    def is_tabular(self) -> bool:
        """Return True if the artifact represents tabular or spatial record data."""
        return (
            self.driver in DriverType.tabular_drivers()
            or self.driver in DriverType.spatial_drivers()
            or self.driver == "sql"
        )

    @property
    def created_at_iso(self) -> Optional[str]:
        """Return ``created_at`` as an ISO-8601 string."""
        return self.created_at.isoformat() if self.created_at else None

    # --- Serialization & Pydantic Integration (0 SQL queries) ---

    def to_json_dict(self) -> dict[str, Any]:
        """Serialize all persisted fields into a JSON-ready dict using pure memory reads."""
        return {
            "id": str(self.id),
            "key": self.key,
            "uri": self.uri,
            "driver": self.driver,
            "digest": str(self.digest) if self.digest is not None else None,
            "member": self.member,
            "run_id": self.run_id,
            "parent_artifact_id": (
                str(self.parent_artifact_id)
                if self.parent_artifact_id is not None
                else None
            ),
            "facet_id": self.facet_id,
            "created_at": self.created_at.isoformat(),
            "meta": _deep_copy_meta_value(self.meta),
        }

    @classmethod
    def from_json_dict(cls, data: MappingABC[str, Any]) -> "Artifact":
        """Hydrate an ``Artifact`` from either a ``v0.5`` or legacy ``v0.4`` JSON mapping."""
        if "uri" in data and data["uri"] is not None:
            uri_val = str(data["uri"])
        elif "container_uri" in data and data["container_uri"] is not None:
            uri_val = str(data["container_uri"])
        else:
            raise ValueError(
                "Artifact mapping requires 'uri' (or legacy 'container_uri')."
            )

        driver_val = str(data.get("driver") or "unknown")
        member_val = data.get("member")
        if member_val is None:
            member_val = data.get("table_path")
        if member_val is None:
            member_val = data.get("array_path")
        member_str = str(member_val) if member_val is not None else None

        raw_meta = data.get("meta")
        meta_copy: dict[str, Any] = (
            dict(raw_meta) if isinstance(raw_meta, MappingABC) else {}
        )

        if "digest" in data:
            raw_digest = data.get("digest")
            digest_obj = (
                raw_digest
                if isinstance(raw_digest, Digest) or raw_digest is None
                else Digest.parse(raw_digest)
            )
        else:
            digest_obj = _coerce_legacy_v04_hash(
                data.get("hash"),
                driver=driver_val,
                member=member_str,
                meta=meta_copy,
            )

        facet_id_raw = data.get("facet_id")
        if facet_id_raw is None and isinstance(meta_copy.get("artifact_facet_id"), str):
            facet_id_raw = meta_copy["artifact_facet_id"]

        return cls(
            id=data.get("id"),
            key=str(data["key"]),
            uri=uri_val,
            driver=driver_val,
            digest=digest_obj,
            member=member_str,
            run_id=(str(data["run_id"]) if data.get("run_id") is not None else None),
            parent_artifact_id=data.get("parent_artifact_id"),
            facet_id=(str(facet_id_raw) if facet_id_raw is not None else None),
            created_at=data.get("created_at"),
            meta=meta_copy,
        )

    @classmethod
    def model_validate(cls, value: Any) -> "Artifact":
        """Validate an ``Artifact`` instance or JSON-compatible mapping."""
        if isinstance(value, cls):
            return value
        if isinstance(value, MappingABC):
            return cls.from_json_dict(value)
        raise ValueError(f"Expected Artifact or mapping, got {type(value).__name__}")

    def model_dump(self, *, mode: str = "python") -> dict[str, Any]:
        """Return a detached dictionary of persisted fields (0 SQL queries)."""
        if mode == "json":
            return self.to_json_dict()
        return {
            "id": self.id,
            "key": self.key,
            "uri": self.uri,
            "driver": self.driver,
            "digest": self.digest,
            "member": self.member,
            "run_id": self.run_id,
            "parent_artifact_id": self.parent_artifact_id,
            "facet_id": self.facet_id,
            "created_at": self.created_at,
            "meta": _deep_copy_meta_value(self.meta),
        }

    def model_copy(
        self,
        *,
        update: Optional[MappingABC[str, Any]] = None,
        deep: bool = False,
    ) -> "Artifact":
        """Return an updated copy preserving ``_resolver_ref`` and ``_abs_path``."""
        base = copy.deepcopy(self) if deep else copy.copy(self)
        if not update:
            return base
        return replace(base, **dict(update))

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: Any,
    ) -> core_schema.CoreSchema:
        del source_type, handler
        return core_schema.no_info_plain_validator_function(
            cls.model_validate,
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda inst: inst.to_json_dict(),
                return_schema=core_schema.dict_schema(),
            ),
        )

    def __repr__(self) -> str:
        return f"<Artifact key='{self.key}' driver='{self.driver}' uri='{self.uri}'>"

    def __str__(self) -> str:
        return f"Artifact(key={self.key}, path={self.uri})"

    def __copy__(self) -> "Artifact":
        return self._fast_clone()

    def __deepcopy__(self, memo: Optional[dict[int, Any]] = None) -> "Artifact":
        if memo is not None and id(self) in memo:
            return cast("Artifact", memo[id(self)])
        cloned = self._fast_clone(meta=_freeze_meta_mapping(self.meta))
        if memo is not None:
            memo[id(self)] = cloned
        return cloned

    def __reduce__(self) -> tuple[Any, ...]:
        return (
            _reconstruct_artifact,
            (
                self.id,
                self.key,
                self.uri,
                self.driver,
                self.digest,
                self.member,
                self.run_id,
                self.parent_artifact_id,
                self.facet_id,
                self.created_at,
                _deep_copy_meta_value(self.meta),
                self._abs_path,
            ),
        )


def _reconstruct_artifact(
    id_val: uuid.UUID,
    key: str,
    uri: str,
    driver: str,
    digest: Optional[Digest],
    member: Optional[str],
    run_id: Optional[str],
    parent_artifact_id: Optional[uuid.UUID],
    facet_id: Optional[str],
    created_at: datetime,
    meta: dict[str, Any],
    abs_path: Optional[str],
) -> Artifact:
    return Artifact(
        id=id_val,
        key=key,
        uri=uri,
        driver=driver,
        digest=digest,
        member=member,
        run_id=run_id,
        parent_artifact_id=parent_artifact_id,
        facet_id=facet_id,
        created_at=created_at,
        meta=meta,
        _abs_path=abs_path,
    )


def extract_recovery_roots_from_meta(meta: MappingABC[str, Any] | None) -> list[str]:
    """Extract ordered string recovery roots from an artifact metadata mapping."""
    if not isinstance(meta, MappingABC):
        return []
    roots = meta.get("recovery_roots")
    if roots is None:
        return []
    if isinstance(roots, (str, Path)):
        return [str(roots)]
    if isinstance(roots, Sequence):
        return [str(item) for item in roots if isinstance(item, (str, Path))]
    return []


class ArchivedOutputs(MappingABC[str, Path]):
    """Read-only mapping of archived output paths plus refreshed ``Artifact`` values."""

    def __init__(
        self,
        paths: dict[str, Path],
        outputs: dict[str, Artifact],
    ) -> None:
        self._paths: dict[str, Path] = dict(paths)
        self.outputs: dict[str, Artifact] = outputs

    @property
    def paths(self) -> MappingABC[str, Path]:
        """Read-only view of the archived paths exposed by mapping access."""
        return MappingProxyType(self._paths)

    def __getitem__(self, key: str) -> Path:
        return self._paths[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._paths)

    def __len__(self) -> int:
        return len(self._paths)

    def __repr__(self) -> str:
        return f"ArchivedOutputs(paths={self._paths!r}, outputs={self.outputs!r})"


_SYSTEM_ARTIFACT_META_PREFIXES: tuple[str, ...] = (
    "_dlt_",
    "directory_",
    "output_set_",
    "file_bundle_",
)

_SYSTEM_ARTIFACT_META_KEYS: frozenset[str] = frozenset(
    {
        "is_ingested",
        "dlt_table_name",
        "recovery_roots",
        "schema_id",
        "schema_summary",
        "file_schema",
        "facet",
        "facet_schema_version",
        "facet_index",
        "year",
        "iteration",
        "tags",
    }
)


def preserve_system_artifact_meta(
    existing_meta: MappingABC[str, Any] | None,
) -> Dict[str, Any]:
    """Return a copy of internal/system metadata keys from ``existing_meta``."""
    if not isinstance(existing_meta, MappingABC):
        return {}
    return {
        key: value
        for key, value in existing_meta.items()
        if key.startswith(_SYSTEM_ARTIFACT_META_PREFIXES)
        or key in _SYSTEM_ARTIFACT_META_KEYS
    }


def merge_artifact_output_meta(
    existing_meta: MappingABC[str, Any] | None,
    updates: MappingABC[str, Any],
    *,
    merge: bool = True,
) -> Dict[str, Any]:
    """Apply user output metadata updates while preserving system metadata on replace."""
    base = (
        dict(existing_meta)
        if merge and isinstance(existing_meta, MappingABC)
        else preserve_system_artifact_meta(existing_meta)
    )
    base.update(updates)
    return base
