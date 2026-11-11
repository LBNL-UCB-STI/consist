"""
Frozen, hashable, picklable v2 `Artifact` handle, `HintLedger`, and `MissingOutputError`.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime
import importlib
import os
from pathlib import Path
import re
import threading
from types import MappingProxyType
from typing import Any, Literal, Protocol, cast, runtime_checkable
import uuid
import warnings
import weakref

from consist.digest import Digest, Identity, Kind
from consist.location import (
    EMPTY_META,
    FrozenMeta,
    LedgerRef,
    Location,
    Observation,
    SiteContext,
    deserialize_witness_json,
    lookup_live_ledger,
    serialize_witness_json,
)
from consist.meta import merge_artifact_output_meta, strip_system_meta
from consist.store.detect import ConsistError

_TABLE_MEMBER_DRIVERS: frozenset[str] = frozenset(
    {"hdf5", "h5", "h5_table", "db", "dlt", "duckdb", "sqlite", "table"}
)
_ARRAY_MEMBER_DRIVERS: frozenset[str] = frozenset(
    {"zarr", "omx", "netcdf", "nc", "openmatrix", "matrix"}
)
_TABULAR_DRIVERS: frozenset[str] = frozenset(
    {
        "parquet",
        "csv",
        "tsv",
        "h5_table",
        "json",
        "duckdb",
        "sqlite",
        "feather",
        "arrow",
        "table",
        "geoparquet",
        "geojson",
        "shapefile",
        "geopackage",
        "sql",
    }
)
_HEX64_RE = re.compile(r"^[0-9a-fA-F]{64}$")

_bound_tracker_lock = threading.Lock()
_bound_trackers: weakref.WeakValueDictionary[uuid.UUID, Any] = (
    weakref.WeakValueDictionary()
)


def _reset_bound_trackers_after_fork() -> None:
    global _bound_tracker_lock, _bound_trackers
    _bound_tracker_lock = threading.Lock()
    _bound_trackers = weakref.WeakValueDictionary()


if "register_at_fork" in os.__dict__:
    os.register_at_fork(after_in_child=_reset_bound_trackers_after_fork)


@runtime_checkable
class _TrackerDbPathProtocol(Protocol):
    db_path: Any


@runtime_checkable
class _TrackerMountsProtocol(Protocol):
    mounts: Mapping[str, Any]


@runtime_checkable
class _LedgerLocationsProtocol(Protocol):
    mounts: Mapping[str, Any]

    def locate_current(
        self,
        anchors: Sequence[Digest | str],
        *,
        site: str | None = None,
        include_evicted: bool = False,
    ) -> list[dict[str, Any]]: ...


@runtime_checkable
class _LedgerExecCursorProtocol(Protocol):
    def exec_cursor(self) -> Any: ...


@runtime_checkable
class _LedgerCursorProtocol(Protocol):
    def cursor(self) -> Any: ...


@runtime_checkable
class _SiteContextProtocol(Protocol):
    site: str


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
    def best_location(self) -> ArtifactLocationStatus | None:
        """Return the first reachable location, if any."""
        for location in self.locations:
            if location.exists:
                return location
        return None


class ArchivedOutputs(Mapping[str, Path]):
    """Read-only mapping of archived output paths paired with refreshed `Artifact` handles."""

    def __init__(
        self,
        paths: Mapping[str, Path],
        outputs: dict[str, Artifact],
    ) -> None:
        self._paths: dict[str, Path] = dict(paths)
        self.outputs: dict[str, Artifact] = dict(outputs)

    @property
    def paths(self) -> Mapping[str, Path]:
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


def read_local_df(
    path: Path,
    *,
    driver: str | None = None,
    member: str | None = None,
    default_h5_key: str | None = None,
    columns: Sequence[str] | None = None,
    **kwargs: Any,
) -> Any:
    """Load a local tabular file into a `pandas.DataFrame`."""
    pd = importlib.import_module("pandas")
    suffix = path.suffix.lower().lstrip(".")
    drv = (driver or "").strip().lower().lstrip(".")
    fmt = suffix if drv in ("", "file", "table") else drv

    read_kwargs = dict(kwargs)
    if columns is not None and "columns" not in read_kwargs:
        read_kwargs["columns"] = list(columns)

    if fmt in ("parquet", "pq") or suffix in ("parquet", "pq"):
        return pd.read_parquet(path, **read_kwargs)
    if fmt == "csv" or suffix == "csv":
        if "columns" in read_kwargs and "usecols" not in read_kwargs:
            read_kwargs["usecols"] = read_kwargs.pop("columns")
        return pd.read_csv(path, **read_kwargs)
    if fmt == "tsv" or suffix == "tsv":
        if "columns" in read_kwargs and "usecols" not in read_kwargs:
            read_kwargs["usecols"] = read_kwargs.pop("columns")
        sep = read_kwargs.pop("sep", "\t")
        return pd.read_csv(path, sep=sep, **read_kwargs)
    if fmt == "json" or suffix == "json":
        df = pd.read_json(path, **kwargs)
        return df[list(columns)] if columns is not None else df
    if fmt in ("feather", "arrow") or suffix in ("feather", "arrow"):
        return pd.read_feather(path, **read_kwargs)
    if fmt in ("pickle", "pkl") or suffix in ("pickle", "pkl"):
        df = pd.read_pickle(path, **kwargs)
        return df[list(columns)] if columns is not None else df
    if fmt in ("hdf5", "h5", "h5_table", "hdf", "openmatrix", "omx") or suffix in (
        "h5",
        "hdf5",
        "hdf",
        "omx",
    ):
        h5_key = member if member is not None else default_h5_key
        if h5_key is not None and "key" not in read_kwargs:
            read_kwargs["key"] = h5_key
        return pd.read_hdf(path, **read_kwargs)
    raise ValueError(
        f"Unsupported driver/format {driver or path.suffix!r} for read_local_df() on {path}"
    )


class MissingOutputError(ConsistError, FileNotFoundError):
    """Raised when a declared `PendingOutput` is missing on disk at `seal()` time."""

    def __init__(
        self,
        role: str,
        path: Path | str,
        message: str | None = None,
    ) -> None:
        self.role: str = str(role)
        self.path: Path = Path(path)
        super().__init__(
            message
            if message is not None
            else (
                f"Declared output for role {self.role!r} was not written at "
                f"expected path: {self.path}"
            )
        )


@runtime_checkable
class _CandidateLedgerProtocol(Protocol):
    def memoized_candidates(
        self,
        anchor: Digest | str,
        *,
        limit: int = 4,
    ) -> tuple[Observation, ...]: ...


class HintLedger:
    """Lightweight in-memory fallback ledger wrapping pickled `Observation` hints.

    Used in child worker processes where the originating `StoreLedger` is not open
    in `_live_ledgers`, allowing `locate()` and `Artifact.path` to resolve and
    verify local paths without opening `provenance.duckdb`.
    """

    def __init__(self, hints: Sequence[Observation] = ()) -> None:
        self.hints: tuple[Observation, ...] = tuple(hints)
        self._memoized_locations: dict[tuple[str, str, str], Any] = {}

    def memoized_candidates(
        self,
        anchor: Digest | str,
        *,
        limit: int = 4,
    ) -> tuple[Observation, ...]:
        """Return up to `limit` observations matching `anchor`."""
        return self.candidates_for(anchor)[: max(0, int(limit))]

    def candidates_for(
        self,
        anchors: Sequence[Digest | str] | Digest | str,
        *,
        site: str | None = None,
    ) -> tuple[Observation, ...]:
        """Return observations matching any of `anchors` (and optional `site`)."""
        anchor_set = (
            {str(anchors)}
            if isinstance(anchors, (Digest, str))
            else {str(a) for a in anchors}
        )
        if not anchor_set:
            return ()
        site_filtered = [obs for obs in self.hints if site is None or obs.site == site]
        matched = [
            obs for obs in site_filtered if str(obs.identity.anchor) in anchor_set
        ]
        return tuple(matched)

    def locate_current(
        self,
        anchors: Sequence[Digest | str],
        *,
        site: str | None = None,
        include_evicted: bool = False,
    ) -> list[dict[str, Any]]:
        """Return location rows compatible with `StoreLedger.locate_current`."""
        _ = include_evicted
        return [
            obs.to_location_row() for obs in self.candidates_for(anchors, site=site)
        ]

    def memoize_location(
        self,
        anchor: Digest | str,
        resolved: Any,
        *,
        member: str | None = None,
        site: str = "local",
    ) -> None:
        """Record a verified `ResolvedLocation` for `(anchor, member, site)` in memory."""
        self._memoized_locations[(str(anchor), member or "", site)] = resolved

    def get_memoized_location(
        self,
        anchor: Digest | str,
        *,
        member: str | None = None,
        site: str = "local",
    ) -> Any | None:
        """Look up a previously verified `ResolvedLocation` for `(anchor, member, site)`."""
        return self._memoized_locations.get((str(anchor), member or "", site))


def _dedupe_observations(
    observations: Sequence[Observation],
    *,
    limit: int = 4,
) -> tuple[Observation, ...]:
    seen: set[tuple[str, str, str, str, str]] = set()
    out: list[Observation] = []
    for obs in observations:
        if not isinstance(obs, Observation):
            raise TypeError(
                f"Artifact.hints elements must be Observation instances, got {type(obs).__name__}"
            )
        place = obs.place_key
        if place in seen:
            continue
        seen.add(place)
        out.append(obs)
        if len(out) >= limit:
            break
    return tuple(out)


def _quote_sql_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


@dataclass(frozen=True, slots=True, eq=False)
class Artifact:
    """Immutable, hashable, picklable public v2 artifact handle."""

    id: uuid.UUID
    key: str
    driver: str
    identity: Identity
    recorded_anchor: Digest | None = None
    origin: LedgerRef | None = None
    run_id: str | None = None
    parent_artifact_id: uuid.UUID | None = None
    schema_id: str | None = None
    created_at: datetime | None = None
    meta: FrozenMeta = field(default=EMPTY_META)
    hints: tuple[Observation, ...] = field(default=())

    def __post_init__(self) -> None:
        if isinstance(self.id, str):
            object.__setattr__(self, "id", uuid.UUID(self.id))
        elif not isinstance(self.id, uuid.UUID):
            raise TypeError(
                f"Artifact.id must be a UUID or UUID string, got {type(self.id).__name__}"
            )

        if not isinstance(self.key, str) or not self.key:
            raise ValueError("Artifact.key must be a non-empty string")
        if not isinstance(self.driver, str) or not self.driver:
            raise ValueError("Artifact.driver must be a non-empty string")
        if not isinstance(self.identity, Identity):
            raise TypeError(
                f"Artifact.identity must be an Identity instance, got {type(self.identity).__name__}"
            )

        if isinstance(self.recorded_anchor, str):
            object.__setattr__(
                self, "recorded_anchor", Digest.parse(self.recorded_anchor)
            )
        elif self.recorded_anchor is not None and not isinstance(
            self.recorded_anchor, Digest
        ):
            raise TypeError("Artifact.recorded_anchor must be a Digest or None")

        if isinstance(self.parent_artifact_id, str):
            object.__setattr__(
                self, "parent_artifact_id", uuid.UUID(self.parent_artifact_id)
            )
        elif self.parent_artifact_id is not None and not isinstance(
            self.parent_artifact_id, uuid.UUID
        ):
            raise TypeError("Artifact.parent_artifact_id must be a UUID or None")

        if isinstance(self.created_at, str):
            raw_iso = (
                f"{self.created_at[:-1]}+00:00"
                if self.created_at.endswith("Z")
                else self.created_at
            )
            object.__setattr__(self, "created_at", datetime.fromisoformat(raw_iso))
        elif self.created_at is not None and not isinstance(self.created_at, datetime):
            raise TypeError("Artifact.created_at must be a datetime or None")

        if self.meta is None:
            object.__setattr__(self, "meta", EMPTY_META)
        elif type(self.meta) is not FrozenMeta:
            object.__setattr__(self, "meta", FrozenMeta(self.meta))

        if not isinstance(self.hints, tuple):
            object.__setattr__(self, "hints", tuple(self.hints))
        for obs in self.hints:
            if not isinstance(obs, Observation):
                raise TypeError(
                    f"Artifact.hints elements must be Observation instances, got {type(obs).__name__}"
                )

    def __eq__(self, other: object) -> bool:
        return self.id == other.id if isinstance(other, Artifact) else NotImplemented

    def __hash__(self) -> int:
        return hash(self.id)

    def __reduce__(self) -> tuple[Any, ...]:
        bounded_hints = self.hints[:4]
        led = lookup_live_ledger(self.origin)
        if isinstance(led, _CandidateLedgerProtocol):
            live_cands = list(led.memoized_candidates(self.identity.anchor, limit=4))
            if len(live_cands) < 4 and self.recorded_anchor is not None:
                live_cands.extend(
                    led.memoized_candidates(
                        self.recorded_anchor,
                        limit=4 - len(live_cands),
                    )
                )
            if live_cands:
                bounded_hints = _dedupe_observations(
                    (*live_cands, *self.hints),
                    limit=4,
                )
        return (
            Artifact,
            (
                self.id,
                self.key,
                self.driver,
                self.identity,
                self.recorded_anchor,
                self.origin,
                self.run_id,
                self.parent_artifact_id,
                self.schema_id,
                self.created_at,
                self.meta,
                bounded_hints,
            ),
        )

    def bind_tracker(self, tracker: Any) -> Artifact:
        """Bind a weak runtime `Tracker` reference keyed by `self.id` for DB-less (`db_path=None`) runs."""
        if tracker is None:
            return self
        if isinstance(tracker, _TrackerDbPathProtocol) and tracker.db_path is not None:
            return self
        with _bound_tracker_lock:
            _bound_trackers[self.id] = tracker
        return self

    def resolve_bound_tracker(self) -> Any | None:
        """Return the bound runtime `Tracker` for this DB-less `Artifact`, if still alive."""
        with _bound_tracker_lock:
            return _bound_trackers.get(self.id)

    def _effective_tracker_mounts(
        self, explicit_tracker: Any = None
    ) -> Mapping[str, Any] | None:
        trk = (
            explicit_tracker
            if explicit_tracker is not None
            else self.resolve_bound_tracker()
        )
        if isinstance(trk, _TrackerMountsProtocol) and trk.mounts:
            return trk.mounts
        return None

    @property
    def anchor(self) -> Digest:
        """Return the effective primary `Digest` anchor for this artifact."""
        return self.identity.anchor

    @property
    def hash(self) -> str:
        """Return the canonical scheme-qualified anchor string (e.g. `'sha256:file:<hex>'`)."""
        return str(self.identity.anchor)

    @property
    def created_at_iso(self) -> str | None:
        """Return `created_at` as an ISO-8601 string, or `None` if unset."""
        return self.created_at.isoformat() if self.created_at is not None else None

    @property
    def path(self) -> Path:
        """Resolve and return the verified local filesystem `Path` for this artifact."""
        from consist.engine import locate

        return locate(self, mounts=self._effective_tracker_mounts()).path

    def as_path(self, tracker: Any = None) -> Path:
        """Resolve and return the local filesystem `Path`, using `tracker`'s mounts when provided."""
        from consist.engine import locate

        return locate(self, mounts=self._effective_tracker_mounts(tracker)).path

    def locate(
        self,
        *,
        ledger: Any = None,
        site_context: Any = None,
        mounts: Any = None,
        verify_on_stale: bool = True,
    ) -> Path | None:
        """Resolve and return a verified local filesystem `Path` for this artifact, or `None` if not found."""
        from consist.engine import locate

        effective_mounts = (
            mounts if mounts is not None else self._effective_tracker_mounts()
        )
        try:
            return locate(
                self,
                ledger=ledger,
                site_context=site_context,
                mounts=effective_mounts,
                verify_on_stale=verify_on_stale,
            ).path
        except FileNotFoundError:
            return None

    @property
    def member(self) -> str | None:
        """Return the sub-container member path (table or array key) if present."""
        _, mem = self._first_candidate_location()
        return mem

    @property
    def primary_location(self) -> Location | None:
        """Return the first candidate `Location` for this artifact if available."""
        uri, mem = self._first_candidate_location()
        if uri is None:
            return None
        return Location(uri=uri, member=mem)

    @property
    def recovery_roots(self) -> list[str]:
        """Return ordered non-workspace mount roots where this artifact has been observed."""
        from consist.engine import _default_mounts, _normalize_mounts

        mount_sources: dict[str, Any] = dict(_default_mounts)
        led = lookup_live_ledger(self.origin)
        if isinstance(led, _LedgerLocationsProtocol):
            mount_sources.update(led.mounts)
        trk_mounts = self._effective_tracker_mounts()
        if trk_mounts:
            mount_sources.update(_normalize_mounts(trk_mounts))

        candidate_uris: list[str] = [obs.location.uri for obs in self.hints]
        if isinstance(led, _LedgerLocationsProtocol):
            anchors = [self.identity.anchor]
            if self.recorded_anchor is not None:
                anchors.append(self.recorded_anchor)
            candidate_uris.extend(
                u
                for row in led.locate_current(anchors, include_evicted=False)
                if (u := str(row.get("uri") or ""))
            )

        raw_roots: list[str] = []
        for uri in candidate_uris:
            if "://" not in uri:
                continue
            scheme, _ = uri.split("://", 1)
            if scheme in ("workspace", "file", "db", "http", "https", "s3", "gs"):
                continue
            mount = mount_sources.get(scheme)
            if mount is not None:
                raw_roots.append(str(mount.root.expanduser().resolve()))

        raw_meta_roots = self.meta.get("recovery_roots")
        if isinstance(raw_meta_roots, str):
            raw_roots.append(raw_meta_roots)
        elif isinstance(raw_meta_roots, Sequence) and not isinstance(
            raw_meta_roots, (str, bytes)
        ):
            raw_roots.extend(
                str(item) for item in raw_meta_roots if isinstance(item, (str, Path))
            )
        return list(dict.fromkeys(raw_roots))

    def _active_hot_table_name(
        self, explicit_ledger: Any = None
    ) -> tuple[str | None, Any]:
        led = (
            explicit_ledger
            if explicit_ledger is not None
            else lookup_live_ledger(self.origin)
        )
        anchors = [self.identity.anchor]
        if self.recorded_anchor is not None:
            anchors.append(self.recorded_anchor)

        if isinstance(led, _LedgerLocationsProtocol):
            rows = led.locate_current(anchors, include_evicted=False)
            for row in rows:
                uri = str(row.get("uri") or "")
                if uri.startswith("db://global_tables/"):
                    tbl = uri[len("db://global_tables/") :].split("?", 1)[0].strip("/")
                    if tbl:
                        return (tbl, led)
            if rows:
                return (None, led)

        for obs in self.hints:
            if obs.location.uri.startswith("db://global_tables/"):
                tbl = (
                    obs.location.uri[len("db://global_tables/") :]
                    .split("?", 1)[0]
                    .strip("/")
                )
                if tbl:
                    return (tbl, led)
        return (None, led)

    @property
    def is_ingested(self) -> bool:
        """Return `True` when an active `db://global_tables/...` observation exists."""
        tbl, _ = self._active_hot_table_name()
        return tbl is not None

    @property
    def is_matrix(self) -> bool:
        """Return `True` if `driver` represents a matrix or multi-dimensional array format."""
        return self.driver.strip().lower() in _ARRAY_MEMBER_DRIVERS | {"h5", "hdf5"}

    @property
    def is_tabular(self) -> bool:
        """Return `True` if `driver` represents a tabular or spatial-table format."""
        return self.driver.strip().lower() in _TABULAR_DRIVERS

    def get_meta(self, key: str, default: Any = None) -> Any:
        """Safely retrieve `key` from `self.meta`."""
        return self.meta.get(key, default)

    def with_meta(
        self,
        meta: Mapping[str, Any] | None = None,
        *,
        merge: bool = True,
        **updates: Any,
    ) -> Artifact:
        """Return a new `Artifact` with updated `FrozenMeta` (preserving domain-profile keys on replace)."""
        incoming: dict[str, Any] = {}
        if meta is not None:
            incoming.update(meta)
        if updates:
            incoming.update(updates)
        if merge and meta is None and not updates:
            return self
        merged_dict = merge_artifact_output_meta(self.meta, incoming, merge=merge)
        return replace(
            self,
            meta=FrozenMeta(merged_dict) if merged_dict else EMPTY_META,
        )

    def with_schema(
        self,
        schema_id: str | None,
        summary: Mapping[str, Any] | None = None,
        *,
        profile: Mapping[str, Any] | None = None,
    ) -> Artifact:
        """Return a new `Artifact` with `schema_id` and optional schema summary/profile metadata."""
        meta_updates: dict[str, Any] = {}
        if summary is not None:
            meta_updates["schema_summary"] = dict(summary)
        if profile is not None:
            meta_updates["schema_profile"] = dict(profile)
        new_meta = (
            FrozenMeta(merge_artifact_output_meta(self.meta, meta_updates, merge=True))
            if meta_updates
            else self.meta
        )
        return replace(self, schema_id=schema_id, meta=new_meta)

    def with_hints(self, hints: Sequence[Observation]) -> Artifact:
        """Return a new `Artifact` with deduplicated `Observation` hints."""
        seen: set[tuple[tuple[str, str, str, str, str], str]] = set()
        deduped: list[Observation] = []
        for obs in hints:
            if not isinstance(obs, Observation):
                raise TypeError(
                    f"Artifact.hints elements must be Observation instances, got {type(obs).__name__}"
                )
            key = (obs.place_key, str(obs.identity.anchor))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(obs)
        return replace(self, hints=tuple(deduped))

    def accessibility(
        self,
        tracker: Any = None,
        *,
        site: Any = None,
    ) -> ArtifactAccessibility:
        """Inspect primary and secondary/recovery locations for this artifact."""
        from consist.engine import (
            _default_mounts,
            _normalize_mounts,
            resolve_uri_to_path,
        )

        effective_mounts: dict[str, Any] = dict(_default_mounts)
        led = lookup_live_ledger(self.origin)
        if isinstance(led, _LedgerLocationsProtocol):
            effective_mounts.update(led.mounts)
        trk_mounts = self._effective_tracker_mounts(tracker)
        if trk_mounts:
            effective_mounts.update(_normalize_mounts(trk_mounts))

        site_name: str | None = None
        if isinstance(site, str):
            site_name = site
        elif isinstance(site, (SiteContext, _SiteContextProtocol)):
            site_name = str(site.site)

        candidate_uris: list[str] = []
        if isinstance(led, _LedgerLocationsProtocol):
            anchors = [self.identity.anchor]
            if self.recorded_anchor is not None:
                anchors.append(self.recorded_anchor)
            for row in led.locate_current(
                anchors, site=site_name, include_evicted=False
            ):
                u = str(row.get("uri") or "")
                if u and not u.startswith("db://") and u not in candidate_uris:
                    candidate_uris.append(u)

        for obs in self.hints:
            if site_name is not None and obs.site not in ("local", site_name):
                continue
            u = obs.location.uri
            if u and not u.startswith("db://") and u not in candidate_uris:
                candidate_uris.append(u)

        statuses: list[ArtifactLocationStatus] = []
        seen_paths: set[Path] = set()
        for idx, uri in enumerate(candidate_uris):
            resolved_path, _ = resolve_uri_to_path(uri, effective_mounts)
            check_path = resolved_path if resolved_path is not None else Path(uri)
            if check_path in seen_paths:
                continue
            seen_paths.add(check_path)
            label = "primary" if idx == 0 else f"recovery_root #{len(statuses)}"
            try:
                exists = resolved_path is not None and check_path.exists()
            except OSError:
                exists = False
            statuses.append(
                ArtifactLocationStatus(label=label, path=check_path, exists=exists)
            )
        return ArtifactAccessibility(locations=tuple(statuses))

    @property
    def locations(self) -> tuple[ArtifactLocationStatus, ...]:
        """Return the currently checked locations for this artifact."""
        return self.accessibility().locations

    def as_df(
        self,
        tracker: Any = None,
        *,
        db_fallback: Literal["inputs-only", "always", "never"] = "inputs-only",
        columns: Sequence[str] | None = None,
        close: bool = True,
        **kwargs: Any,
    ) -> Any:
        """Load this artifact into a `pandas.DataFrame` (local disk first, then `global_tables` fallback)."""
        if db_fallback not in ("inputs-only", "always", "never"):
            raise ValueError(
                f"Invalid db_fallback={db_fallback!r}. Expected 'inputs-only', 'always', or 'never'."
            )
        from consist.engine import locate

        try:
            resolved = locate(self, mounts=self._effective_tracker_mounts(tracker))
        except FileNotFoundError:
            resolved = None

        if resolved is not None:
            try:
                return read_local_df(
                    resolved.path,
                    driver=self.driver,
                    member=resolved.member,
                    default_h5_key="data",
                    columns=columns,
                    **kwargs,
                )
            except ValueError:
                pass

        hot_table, led = self._active_hot_table_name()
        if hot_table is not None and isinstance(
            led, (_LedgerExecCursorProtocol, _LedgerCursorProtocol)
        ):
            if db_fallback == "never":
                raise FileNotFoundError(
                    f"Artifact {self.key!r} (id={self.id}) is missing on disk and db_fallback='never'."
                )
            if db_fallback == "inputs-only" and tracker is not None:
                # Enforce active-run input check when an explicit tracker is supplied
                cur_rec = tracker.current_consist
                if cur_rec is None or tracker.is_cached:
                    raise FileNotFoundError(
                        f"Artifact {self.key!r} (id={self.id}) is missing on disk and db_fallback='inputs-only' requires an active non-cached run."
                    )
                if not any(a.id == self.id for a in cur_rec.inputs):
                    raise FileNotFoundError(
                        f"Artifact {self.key!r} (id={self.id}) is not a declared input to the active run."
                    )

            def _read_hot_df(cur: Any) -> Any:
                q_table = _quote_sql_ident(hot_table)
                col_rows = cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'global_tables' AND table_name = ? "
                    "ORDER BY ordinal_position",
                    [hot_table],
                ).fetchall()
                table_cols = [str(r[0]) for r in col_rows]
                if columns is not None:
                    select_sql = ", ".join(_quote_sql_ident(str(c)) for c in columns)
                else:
                    user_cols = [
                        c
                        for c in table_cols
                        if not c.startswith("_dlt_")
                        and c not in ("consist_artifact_id", "consist_run_id")
                    ]
                    select_sql = (
                        ", ".join(_quote_sql_ident(c) for c in user_cols)
                        if user_cols
                        else "*"
                    )
                params: list[Any] = []
                where_sql = ""
                if "consist_artifact_id" in table_cols:
                    where_sql = " WHERE CAST(consist_artifact_id AS VARCHAR) = ?"
                    params.append(str(self.id))
                limit_sql = ""
                if kwargs.get("nrows") is not None:
                    limit_sql = f" LIMIT {int(kwargs['nrows'])}"
                return cur.execute(
                    f"SELECT {select_sql} FROM global_tables.{q_table}{where_sql}{limit_sql}",
                    params,
                ).df()

            if isinstance(led, _LedgerExecCursorProtocol):
                with led.exec_cursor() as cur:
                    return _read_hot_df(cur)
            cur = led.cursor()
            try:
                return _read_hot_df(cur)
            finally:
                cur.close()

        from consist.api import load_df

        return load_df(
            cast(Any, self),
            tracker=tracker,
            db_fallback=db_fallback,
            columns=columns,
            close=close,
            **kwargs,
        )

    def _first_candidate_location(self) -> tuple[str | None, str | None]:
        from consist.engine import locate

        try:
            resolved = locate(
                self,
                mounts=self._effective_tracker_mounts(),
                verify_on_stale=False,
            )
            return resolved.uri, resolved.member
        except Exception:
            pass
        if self.hints:
            first_loc = self.hints[0].location
            return first_loc.uri, first_loc.member
        led = lookup_live_ledger(self.origin)
        if isinstance(led, _CandidateLedgerProtocol):
            cands = led.memoized_candidates(self.identity.anchor, limit=1)
            if cands:
                return cands[0].location.uri, cands[0].location.member
        if isinstance(led, _LedgerLocationsProtocol):
            rows = led.locate_current([self.identity.anchor], include_evicted=True)
            if rows:
                mem = (
                    str(rows[0]["member"])
                    if rows[0].get("member") is not None
                    else None
                )
                return str(rows[0]["uri"]), mem
        return None, None

    @property
    def container_uri(self) -> str:
        """Deprecated v1 compatibility property; use `Artifact.primary_location` or `consist.locate(artifact)`."""
        warnings.warn(
            "Artifact.container_uri is deprecated in Consist 0.5; "
            "use Artifact.path or consist.locate(artifact)",
            DeprecationWarning,
            stacklevel=2,
        )
        uri, _ = self._first_candidate_location()
        return uri or ""

    @property
    def abs_path(self) -> str | None:
        """Deprecated v1 compatibility property; use `str(Artifact.path)` or `Artifact.locate()`."""
        warnings.warn(
            "Artifact.abs_path is deprecated in Consist 0.5; "
            "use str(Artifact.path) or consist.locate(artifact)",
            DeprecationWarning,
            stacklevel=2,
        )
        resolved = self.locate()
        return str(resolved) if resolved is not None else None

    @property
    def table_path(self) -> str | None:
        """Deprecated v1 compatibility property; use `Artifact.member`."""
        warnings.warn(
            "Artifact.table_path is deprecated in Consist 0.5; "
            "use consist.locate(artifact).member",
            DeprecationWarning,
            stacklevel=2,
        )
        if self.driver.strip().lower() not in _TABLE_MEMBER_DRIVERS:
            return None
        _, mem = self._first_candidate_location()
        return mem

    @property
    def array_path(self) -> str | None:
        """Deprecated v1 compatibility property; use `Artifact.member`."""
        warnings.warn(
            "Artifact.array_path is deprecated in Consist 0.5; "
            "use consist.locate(artifact).member",
            DeprecationWarning,
            stacklevel=2,
        )
        if self.driver.strip().lower() not in _ARRAY_MEMBER_DRIVERS:
            return None
        _, mem = self._first_candidate_location()
        return mem

    def _pydantic_serialize(self) -> dict[str, Any]:
        loc = self.primary_location
        return {
            "id": str(self.id),
            "key": self.key,
            "driver": self.driver,
            "hash": self.hash,
            "anchor": str(self.identity.anchor),
            "claims": [str(c) for c in self.identity.claims],
            "recorded_anchor": (
                str(self.recorded_anchor) if self.recorded_anchor is not None else None
            ),
            "origin": (
                {
                    "db_uid": self.origin.db_uid,
                    "instance_key": self.origin.instance_key,
                    "db_uri": self.origin.db_uri,
                }
                if self.origin is not None
                else None
            ),
            "run_id": self.run_id,
            "parent_artifact_id": (
                str(self.parent_artifact_id)
                if self.parent_artifact_id is not None
                else None
            ),
            "schema_id": self.schema_id,
            "created_at": self.created_at_iso,
            "container_uri": loc.uri if loc is not None else "",
            "member": loc.member if loc is not None else None,
            "meta": self.meta.to_dict(),
            "locations": [
                {
                    "uri": obs.location.uri,
                    "member": obs.location.member,
                    "anchor": str(obs.identity.anchor),
                    "claims": [str(c) for c in obs.identity.claims],
                    "access_driver": obs.access_driver,
                    "basis": obs.basis,
                    "site": obs.site,
                    "scope_key": obs.scope_key,
                    "mount_instance_id": obs.mount_instance_id,
                    "observed_at_ns": obs.observed_at_ns,
                    "witness": serialize_witness_json(obs.witness),
                }
                for obs in self.hints
            ],
        }

    @classmethod
    def _pydantic_validate(cls, value: Any) -> Artifact:
        if isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise ValueError(
                f"Expected Artifact or mapping, got {type(value).__name__}"
            )

        raw_id = value.get("id")
        art_id = uuid.UUID(str(raw_id)) if raw_id is not None else uuid.uuid4()
        key = str(value["key"])
        driver = str(value.get("driver") or "file")

        raw_identity = value.get("identity")
        is_v2_payload = "anchor" in value or isinstance(raw_identity, Identity)
        if isinstance(raw_identity, Identity):
            identity = raw_identity
        else:
            raw_anchor = value.get("anchor") or value.get("hash")
            if isinstance(raw_anchor, Digest):
                anchor = raw_anchor
            elif isinstance(raw_anchor, str) and ":" in raw_anchor:
                anchor = Digest.parse(raw_anchor)
            elif isinstance(raw_anchor, str) and _HEX64_RE.fullmatch(
                raw_anchor.strip()
            ):
                anchor = Digest("legacy-v1:unverified", raw_anchor.strip().lower())
            else:
                import hashlib

                fallback_hex = hashlib.sha256(
                    f"{art_id}:{key}".encode("utf-8")
                ).hexdigest()
                anchor = Digest("legacy-v1:unverified", fallback_hex)

            raw_claims = value.get("claims") or ()
            claims = (
                ()
                if anchor.kind is Kind.HISTORICAL
                else tuple(
                    c if isinstance(c, Digest) else Digest.parse(str(c))
                    for c in raw_claims
                )
            )
            identity = Identity(anchor=anchor, claims=claims)

        raw_rec = value.get("recorded_anchor")
        recorded_anchor = (
            raw_rec
            if isinstance(raw_rec, Digest)
            else (Digest.parse(str(raw_rec)) if raw_rec else None)
        )

        raw_origin = value.get("origin")
        if isinstance(raw_origin, LedgerRef):
            origin: LedgerRef | None = raw_origin
        elif (
            isinstance(raw_origin, Mapping)
            and raw_origin.get("db_uid")
            and raw_origin.get("instance_key")
        ):
            origin = LedgerRef(
                db_uid=str(raw_origin["db_uid"]),
                instance_key=str(raw_origin["instance_key"]),
                db_uri=(
                    str(raw_origin["db_uri"])
                    if raw_origin.get("db_uri") is not None
                    else None
                ),
            )
        else:
            origin = None

        raw_hints = value.get("hints")
        hints: list[Observation] = []
        if isinstance(raw_hints, Sequence) and not isinstance(raw_hints, (str, bytes)):
            hints.extend(h for h in raw_hints if isinstance(h, Observation))
        elif isinstance(value.get("locations"), Sequence):
            for loc_item in value["locations"]:
                if isinstance(loc_item, Mapping) and loc_item.get("uri"):
                    raw_basis = str(loc_item.get("basis") or "declared")
                    basis_val: Any = (
                        raw_basis
                        if raw_basis
                        in (
                            "computed",
                            "derived",
                            "implied",
                            "declared",
                            "trusted_foreign",
                            "migrated_v1",
                        )
                        else "declared"
                    )
                    raw_obs_anchor = loc_item.get("anchor")
                    if raw_obs_anchor:
                        obs_anchor = (
                            raw_obs_anchor
                            if isinstance(raw_obs_anchor, Digest)
                            else Digest.parse(str(raw_obs_anchor))
                        )
                        raw_obs_claims = loc_item.get("claims") or ()
                        obs_claims = (
                            ()
                            if obs_anchor.kind is Kind.HISTORICAL
                            else tuple(
                                c if isinstance(c, Digest) else Digest.parse(str(c))
                                for c in raw_obs_claims
                            )
                        )
                        obs_identity = Identity(anchor=obs_anchor, claims=obs_claims)
                    else:
                        obs_identity = identity
                    hints.append(
                        Observation(
                            location=Location(
                                uri=str(loc_item["uri"]),
                                member=(
                                    str(loc_item["member"])
                                    if loc_item.get("member") is not None
                                    else None
                                ),
                            ),
                            identity=obs_identity,
                            access_driver=str(loc_item.get("access_driver") or driver),
                            witness=deserialize_witness_json(loc_item.get("witness")),
                            basis=basis_val,
                            site=str(loc_item.get("site") or "local"),
                            scope_key=(
                                str(loc_item["scope_key"])
                                if loc_item.get("scope_key") is not None
                                else None
                            ),
                            mount_instance_id=(
                                str(loc_item["mount_instance_id"])
                                if loc_item.get("mount_instance_id") is not None
                                else None
                            ),
                            observed_at_ns=int(loc_item.get("observed_at_ns") or 0),
                        )
                    )

        if not hints and origin is None and value.get("container_uri"):
            raw_member = (
                value.get("member")
                or value.get("table_path")
                or value.get("array_path")
            )
            hints.append(
                Observation(
                    location=Location(
                        uri=str(value["container_uri"]),
                        member=str(raw_member) if raw_member is not None else None,
                    ),
                    identity=identity,
                    access_driver=driver,
                    basis="declared",
                )
            )

        raw_meta = value.get("meta")
        if isinstance(raw_meta, Mapping):
            cleaned_meta = (
                FrozenMeta(raw_meta) if is_v2_payload else strip_system_meta(raw_meta)
            )
        else:
            cleaned_meta = EMPTY_META
        raw_parent = value.get("parent_artifact_id")
        raw_schema = value.get("schema_id")
        raw_run = value.get("run_id")
        raw_created = value.get("created_at")

        return cls(
            id=art_id,
            key=key,
            driver=driver,
            identity=identity,
            recorded_anchor=recorded_anchor,
            origin=origin,
            run_id=str(raw_run) if raw_run is not None else None,
            parent_artifact_id=(
                raw_parent
                if isinstance(raw_parent, uuid.UUID)
                else (uuid.UUID(str(raw_parent)) if raw_parent else None)
            ),
            schema_id=str(raw_schema) if raw_schema is not None else None,
            created_at=raw_created,  # type: ignore[arg-type]
            meta=cleaned_meta,
            hints=tuple(hints),
        )

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: Any,
    ) -> Any:
        _ = (source_type, handler)
        from pydantic_core import core_schema

        return core_schema.no_info_plain_validator_function(
            cls._pydantic_validate,
            serialization=core_schema.plain_serializer_function_ser_schema(
                cls._pydantic_serialize,
                info_arg=False,
                return_schema=core_schema.dict_schema(),
            ),
        )


__all__ = [
    "ArchivedOutputs",
    "Artifact",
    "ArtifactAccessibility",
    "ArtifactLocationStatus",
    "HintLedger",
    "MissingOutputError",
    "read_local_df",
]
