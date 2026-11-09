"""
Frozen, hashable, picklable v2 `Artifact` handle, `HintLedger`, and `MissingOutputError`.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field, replace
import importlib
from pathlib import Path
from typing import Any, Protocol, cast, runtime_checkable
import uuid
import warnings

from consist.digest import Digest, Identity
from consist.location import (
    EMPTY_META,
    FrozenMeta,
    LedgerRef,
    Observation,
    lookup_live_ledger,
)
from consist.store.detect import ConsistError

_TABLE_MEMBER_DRIVERS: frozenset[str] = frozenset(
    {"hdf5", "h5", "h5_table", "db", "dlt", "duckdb", "sqlite", "table"}
)
_ARRAY_MEMBER_DRIVERS: frozenset[str] = frozenset(
    {"zarr", "omx", "netcdf", "nc", "openmatrix", "matrix"}
)


class MissingOutputError(ConsistError):
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
        target = str(anchor)
        matched = [obs for obs in self.hints if str(obs.identity.anchor) == target]
        if not matched:
            matched = list(self.hints)
        return tuple(matched[: max(0, int(limit))])

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
        site_filtered = [obs for obs in self.hints if site is None or obs.site == site]
        matched = [
            obs
            for obs in site_filtered
            if not anchor_set or str(obs.identity.anchor) in anchor_set
        ]
        return tuple(matched if matched else site_filtered)

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
    schema_id: str | None = None
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
                self.schema_id,
                self.meta,
                bounded_hints,
            ),
        )

    @property
    def anchor(self) -> Digest:
        """Return the effective primary `Digest` anchor for this artifact."""
        return self.identity.anchor

    @property
    def hash(self) -> str:
        """Return the canonical scheme-qualified anchor string (e.g. `'sha256:file:<hex>'`)."""
        return str(self.identity.anchor)

    @property
    def path(self) -> Path:
        """Resolve and return the verified local filesystem `Path` for this artifact."""
        from consist.engine import locate

        return locate(self).path

    def with_meta(self, **changes: Any) -> Artifact:
        """Return a new `Artifact` with `changes` merged into `self.meta`."""
        merged: dict[str, Any] = {**self.meta.to_dict(), **changes}
        return replace(self, meta=FrozenMeta(merged))

    def as_df(self, **kwargs: Any) -> Any:
        """Load this artifact into a `pandas.DataFrame`."""
        from consist.engine import locate

        drv = self.driver.strip().lower()
        pd = importlib.import_module("pandas")
        try:
            resolved = locate(self)
        except FileNotFoundError:
            resolved = None

        if resolved is not None:
            local_path = resolved.path
            fmt = (
                local_path.suffix.lower().lstrip(".")
                if drv in ("file", "table")
                else drv
            )
            if fmt == "parquet":
                return pd.read_parquet(local_path, **kwargs)
            if fmt == "csv":
                return pd.read_csv(local_path, **kwargs)
            if fmt == "tsv":
                sep = kwargs.pop("sep", "\t")
                return pd.read_csv(local_path, sep=sep, **kwargs)
            if fmt == "json":
                return pd.read_json(local_path, **kwargs)
            if fmt in ("feather", "arrow"):
                return pd.read_feather(local_path, **kwargs)
            if fmt in ("hdf5", "h5", "h5_table", "hdf"):
                call_kwargs = (
                    {"key": resolved.member, **kwargs}
                    if resolved.member is not None
                    else kwargs
                )
                return pd.read_hdf(local_path, **call_kwargs)

        from consist.api import load_df

        return load_df(cast(Any, self), **kwargs)

    def _first_candidate_location(self) -> tuple[str | None, str | None]:
        from consist.engine import locate

        try:
            resolved = locate(self, verify_on_stale=False)
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
        return None, None

    @property
    def container_uri(self) -> str:
        """Deprecated v1 compatibility property; use `Artifact.path` or `consist.locate(artifact)`."""
        warnings.warn(
            "Artifact.container_uri is deprecated in Consist 0.5; "
            "use Artifact.path or consist.locate(artifact)",
            DeprecationWarning,
            stacklevel=2,
        )
        uri, _ = self._first_candidate_location()
        return uri or ""

    @property
    def abs_path(self) -> str:
        """Deprecated v1 compatibility property; use `str(Artifact.path)` or `consist.locate(artifact)`."""
        warnings.warn(
            "Artifact.abs_path is deprecated in Consist 0.5; "
            "use str(Artifact.path) or consist.locate(artifact)",
            DeprecationWarning,
            stacklevel=2,
        )
        return str(self.path)

    @property
    def table_path(self) -> str | None:
        """Deprecated v1 compatibility property; use `consist.locate(artifact).member`."""
        warnings.warn(
            "Artifact.table_path is deprecated in Consist 0.5; "
            "use consist.locate(artifact).member",
            DeprecationWarning,
            stacklevel=2,
        )
        if self.driver.strip().lower() not in _TABLE_MEMBER_DRIVERS:
            return None
        _, member = self._first_candidate_location()
        return member

    @property
    def array_path(self) -> str | None:
        """Deprecated v1 compatibility property; use `consist.locate(artifact).member`."""
        warnings.warn(
            "Artifact.array_path is deprecated in Consist 0.5; "
            "use consist.locate(artifact).member",
            DeprecationWarning,
            stacklevel=2,
        )
        if self.driver.strip().lower() not in _ARRAY_MEMBER_DRIVERS:
            return None
        _, member = self._first_candidate_location()
        return member


__all__ = [
    "Artifact",
    "HintLedger",
    "MissingOutputError",
]
