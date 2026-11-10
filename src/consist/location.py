"""
Physical location spoke primitives: `Freshness`, `Witness` types, `Mount`,
`SiteContext`, `Location`, `Observation`, `LedgerRef`, and re-exported
`FrozenMeta` / `FrozenList` / `EMPTY_META`.

This is a stdlib-only leaf module in Consist's dependency graph.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import os
from pathlib import Path
import socket
import threading
import time
from typing import Any, Literal, TypeAlias
import uuid
import weakref

from consist.digest import Identity
from consist.meta import EMPTY_META, FrozenList, FrozenMeta

_RACY_WINDOW_NS: int = 2_000_000_000


class Freshness(Enum):
    """Result of checking a stored `Witness` against the live filesystem or store."""

    FRESH = "fresh"
    STALE = "stale"
    MISSING = "missing"


def _scandir_files(root_path: Path) -> list[tuple[str, Path]]:
    files: list[tuple[str, Path]] = []

    def visit(directory: Path) -> None:
        with os.scandir(directory) as scan:
            children = sorted(scan, key=lambda entry: entry.name)
        for child in children:
            child_path = Path(child.path)
            if child.is_dir(follow_symlinks=False):
                visit(child_path)
            elif child.is_file(follow_symlinks=False):
                files.append((child_path.relative_to(root_path).as_posix(), child_path))

    visit(root_path)
    files.sort(key=lambda pair: pair[0])
    return files


@dataclass(frozen=True, slots=True)
class FileStat:
    """Stat witness for a single regular file.

    Treats `racy=True` (`observed_at_ns - mtime_ns < 2_000_000_000`) as `STALE`
    once the 2-second filesystem timestamp granularity window has elapsed.
    """

    size: int
    mtime_ns: int
    ctime_ns: int | None = None
    ino: int | None = None
    racy: bool = False

    def __post_init__(self) -> None:
        if self.size < 0:
            raise ValueError(f"FileStat.size must be non-negative, got {self.size}")

    @classmethod
    def from_os_stat(
        cls,
        st: os.stat_result,
        *,
        observed_at_ns: int | None = None,
    ) -> FileStat:
        """Construct a `FileStat` witness from an `os.stat_result`."""
        now_ns = time.time_ns() if observed_at_ns is None else observed_at_ns
        mtime_ns = (
            st.st_mtime_ns
            if st.st_mtime_ns is not None
            else int(st.st_mtime * 1_000_000_000)
        )
        ctime_ns = (
            st.st_ctime_ns
            if st.st_ctime_ns is not None
            else int(st.st_ctime * 1_000_000_000)
        )
        return cls(
            size=st.st_size,
            mtime_ns=mtime_ns,
            ctime_ns=ctime_ns,
            ino=st.st_ino,
            racy=(now_ns - mtime_ns) < _RACY_WINDOW_NS,
        )

    @classmethod
    def capture(
        cls,
        path: Path | str,
        *,
        observed_at_ns: int | None = None,
    ) -> FileStat:
        """Stat `path` and construct a `FileStat` witness."""
        return cls.from_os_stat(
            Path(path).stat(),
            observed_at_ns=observed_at_ns,
        )

    def check(
        self,
        path: Path | str,
        *,
        now_ns: int | None = None,
    ) -> Freshness:
        """Check whether `path` still matches this `FileStat` witness."""
        try:
            st = Path(path).stat()
        except (FileNotFoundError, NotADirectoryError):
            return Freshness.MISSING

        effective_now_ns = time.time_ns() if now_ns is None else now_ns
        if self.racy and (effective_now_ns - self.mtime_ns) >= _RACY_WINDOW_NS:
            return Freshness.STALE
        if st.st_size != self.size or st.st_mtime_ns != self.mtime_ns:
            return Freshness.STALE
        if self.ctime_ns is not None and st.st_ctime_ns != self.ctime_ns:
            return Freshness.STALE
        if self.ino is not None and st.st_ino != self.ino:
            return Freshness.STALE
        return Freshness.FRESH

    def to_json_dict(self) -> dict[str, Any]:
        """Serialize this `FileStat` to a JSON-compatible dictionary."""
        return {
            "size": self.size,
            "mtime_ns": self.mtime_ns,
            "ctime_ns": self.ctime_ns,
            "ino": self.ino,
            "racy": self.racy,
        }

    @classmethod
    def from_json_dict(cls, data: Mapping[str, Any]) -> FileStat:
        """Deserialize a `FileStat` from a JSON dictionary."""
        return cls(
            size=int(data["size"]),
            mtime_ns=int(data["mtime_ns"]),
            ctime_ns=(
                int(data["ctime_ns"]) if data.get("ctime_ns") is not None else None
            ),
            ino=int(data["ino"]) if data.get("ino") is not None else None,
            racy=bool(data.get("racy", False)),
        )


@dataclass(frozen=True, slots=True)
class DirTreeStat:
    """Stat witness for a directory tree."""

    file_count: int = 0
    total_bytes: int = 0
    max_mtime_ns: int = 0
    entries: tuple[tuple[str, FileStat], ...] = ()
    racy: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.entries, tuple):
            object.__setattr__(self, "entries", tuple(self.entries))

    @classmethod
    def capture(
        cls,
        root: Path | str,
        *,
        observed_at_ns: int | None = None,
    ) -> DirTreeStat:
        """Walk `root` deterministically and capture a `DirTreeStat` witness."""
        root_path = Path(root)
        if not root_path.is_dir():
            raise ValueError(f"DirTreeStat root is not a directory: {root_path}")
        now_ns = time.time_ns() if observed_at_ns is None else observed_at_ns
        root_st = root_path.stat()

        captured_entries = [
            (rel, FileStat.capture(fpath, observed_at_ns=now_ns))
            for rel, fpath in _scandir_files(root_path)
        ]
        total_bytes = sum(fs.size for _, fs in captured_entries)
        max_mtime_ns = max(
            (fs.mtime_ns for _, fs in captured_entries),
            default=root_st.st_mtime_ns,
        )
        racy = any(fs.racy for _, fs in captured_entries) or (
            (now_ns - max_mtime_ns) < _RACY_WINDOW_NS
        )
        return cls(
            file_count=len(captured_entries),
            total_bytes=total_bytes,
            max_mtime_ns=max_mtime_ns,
            entries=tuple(captured_entries),
            racy=racy,
        )

    def check(
        self,
        root: Path | str,
        *,
        now_ns: int | None = None,
    ) -> Freshness:
        """Check whether `root` still matches this `DirTreeStat` witness."""
        root_path = Path(root)
        if not root_path.exists() or not root_path.is_dir():
            return Freshness.MISSING

        effective_now_ns = time.time_ns() if now_ns is None else now_ns
        if self.racy and (effective_now_ns - self.max_mtime_ns) >= _RACY_WINDOW_NS:
            return Freshness.STALE

        try:
            current_files = _scandir_files(root_path)
        except OSError:
            return Freshness.MISSING

        if self.entries:
            if len(current_files) != len(self.entries):
                return Freshness.STALE
            for (expected_rel, expected_stat), (actual_rel, actual_path) in zip(
                self.entries, current_files, strict=True
            ):
                if expected_rel != actual_rel:
                    return Freshness.STALE
                if (
                    expected_stat.check(actual_path, now_ns=effective_now_ns)
                    is not Freshness.FRESH
                ):
                    return Freshness.STALE
            return Freshness.FRESH

        if len(current_files) != self.file_count:
            return Freshness.STALE
        stats = [fpath.stat() for _, fpath in current_files]
        total_bytes = sum(st.st_size for st in stats)
        max_mtime = max(
            (st.st_mtime_ns for st in stats),
            default=root_path.stat().st_mtime_ns,
        )
        if total_bytes != self.total_bytes or max_mtime != self.max_mtime_ns:
            return Freshness.STALE
        return Freshness.FRESH

    def to_json_dict(self) -> dict[str, Any]:
        """Serialize this `DirTreeStat` to a JSON-compatible dictionary."""
        return {
            "file_count": self.file_count,
            "total_bytes": self.total_bytes,
            "max_mtime_ns": self.max_mtime_ns,
            "entries": [
                {"path": rel, "stat": fs.to_json_dict()} for rel, fs in self.entries
            ],
            "racy": self.racy,
        }

    @classmethod
    def from_json_dict(cls, data: Mapping[str, Any]) -> DirTreeStat:
        """Deserialize a `DirTreeStat` from a JSON dictionary."""
        raw_entries = data.get("entries", ())
        entries = tuple(
            (str(item["path"]), FileStat.from_json_dict(item["stat"]))
            for item in raw_entries
        )
        return cls(
            file_count=int(data.get("file_count", len(entries))),
            total_bytes=int(data.get("total_bytes", 0)),
            max_mtime_ns=int(data.get("max_mtime_ns", 0)),
            entries=entries,
            racy=bool(data.get("racy", False)),
        )


@dataclass(frozen=True, slots=True)
class MemberOf:
    """Container-child witness (e.g. HDF5 table or OMX matrix) backed by parent `FileStat`."""

    parent: FileStat
    member: str | None = None

    @classmethod
    def capture(
        cls,
        parent_path: Path | str,
        member: str | None = None,
        *,
        observed_at_ns: int | None = None,
    ) -> MemberOf:
        """Capture a `MemberOf` witness from `parent_path`."""
        return cls(
            parent=FileStat.capture(parent_path, observed_at_ns=observed_at_ns),
            member=member,
        )

    def check(
        self,
        parent_path: Path | str,
        *,
        now_ns: int | None = None,
    ) -> Freshness:
        """Check whether the parent container at `parent_path` is still `FRESH`."""
        return self.parent.check(parent_path, now_ns=now_ns)

    def to_json_dict(self) -> dict[str, Any]:
        """Serialize this `MemberOf` witness to a JSON-compatible dictionary."""
        return {
            "parent": self.parent.to_json_dict(),
            "member": self.member,
        }

    @classmethod
    def from_json_dict(cls, data: Mapping[str, Any]) -> MemberOf:
        """Deserialize a `MemberOf` witness from a JSON dictionary."""
        return cls(
            parent=FileStat.from_json_dict(data["parent"]),
            member=str(data["member"]) if data.get("member") is not None else None,
        )


@dataclass(frozen=True, slots=True)
class ObjectETag:
    """Remote object-store witness (`s3://`, `gs://`, `http://`)."""

    etag: str
    size: int | None = None
    generation: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.etag, str) or not self.etag:
            raise ValueError("ObjectETag.etag must be a non-empty string")

    def to_json_dict(self) -> dict[str, Any]:
        """Serialize this `ObjectETag` to a JSON-compatible dictionary."""
        return {
            "etag": self.etag,
            "size": self.size,
            "generation": self.generation,
        }

    @classmethod
    def from_json_dict(cls, data: Mapping[str, Any]) -> ObjectETag:
        """Deserialize an `ObjectETag` from a JSON dictionary."""
        return cls(
            etag=str(data["etag"]),
            size=int(data["size"]) if data.get("size") is not None else None,
            generation=(
                str(data["generation"]) if data.get("generation") is not None else None
            ),
        )


@dataclass(frozen=True, slots=True)
class DbTableChangeToken:
    """Database table witness (`duckdb://`, `dlt` load ID / row count token)."""

    token: str = ""
    row_count: int | None = None
    load_id: str | None = None

    def __post_init__(self) -> None:
        if (not isinstance(self.token, str) or not self.token) and self.load_id:
            object.__setattr__(
                self,
                "token",
                f"{self.load_id}:{self.row_count if self.row_count is not None else 0}",
            )
        if not isinstance(self.token, str) or not self.token:
            raise ValueError("DbTableChangeToken.token must be a non-empty string")

    def to_json_dict(self) -> dict[str, Any]:
        """Serialize this `DbTableChangeToken` to a JSON-compatible dictionary."""
        return {
            "token": self.token,
            "row_count": self.row_count,
            "load_id": self.load_id,
        }

    @classmethod
    def from_json_dict(cls, data: Mapping[str, Any]) -> DbTableChangeToken:
        """Deserialize a `DbTableChangeToken` from a JSON dictionary."""
        return cls(
            token=str(data["token"]),
            row_count=(
                int(data["row_count"]) if data.get("row_count") is not None else None
            ),
            load_id=(str(data["load_id"]) if data.get("load_id") is not None else None),
        )


Witness: TypeAlias = FileStat | DirTreeStat | MemberOf | ObjectETag | DbTableChangeToken

_WITNESS_TYPES: dict[str, type[Witness]] = {
    "file_stat": FileStat,
    "dir_tree_stat": DirTreeStat,
    "member_of": MemberOf,
    "object_etag": ObjectETag,
    "db_table_change_token": DbTableChangeToken,
}


def witness_to_json(
    w: Witness | None,
) -> tuple[str | None, dict[str, Any] | None]:
    """Return `(witness_kind, witness_json_dict)` for persistence."""
    if w is None:
        return (None, None)
    for kind, cls in _WITNESS_TYPES.items():
        if isinstance(w, cls):
            return (kind, w.to_json_dict())
    raise TypeError(f"Unsupported Witness type: {type(w).__name__}")


def witness_from_json(
    kind: str | None,
    payload: Mapping[str, Any] | None,
) -> Witness | None:
    """Reconstruct a `Witness` instance from `(witness_kind, witness_json_dict)`."""
    if kind is None or payload is None:
        return None
    cls = _WITNESS_TYPES.get(kind.strip())
    if cls is None:
        raise ValueError(f"Unknown witness kind {kind!r}")
    return cls.from_json_dict(payload)


MountScope: TypeAlias = Literal["site", "node", "job", "global"]
_VALID_MOUNT_SCOPES: frozenset[str] = frozenset({"site", "node", "job", "global"})


@dataclass(frozen=True, slots=True)
class Mount:
    """Named filesystem root paired with storage visibility scope across nodes and jobs."""

    name: str
    root: Path
    scope: MountScope = "site"
    check_device_id: bool = False
    instance_id: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise ValueError("Mount.name must be a non-empty string")
        if not isinstance(self.root, Path):
            object.__setattr__(self, "root", Path(self.root))
        if self.scope not in _VALID_MOUNT_SCOPES:
            raise ValueError(
                f"Unknown mount scope {self.scope!r}; expected one of {sorted(_VALID_MOUNT_SCOPES)}"
            )

    def resolve_instance_id(self) -> str | None:
        """Resolve explicit or `.consist-mount-instance` marker ID for this mount."""
        if self.instance_id is not None:
            return self.instance_id
        if self.scope in ("node", "job"):
            marker = self.root / ".consist-mount-instance"
            try:
                if marker.is_file():
                    val = marker.read_text(encoding="utf-8").strip()
                    return val or None
            except OSError:
                return None
        return None


@dataclass(frozen=True, slots=True)
class SiteContext:
    """Cluster/host identity plus current node and Slurm/batch job context."""

    site: str
    node_name: str = field(default_factory=socket.gethostname)
    job_id: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.site, str) or not self.site.strip():
            raise ValueError("SiteContext.site must be a non-empty string")
        if not isinstance(self.node_name, str) or not self.node_name.strip():
            raise ValueError("SiteContext.node_name must be a non-empty string")

    def scope_key_for(self, mount: Mount) -> str | None:
        """Return the visibility partition key for `mount` in the current process context.

        - `scope="site"` or `"global"`: `None` (shared across all nodes in the site)
        - `scope="node"`: `"node:<node_name>"`
        - `scope="job"`: `"node:<node_name>:job:<job_id or pid-<pid>>"` (includes both
          node and job ID so multi-node Slurm jobs do not confuse node-local NVMe scratch)
        """
        if mount.scope in ("site", "global"):
            return None
        if mount.scope == "node":
            return f"node:{self.node_name}"
        if mount.scope == "job":
            return f"node:{self.node_name}:job:{self.job_id or f'pid-{os.getpid()}'}"
        raise ValueError(f"Unknown mount scope {mount.scope!r}")

    @classmethod
    def detect(
        cls,
        explicit_site: str | None = None,
        mounts: Mapping[str, Mount | Path | str] | Sequence[Mount] | None = None,
        *,
        env: Mapping[str, str] | None = None,
        node_name: str | None = None,
        job_id: str | None = None,
    ) -> SiteContext:
        """Detect `SiteContext` without touching disk when site or mounts are implied.

        Priority order for `site`:
        1. `explicit_site` argument
        2. `$CONSIST_SITE` environment variable
        3. Cluster environment variables (`$LMOD_SYSTEM_NAME`, `$NERSC_HOST`, `$SLURM_CLUSTER_NAME`)
        4. Deterministic hash of configured `mounts` (`mounts:<12-hex>`)
        5. Local hostname fallback (`host:<node_name>`)
        """
        environ = os.environ if env is None else env
        resolved_node = (
            node_name.strip()
            if isinstance(node_name, str) and node_name.strip()
            else (environ.get("SLURMD_NODENAME", "").strip() or socket.gethostname())
        )
        raw_job = (
            job_id
            if job_id is not None
            else (
                environ.get("SLURM_JOB_ID", "").strip()
                or environ.get("PBS_JOBID", "").strip()
            )
        )
        resolved_job = raw_job.strip() or None

        site = (
            (explicit_site.strip() if isinstance(explicit_site, str) else "")
            or environ.get("CONSIST_SITE", "").strip()
            or next(
                (
                    v
                    for k in ("LMOD_SYSTEM_NAME", "NERSC_HOST", "SLURM_CLUSTER_NAME")
                    if (v := environ.get(k, "").strip())
                ),
                "",
            )
        )
        if not site and mounts:
            items = (
                [
                    (
                        t.name or str(k) if isinstance(t, Mount) else str(k),
                        t.root if isinstance(t, Mount) else Path(t),
                    )
                    for k, t in mounts.items()
                ]
                if isinstance(mounts, Mapping)
                else [(m.name, m.root) for m in mounts if isinstance(m, Mount)]
            )
            if items:
                payload = "".join(
                    f"{k}={p}\n"
                    for k, p in sorted(
                        (k, str(r.expanduser().resolve())) for k, r in items
                    )
                )
                site = (
                    f"mounts:{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:12]}"
                )
        return cls(
            site=site or f"host:{resolved_node}",
            node_name=resolved_node,
            job_id=resolved_job,
        )


@dataclass(frozen=True, slots=True)
class Location:
    """Physical or logical URI plus optional intra-container member key."""

    uri: str
    member: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.uri, str) or not self.uri.strip():
            raise ValueError("Location.uri must be a non-empty string")


ObservationBasis: TypeAlias = Literal[
    "computed", "derived", "implied", "declared", "trusted_foreign"
]


@dataclass(frozen=True, slots=True)
class Observation:
    """Timestamped, site-scoped observation of an `Identity` at a `Location`."""

    location: Location
    identity: Identity
    access_driver: str = "file"
    witness: Witness | None = None
    basis: ObservationBasis = "computed"
    site: str = "local"
    scope_key: str | None = None
    mount_instance_id: str | None = None
    observed_at_ns: int = field(default_factory=time.time_ns)

    @property
    def place_key(self) -> tuple[str, str, str, str, str]:
        """Return the 5-tuple physical place partition key for supersession."""
        return (
            self.site,
            self.scope_key or "",
            self.mount_instance_id or "",
            self.location.uri,
            self.location.member or "",
        )

    def to_location_row(
        self,
        *,
        observation_id: uuid.UUID | None = None,
    ) -> dict[str, Any]:
        """Convert this `Observation` into a location dictionary compatible with `locate_current`."""
        return {
            "observation_id": observation_id,
            "anchor": str(self.identity.anchor),
            "uri": self.location.uri,
            "member": self.location.member,
            "access_driver": self.access_driver,
            "witness": self.witness,
            "basis": self.basis,
            "site": self.site,
            "scope_key": self.scope_key,
            "mount_instance_id": self.mount_instance_id,
            "observed_at_ns": self.observed_at_ns,
            "status": "active",
            "transfer_ref": None,
        }


@dataclass(frozen=True, slots=True)
class LedgerRef:
    """Picklable affinity pointer to an Artifact's originating provenance store."""

    db_uid: str
    instance_key: str
    db_uri: str | None = None


_lock = threading.Lock()
_live_ledgers: weakref.WeakValueDictionary[str, Any] = weakref.WeakValueDictionary()


def _reset_ledgers_after_fork() -> None:
    global _live_ledgers, _lock
    _lock = threading.Lock()
    _live_ledgers = weakref.WeakValueDictionary()


if "register_at_fork" in os.__dict__:
    os.register_at_fork(after_in_child=_reset_ledgers_after_fork)


def _normalize_ledger_path_key(db_path: Path | str | None) -> str | None:
    if db_path is None:
        return None
    raw = str(db_path).strip()
    if not raw or raw == ":memory:":
        return None
    return str(Path(raw).expanduser().resolve())


def register_live_ledger(
    ledger: Any,
    *,
    instance_key: str,
    db_path: Path | str | None = None,
) -> None:
    """Register a live ledger in the process-local weak registry by instance key and path."""
    norm = _normalize_ledger_path_key(db_path)
    with _lock:
        _live_ledgers[instance_key] = ledger
        if norm is not None:
            _live_ledgers[norm] = ledger
            _live_ledgers[f"path:{norm}"] = ledger


def unregister_live_ledger(
    ledger: Any | None = None,
    *,
    instance_key: str | None = None,
    db_path: Path | str | None = None,
) -> None:
    """Remove a ledger from the process-local weak registry."""
    norm = _normalize_ledger_path_key(db_path)
    with _lock:
        if instance_key is not None and (
            ledger is None or _live_ledgers.get(instance_key) is ledger
        ):
            _live_ledgers.pop(instance_key, None)
        if norm is not None:
            if ledger is None or _live_ledgers.get(norm) is ledger:
                _live_ledgers.pop(norm, None)
            if ledger is None or _live_ledgers.get(f"path:{norm}") is ledger:
                _live_ledgers.pop(f"path:{norm}", None)


def lookup_live_ledger(ref: LedgerRef | str | None) -> Any | None:
    """Look up a live in-process ledger by `LedgerRef` or `instance_key`."""
    if ref is None:
        return None
    key = ref.instance_key if isinstance(ref, LedgerRef) else str(ref)
    with _lock:
        led = _live_ledgers.get(key)
        if led is not None:
            return led
        if isinstance(ref, LedgerRef) and ref.db_uri:
            norm = _normalize_ledger_path_key(ref.db_uri)
            if norm is not None:
                return _live_ledgers.get(f"path:{norm}") or _live_ledgers.get(norm)
        return None


def lookup_live_ledger_by_path(db_path: Path | str) -> Any | None:
    """Look up a live in-process ledger by database path."""
    norm = _normalize_ledger_path_key(db_path)
    if norm is None:
        return None
    with _lock:
        return _live_ledgers.get(f"path:{norm}") or _live_ledgers.get(norm)


__all__ = [
    "EMPTY_META",
    "DbTableChangeToken",
    "DirTreeStat",
    "FileStat",
    "Freshness",
    "FrozenList",
    "FrozenMeta",
    "LedgerRef",
    "Location",
    "MemberOf",
    "Mount",
    "MountScope",
    "ObjectETag",
    "Observation",
    "ObservationBasis",
    "SiteContext",
    "Witness",
    "lookup_live_ledger",
    "lookup_live_ledger_by_path",
    "register_live_ledger",
    "unregister_live_ledger",
    "witness_from_json",
    "witness_to_json",
]
