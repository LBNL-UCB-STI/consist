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
import json
import os
from pathlib import Path
import socket
import stat as stat_mod
import threading
import time
from typing import Any, Literal, Protocol, TypeAlias, runtime_checkable
from urllib.parse import parse_qsl, unquote, urlencode, urlparse
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


@dataclass(frozen=True, slots=True)
class FileStat:
    """Stat witness for a single regular file.

    Treats `racy=True` (`abs(observed_at_ns - mtime_ns) < 2_000_000_000`) as
    `Freshness.STALE`, forcing content digest verification until a non-racy
    witness is recorded.
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
            racy=abs(now_ns - mtime_ns) < _RACY_WINDOW_NS,
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
        _ = now_ns
        try:
            st = Path(path).stat()
        except (FileNotFoundError, NotADirectoryError):
            return Freshness.MISSING
        except OSError:
            return Freshness.STALE

        if self.racy:
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


def _scan_dir_tree(
    root_path: Path,
    *,
    abort_on_symlink: bool,
) -> tuple[list[tuple[str, os.stat_result]], list[tuple[str, int]], bool]:
    files: list[tuple[str, os.stat_result]] = []
    subdirs: list[tuple[str, int]] = []
    has_symlink = False

    def _walk(cur_dir: Path, rel_prefix: str) -> bool:
        nonlocal has_symlink
        with os.scandir(cur_dir) as it:
            entries = sorted(it, key=lambda e: e.name)
        for entry in entries:
            if entry.is_symlink():
                has_symlink = True
                if abort_on_symlink:
                    return False
                continue
            rel = f"{rel_prefix}/{entry.name}" if rel_prefix else entry.name
            st = entry.stat(follow_symlinks=False)
            if stat_mod.S_ISDIR(st.st_mode):
                subdirs.append((rel, int(st.st_mtime_ns)))
                if not _walk(Path(entry.path), rel):
                    return False
            elif stat_mod.S_ISREG(st.st_mode):
                files.append((rel, st))
        return True

    _walk(root_path, "")
    files.sort(key=lambda pair: pair[0])
    subdirs.sort(key=lambda pair: pair[0])
    return (files, subdirs, has_symlink)


@dataclass(frozen=True, slots=True)
class DirTreeStat:
    """Stat witness for a directory tree."""

    file_count: int = 0
    total_bytes: int = 0
    max_mtime_ns: int = 0
    entries: tuple[tuple[str, FileStat], ...] = ()
    subdirs: tuple[tuple[str, int], ...] = ()
    racy: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.entries, tuple):
            object.__setattr__(self, "entries", tuple(self.entries))
        object.__setattr__(
            self,
            "subdirs",
            tuple((str(rel), int(mtime)) for rel, mtime in self.subdirs),
        )

    @classmethod
    def capture(
        cls,
        root: Path | str,
        *,
        observed_at_ns: int | None = None,
    ) -> DirTreeStat:
        """Walk `root` deterministically and capture a `DirTreeStat` witness."""
        raw_root = Path(root).expanduser().absolute()
        root_is_symlink = raw_root.is_symlink()
        root_path = (
            raw_root if root_is_symlink else (raw_root.parent.resolve() / raw_root.name)
        )
        if not root_path.is_dir():
            raise ValueError(f"DirTreeStat root is not a directory: {root_path}")
        now_ns = time.time_ns() if observed_at_ns is None else observed_at_ns
        root_st = os.lstat(root_path)

        scanned_files, captured_subdirs, scanned_symlink = _scan_dir_tree(
            root_path,
            abort_on_symlink=False,
        )
        has_symlink = root_is_symlink or scanned_symlink
        captured_entries = [
            (rel_file, FileStat.from_os_stat(lst, observed_at_ns=now_ns))
            for rel_file, lst in scanned_files
        ]
        total_bytes = sum(fs.size for _, fs in captured_entries)
        all_mtimes = [
            *(fs.mtime_ns for _, fs in captured_entries),
            *(sm for _, sm in captured_subdirs),
        ]
        max_mtime_ns = max(all_mtimes, default=int(root_st.st_mtime_ns))
        racy = (
            has_symlink
            or any(fs.racy for _, fs in captured_entries)
            or any(abs(now_ns - sm) < _RACY_WINDOW_NS for _, sm in captured_subdirs)
            or (abs(now_ns - max_mtime_ns) < _RACY_WINDOW_NS)
        )
        return cls(
            file_count=len(captured_entries),
            total_bytes=total_bytes,
            max_mtime_ns=max_mtime_ns,
            entries=tuple(captured_entries),
            subdirs=tuple(captured_subdirs),
            racy=racy,
        )

    def check(
        self,
        root: Path | str,
        *,
        now_ns: int | None = None,
    ) -> Freshness:
        """Check whether `root` still matches this `DirTreeStat` witness."""
        _ = now_ns
        raw_root = Path(root).expanduser().absolute()
        if raw_root.is_symlink():
            return Freshness.STALE
        if not raw_root.exists():
            return Freshness.MISSING
        root_path = raw_root.parent.resolve() / raw_root.name
        if not root_path.exists() or not root_path.is_dir():
            return Freshness.MISSING

        if self.racy:
            return Freshness.STALE

        try:
            current_files, current_subdirs, has_symlink = _scan_dir_tree(
                root_path,
                abort_on_symlink=True,
            )
        except OSError:
            return Freshness.MISSING
        if has_symlink:
            return Freshness.STALE

        if not self.subdirs and any("/" in rel for rel, _ in self.entries):
            implied_dirs: set[str] = set()
            for rel, _ in self.entries:
                parts = rel.split("/")[:-1]
                for idx in range(1, len(parts) + 1):
                    implied_dirs.add("/".join(parts[:idx]))
            if {rel_d for rel_d, _ in current_subdirs} != implied_dirs:
                return Freshness.STALE
        elif tuple(current_subdirs) != self.subdirs:
            return Freshness.STALE

        if self.entries:
            if len(current_files) != len(self.entries):
                return Freshness.STALE
            for (expected_rel, expected_stat), (actual_rel, st) in zip(
                self.entries, current_files, strict=True
            ):
                if expected_rel != actual_rel:
                    return Freshness.STALE
                if expected_stat.racy:
                    return Freshness.STALE
                if (
                    st.st_size != expected_stat.size
                    or st.st_mtime_ns != expected_stat.mtime_ns
                ):
                    return Freshness.STALE
                if (
                    expected_stat.ctime_ns is not None
                    and st.st_ctime_ns != expected_stat.ctime_ns
                ):
                    return Freshness.STALE
                if expected_stat.ino is not None and st.st_ino != expected_stat.ino:
                    return Freshness.STALE
            return Freshness.FRESH

        if len(current_files) != self.file_count:
            return Freshness.STALE
        total_bytes = sum(st.st_size for _, st in current_files)
        all_mtimes = [
            *(st.st_mtime_ns for _, st in current_files),
            *(sm for _, sm in current_subdirs),
        ]
        max_mtime = max(
            all_mtimes,
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
            "subdirs": [
                {"path": rel, "mtime_ns": mtime} for rel, mtime in self.subdirs
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
        raw_subdirs = data.get("subdirs", ())
        subdirs: list[tuple[str, int]] = []
        for item in raw_subdirs:
            if isinstance(item, Mapping):
                subdirs.append((str(item["path"]), int(item["mtime_ns"])))
            elif isinstance(item, (list, tuple)) and len(item) == 2:
                subdirs.append((str(item[0]), int(item[1])))
        return cls(
            file_count=int(data.get("file_count", len(entries))),
            total_bytes=int(data.get("total_bytes", 0)),
            max_mtime_ns=int(data.get("max_mtime_ns", 0)),
            entries=entries,
            subdirs=tuple(subdirs),
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
            "parent": {"kind": "file_stat", **self.parent.to_json_dict()},
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


def serialize_witness_json(
    witness: Witness | str | Mapping[str, Any] | None,
) -> str | None:
    """Serialize a `Witness` or JSON mapping/string into canonical compact JSON."""
    if witness is None:
        return None
    if isinstance(witness, str):
        try:
            payload = json.loads(witness)
        except ValueError:
            return None
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))
    if isinstance(witness, Mapping):
        return json.dumps(dict(witness), sort_keys=True, separators=(",", ":"))
    kind, payload = witness_to_json(witness)
    if kind is None or payload is None:
        return None
    return json.dumps({"kind": kind, **payload}, sort_keys=True, separators=(",", ":"))


def deserialize_witness_json(raw: str | Mapping[str, Any] | None) -> Witness | None:
    """Reconstruct a `Witness` from `artifact_location.witness_json` or manifest JSON."""
    if not raw:
        return None
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
        except ValueError:
            return None
    elif isinstance(raw, Mapping):
        data = dict(raw)
    else:
        return None
    if not isinstance(data, Mapping):
        return None
    if isinstance(kind := data.get("kind"), str):
        inner = data["payload"] if isinstance(data.get("payload"), Mapping) else data
        return witness_from_json(kind, inner)
    for inferred_kind, req_keys in (
        ("dir_tree_stat", ("file_count", "entries")),
        ("member_of", ("parent", "member")),
        ("db_table_change_token", ("token",)),
        ("object_etag", ("etag",)),
        ("file_stat", ("size", "mtime_ns")),
    ):
        if all(k in data for k in req_keys):
            return witness_from_json(inferred_kind, data)
    return None


def normalize_mounts(
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None,
    *,
    strip_trailing_punct: bool = False,
) -> dict[str, Mount]:
    """Normalize a sequence or mapping of mounts into a `name -> Mount` dictionary."""
    if mounts is None:
        return {}
    out: dict[str, Mount] = {}
    if isinstance(mounts, Mapping):
        for name, target in mounts.items():
            key = str(name).strip().rstrip(":/") if strip_trailing_punct else str(name)
            out[key] = (
                target
                if isinstance(target, Mount)
                else Mount(name=key, root=Path(target))
            )
    else:
        for item in mounts:
            if isinstance(item, Mount):
                out[item.name] = item
    return out


_SUFFIX_DRIVERS: dict[str, str] = {
    ".parquet": "parquet",
    ".pq": "parquet",
    ".csv": "csv",
    ".tsv": "tsv",
    ".json": "json",
    ".geojson": "geojson",
    ".omx": "omx",
    ".nc": "netcdf",
    ".netcdf": "netcdf",
    ".feather": "feather",
    ".arrow": "feather",
    ".zarr": "zarr",
}


def infer_path_driver(
    path: Path,
    *,
    is_dir: bool = False,
    member: str | None = None,
    h5_default: str = "hdf5",
    fallback_to_ext: bool = False,
) -> str:
    """Infer a storage driver string from `path`, `is_dir`, and optional `member`."""
    suffix = path.suffix.lower()
    if is_dir or path.is_dir():
        return "zarr" if suffix == ".zarr" else "directory"
    if suffix in (".h5", ".hdf5", ".hdf"):
        return "h5_table" if member is not None else h5_default
    if suffix in _SUFFIX_DRIVERS:
        return _SUFFIX_DRIVERS[suffix]
    ext = suffix.lstrip(".")
    return ext if (fallback_to_ext and ext) else "file"


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
        if not site and (norm_mounts := normalize_mounts(mounts)):
            payload = "".join(
                f"{k}={p}\n"
                for k, p in sorted(
                    (m.name or k, str(m.root.expanduser().resolve()))
                    for k, m in norm_mounts.items()
                )
            )
            site = f"mounts:{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:12]}"
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
    "computed",
    "derived",
    "implied",
    "declared",
    "trusted_foreign",
    "migrated_v1",
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
    if raw.startswith("file://"):
        parsed = urlparse(raw)
        return str(Path(unquote(parsed.path)).expanduser().resolve())
    return str(Path(raw).expanduser().resolve())


@runtime_checkable
class _LiveLedgerWithUid(Protocol):
    @property
    def db_uid(self) -> str: ...


def _ledger_matches_uid(ledger: Any, expected_uid: str) -> bool:
    if not expected_uid:
        return True
    if isinstance(ledger, _LiveLedgerWithUid):
        return ledger.db_uid == expected_uid
    return False


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
    with _lock:
        if not isinstance(ref, LedgerRef):
            return _live_ledgers.get(str(ref))
        expected_uid = ref.db_uid
        if ref.instance_key:
            led = _live_ledgers.get(ref.instance_key)
            if led is not None and _ledger_matches_uid(led, expected_uid):
                return led
        if ref.db_uri:
            norm = _normalize_ledger_path_key(ref.db_uri)
            if norm is not None:
                led = _live_ledgers.get(f"path:{norm}") or _live_ledgers.get(norm)
                if led is not None and _ledger_matches_uid(led, expected_uid):
                    return led
        return None


def lookup_live_ledger_by_path(db_path: Path | str) -> Any | None:
    """Look up a live in-process ledger by database path."""
    norm = _normalize_ledger_path_key(db_path)
    if norm is None:
        return None
    with _lock:
        return _live_ledgers.get(f"path:{norm}") or _live_ledgers.get(norm)


_HOT_DB_SCOPE_QUERY_KEYS: frozenset[str] = frozenset(
    {"artifact_id", "consist_artifact_id", "consist_run_id"}
)


@dataclass(frozen=True, slots=True)
class HotDbMemberInfo:
    """Parsed table and scoping metadata for a `db://...` observation."""

    schema_name: str
    table_name: str
    base_member: str | None
    scoped_artifact_id: str | None = None
    scoped_run_id: str | None = None
    has_unrecognized_query: bool = False


def format_hot_db_member(
    table_name: str,
    *,
    artifact_id: uuid.UUID | str | None = None,
    consist_artifact_id: uuid.UUID | str | None = None,
    consist_run_id: str | None = None,
) -> str:
    """Format a canonical `db://` `Location.member` string with optional per-row scoping."""
    if consist_artifact_id is not None:
        return f"{table_name}?consist_artifact_id={consist_artifact_id}"
    if artifact_id is not None:
        return f"{table_name}?artifact_id={artifact_id}"
    if consist_run_id is not None:
        return f"{table_name}?consist_run_id={consist_run_id}"
    return table_name


def parse_hot_db_member(
    uri: str | None,
    member: Any = None,
) -> HotDbMemberInfo | None:
    """Parse `db://<schema>/<table>` and optional `?artifact_id=` / `?consist_artifact_id=` / `?consist_run_id=` scoping."""
    if not isinstance(uri, str) or not uri.startswith("db://"):
        return None
    tail = uri[len("db://") :].split("?", 1)[0].split("#", 1)[0].strip("/")
    if not tail:
        return None
    if "/" in tail:
        schema_name, table_name = tail.split("/", 1)
    elif "." in tail:
        schema_name, table_name = tail.split(".", 1)
    else:
        schema_name, table_name = "global_tables", tail
    schema_name = schema_name.strip()
    table_name = table_name.strip()
    if not schema_name or not table_name:
        return None

    if member is None:
        return HotDbMemberInfo(
            schema_name=schema_name,
            table_name=table_name,
            base_member=None,
        )

    mem_str = str(member)
    if "?" not in mem_str:
        return HotDbMemberInfo(
            schema_name=schema_name,
            table_name=table_name,
            base_member=mem_str or None,
        )

    head, query_str = mem_str.split("?", 1)
    pairs = parse_qsl(query_str, keep_blank_values=True)
    scoped_artifact_id: str | None = None
    scoped_run_id: str | None = None
    remaining_pairs: list[tuple[str, str]] = []
    recognized_any = False

    for k, v in pairs:
        if k in ("artifact_id", "consist_artifact_id"):
            scoped_artifact_id = v
            recognized_any = True
        elif k == "consist_run_id":
            scoped_run_id = v
            recognized_any = True
        else:
            remaining_pairs.append((k, v))

    if remaining_pairs:
        rebuilt_query = urlencode(remaining_pairs)
        base_member: str | None = (
            f"{head}?{rebuilt_query}" if head else f"?{rebuilt_query}"
        )
    else:
        base_member = head or None

    has_unrecognized_query = not recognized_any
    return HotDbMemberInfo(
        schema_name=schema_name,
        table_name=table_name,
        base_member=base_member,
        scoped_artifact_id=scoped_artifact_id,
        scoped_run_id=scoped_run_id,
        has_unrecognized_query=has_unrecognized_query,
    )


def matches_hot_db_info(
    info: HotDbMemberInfo,
    *,
    artifact_id: uuid.UUID | str,
    run_id: str | None,
) -> tuple[bool, bool]:
    """Return `(matched, v1_run_scoped)` for `info` against `(artifact_id, run_id)`."""
    if info.has_unrecognized_query:
        return (False, False)
    if info.scoped_artifact_id is not None:
        return (info.scoped_artifact_id == str(artifact_id), False)
    if info.scoped_run_id is not None:
        if run_id is None or info.scoped_run_id != str(run_id):
            return (False, False)
        return (True, True)
    return (True, False)


def strip_hot_db_member_scope(
    uri: str | None,
    member: str | None,
) -> str | None:
    """Strip synthetic `db://` scoping query suffixes (`?artifact_id=...`, `?consist_artifact_id=...`, `?consist_run_id=...`)."""
    if member is None or uri is None or not uri.startswith("db://"):
        return member
    info = parse_hot_db_member(uri, member)
    if info is None:
        return member
    return info.base_member


__all__ = [
    "EMPTY_META",
    "DbTableChangeToken",
    "DirTreeStat",
    "FileStat",
    "Freshness",
    "FrozenList",
    "FrozenMeta",
    "HotDbMemberInfo",
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
    "deserialize_witness_json",
    "format_hot_db_member",
    "infer_path_driver",
    "lookup_live_ledger",
    "lookup_live_ledger_by_path",
    "matches_hot_db_info",
    "normalize_mounts",
    "parse_hot_db_member",
    "register_live_ledger",
    "serialize_witness_json",
    "strip_hot_db_member_scope",
    "unregister_live_ledger",
    "witness_from_json",
    "witness_to_json",
]
