"""Layer-2 location coordinates and filesystem stat witnesses."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import os
from pathlib import Path
import stat
import time
from typing import Any, Final, Literal, Mapping, Sequence, TypeAlias, Union
import uuid

from consist.digest import Digest, is_shapefile_sidecar_filename

_RACY_WINDOW_NS: Final[int] = 2_000_000_000
_LOCATION_ID_NAMESPACE: Final[uuid.UUID] = uuid.UUID(
    "c0b51570-0005-4000-8000-000000000003"
)
_NON_PORTABLE_DIGEST_PREFIXES: Final[tuple[str, ...]] = (
    "stat_file_v1:",
    "stat_dir_v1:",
    "caller_supplied_v1:",
)

LocationSite: TypeAlias = Literal["workspace", "archive", "advisory", "external"]
LocationStatus: TypeAlias = Literal["active", "stale", "evicted"]


class WitnessFreshness(str, Enum):
    """Outcome of checking a cached filesystem witness against on-disk stat."""

    FRESH = "fresh"
    RACY_NEEDS_VERIFY = "racy_needs_verify"
    STAT_MISMATCH = "stat_mismatch"
    MISSING = "missing"


@dataclass(frozen=True, slots=True)
class WitnessCheckResult:
    """Detailed result from checking a ``FileStat`` or ``DirTreeStat`` witness."""

    freshness: WitnessFreshness
    can_settle_racy: bool = False
    current_stat: Union["FileStat", "DirTreeStat", None] = None


def _lstat_leaf(path: str | Path) -> os.stat_result:
    target = os.fspath(Path(path).expanduser())
    if len(target) > 1:
        target = target.rstrip("/\\")
    st = os.lstat(target)
    if stat.S_ISLNK(st.st_mode):
        raise ValueError(f"Symlink leaf is not a regular artifact file: {target}")
    return st


@dataclass(frozen=True, slots=True)
class FileStat:
    """Stat-tuple freshness witness for a single regular file."""

    size: int
    mtime_ns: int
    ctime_ns: int
    ino: int
    racy: bool = False

    def _stat_tuple(self) -> tuple[int, int, int, int]:
        return (self.size, self.mtime_ns, self.ctime_ns, self.ino)

    @classmethod
    def from_os_stat(
        cls, st: os.stat_result, *, observed_at_ns: int | None = None
    ) -> "FileStat":
        now_ns = time.time_ns() if observed_at_ns is None else int(observed_at_ns)
        return cls(
            size=int(st.st_size),
            mtime_ns=int(st.st_mtime_ns),
            ctime_ns=int(st.st_ctime_ns),
            ino=int(st.st_ino),
            racy=abs(now_ns - int(st.st_mtime_ns)) < _RACY_WINDOW_NS,
        )

    @classmethod
    def capture(
        cls, path: str | Path, *, observed_at_ns: int | None = None
    ) -> "FileStat":
        st = _lstat_leaf(path)
        if not stat.S_ISREG(st.st_mode):
            raise ValueError(f"Expected regular file for FileStat witness: {path}")
        return cls.from_os_stat(st, observed_at_ns=observed_at_ns)

    def settled(self) -> "FileStat":
        if not self.racy:
            return self
        return FileStat(self.size, self.mtime_ns, self.ctime_ns, self.ino, racy=False)

    def check(
        self, path: str | Path, *, now_ns: int | None = None
    ) -> WitnessCheckResult:
        effective_now_ns = time.time_ns() if now_ns is None else int(now_ns)
        try:
            st = _lstat_leaf(path)
        except FileNotFoundError:
            return WitnessCheckResult(freshness=WitnessFreshness.MISSING)
        except (OSError, ValueError):
            return WitnessCheckResult(freshness=WitnessFreshness.STAT_MISMATCH)
        if not stat.S_ISREG(st.st_mode):
            return WitnessCheckResult(freshness=WitnessFreshness.STAT_MISMATCH)
        cur = FileStat.from_os_stat(st, observed_at_ns=effective_now_ns)
        if cur._stat_tuple() != self._stat_tuple():
            return WitnessCheckResult(WitnessFreshness.STAT_MISMATCH, current_stat=cur)
        if not self.racy:
            return WitnessCheckResult(WitnessFreshness.FRESH, current_stat=self)
        can_settle = (effective_now_ns - int(st.st_mtime_ns)) >= _RACY_WINDOW_NS
        return WitnessCheckResult(
            WitnessFreshness.RACY_NEEDS_VERIFY,
            can_settle_racy=can_settle,
            current_stat=self.settled() if can_settle else self,
        )

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "kind": "file",
            "size": self.size,
            "mtime_ns": self.mtime_ns,
            "ctime_ns": self.ctime_ns,
            "ino": self.ino,
            "racy": self.racy,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_json_dict(), separators=(",", ":"), sort_keys=True)

    @classmethod
    def from_json_dict(cls, data: Mapping[str, Any]) -> "FileStat":
        return cls(
            size=int(data["size"]),
            mtime_ns=int(data["mtime_ns"]),
            ctime_ns=int(data["ctime_ns"]),
            ino=int(data["ino"]),
            racy=bool(data.get("racy", False)),
        )


@dataclass(frozen=True, slots=True)
class DirTreeStat:
    """Stat-tree freshness witness for directory artifacts and shapefile bundles."""

    file_count: int
    total_bytes: int
    max_mtime_ns: int
    entries: tuple[tuple[str, FileStat], ...]
    subdirs: tuple[str, ...] = ()
    bundle_entry: str | None = None
    racy: bool = False

    @classmethod
    def _from_parts(
        cls,
        *,
        entries: Sequence[tuple[str, FileStat]],
        subdirs: Sequence[str] = (),
        bundle_entry: str | None = None,
        root_mtime_ns: int = 0,
        observed_at_ns: int | None = None,
    ) -> "DirTreeStat":
        now_ns = time.time_ns() if observed_at_ns is None else int(observed_at_ns)
        sorted_entries = tuple(sorted(entries, key=lambda item: item[0]))
        max_mtime = max(
            (fs.mtime_ns for _, fs in sorted_entries), default=int(root_mtime_ns)
        )
        is_racy = any(fs.racy for _, fs in sorted_entries) or (
            max_mtime > 0 and abs(now_ns - max_mtime) < _RACY_WINDOW_NS
        )
        return cls(
            file_count=len(sorted_entries),
            total_bytes=sum(fs.size for _, fs in sorted_entries),
            max_mtime_ns=max_mtime,
            entries=sorted_entries,
            subdirs=tuple(sorted(set(subdirs))),
            bundle_entry=bundle_entry,
            racy=is_racy,
        )

    @classmethod
    def capture_dir(
        cls, root: str | Path, *, observed_at_ns: int | None = None
    ) -> "DirTreeStat":
        now_ns = time.time_ns() if observed_at_ns is None else int(observed_at_ns)
        raw_root = Path(root).expanduser()
        if raw_root.is_symlink():
            raise ValueError(f"Directory witness root cannot be a symlink: {raw_root}")
        real_root = Path(os.path.realpath(raw_root))
        root_st = os.lstat(real_root)
        if not stat.S_ISDIR(root_st.st_mode):
            raise FileNotFoundError(
                f"Directory witness root is not a directory: {root}"
            )

        entries: list[tuple[str, FileStat]] = []
        subdirs: list[str] = []
        real_root_str = os.fspath(real_root)
        prefix_len = (
            1 if real_root_str == os.sep else len(real_root_str.rstrip(os.sep)) + 1
        )
        need_slash_norm = os.sep != "/"

        def _visit(directory: str) -> None:
            with os.scandir(directory) as scan:
                children = sorted(scan, key=lambda entry: entry.name)
            for child in children:
                rel = child.path[prefix_len:]
                if need_slash_norm:
                    rel = rel.replace(os.sep, "/")
                if child.is_symlink():
                    raise ValueError(f"Directory cannot contain symlink: {child.path}")
                if child.is_dir(follow_symlinks=False):
                    subdirs.append(rel)
                    _visit(child.path)
                elif child.is_file(follow_symlinks=False):
                    st = child.stat(follow_symlinks=False)
                    entries.append(
                        (rel, FileStat.from_os_stat(st, observed_at_ns=now_ns))
                    )
                else:
                    raise ValueError(f"Non-regular directory entry: {child.path}")

        _visit(real_root_str)
        return cls._from_parts(
            entries=entries,
            subdirs=subdirs,
            root_mtime_ns=int(root_st.st_mtime_ns),
            observed_at_ns=now_ns,
        )

    @classmethod
    def capture_shapefile_bundle(
        cls,
        path: str | Path,
        *,
        entry_name: str | None = None,
        observed_at_ns: int | None = None,
    ) -> "DirTreeStat":
        now_ns = time.time_ns() if observed_at_ns is None else int(observed_at_ns)
        raw = Path(path).expanduser()
        if raw.is_symlink():
            raise ValueError(f"Shapefile bundle path cannot be a symlink: {raw}")
        parent_real = Path(os.path.realpath(raw.parent))
        target = parent_real / raw.name
        target_st = os.lstat(target)
        if stat.S_ISLNK(target_st.st_mode):
            raise ValueError(f"Shapefile bundle path cannot be a symlink: {target}")

        resolved_entry = entry_name or raw.name
        if stat.S_ISDIR(target_st.st_mode):
            bundle_root = Path(os.path.realpath(target))
            root_mtime_ns = int(target_st.st_mtime_ns)
        elif stat.S_ISREG(target_st.st_mode):
            bundle_root = parent_real
            root_mtime_ns = int(os.lstat(bundle_root).st_mtime_ns)
        else:
            raise FileNotFoundError(f"Shapefile bundle path is invalid: {path}")

        if Path(
            resolved_entry
        ).name != resolved_entry or not resolved_entry.lower().endswith(".shp"):
            raise ValueError(f"Invalid shapefile bundle entry name: {resolved_entry!r}")

        stem = Path(resolved_entry).stem
        entries: list[tuple[str, FileStat]] = []
        has_shp = False
        with os.scandir(bundle_root) as scan:
            candidates = sorted(
                (
                    item
                    for item in scan
                    if is_shapefile_sidecar_filename(item.name, stem)
                ),
                key=lambda item: item.name,
            )
        for cand in candidates:
            if cand.is_symlink() or not cand.is_file(follow_symlinks=False):
                raise ValueError(f"Invalid shapefile sidecar entry: {cand.path}")
            if cand.name.lower() == resolved_entry.lower():
                has_shp = True
            st = cand.stat(follow_symlinks=False)
            entries.append(
                (cand.name, FileStat.from_os_stat(st, observed_at_ns=now_ns))
            )
        if not has_shp:
            raise FileNotFoundError(
                f"Shapefile bundle missing primary entry {resolved_entry!r} in {bundle_root}"
            )
        return cls._from_parts(
            entries=entries,
            bundle_entry=resolved_entry,
            root_mtime_ns=root_mtime_ns,
            observed_at_ns=now_ns,
        )

    def settled(self) -> "DirTreeStat":
        if not self.racy and not any(fs.racy for _, fs in self.entries):
            return self
        return DirTreeStat(
            file_count=self.file_count,
            total_bytes=self.total_bytes,
            max_mtime_ns=self.max_mtime_ns,
            entries=tuple((rel, fs.settled()) for rel, fs in self.entries),
            subdirs=self.subdirs,
            bundle_entry=self.bundle_entry,
            racy=False,
        )

    def check(
        self, path: str | Path, *, now_ns: int | None = None
    ) -> WitnessCheckResult:
        effective_now_ns = time.time_ns() if now_ns is None else int(now_ns)
        try:
            current = (
                DirTreeStat.capture_shapefile_bundle(
                    path, entry_name=self.bundle_entry, observed_at_ns=effective_now_ns
                )
                if self.bundle_entry is not None
                else DirTreeStat.capture_dir(path, observed_at_ns=effective_now_ns)
            )
        except FileNotFoundError:
            return WitnessCheckResult(freshness=WitnessFreshness.MISSING)
        except (OSError, ValueError):
            return WitnessCheckResult(freshness=WitnessFreshness.STAT_MISMATCH)

        if (
            current.file_count != self.file_count
            or current.total_bytes != self.total_bytes
            or current.subdirs != self.subdirs
            or any(
                rel_a != rel_b or fs_a._stat_tuple() != fs_b._stat_tuple()
                for (rel_a, fs_a), (rel_b, fs_b) in zip(
                    self.entries, current.entries, strict=True
                )
            )
        ):
            return WitnessCheckResult(
                WitnessFreshness.STAT_MISMATCH, current_stat=current
            )
        if not self.racy:
            return WitnessCheckResult(WitnessFreshness.FRESH, current_stat=self)
        can_settle = (effective_now_ns - self.max_mtime_ns) >= _RACY_WINDOW_NS
        return WitnessCheckResult(
            WitnessFreshness.RACY_NEEDS_VERIFY,
            can_settle_racy=can_settle,
            current_stat=self.settled() if can_settle else self,
        )

    def to_json_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "kind": "dir_tree",
            "file_count": self.file_count,
            "total_bytes": self.total_bytes,
            "max_mtime_ns": self.max_mtime_ns,
            "entries": [[rel, fs.to_json_dict()] for rel, fs in self.entries],
            "subdirs": list(self.subdirs),
            "racy": self.racy,
        }
        if self.bundle_entry is not None:
            payload["bundle_entry"] = self.bundle_entry
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_json_dict(), separators=(",", ":"), sort_keys=True)

    @classmethod
    def from_json_dict(cls, data: Mapping[str, Any]) -> "DirTreeStat":
        parsed_entries = tuple(
            sorted(
                (
                    (str(item[0]), FileStat.from_json_dict(item[1]))
                    for item in (data.get("entries") or ())
                ),
                key=lambda pair: pair[0],
            )
        )
        raw_bundle = data.get("bundle_entry")
        return cls(
            file_count=int(data.get("file_count", len(parsed_entries))),
            total_bytes=int(
                data.get("total_bytes", sum(fs.size for _, fs in parsed_entries))
            ),
            max_mtime_ns=int(data.get("max_mtime_ns", 0)),
            entries=parsed_entries,
            subdirs=tuple(sorted(str(s) for s in (data.get("subdirs") or ()))),
            bundle_entry=str(raw_bundle)
            if isinstance(raw_bundle, str) and raw_bundle
            else None,
            racy=bool(data.get("racy", False)),
        )


Witness = Union[FileStat, DirTreeStat]


def witness_from_json(raw: str | Mapping[str, Any] | None) -> Witness | None:
    """Deserialize a persisted ``witness_json`` payload into a typed witness."""
    if raw is None:
        return None
    data: Any = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(data, Mapping):
        return None
    kind = data.get("kind")
    if kind == "dir_tree":
        return DirTreeStat.from_json_dict(data)
    if kind == "file" or "mtime_ns" in data:
        return FileStat.from_json_dict(data)
    return None


def capture_witness(
    path: str | Path,
    *,
    driver: str | None = None,
    is_directory: bool = False,
    is_shapefile_bundle: bool = False,
    bundle_entry: str | None = None,
    observed_at_ns: int | None = None,
) -> Witness | None:
    """Capture the appropriate ``FileStat`` or ``DirTreeStat`` witness for ``path``."""
    p = Path(path).expanduser()
    try:
        if is_shapefile_bundle or driver == "shapefile" or p.suffix.lower() == ".shp":
            return DirTreeStat.capture_shapefile_bundle(
                p, entry_name=bundle_entry, observed_at_ns=observed_at_ns
            )
        if is_directory or driver in {"zarr", "artifact_set"}:
            return DirTreeStat.capture_dir(p, observed_at_ns=observed_at_ns)
        parent_real = Path(os.path.realpath(p.parent))
        st = os.lstat(parent_real / p.name)
        if stat.S_ISLNK(st.st_mode):
            return None
        if stat.S_ISDIR(st.st_mode):
            return DirTreeStat.capture_dir(p, observed_at_ns=observed_at_ns)
        if stat.S_ISREG(st.st_mode):
            return FileStat.from_os_stat(st, observed_at_ns=observed_at_ns)
    except (OSError, ValueError):
        return None
    return None


def workspace_mount_instance_id(run_dir: str | Path) -> str:
    """Return a deterministic 16-hex instance identifier for a local ``run_dir``."""
    resolved = str(Path(os.path.realpath(Path(run_dir).expanduser())))
    return hashlib.sha256(resolved.encode("utf-8")).hexdigest()[:16]


def compute_location_id(
    *,
    digest: Digest | str | None,
    artifact_id: uuid.UUID | str | None,
    site: str,
    mount_name: str,
    mount_instance_id: str,
    uri: str,
    member: str = "",
) -> uuid.UUID:
    """Compute deterministic ``location_id`` for an ``artifact_location`` coordinate."""
    digest_str = str(digest) if digest is not None else None
    aid_token = str(artifact_id) if artifact_id is not None else ""
    if (
        digest_str is not None
        and site in ("workspace", "external")
        and not digest_str.startswith(_NON_PORTABLE_DIGEST_PREFIXES)
    ):
        identity_token = digest_str
    elif digest_str is not None:
        identity_token = f"id:{aid_token}\x1f{digest_str}" if aid_token else digest_str
    else:
        identity_token = f"id:{aid_token}"
    key = "\x1f".join(
        (
            identity_token,
            site,
            mount_name,
            mount_instance_id,
            uri,
            member or "",
        )
    )
    return uuid.uuid5(_LOCATION_ID_NAMESPACE, key)


@dataclass(frozen=True, slots=True)
class ArtifactLocationRow:
    """Typed value representation of one ``artifact_location`` ledger row."""

    location_id: uuid.UUID
    digest: str | None
    artifact_id: uuid.UUID | None
    site: LocationSite
    mount_name: str
    mount_instance_id: str
    uri: str
    member: str = ""
    status: LocationStatus = "active"
    witness: Witness | None = None
    observed_at_ns: int = 0

    @classmethod
    def create(
        cls,
        *,
        digest: str | None,
        artifact_id: uuid.UUID | None,
        site: LocationSite,
        mount_name: str,
        mount_instance_id: str,
        uri: str,
        member: str = "",
        status: LocationStatus = "active",
        witness: Witness | None = None,
        observed_at_ns: int | None = None,
    ) -> "ArtifactLocationRow":
        obs = time.time_ns() if observed_at_ns is None else int(observed_at_ns)
        lid = compute_location_id(
            digest=digest,
            artifact_id=artifact_id,
            site=site,
            mount_name=mount_name,
            mount_instance_id=mount_instance_id,
            uri=uri,
            member=member,
        )
        return cls(
            lid,
            digest,
            artifact_id,
            site,
            mount_name,
            mount_instance_id,
            uri,
            member,
            status,
            witness,
            obs,
        )
