"""
Consist v2 core engine functions: `locate()`, `verify()`, `ensure_digest()`,
`stage()`, `PendingOutput`, and high-throughput `seal()`.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
import hashlib
import importlib
import io
import os
from pathlib import Path
import shutil
import time
from typing import Any, Literal, TypeVar
from urllib.parse import unquote, urlparse
import uuid

from consist.artifact import Artifact, HintLedger, MissingOutputError
from consist.digest import (
    Contract,
    Digest,
    Identity,
    Kind,
    SchemeRegistry,
    canonical_json_str,
    compute_ctd1_digest,
    compute_logical_matrix_digest,
    compute_omx_logical_matrices_sha256,
    compute_zarr_logical_matrices_sha256,
    digest_bytes,
    open_h5py_read_only,
)
from consist.location import (
    DirTreeStat,
    FileStat,
    Freshness,
    Location,
    MemberOf,
    Mount,
    Observation,
    SiteContext,
    Witness,
    lookup_live_ledger,
)
from consist.meta import EMPTY_META, FrozenMeta
from consist.store.detect import ConsistError
from consist.store.ledger import StoreLedger

_STREAM_CHUNK_BYTES: int = 1024 * 1024  # 1 MiB
_default_mounts: dict[str, Mount] = {}
_T = TypeVar("_T")

_SUFFIX_DRIVERS: dict[str, str] = {
    ".parquet": "parquet",
    ".csv": "csv",
    ".tsv": "tsv",
    ".json": "json",
    ".geojson": "geojson",
    ".omx": "omx",
    ".nc": "netcdf",
    ".netcdf": "netcdf",
    ".feather": "feather",
    ".arrow": "feather",
}


@dataclass(frozen=True, slots=True)
class ResolvedLocation:
    """Verified local filesystem location resolved by `locate()`."""

    path: Path
    uri: str
    member: str | None = None
    observation_id: uuid.UUID | None = None
    freshness: Freshness = Freshness.FRESH
    witness: Witness | None = None


@dataclass(frozen=True, slots=True)
class PendingOutput:
    """Declared output to be verified, hashed, and recorded by `seal()`."""

    role: str
    path: Path
    driver: str | None = None
    member: str | None = None
    is_dir: bool = False
    key: str | None = None
    schema_id: str | None = None
    meta: Mapping[str, Any] | None = None
    artifact_id: uuid.UUID = field(default_factory=uuid.uuid4)

    def __post_init__(self) -> None:
        if not isinstance(self.role, str) or not self.role:
            raise ValueError("PendingOutput.role must be a non-empty string")
        if not isinstance(self.path, Path):
            object.__setattr__(self, "path", Path(self.path))
        if isinstance(self.artifact_id, str):
            object.__setattr__(self, "artifact_id", uuid.UUID(self.artifact_id))


def _normalize_mounts(
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None,
) -> dict[str, Mount]:
    if mounts is None:
        return {}
    out: dict[str, Mount] = {}
    if isinstance(mounts, Mapping):
        for name, target in mounts.items():
            if isinstance(target, Mount):
                out[str(name)] = target
            else:
                out[str(name)] = Mount(name=str(name), root=Path(target))
    else:
        for item in mounts:
            if isinstance(item, Mount):
                out[item.name] = item
    return out


def virtualize_path(
    path: Path | str,
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
) -> tuple[str, Mount | None]:
    """Map a local `Path` to `(uri, matched_mount)` using longest mount-root prefix."""
    abs_path = Path(path).expanduser().resolve()
    mount_map = _normalize_mounts(mounts)

    best_mount: Mount | None = None
    best_rel: Path | None = None
    best_parts_len = -1

    for mount in mount_map.values():
        mount_root = mount.root.expanduser().resolve()
        try:
            rel = abs_path.relative_to(mount_root)
        except ValueError:
            continue
        parts_len = len(mount_root.parts)
        if parts_len > best_parts_len:
            best_parts_len = parts_len
            best_mount = mount
            best_rel = rel

    if best_mount is not None and best_rel is not None:
        rel_str = best_rel.as_posix()
        uri = (
            f"{best_mount.name}://"
            if rel_str == "."
            else f"{best_mount.name}://{rel_str}"
        )
        return (uri, best_mount)

    return (abs_path.as_uri(), None)


def resolve_uri_to_path(
    uri: str,
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
) -> tuple[Path | None, Mount | None]:
    """Resolve a Consist URI (`workspace://...`, `archive://...`, `file://...`) to `(Path, Mount | None)`."""
    raw = uri.strip()
    if not raw:
        return (None, None)

    mount_map = {**_default_mounts, **_normalize_mounts(mounts)}

    if raw.startswith("file://"):
        parsed = urlparse(raw)
        local_path = Path(unquote(parsed.path)).expanduser().resolve()
        _, matched_mount = virtualize_path(local_path, mount_map)
        return (local_path, matched_mount)

    if "://" in raw:
        scheme, rel_part = raw.split("://", 1)
        mount = mount_map.get(scheme)
        if mount is None:
            return (None, None)
        clean_rel = rel_part.lstrip("/")
        base_root = mount.root.expanduser().resolve()
        resolved = (base_root / clean_rel).resolve() if clean_rel else base_root
        return (resolved, mount)

    local_path = Path(raw).expanduser().resolve()
    _, matched_mount = virtualize_path(local_path, mount_map)
    return (local_path, matched_mount)


def _is_candidate_visible(
    row: Mapping[str, Any],
    *,
    site_context: SiteContext | None,
    matched_mount: Mount | None,
) -> bool:
    cand_mount_inst = row.get("mount_instance_id")
    if cand_mount_inst is not None and str(cand_mount_inst):
        if matched_mount is None:
            return False
        if matched_mount.resolve_instance_id() != str(cand_mount_inst):
            return False

    if site_context is None:
        return True

    cand_site = str(row.get("site") or "")
    mount_scope = matched_mount.scope if matched_mount is not None else "site"
    if (
        cand_site
        and cand_site != "local"
        and cand_site != site_context.site
        and mount_scope != "global"
    ):
        return False

    cand_scope_key = row.get("scope_key")
    if cand_scope_key is not None and str(cand_scope_key):
        if matched_mount is not None:
            expected_key = site_context.scope_key_for(matched_mount)
            if expected_key != str(cand_scope_key):
                return False
        else:
            node_key = f"node:{site_context.node_name}"
            job_key = (
                f"node:{site_context.node_name}:job:"
                f"{site_context.job_id or f'pid-{os.getpid()}'}"
            )
            if str(cand_scope_key) not in (node_key, job_key):
                return False

    return True


def _effective_mounts(
    active_ledger: StoreLedger | HintLedger | None,
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None,
) -> dict[str, Mount]:
    ledger_mounts = (
        active_ledger.mounts if isinstance(active_ledger, StoreLedger) else {}
    )
    return {**_default_mounts, **ledger_mounts, **_normalize_mounts(mounts)}


def _stat_tuple(st: FileStat) -> tuple[int, int, int | None, int | None]:
    return (st.size, st.mtime_ns, st.ino, st.ctime_ns)


def _run_fd_pinned(
    path: Path,
    fn: Callable[[int], _T],
) -> tuple[_T, FileStat]:
    """Execute `fn(fd)` on an open file descriptor and verify `fstat(fd)` did not change."""
    fd = os.open(path, os.O_RDONLY)
    try:
        stat_before = FileStat.from_os_stat(
            os.fstat(fd),
            observed_at_ns=time.time_ns(),
        )
        result = fn(fd)
        stat_after = FileStat.from_os_stat(
            os.fstat(fd),
            observed_at_ns=time.time_ns(),
        )
    finally:
        os.close(fd)

    if _stat_tuple(stat_before) != _stat_tuple(stat_after):
        raise ConsistError(
            f"Concurrent mid-read mutation detected while hashing {path}"
        )
    return (result, stat_after)


def _hash_file_descriptor_pinned(path: Path) -> tuple[Digest, FileStat]:
    """Compute `sha256:file` on an open file descriptor, guarding against mid-read mutation."""

    def _digest_fd(fd: int) -> Digest:
        with io.FileIO(fd, mode="rb", closefd=False) as raw_io:
            hex_val = hashlib.file_digest(raw_io, "sha256").hexdigest()
        return Digest("sha256:file", hex_val)

    return _run_fd_pinned(path, _digest_fd)


def _tee_hash_and_copy_file(
    src_path: Path,
    dst_path: Path,
) -> tuple[Digest, FileStat, FileStat]:
    """Stream `src_path` once into both `hashlib.sha256()` and `dst_path`."""
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    src_fd = os.open(src_path, os.O_RDONLY)
    try:
        stat_before = FileStat.from_os_stat(
            os.fstat(src_fd),
            observed_at_ns=time.time_ns(),
        )
        hasher = hashlib.sha256()
        with dst_path.open("wb") as dst_f:
            while chunk := os.read(src_fd, _STREAM_CHUNK_BYTES):
                hasher.update(chunk)
                dst_f.write(chunk)
            dst_f.flush()
        stat_after = FileStat.from_os_stat(
            os.fstat(src_fd),
            observed_at_ns=time.time_ns(),
        )
    finally:
        os.close(src_fd)

    if _stat_tuple(stat_before) != _stat_tuple(stat_after):
        dst_path.unlink(missing_ok=True)
        raise ConsistError(
            f"Concurrent mid-read mutation detected while hashing {src_path}"
        )

    try:
        os.utime(dst_path, ns=(stat_after.mtime_ns, stat_after.mtime_ns))
    except OSError:
        pass
    dst_stat = FileStat.capture(dst_path, observed_at_ns=time.time_ns())
    return (Digest("sha256:file", hasher.hexdigest()), stat_after, dst_stat)


def _reject_symlink_ancestors(path: Path) -> None:
    candidate = path.expanduser().absolute()
    for p in reversed((candidate, *candidate.parents)):
        if p.is_symlink():
            raise ValueError(f"directory artifact root cannot traverse a symlink: {p}")


def _build_manifest_v1_parallel(
    root: Path,
    *,
    max_workers: int = 4,
) -> tuple[Digest, bytes, int, int, DirTreeStat]:
    """Walk `root`, hash regular files in parallel, and build `sha256:manifest-v1` + `DirTreeStat`."""
    root_path = Path(root).expanduser().absolute()
    _reject_symlink_ancestors(root_path)
    if not root_path.is_dir():
        raise ValueError(f"directory artifact root must be a directory: {root_path}")

    dir_entries: list[dict[str, Any]] = []
    file_items: list[tuple[str, Path]] = []

    def visit(directory: Path) -> None:
        with os.scandir(directory) as scan:
            children = sorted(scan, key=lambda entry: entry.name)
        for child in children:
            child_path = Path(child.path)
            rel_posix = child_path.relative_to(root_path).as_posix()
            if child.is_symlink():
                raise ValueError(
                    f"directory artifact cannot contain a symlink: {child_path}"
                )
            if child.is_dir(follow_symlinks=False):
                dir_entries.append({"kind": "directory", "path": rel_posix})
                visit(child_path)
            elif child.is_file(follow_symlinks=False):
                file_items.append((rel_posix, child_path))
            else:
                raise ValueError(
                    "directory artifact cannot contain a non-regular filesystem entry: "
                    f"{child_path}"
                )

    visit(root_path)

    def _hash_one(item: tuple[str, Path]) -> tuple[str, str, FileStat]:
        rel_posix, fpath = item
        file_digest, fstat = _hash_file_descriptor_pinned(fpath)
        return (rel_posix, file_digest.hex, fstat)

    if len(file_items) > 1 and max_workers > 1:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            hashed_files = list(pool.map(_hash_one, file_items))
    else:
        hashed_files = [_hash_one(item) for item in file_items]

    entries: list[dict[str, Any]] = list(dir_entries)
    stat_entries: list[tuple[str, FileStat]] = []
    for rel_posix, sha_hex, fstat in hashed_files:
        entries.append(
            {
                "kind": "file",
                "path": rel_posix,
                "sha256": sha_hex,
                "size": fstat.size,
            }
        )
        stat_entries.append((rel_posix, fstat))

    entries.sort(key=lambda e: (e["path"], e["kind"]))
    stat_entries.sort(key=lambda pair: pair[0])

    identity_payload = {"version": 1, "entries": entries}
    canonical_bytes = canonical_json_str(identity_payload).encode("utf-8")
    tree_hash = digest_bytes(canonical_bytes)
    digest = Digest("sha256:manifest-v1", tree_hash)

    now_ns = time.time_ns()
    root_mtime_ns = root_path.stat().st_mtime_ns
    total_bytes = sum(fs.size for _, fs in stat_entries)
    max_mtime_ns = max(
        (fs.mtime_ns for _, fs in stat_entries),
        default=root_mtime_ns,
    )
    racy = any(fs.racy for _, fs in stat_entries) or (
        (now_ns - max_mtime_ns) < 2_000_000_000
    )
    dir_witness = DirTreeStat(
        file_count=len(stat_entries),
        total_bytes=total_bytes,
        max_mtime_ns=max_mtime_ns,
        entries=tuple(stat_entries),
        racy=racy,
    )
    return (digest, canonical_bytes, len(entries), total_bytes, dir_witness)


def _read_single_matrix_from_path(
    path: Path,
    member: str | None,
) -> tuple[str, Any, Any]:
    np = importlib.import_module("numpy")
    suffix = path.suffix.lower()
    if path.is_dir() or suffix == ".zarr":
        zarr = importlib.import_module("zarr")
        store = zarr.open(str(path), mode="r")
        if member is not None and member.strip("/"):
            clean = member.strip("/")
            if clean in store:
                ds = store[clean]
            elif f"data/{clean}" in store:
                ds = store[f"data/{clean}"]
            else:
                raise KeyError(f"Matrix member {member!r} not found in {path}")
            logical_name = clean.split("/")[-1]
        elif isinstance(store, Mapping):
            container = store["data"] if "data" in store else store
            keys = sorted(str(k) for k in container.keys())
            if len(keys) != 1:
                raise ValueError(
                    f"Zarr matrix store {path} has keys {keys}; specify member"
                )
            logical_name = keys[0]
            ds = container[logical_name]
        else:
            logical_name = path.stem
            ds = store
        arr = ds[:]
        na_val = ds.attrs["NA"] if "NA" in ds.attrs else None
        return (logical_name, arr, na_val)

    if suffix == ".npy":
        arr = np.load(path, allow_pickle=False)
        name = (member or path.stem).strip("/") or path.stem
        return (name, arr, None)
    if suffix == ".npz":
        with np.load(path, allow_pickle=False) as npz:
            key = (member or "").strip("/")
            if not key:
                if len(npz.files) != 1:
                    raise ValueError(
                        f"NPZ archive {path} has multiple arrays {npz.files}; specify member"
                    )
                key = str(npz.files[0])
            if key not in npz.files:
                raise KeyError(f"Matrix member {member!r} not found in {path}")
            return (key.split("/")[-1], npz[key], None)

    with open_h5py_read_only(path) as h5f:
        if member is not None and member.strip("/"):
            clean = member.strip("/")
            if clean in h5f:
                ds = h5f[clean]
            elif f"data/{clean}" in h5f:
                ds = h5f[f"data/{clean}"]
            elif "data" in h5f and clean in h5f["data"]:
                ds = h5f["data"][clean]
            else:
                raise KeyError(f"Matrix member {member!r} not found in {path}")
            logical_name = clean.split("/")[-1]
        else:
            container = h5f["data"] if "data" in h5f else h5f
            keys = sorted(str(k) for k in container.keys())
            if len(keys) != 1:
                raise ValueError(
                    f"Matrix container {path} has keys {keys}; specify member"
                )
            logical_name = keys[0]
            ds = container[logical_name]

        arr = ds[...]
        na_val = ds.attrs["NA"] if "NA" in ds.attrs else None
        return (logical_name, arr, na_val)


def _dir_stat_signature(
    st: DirTreeStat,
) -> tuple[
    int, int, int, tuple[tuple[str, tuple[int, int, int | None, int | None]], ...]
]:
    return (
        st.file_count,
        st.total_bytes,
        st.max_mtime_ns,
        tuple((rel, _stat_tuple(fs)) for rel, fs in st.entries),
    )


def _run_dir_pinned(
    path: Path,
    fn: Callable[[], _T],
) -> tuple[_T, DirTreeStat]:
    stat_before = DirTreeStat.capture(path)
    result = fn()
    stat_after = DirTreeStat.capture(path)
    if _dir_stat_signature(stat_before) != _dir_stat_signature(stat_after):
        raise ConsistError(
            f"Concurrent mid-read mutation detected while hashing {path}"
        )
    return (result, stat_after)


def compute_scheme_digest(
    path: Path,
    scheme: str,
    *,
    driver: str = "file",
    member: str | None = None,
    ledger: StoreLedger | None = None,
    max_workers: int = 4,
) -> tuple[Digest, Witness]:
    """Compute a descriptor-pinned `(Digest, Witness)` for `path` under `scheme`."""
    canonical_scheme = SchemeRegistry.get(scheme).name

    if canonical_scheme == "sha256:file":
        digest, fstat = _hash_file_descriptor_pinned(path)
        witness: Witness = (
            MemberOf(parent=fstat, member=member) if member is not None else fstat
        )
        return (digest, witness)

    if canonical_scheme == "sha256:manifest-v1":
        digest, body_bytes, entry_count, total_bytes, dir_stat = (
            _build_manifest_v1_parallel(path, max_workers=max_workers)
        )
        if ledger is not None and not ledger.read_only:
            ledger.record_content(digest, "tree")
            ledger.record_content_manifest(
                digest,
                format="manifest-v1",
                entry_count=entry_count,
                total_bytes=total_bytes,
                body=body_bytes,
            )
        return (digest, dir_stat)

    if canonical_scheme == "ctd1:sha256":
        digest, stat_after = _run_fd_pinned(
            path,
            lambda _fd: compute_ctd1_digest(path, member=member),
        )
        witness = (
            MemberOf(parent=stat_after, member=member)
            if member is not None
            else stat_after
        )
        return (digest, witness)

    if canonical_scheme == "sha256:logical_matrix:v1":

        def _compute_single_mat() -> Digest:
            mat_name, arr, na_val = _read_single_matrix_from_path(path, member)
            return compute_logical_matrix_digest(mat_name, arr, na_value=na_val)

        if path.is_dir() or driver.strip().lower() == "zarr":
            return _run_dir_pinned(path, _compute_single_mat)

        digest, stat_after = _run_fd_pinned(path, lambda _fd: _compute_single_mat())
        witness = (
            MemberOf(parent=stat_after, member=member)
            if member is not None
            else stat_after
        )
        return (digest, witness)

    if canonical_scheme == "sha256:logical_matrices:v1":
        if path.is_dir() or driver.strip().lower() == "zarr":
            digest, dir_stat = _run_dir_pinned(
                path,
                lambda: Digest(
                    "sha256:logical_matrices:v1",
                    compute_zarr_logical_matrices_sha256(path),
                ),
            )
            return (digest, dir_stat)

        digest, stat_after = _run_fd_pinned(
            path,
            lambda _fd: Digest(
                "sha256:logical_matrices:v1",
                compute_omx_logical_matrices_sha256(path),
            ),
        )
        witness = (
            MemberOf(parent=stat_after, member=member)
            if member is not None
            else stat_after
        )
        return (digest, witness)

    raise ValueError(
        f"Unsupported digest scheme for verification/computation: {scheme!r}"
    )


def verify_path_digest(
    path: Path,
    expected: Digest,
    *,
    driver: str = "file",
    member: str | None = None,
    ledger: StoreLedger | None = None,
) -> tuple[bool, Digest, Witness | None]:
    """Compute `expected.scheme` on `path` with descriptor pinning and compare to `expected`."""
    actual, witness = compute_scheme_digest(
        Path(path),
        expected.scheme,
        driver=driver,
        member=member,
        ledger=ledger,
    )
    return (actual == expected, actual, witness)


def _non_racy_witness(witness: Witness | None) -> Witness | None:
    """Clear the `racy` flag on a witness that has just passed descriptor-pinned verification."""
    if isinstance(witness, FileStat):
        return replace(witness, racy=False) if witness.racy else witness
    if isinstance(witness, MemberOf):
        return (
            replace(witness, parent=replace(witness.parent, racy=False))
            if witness.parent.racy
            else witness
        )
    if isinstance(witness, DirTreeStat):
        if not witness.racy and not any(fs.racy for _, fs in witness.entries):
            return witness
        return replace(
            witness,
            entries=tuple(
                (rel, replace(fs, racy=False) if fs.racy else fs)
                for rel, fs in witness.entries
            ),
            racy=False,
        )
    return witness


def _resolve_active_ledger(
    artifact: Artifact,
    explicit_ledger: StoreLedger | HintLedger | None,
) -> StoreLedger | HintLedger:
    if explicit_ledger is not None:
        return explicit_ledger
    live = lookup_live_ledger(artifact.origin)
    if isinstance(live, (StoreLedger, HintLedger)):
        return live
    return HintLedger(artifact.hints)


def _build_path_observation(
    path: Path,
    *,
    identity: Identity,
    driver: str,
    witness: Witness | None,
    basis: Literal["computed", "derived", "implied", "declared", "trusted_foreign"],
    member: str | None = None,
    site_context: SiteContext | None = None,
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
    observed_at_ns: int | None = None,
) -> Observation:
    uri, matched_mount = virtualize_path(path, mounts)
    site = site_context.site if site_context is not None else "local"
    scope_key = (
        site_context.scope_key_for(matched_mount)
        if (site_context is not None and matched_mount is not None)
        else None
    )
    mount_instance_id = (
        matched_mount.resolve_instance_id() if matched_mount is not None else None
    )
    return Observation(
        location=Location(uri=uri, member=member),
        identity=identity,
        access_driver=driver,
        witness=witness,
        basis=basis,
        site=site,
        scope_key=scope_key,
        mount_instance_id=mount_instance_id,
        observed_at_ns=observed_at_ns if observed_at_ns is not None else time.time_ns(),
    )


def locate(
    artifact: Artifact,
    *,
    ledger: StoreLedger | HintLedger | None = None,
    site_context: SiteContext | None = None,
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
    verify_on_stale: bool = True,
) -> ResolvedLocation:
    """Locate a verified local filesystem path for `artifact`."""
    active_ledger = _resolve_active_ledger(artifact, ledger)
    effective_mounts = _effective_mounts(active_ledger, mounts)
    primary_anchor = artifact.identity.anchor

    # Fast-path: check memoized ResolvedLocation when no custom mounts override resolution
    if not mounts:
        hint_member = (
            artifact.hints[0].location.member if len(artifact.hints) == 1 else None
        )
        memo_site = site_context.site if site_context is not None else "local"
        memo = active_ledger.get_memoized_location(
            primary_anchor,
            member=hint_member,
            site=memo_site,
        )
        if (
            isinstance(memo, ResolvedLocation)
            and isinstance(memo.witness, (FileStat, DirTreeStat, MemberOf))
            and memo.witness.check(memo.path) is Freshness.FRESH
        ):
            return memo

    anchors_to_search: list[Digest] = [primary_anchor]
    promoted_verified_anchor: Digest | None = None

    if (
        isinstance(active_ledger, StoreLedger)
        and primary_anchor.kind is Kind.HISTORICAL
    ):
        promo = active_ledger.get_promotion(primary_anchor)
        if promo is not None:
            ver_anchor, _basis, _ev = promo
            promoted_verified_anchor = ver_anchor
            if ver_anchor not in anchors_to_search:
                anchors_to_search.append(ver_anchor)

    if (
        artifact.recorded_anchor is not None
        and artifact.recorded_anchor not in anchors_to_search
    ):
        anchors_to_search.append(artifact.recorded_anchor)

    candidate_rows = active_ledger.locate_current(
        anchors_to_search,
        include_evicted=False,
    )

    if artifact.hints:
        fallback_rows = HintLedger(artifact.hints).locate_current(
            anchors_to_search,
            include_evicted=False,
        )
        if not candidate_rows:
            candidate_rows = fallback_rows
        else:
            seen_places = {
                (
                    str(r.get("site") or ""),
                    str(r.get("scope_key") or ""),
                    str(r.get("mount_instance_id") or ""),
                    str(r.get("uri") or ""),
                    str(r.get("member") or ""),
                )
                for r in candidate_rows
            }
            for f_row in fallback_rows:
                place = (
                    str(f_row.get("site") or ""),
                    str(f_row.get("scope_key") or ""),
                    str(f_row.get("mount_instance_id") or ""),
                    str(f_row.get("uri") or ""),
                    str(f_row.get("member") or ""),
                )
                if place not in seen_places:
                    seen_places.add(place)
                    candidate_rows.append(f_row)

    for row in candidate_rows:
        uri = str(row["uri"])
        cand_member = str(row["member"]) if row.get("member") is not None else None
        local_path, matched_mount = resolve_uri_to_path(uri, effective_mounts)
        if local_path is None:
            continue
        if not _is_candidate_visible(
            row,
            site_context=site_context,
            matched_mount=matched_mount,
        ):
            continue

        witness = row.get("witness")
        if isinstance(witness, (FileStat, DirTreeStat, MemberOf)):
            freshness = witness.check(local_path)
        else:
            if not local_path.exists():
                continue
            freshness = Freshness.STALE

        if freshness is Freshness.MISSING:
            continue

        effective_site = (
            site_context.site
            if site_context is not None
            else str(row.get("site") or "local")
        )
        if freshness is Freshness.FRESH:
            resolved_loc = ResolvedLocation(
                path=local_path,
                uri=uri,
                member=cand_member,
                observation_id=row.get("observation_id"),
                freshness=Freshness.FRESH,
                witness=witness,
            )
            active_ledger.memoize_location(
                primary_anchor,
                resolved_loc,
                member=cand_member,
                site=effective_site,
            )
            return resolved_loc

        # Freshness is STALE: determine whether we can/should re-verify
        expected_digest: Digest | None = (
            primary_anchor
            if primary_anchor.kind is not Kind.HISTORICAL
            else promoted_verified_anchor
        )
        if expected_digest is None and row.get("anchor"):
            try:
                cand_anchor = Digest.parse(str(row["anchor"]))
                if cand_anchor.kind is not Kind.HISTORICAL:
                    expected_digest = cand_anchor
            except ValueError:
                pass

        if not verify_on_stale or expected_digest is None:
            return ResolvedLocation(
                path=local_path,
                uri=uri,
                member=cand_member,
                observation_id=row.get("observation_id"),
                freshness=Freshness.STALE,
                witness=witness,
            )

        try:
            matched, _actual, new_witness = verify_path_digest(
                local_path,
                expected_digest,
                driver=artifact.driver,
                member=cand_member,
                ledger=(
                    active_ledger if isinstance(active_ledger, StoreLedger) else None
                ),
            )
        except ConsistError:
            raise
        except Exception:
            matched = False
            new_witness = None

        if not matched:
            continue

        refreshed_witness = _non_racy_witness(new_witness)
        new_obs_id = row.get("observation_id")
        if isinstance(active_ledger, StoreLedger):
            obs_anchor = (
                primary_anchor
                if primary_anchor.kind is not Kind.HISTORICAL
                else expected_digest
            )
            obs_identity = active_ledger.get_identity(obs_anchor)
            repr_kind = (
                "member"
                if cand_member is not None
                else ("tree" if isinstance(refreshed_witness, DirTreeStat) else "file")
            )
            refreshed_obs = Observation(
                location=Location(uri=uri, member=cand_member),
                identity=obs_identity,
                access_driver=str(row.get("access_driver") or artifact.driver),
                witness=refreshed_witness,
                basis="computed",
                site=effective_site,
                scope_key=(
                    site_context.scope_key_for(matched_mount)
                    if (site_context is not None and matched_mount is not None)
                    else (
                        str(row["scope_key"])
                        if row.get("scope_key") is not None
                        else None
                    )
                ),
                mount_instance_id=(
                    matched_mount.resolve_instance_id()
                    if matched_mount is not None
                    else (
                        str(row["mount_instance_id"])
                        if row.get("mount_instance_id") is not None
                        else None
                    )
                ),
                observed_at_ns=time.time_ns(),
            )
            new_obs_id = active_ledger.record_observation(
                refreshed_obs,
                repr_kind=repr_kind,
            )
        elif (
            artifact.origin is None and refreshed_witness is not None and artifact.hints
        ):
            updated_hints = tuple(
                replace(obs, witness=refreshed_witness)
                if (obs.location.uri == uri and obs.location.member == cand_member)
                else obs
                for obs in artifact.hints
            )
            object.__setattr__(artifact, "hints", updated_hints)

        resolved_loc = ResolvedLocation(
            path=local_path,
            uri=uri,
            member=cand_member,
            observation_id=new_obs_id,
            freshness=Freshness.FRESH,
            witness=refreshed_witness,
        )
        active_ledger.memoize_location(
            primary_anchor,
            resolved_loc,
            member=cand_member,
            site=effective_site,
        )
        return resolved_loc

    raise FileNotFoundError(
        f"Could not locate artifact {artifact.key!r} "
        f"(id={artifact.id}, anchor={artifact.identity.anchor})"
    )


def _resolve_target_path_and_member(
    artifact: Artifact,
    path: Path | str | None,
    *,
    active_ledger: StoreLedger | HintLedger,
    site_context: SiteContext | None,
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None,
    verify_on_stale: bool = True,
) -> tuple[Path, str | None]:
    if path is not None:
        target_path = Path(path).expanduser().resolve()
        member: str | None = (
            artifact.hints[0].location.member if artifact.hints else None
        )
        return (target_path, member)
    resolved = locate(
        artifact,
        ledger=active_ledger,
        site_context=site_context,
        mounts=mounts,
        verify_on_stale=verify_on_stale,
    )
    return (resolved.path, resolved.member)


def verify(
    artifact: Artifact,
    path: Path | str | None = None,
    *,
    ledger: StoreLedger | HintLedger | None = None,
    site_context: SiteContext | None = None,
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
) -> bool:
    """Verify that `artifact` matches its primary anchor digest on disk."""
    if artifact.identity.anchor.kind is Kind.HISTORICAL:
        return False

    active_ledger = _resolve_active_ledger(artifact, ledger)
    store_ledger = active_ledger if isinstance(active_ledger, StoreLedger) else None

    try:
        target_path, member = _resolve_target_path_and_member(
            artifact,
            path,
            active_ledger=active_ledger,
            site_context=site_context,
            mounts=mounts,
            verify_on_stale=True,
        )
    except FileNotFoundError:
        return False

    if not target_path.exists():
        return False

    matched, _actual, _witness = verify_path_digest(
        target_path,
        artifact.identity.anchor,
        driver=artifact.driver,
        member=member,
        ledger=store_ledger,
    )
    return matched


def ensure_digest(
    artifact: Artifact,
    scheme: str,
    *,
    path: Path | str | None = None,
    ledger: StoreLedger | HintLedger | None = None,
    site_context: SiteContext | None = None,
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
) -> tuple[Artifact, Digest]:
    """Ensure `artifact.identity` carries a verified digest for `scheme`, computing and persisting on demand."""
    if artifact.identity.anchor.kind is Kind.HISTORICAL:
        raise ValueError("Cannot compute digests for a Kind.HISTORICAL artifact")

    canonical_scheme = SchemeRegistry.get(scheme).name
    if (existing := artifact.identity.get(canonical_scheme)) is not None:
        return (artifact, existing)

    active_ledger = _resolve_active_ledger(artifact, ledger)
    store_ledger = active_ledger if isinstance(active_ledger, StoreLedger) else None

    if store_ledger is not None:
        stored_identity = store_ledger.get_identity(artifact.identity.anchor)
        if (stored_claim := stored_identity.get(canonical_scheme)) is not None:
            updated_art = replace(
                artifact,
                identity=artifact.identity.with_claim(stored_claim),
            )
            return (updated_art, stored_claim)

    target_path, member = _resolve_target_path_and_member(
        artifact,
        path,
        active_ledger=active_ledger,
        site_context=site_context,
        mounts=mounts,
        verify_on_stale=True,
    )

    # Verify the target path still matches the artifact's primary anchor before
    # attaching a secondary digest claim to the anchor.
    anchor_ok, _actual_anchor, _anchor_w = verify_path_digest(
        target_path,
        artifact.identity.anchor,
        driver=artifact.driver,
        member=member,
        ledger=store_ledger,
    )
    if not anchor_ok:
        raise ConsistError(
            f"Cannot compute {canonical_scheme!r} for artifact {artifact.key!r}: "
            f"bytes at {target_path} no longer match anchor {artifact.identity.anchor}"
        )

    new_digest, _witness = compute_scheme_digest(
        target_path,
        canonical_scheme,
        driver=artifact.driver,
        member=member,
        ledger=store_ledger,
    )
    if store_ledger is not None:
        store_ledger.record_content_digest(
            artifact.identity.anchor,
            new_digest,
            basis="computed",
        )
    updated_artifact = replace(
        artifact,
        identity=artifact.identity.with_claim(new_digest),
    )
    return (updated_artifact, new_digest)


def stage(
    artifact: Artifact,
    dest_path: Path | str,
    *,
    mode: Literal["auto", "hardlink", "reflink", "symlink", "copy"] = "auto",
    contract: Contract = Contract(),
    ledger: StoreLedger | HintLedger | None = None,
    site_context: SiteContext | None = None,
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
) -> Path:
    """Stage `artifact` at `dest_path`, enforcing `contract.mutates` link safety."""
    if mode not in ("auto", "hardlink", "reflink", "symlink", "copy"):
        raise ValueError(f"Unsupported stage mode: {mode!r}")
    if contract.mutates and mode in ("hardlink", "symlink"):
        raise ValueError(
            f"Cannot stage artifact with mode={mode!r} when contract.mutates=True"
        )

    active_ledger = _resolve_active_ledger(artifact, ledger)
    effective_mounts = _effective_mounts(active_ledger, mounts)
    resolved = locate(
        artifact,
        ledger=active_ledger,
        site_context=site_context,
        mounts=effective_mounts,
    )
    src = resolved.path.expanduser().resolve()
    raw_dest = Path(dest_path).expanduser().absolute()
    raw_dest.parent.mkdir(parents=True, exist_ok=True)
    dest = raw_dest.parent.resolve() / raw_dest.name

    if dest == src and not dest.is_symlink():
        if contract.mutates:
            raise ValueError(
                f"Cannot stage artifact onto its own source path {src} when contract.mutates=True"
            )
        return dest

    if dest.is_symlink() or dest.is_file():
        dest.unlink()
    elif dest.exists():
        shutil.rmtree(dest)

    effective_mode = (
        ("copy" if contract.mutates else "hardlink") if mode == "auto" else mode
    )

    if src.is_dir():
        if effective_mode == "symlink":
            os.symlink(src, dest, target_is_directory=True)
        elif effective_mode == "hardlink":
            try:
                shutil.copytree(src, dest, copy_function=os.link)
            except OSError:
                if mode == "hardlink":
                    raise
                if dest.exists():
                    shutil.rmtree(dest)
                shutil.copytree(src, dest, copy_function=shutil.copy2)
        else:
            shutil.copytree(src, dest, copy_function=shutil.copy2)
    else:
        if effective_mode == "symlink":
            os.symlink(src, dest)
        elif effective_mode == "hardlink":
            try:
                os.link(src, dest)
            except OSError:
                if mode == "hardlink":
                    raise
                shutil.copy2(src, dest)
        else:
            shutil.copy2(src, dest)

    if isinstance(active_ledger, StoreLedger) and not contract.mutates:
        now_ns = time.time_ns()
        if src.is_dir():
            dest_witness: Witness = DirTreeStat.capture(dest, observed_at_ns=now_ns)
            repr_kind = "tree"
        elif resolved.member is not None:
            dest_witness = MemberOf.capture(
                dest,
                member=resolved.member,
                observed_at_ns=now_ns,
            )
            repr_kind = "member"
        else:
            dest_witness = FileStat.capture(dest, observed_at_ns=now_ns)
            repr_kind = "file"

        staged_obs = _build_path_observation(
            dest,
            identity=artifact.identity,
            driver=artifact.driver,
            witness=dest_witness,
            basis="derived",
            member=resolved.member,
            site_context=site_context,
            mounts=effective_mounts,
            observed_at_ns=now_ns,
        )
        active_ledger.record_observation(staged_obs, repr_kind=repr_kind)

    return dest


def _infer_driver(po: PendingOutput, resolved_path: Path) -> str:
    if po.driver is not None and po.driver.strip():
        return po.driver.strip()
    if po.is_dir or resolved_path.is_dir():
        return "zarr" if resolved_path.suffix.lower() == ".zarr" else "directory"
    suffix = resolved_path.suffix.lower()
    if suffix in (".h5", ".hdf5", ".hdf"):
        return "h5_table" if po.member is not None else "hdf5"
    return _SUFFIX_DRIVERS.get(suffix, "file")


def _resolve_archive_dest(
    resolved_path: Path,
    role: str,
    *,
    archive_root: Path,
    mount_map: Mapping[str, Mount],
    archive_mount_name: str,
    used_dests: dict[Path, Path],
) -> Path:
    best_rel: Path | None = None
    best_parts_len = -1
    for m_name, mount in mount_map.items():
        if m_name == archive_mount_name:
            continue
        m_root = mount.root.expanduser().resolve()
        try:
            rel = resolved_path.relative_to(m_root)
        except ValueError:
            continue
        if len(m_root.parts) > best_parts_len:
            best_parts_len = len(m_root.parts)
            best_rel = rel

    candidate = (
        archive_root / best_rel
        if (best_rel is not None and best_rel.parent != Path("."))
        else archive_root / resolved_path.name
    )
    existing_src = used_dests.get(candidate)
    if existing_src is not None and existing_src != resolved_path:
        candidate = archive_root / role / resolved_path.name
    used_dests[candidate] = resolved_path
    return candidate


@dataclass(frozen=True, slots=True)
class _SealedItemWork:
    po: PendingOutput
    resolved_path: Path
    driver: str
    repr_kind: str
    anchor: Digest
    primary_witness: Witness
    manifest_payload: tuple[bytes, int, int] | None = None
    archive_path: Path | None = None
    archive_witness: Witness | None = None


def seal(
    pending_outputs: Sequence[PendingOutput] | Mapping[str, PendingOutput | Path | str],
    *,
    ledger: StoreLedger | None = None,
    run_id: str | None = None,
    site_context: SiteContext | None = None,
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
    archive_root: Path | str | None = None,
    archive_mount_name: str = "archive",
    max_workers: int = 4,
    _update_default_mounts: bool = True,
) -> dict[str, Artifact]:
    """Validate, hash (with single-pass tee copy when archiving), and record declared outputs."""
    items: list[PendingOutput] = []
    if isinstance(pending_outputs, Mapping):
        for role_key, spec in pending_outputs.items():
            if isinstance(spec, PendingOutput):
                items.append(spec)
            else:
                raw_str = str(spec)
                items.append(
                    PendingOutput(
                        role=str(role_key),
                        path=Path(spec),
                        is_dir=raw_str.endswith(("/", os.sep)),
                    )
                )
    else:
        items.extend(pending_outputs)

    # Step 1: Validate existence of ALL declared PendingOutputs first.
    for po in items:
        expanded = po.path.expanduser()
        if not expanded.exists() or (po.is_dir and not expanded.is_dir()):
            raise MissingOutputError(role=po.role, path=po.path)

    mount_map = _normalize_mounts(mounts)
    resolved_archive_root: Path | None = None
    if archive_root is not None:
        resolved_archive_root = Path(archive_root).expanduser().resolve()
        resolved_archive_root.mkdir(parents=True, exist_ok=True)
        if archive_mount_name not in mount_map:
            mount_map[archive_mount_name] = Mount(
                name=archive_mount_name,
                root=resolved_archive_root,
            )

    if ledger is not None:
        ledger.mounts.update(mount_map)
    if _update_default_mounts:
        _default_mounts.update(mount_map)

    # Pre-compute collision-safe archive destination paths before parallel work.
    archive_dests: dict[int, Path] = {}
    if resolved_archive_root is not None:
        used_dests: dict[Path, Path] = {}
        for idx, po in enumerate(items):
            res_p = po.path.expanduser().resolve()
            archive_dests[idx] = _resolve_archive_dest(
                res_p,
                po.role,
                archive_root=resolved_archive_root,
                mount_map=mount_map,
                archive_mount_name=archive_mount_name,
                used_dests=used_dests,
            )

    def _process_one(idx_and_po: tuple[int, PendingOutput]) -> _SealedItemWork:
        idx, po = idx_and_po
        resolved_path = po.path.expanduser().resolve()
        drv = _infer_driver(po, resolved_path)
        target_arch_path = archive_dests.get(idx)

        if po.member is not None:
            scheme = (
                "sha256:logical_matrix:v1"
                if drv.lower() in ("omx", "openmatrix", "matrix", "zarr", "npy", "npz")
                else "ctd1:sha256"
            )
            try:
                anchor, primary_w = compute_scheme_digest(
                    resolved_path,
                    scheme,
                    driver=drv,
                    member=po.member,
                    ledger=None,
                    max_workers=1,
                )
            except (KeyError, ValueError) as exc:
                raise MissingOutputError(role=po.role, path=po.path) from exc
            return _SealedItemWork(
                po=po,
                resolved_path=resolved_path,
                driver=drv,
                repr_kind="member",
                anchor=anchor,
                primary_witness=primary_w,
                archive_path=target_arch_path,
            )

        if po.is_dir or resolved_path.is_dir():
            anchor, body_bytes, entry_count, total_bytes, dir_stat = (
                _build_manifest_v1_parallel(
                    resolved_path,
                    max_workers=max(1, max_workers),
                )
            )
            arch_path: Path | None = None
            arch_witness: Witness | None = None
            if target_arch_path is not None:
                arch_path = target_arch_path
                if arch_path != resolved_path:
                    arch_path.parent.mkdir(parents=True, exist_ok=True)
                    if arch_path.exists():
                        shutil.rmtree(arch_path)
                    shutil.copytree(
                        resolved_path,
                        arch_path,
                        copy_function=shutil.copy2,
                    )
                arch_witness = DirTreeStat.capture(arch_path)
            return _SealedItemWork(
                po=po,
                resolved_path=resolved_path,
                driver=drv,
                repr_kind="tree",
                anchor=anchor,
                primary_witness=dir_stat,
                manifest_payload=(body_bytes, entry_count, total_bytes),
                archive_path=arch_path,
                archive_witness=arch_witness,
            )

        if target_arch_path is not None and target_arch_path != resolved_path:
            anchor, primary_stat, arch_stat = _tee_hash_and_copy_file(
                resolved_path,
                target_arch_path,
            )
            return _SealedItemWork(
                po=po,
                resolved_path=resolved_path,
                driver=drv,
                repr_kind="file",
                anchor=anchor,
                primary_witness=primary_stat,
                archive_path=target_arch_path,
                archive_witness=arch_stat,
            )

        anchor, primary_stat = _hash_file_descriptor_pinned(resolved_path)
        return _SealedItemWork(
            po=po,
            resolved_path=resolved_path,
            driver=drv,
            repr_kind="file",
            anchor=anchor,
            primary_witness=primary_stat,
        )

    indexed_items = list(enumerate(items))
    if len(indexed_items) > 1 and max_workers > 1:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            completed_work = list(pool.map(_process_one, indexed_items))
    else:
        completed_work = [_process_one(item) for item in indexed_items]

    # If container children were sealed with archive_root, copy each unique parent container once.
    archived_member_parents: dict[Path, tuple[Path, FileStat]] = {}
    if resolved_archive_root is not None:
        for work in completed_work:
            if (
                work.po.member is not None
                and work.archive_path is not None
                and work.resolved_path not in archived_member_parents
            ):
                arch_parent = work.archive_path
                if arch_parent != work.resolved_path:
                    arch_parent.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(work.resolved_path, arch_parent)
                parent_stat = FileStat.capture(arch_parent)
                archived_member_parents[work.resolved_path] = (
                    arch_parent,
                    parent_stat,
                )

    next_occurrence_idx = 0
    if ledger is not None and run_id is not None and not ledger.read_only:
        row = ledger.connection.execute(
            "SELECT COALESCE(MAX(occurrence_index) + 1, 0) "
            "FROM run_binding WHERE run_id = ? AND direction = 'output'",
            [run_id],
        ).fetchone()
        if row is not None and row[0] is not None:
            next_occurrence_idx = int(row[0])

    sealed_artifacts: dict[str, Artifact] = {}

    for work in completed_work:
        po = work.po
        art_key = po.key if (po.key is not None and po.key.strip()) else po.role
        identity = Identity(anchor=work.anchor)
        now_ns = time.time_ns()

        primary_obs = _build_path_observation(
            work.resolved_path,
            identity=identity,
            driver=work.driver,
            witness=work.primary_witness,
            basis="computed",
            member=po.member,
            site_context=site_context,
            mounts=mount_map,
            observed_at_ns=now_ns + 1,
        )
        obs_list: list[Observation] = [primary_obs]

        arch_path = work.archive_path
        arch_witness = work.archive_witness
        if po.member is not None and work.resolved_path in archived_member_parents:
            arch_parent_path, arch_parent_stat = archived_member_parents[
                work.resolved_path
            ]
            arch_path = arch_parent_path
            arch_witness = MemberOf(parent=arch_parent_stat, member=po.member)

        if arch_path is not None and arch_witness is not None:
            archive_obs = _build_path_observation(
                arch_path,
                identity=identity,
                driver=work.driver,
                witness=arch_witness,
                basis="derived",
                member=po.member,
                site_context=site_context,
                mounts=mount_map,
                observed_at_ns=now_ns,
            )
            obs_list.append(archive_obs)

        frozen_meta = FrozenMeta(po.meta) if po.meta else EMPTY_META

        if ledger is not None:
            ledger.record_identity(
                identity,
                repr_kind=work.repr_kind,
                schema_id=po.schema_id,
            )
            if work.manifest_payload is not None and not ledger.read_only:
                body_bytes, entry_count, total_bytes = work.manifest_payload
                ledger.record_content_manifest(
                    work.anchor,
                    format="manifest-v1",
                    entry_count=entry_count,
                    total_bytes=total_bytes,
                    body=body_bytes,
                )
            if not ledger.read_only:
                ledger.record_artifact(
                    artifact_id=po.artifact_id,
                    key=art_key,
                    driver=work.driver,
                    anchor=work.anchor,
                    run_id=run_id,
                    schema_id=po.schema_id,
                    meta=frozen_meta,
                )
            for obs in reversed(obs_list):
                ledger.record_observation(
                    obs,
                    repr_kind=None,
                )
            if run_id is not None and not ledger.read_only:
                ledger.record_run_binding(
                    run_id=run_id,
                    direction="output",
                    occurrence_index=next_occurrence_idx,
                    slot=po.role,
                    artifact_id=po.artifact_id,
                    anchor=work.anchor,
                    ordinal=0,
                    role=po.role,
                )
                next_occurrence_idx += 1

        artifact = Artifact(
            id=po.artifact_id,
            key=art_key,
            driver=work.driver,
            identity=identity,
            recorded_anchor=None,
            origin=ledger.ref if ledger is not None else None,
            run_id=run_id,
            schema_id=po.schema_id,
            meta=frozen_meta,
            hints=tuple(obs_list[:4]),
        )
        sealed_artifacts[po.role] = artifact

    return sealed_artifacts


def observe(
    path: Path | str,
    *,
    key: str | None = None,
    driver: str | None = None,
    member: str | None = None,
    ledger: StoreLedger | None = None,
    site_context: SiteContext | None = None,
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
) -> Artifact:
    """Observe an existing local file or directory and return a v2 `Artifact`."""
    resolved_path = Path(path).expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Cannot observe non-existent path: {resolved_path}")
    role_key = (
        key
        if (key is not None and key.strip())
        else (resolved_path.stem or resolved_path.name or "artifact")
    )
    po = PendingOutput(
        role=role_key,
        path=resolved_path,
        driver=driver,
        member=member,
        is_dir=resolved_path.is_dir(),
        key=role_key,
    )
    sealed = seal(
        [po],
        ledger=ledger,
        run_id=None,
        site_context=site_context,
        mounts=mounts,
        _update_default_mounts=False,
    )
    return sealed[role_key]


__all__ = [
    "Contract",
    "PendingOutput",
    "ResolvedLocation",
    "compute_scheme_digest",
    "ensure_digest",
    "locate",
    "observe",
    "resolve_uri_to_path",
    "seal",
    "stage",
    "verify",
    "verify_path_digest",
    "virtualize_path",
]
