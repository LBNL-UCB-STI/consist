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
    Agreement,
    Contract,
    Digest,
    Expectation,
    Identity,
    Kind,
    SchemeRegistry,
    Verdict,
    canonical_json_str,
    compare,
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
from consist.store.ledger import StoreLedger, deserialize_witness_json

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
    policy: Any = "preserve_canonical",
) -> tuple[Digest, Witness]:
    """Compute a descriptor-pinned `(Digest, Witness)` for `path` under `scheme`."""
    canonical_scheme = SchemeRegistry.get(scheme).name
    effective_policy = policy if policy is not None else "preserve_canonical"

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
            return compute_logical_matrix_digest(
                mat_name,
                arr,
                na_value=na_val,
                policy=effective_policy,
            )

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
                    compute_zarr_logical_matrices_sha256(
                        path,
                        policy=effective_policy,
                    ),
                ),
            )
            return (digest, dir_stat)

        digest, stat_after = _run_fd_pinned(
            path,
            lambda _fd: Digest(
                "sha256:logical_matrices:v1",
                compute_omx_logical_matrices_sha256(
                    path,
                    policy=effective_policy,
                ),
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


def _expectation_to_identity_and_contract(
    expected: Expectation | Identity | Digest,
    contract: Contract | Expectation | None,
) -> tuple[Identity | None, Expectation]:
    if isinstance(contract, Expectation):
        cmp_contract = contract
    elif isinstance(contract, Contract):
        eq_spec = SchemeRegistry.get(contract.equivalence)
        cmp_contract = Expectation(
            accepts_kinds=frozenset({eq_spec.kind}),
            pinned_schemes=frozenset({eq_spec.name}),
        )
    else:
        cmp_contract = Contract.ANY_SHARED

    if isinstance(expected, Identity):
        return (expected, cmp_contract)
    if isinstance(expected, Digest):
        return (Identity(anchor=expected), cmp_contract)
    if isinstance(expected, Expectation):
        if expected.digests:
            sorted_digests = sorted(
                expected.digests,
                key=lambda d: (SchemeRegistry.get(d.scheme).cost_rank, d.scheme),
            )
            non_hist = [d for d in sorted_digests if d.kind is not Kind.HISTORICAL]
            if not non_hist:
                exp_identity = Identity(anchor=sorted_digests[0])
            else:
                exp_identity = Identity(
                    anchor=non_hist[0],
                    claims=tuple(non_hist[1:]),
                )
            if contract is None:
                cmp_contract = Expectation(
                    accepts_kinds=expected.accepts_kinds,
                    pinned_schemes=expected.pinned_schemes,
                )
            return (exp_identity, cmp_contract)
        return (None, expected if contract is None else cmp_contract)
    return (None, cmp_contract)


def _capture_path_witness(
    path: Path,
    *,
    is_dir: bool,
    member: str | None = None,
    observed_at_ns: int | None = None,
) -> tuple[Witness, str]:
    now_ns = time.time_ns() if observed_at_ns is None else observed_at_ns
    if is_dir:
        return (DirTreeStat.capture(path, observed_at_ns=now_ns), "tree")
    if member is not None:
        return (
            MemberOf.capture(path, member=member, observed_at_ns=now_ns),
            "member",
        )
    return (FileStat.capture(path, observed_at_ns=now_ns), "file")


def _remove_path(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink(missing_ok=True)
    elif path.exists():
        shutil.rmtree(path)


def verify(
    artifact: Artifact,
    path: Path | str | Expectation | Identity | Digest | None = None,
    *,
    contract: Contract | Expectation | None = None,
    ledger: StoreLedger | HintLedger | None = None,
    site_context: SiteContext | None = None,
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
    policy: Any = None,
) -> bool | Verdict:
    """Verify that `artifact` matches its primary anchor digest on disk, or compare against an `Expectation`."""
    if isinstance(path, (Expectation, Identity, Digest)) or contract is not None:
        if artifact.identity.anchor.kind is Kind.HISTORICAL:
            return Verdict(Agreement.UNKNOWN, None, (), False)

        exp_obj = (
            path
            if isinstance(path, (Expectation, Identity, Digest))
            else Expectation(
                frozenset(artifact.identity.all_digests),
                basis=tuple(
                    (d.scheme, "computed") for d in artifact.identity.all_digests
                ),
            )
        )
        explicit_path = path if isinstance(path, (Path, str)) else None
        exp_identity, cmp_contract = _expectation_to_identity_and_contract(
            exp_obj,
            contract,
        )
        if exp_identity is None:
            exp_identity = artifact.identity
        if exp_identity.anchor.kind is Kind.HISTORICAL:
            return Verdict(Agreement.UNKNOWN, None, (), False)

        current_art = artifact
        # First ensure the physical file on disk matches current_art's primary anchor
        active_ledger = _resolve_active_ledger(current_art, ledger)
        store_ledger = active_ledger if isinstance(active_ledger, StoreLedger) else None
        try:
            target_path, member = _resolve_target_path_and_member(
                current_art,
                explicit_path,
                active_ledger=active_ledger,
                site_context=site_context,
                mounts=mounts,
                verify_on_stale=True,
            )
        except FileNotFoundError:
            return Verdict(Agreement.UNKNOWN, None, (), False)

        if not target_path.exists():
            return Verdict(Agreement.UNKNOWN, None, (), False)

        if explicit_path is not None:
            anchor_ok, actual_anchor, _w = verify_path_digest(
                target_path,
                current_art.identity.anchor,
                driver=current_art.driver,
                member=member,
                ledger=store_ledger,
            )
            if not anchor_ok:
                if actual_anchor is not None:
                    current_art = replace(
                        current_art,
                        identity=Identity(anchor=actual_anchor),
                    )
                else:
                    return Verdict(
                        Agreement.MISMATCH,
                        current_art.identity.anchor.scheme,
                        (),
                        True,
                    )
        else:
            # `locate(..., verify_on_stale=True)` already verified freshness / anchor
            anchor_ok = True

        verdict = compare(
            current_art.identity,
            exp_identity,
            cmp_contract,
        )
        if verdict.agreement is Agreement.UNKNOWN and verdict.needed:
            for needed_scheme in verdict.needed:
                if current_art.identity.get(needed_scheme) is None:
                    try:
                        if anchor_ok:
                            current_art, _ = ensure_digest(
                                current_art,
                                needed_scheme,
                                path=explicit_path,
                                ledger=ledger,
                                site_context=site_context,
                                mounts=mounts,
                                policy=policy,
                            )
                        else:
                            new_d, _ = compute_scheme_digest(
                                target_path,
                                needed_scheme,
                                driver=current_art.driver,
                                member=member,
                                ledger=store_ledger,
                                policy=policy,
                            )
                            current_art = replace(
                                current_art,
                                identity=current_art.identity.with_claim(new_d),
                            )
                    except (ValueError, ConsistError, OSError):
                        continue
            verdict = compare(
                current_art.identity,
                exp_identity,
                cmp_contract,
            )
        return verdict

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
    policy: Any = None,
) -> tuple[Artifact, Digest]:
    """Ensure `artifact.identity` carries a verified digest for `scheme`, computing and persisting on demand."""
    if artifact.identity.anchor.kind is Kind.HISTORICAL:
        raise ValueError("Cannot compute digests for a Kind.HISTORICAL artifact")

    canonical_scheme = SchemeRegistry.get(scheme).name
    if (
        policy is None
        and (existing := artifact.identity.get(canonical_scheme)) is not None
    ):
        return (artifact, existing)

    active_ledger = _resolve_active_ledger(artifact, ledger)
    store_ledger = active_ledger if isinstance(active_ledger, StoreLedger) else None

    if policy is None and store_ledger is not None:
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

    # When `path` is explicitly provided, verify the target path matches the
    # artifact's primary anchor before attaching a secondary digest claim.
    # (When `path is None`, `locate(..., verify_on_stale=True)` already verified freshness.)
    if path is not None:
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
        policy=policy,
    )
    if policy is None and store_ledger is not None:
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

    _remove_path(dest)

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
        dest_witness, repr_kind = _capture_path_witness(
            dest,
            is_dir=src.is_dir(),
            member=resolved.member,
            observed_at_ns=now_ns,
        )
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


def archive(
    artifact: Artifact,
    dest_path: Path | str,
    *,
    mode: Literal["copy", "move"] = "copy",
    archive_mount_name: str = "archive",
    ledger: StoreLedger | HintLedger | None = None,
    site_context: SiteContext | None = None,
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
) -> Artifact:
    """Copy or move `artifact` to `dest_path`, recording an archive observation."""
    if mode not in ("copy", "move"):
        raise ValueError(
            f"Unsupported archive mode {mode!r}; expected 'copy' or 'move'"
        )

    active_ledger = _resolve_active_ledger(artifact, ledger)
    effective_mounts = dict(_effective_mounts(active_ledger, mounts))
    resolved = locate(
        artifact,
        ledger=active_ledger,
        site_context=site_context,
        mounts=effective_mounts,
    )
    src = resolved.path.expanduser().resolve()
    raw_dest = Path(dest_path).expanduser().absolute()
    if raw_dest.exists() and raw_dest.is_dir() and not src.is_dir():
        dest = raw_dest.resolve() / src.name
    else:
        raw_dest.parent.mkdir(parents=True, exist_ok=True)
        dest = raw_dest.parent.resolve() / raw_dest.name

    if archive_mount_name not in effective_mounts:
        effective_mounts[archive_mount_name] = Mount(
            name=archive_mount_name,
            root=dest.parent,
        )

    if dest != src:
        if src.is_dir():
            if dest.exists():
                shutil.rmtree(dest)
            shutil.copytree(src, dest, copy_function=shutil.copy2)
        else:
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest)

    store_ledger = active_ledger if isinstance(active_ledger, StoreLedger) else None
    if artifact.identity.verifiable and artifact.identity.anchor.scheme in (
        "sha256:file",
        "sha256:manifest-v1",
    ):
        matched, actual_d, dest_witness = verify_path_digest(
            dest,
            artifact.identity.anchor,
            driver=artifact.driver,
            member=resolved.member,
            ledger=store_ledger,
        )
        if not matched or dest_witness is None:
            raise ValueError(
                f"Archived copy at {dest} failed digest verification against "
                f"anchor {artifact.identity.anchor} (actual={actual_d})"
            )
        repr_kind = (
            "tree"
            if src.is_dir()
            else ("member" if resolved.member is not None else "file")
        )
        now_ns = time.time_ns()
    else:
        now_ns = time.time_ns()
        dest_witness, repr_kind = _capture_path_witness(
            dest,
            is_dir=src.is_dir(),
            member=resolved.member,
            observed_at_ns=now_ns,
        )

    arch_obs = _build_path_observation(
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
    if store_ledger is not None:
        store_ledger.record_observation(arch_obs, repr_kind=repr_kind)
        if resolved.uri != arch_obs.location.uri and not src.is_dir():
            store_ledger.record_archive_copy(
                src_uri=resolved.uri,
                dst_uri=arch_obs.location.uri,
                site=arch_obs.site,
                dst_path=dest,
                exclude_anchor_member=(
                    str(artifact.identity.anchor),
                    resolved.member,
                ),
            )

    updated_hints = (arch_obs, *[h for h in artifact.hints if h != arch_obs])[:4]
    updated_artifact = replace(artifact, hints=tuple(updated_hints))

    if mode == "move" and dest != src:
        evict(
            updated_artifact,
            uri=resolved.uri,
            path=src,
            delete_files=True,
            require_surviving_copy=True,
            ledger=active_ledger,
            site_context=site_context,
            mounts=effective_mounts,
        )
        updated_artifact = replace(
            updated_artifact,
            hints=tuple(
                h for h in updated_artifact.hints if h.location.uri != resolved.uri
            ),
        )

    return updated_artifact


def _surviving_row_is_fresh_on_disk(
    row: Mapping[str, Any],
    *,
    artifact: Artifact,
    effective_mounts: Mapping[str, Mount],
    store_ledger: StoreLedger | None,
) -> bool:
    surv_path, _ = resolve_uri_to_path(
        str(row["uri"]),
        mounts=effective_mounts,
    )
    if surv_path is None or not surv_path.exists():
        return False
    witness = deserialize_witness_json(row.get("witness_json"))
    if isinstance(witness, (FileStat, DirTreeStat, MemberOf)):
        if witness.check(surv_path) is Freshness.FRESH:
            return True
    if artifact.identity.verifiable and artifact.identity.anchor.scheme in (
        "sha256:file",
        "sha256:manifest-v1",
    ):
        matched, _actual, _w = verify_path_digest(
            surv_path,
            artifact.identity.anchor,
            driver=artifact.driver,
            member=row.get("member"),
            ledger=store_ledger,
        )
        return matched
    return witness is None


def evict(
    artifact: Artifact,
    *,
    path: Path | str | None = None,
    uri: str | None = None,
    delete_files: bool = True,
    require_surviving_copy: bool = True,
    ledger: StoreLedger | HintLedger | None = None,
    site_context: SiteContext | None = None,
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
) -> list[uuid.UUID]:
    """Evict a physical copy of `artifact`, optionally requiring at least one surviving copy."""
    active_ledger = _resolve_active_ledger(artifact, ledger)
    store_ledger = active_ledger if isinstance(active_ledger, StoreLedger) else None
    effective_mounts = _effective_mounts(active_ledger, mounts)
    effective_site = site_context.site if site_context is not None else None
    target_path = Path(path).expanduser().resolve() if path is not None else None
    target_uri = uri
    if target_uri is None and target_path is not None:
        v_uri, _ = virtualize_path(target_path, mounts=effective_mounts)
        target_uri = v_uri

    rows = active_ledger.locate_current(
        [str(artifact.identity.anchor)],
        site=effective_site,
        include_evicted=False,
    )
    matching_rows: list[dict[str, Any]] = []
    surviving_rows: list[dict[str, Any]] = []
    for row in rows:
        row_uri = str(row["uri"])
        row_path, _ = resolve_uri_to_path(row_uri, mounts=effective_mounts)
        is_match = False
        if target_uri is not None and row_uri == target_uri:
            is_match = True
        elif (
            target_path is not None
            and row_path is not None
            and row_path.resolve() == target_path
        ):
            is_match = True
        elif target_uri is None and target_path is None:
            is_match = True

        if is_match:
            matching_rows.append(row)
        else:
            surviving_rows.append(row)

    if require_surviving_copy:
        has_verified_survivor = any(
            _surviving_row_is_fresh_on_disk(
                s_row,
                artifact=artifact,
                effective_mounts=effective_mounts,
                store_ledger=store_ledger,
            )
            for s_row in surviving_rows
        )
        if not has_verified_survivor:
            raise ConsistError(
                f"Refusing to evict last active copy of artifact {artifact.key!r} "
                f"(anchor={artifact.identity.anchor})"
            )

    evicted_ids: list[uuid.UUID] = []
    for row in matching_rows:
        raw_obs_id = row.get("observation_id")
        if raw_obs_id is not None and store_ledger is not None:
            obs_uuid = (
                raw_obs_id
                if isinstance(raw_obs_id, uuid.UUID)
                else uuid.UUID(str(raw_obs_id))
            )
            store_ledger.record_location_event(
                observation_id=obs_uuid,
                status="evicted",
            )
            evicted_ids.append(obs_uuid)
        if (
            delete_files
            and row.get("member") is None
            and (
                artifact.primary_location is None
                or artifact.primary_location.member is None
            )
        ):
            row_uri_str = str(row["uri"])
            row_site_str = str(row.get("site") or "local")
            shared_container = False
            if store_ledger is not None:
                remaining_for_uri = store_ledger.query_location_current(
                    site=row_site_str,
                    uri=row_uri_str,
                    include_evicted=False,
                )
                if remaining_for_uri:
                    shared_container = True
            if not shared_container:
                row_path, _ = resolve_uri_to_path(
                    row_uri_str,
                    mounts=effective_mounts,
                )
                if row_path is not None and row_path.exists():
                    _remove_path(row_path)

    return evicted_ids


__all__ = [
    "Contract",
    "PendingOutput",
    "ResolvedLocation",
    "archive",
    "compute_scheme_digest",
    "ensure_digest",
    "evict",
    "locate",
    "observe",
    "resolve_uri_to_path",
    "seal",
    "stage",
    "verify",
    "verify_path_digest",
    "virtualize_path",
]
