"""
Consist v2 core engine functions: `locate()`, `verify()`, `ensure_digest()`,
`stage()`, `PendingOutput`, and high-throughput `seal()`.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from contextlib import nullcontext
import ctypes
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
import errno
import hashlib
import importlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import sys
import time
from typing import TYPE_CHECKING, Any, Literal, TypeGuard, TypeVar
from urllib.parse import unquote, urlparse
import uuid

from consist.artifact import Artifact, HintLedger, MissingOutputError
from consist.digest import (
    Agreement,
    Contract,
    Digest,
    DigestPolicy,
    Expectation,
    Identity,
    Kind,
    SchemeRegistry,
    Verdict,
    build_manifest_v1,
    canonical_json_str,
    compare,
    compute_ctd1_digest,
    compute_logical_matrix_digest,
    compute_omx_logical_matrices_sha256,
    compute_zarr_logical_matrices_sha256,
    open_h5py_read_only,
    resolve_canonicalization_policy,
)
from consist.location import (
    _RACY_WINDOW_NS,
    DirTreeStat,
    FileStat,
    Freshness,
    Location,
    MemberOf,
    Mount,
    Observation,
    SiteContext,
    Witness,
    deserialize_witness_json,
    infer_path_driver,
    lookup_live_ledger,
    normalize_mounts as _normalize_mounts,
)
from consist.meta import EMPTY_META, FrozenMeta
from consist.store.detect import ConsistError

if TYPE_CHECKING:
    from consist.store.ledger import StoreLedger

_STREAM_CHUNK_BYTES: int = 1024 * 1024  # 1 MiB
_default_mounts: dict[str, Mount] = {}
_T = TypeVar("_T")
_MEMBER_SCOPED_SCHEMES: frozenset[str] = frozenset(
    {
        "ctd1:sha256",
        "sha256:logical_matrix:v1",
        "sha256:logical_matrices:v1",
    }
)
_VERIFIABLE_SURVIVOR_SCHEMES: frozenset[str] = frozenset(
    {
        "sha256:file",
        "blake3:file",
        "sha256:manifest-v1",
        "ctd1:sha256",
        "sha256:logical_matrix:v1",
        "sha256:logical_matrices:v1",
    }
)


def _is_store_ledger(obj: object) -> TypeGuard[StoreLedger]:
    if obj is None or isinstance(obj, HintLedger):
        return False
    from consist.store.ledger import StoreLedger

    return isinstance(obj, StoreLedger)


@dataclass(frozen=True, slots=True)
class ResolvedLocation:
    """Verified local filesystem location resolved by `locate()`."""

    path: Path
    uri: str
    member: str | None = None
    observation_id: uuid.UUID | None = None
    freshness: Freshness = Freshness.FRESH
    witness: Witness | None = None
    site: str | None = None
    scope_key: str = ""
    mount_instance_id: str = ""


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


def _reject_dotdot_segments(raw_path: str, uri: str) -> None:
    segments = [seg for seg in raw_path.replace("\\", "/").split("/") if seg]
    if ".." in segments:
        raise ValueError(f"Path traversal '..' is not allowed in URI: {uri!r}")


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
        decoded_path = unquote(parsed.path)
        _reject_dotdot_segments(decoded_path, raw)
        local_path = Path(decoded_path).expanduser().resolve()
        _, matched_mount = virtualize_path(local_path, mount_map)
        return (local_path, matched_mount)

    if "://" in raw:
        scheme, rel_part = raw.split("://", 1)
        decoded_rel = unquote(rel_part)
        _reject_dotdot_segments(decoded_rel, raw)
        mount = mount_map.get(scheme)
        if mount is None:
            return (None, None)
        clean_rel = decoded_rel.replace("\\", "/").lstrip("/")
        base_root = mount.root.expanduser().resolve()
        if not clean_rel:
            return (base_root, mount)
        cur = base_root
        for part in PurePosixPath(clean_rel).parts:
            if part in ("", "."):
                continue
            if part == "..":
                raise ValueError(f"Path traversal '..' is not allowed in URI: {raw!r}")
            cur = cur / part
            if cur.is_symlink():
                link_target = cur.resolve()
                if not link_target.is_relative_to(base_root):
                    raise ValueError(
                        f"Symlink component {cur} in URI {raw!r} escapes mount root {base_root}"
                    )
        candidate = cur.resolve()
        if not candidate.is_relative_to(base_root):
            raise ValueError(f"Resolved URI {raw!r} escapes mount root {base_root}")
        return (candidate, mount)

    _reject_dotdot_segments(raw, raw)
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
    ledger_mounts = active_ledger.mounts if _is_store_ledger(active_ledger) else {}
    return {**_default_mounts, **ledger_mounts, **_normalize_mounts(mounts)}


def _stat_tuple(st: FileStat) -> tuple[int, int, int | None, int | None]:
    return (st.size, st.mtime_ns, st.ino, st.ctime_ns)


def _run_fd_pinned(
    path: Path,
    fn: Callable[[int], _T],
) -> tuple[_T, FileStat]:
    """Execute `fn(fd)` on an open file descriptor and verify `fstat(fd)` and path inode did not change."""
    fd = os.open(path, os.O_RDONLY)
    try:
        raw_before = os.fstat(fd)
        stat_before = FileStat.from_os_stat(
            raw_before,
            observed_at_ns=time.time_ns(),
        )
        result = fn(fd)
        raw_after = os.fstat(fd)
        stat_after = FileStat.from_os_stat(
            raw_after,
            observed_at_ns=time.time_ns(),
        )
        try:
            path_stat_after = os.stat(path)
        except OSError as exc:
            raise ConsistError(
                f"Concurrent mid-read mutation detected while hashing {path}"
            ) from exc
    finally:
        os.close(fd)

    if _stat_tuple(stat_before) != _stat_tuple(stat_after) or (
        path_stat_after.st_dev,
        path_stat_after.st_ino,
    ) != (raw_before.st_dev, raw_before.st_ino):
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


def _remove_path(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink(missing_ok=True)
    elif os.path.lexists(path):
        shutil.rmtree(path)


def _atomic_replace_path(tmp_src: Path, dest: Path) -> None:
    """Atomically publish `tmp_src` onto `dest`, protecting symlinks and existing directories."""
    if not os.path.lexists(dest):
        os.replace(tmp_src, dest)
        return

    dest_is_real_dir = dest.is_dir() and not dest.is_symlink()
    tmp_is_real_dir = tmp_src.is_dir() and not tmp_src.is_symlink()
    if not dest_is_real_dir and not tmp_is_real_dir:
        os.replace(tmp_src, dest)
        return

    backup_path = dest.parent / f".consist-old-{uuid.uuid4().hex}"
    os.replace(dest, backup_path)
    try:
        os.replace(tmp_src, dest)
    except BaseException:
        try:
            if not os.path.lexists(dest) and os.path.lexists(backup_path):
                os.replace(backup_path, dest)
        except OSError:
            pass
        raise
    _remove_path(backup_path)


def _tee_hash_and_copy_file(
    src_path: Path,
    dst_path: Path,
    *,
    expected_digest: Digest | None = None,
) -> tuple[Digest, FileStat, FileStat]:
    """Stream `src_path` once into `hashlib.sha256()` and sibling `.consist-tmp-<uuid4>`, publishing via `_atomic_replace_path`."""
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_dst = dst_path.parent / f".consist-tmp-{uuid.uuid4().hex}"
    try:
        src_fd = os.open(src_path, os.O_RDONLY)
        try:
            raw_before = os.fstat(src_fd)
            stat_before = FileStat.from_os_stat(
                raw_before,
                observed_at_ns=time.time_ns(),
            )
            hasher = hashlib.sha256()
            with tmp_dst.open("wb") as dst_f:
                while chunk := os.read(src_fd, _STREAM_CHUNK_BYTES):
                    hasher.update(chunk)
                    dst_f.write(chunk)
                dst_f.flush()
                os.fsync(dst_f.fileno())
            raw_after = os.fstat(src_fd)
            stat_after = FileStat.from_os_stat(
                raw_after,
                observed_at_ns=time.time_ns(),
            )
            try:
                path_stat_after = os.stat(src_path)
            except OSError as exc:
                raise ConsistError(
                    f"Concurrent mid-read mutation detected while hashing {src_path}"
                ) from exc
        finally:
            os.close(src_fd)

        if _stat_tuple(stat_before) != _stat_tuple(stat_after) or (
            path_stat_after.st_dev,
            path_stat_after.st_ino,
        ) != (raw_before.st_dev, raw_before.st_ino):
            raise ConsistError(
                f"Concurrent mid-read mutation detected while hashing {src_path}"
            )

        computed = Digest("sha256:file", hasher.hexdigest())
        if expected_digest is not None and computed != expected_digest:
            raise ValueError(
                f"Archived copy at {dst_path} failed digest verification against "
                f"anchor {expected_digest} (actual={computed})"
            )

        try:
            os.utime(tmp_dst, ns=(stat_after.mtime_ns, stat_after.mtime_ns))
        except OSError:
            pass
        _atomic_replace_path(tmp_dst, dst_path)
    except BaseException:
        _remove_path(tmp_dst)
        raise

    dst_stat = FileStat.capture(dst_path, observed_at_ns=time.time_ns())
    return (computed, stat_after, dst_stat)


def _compute_manifest_v1_and_witness(
    root: Path,
    *,
    max_workers: int = 4,
    policy: DigestPolicy | None = None,
    executor: Executor | None = None,
) -> tuple[Digest, bytes, int, int, DirTreeStat]:
    """Build `sha256:manifest-v1` (or `+p:<policy_sha256>`) + `DirTreeStat` using `build_manifest_v1`."""

    def _hash_pinned(fpath: Path) -> tuple[str, int]:
        d, fstat = _hash_file_descriptor_pinned(fpath)
        return (d.hex, fstat.size)

    manifest = build_manifest_v1(
        root,
        max_workers=max_workers,
        file_hasher=_hash_pinned,
        policy=policy,
        executor=executor,
    )
    entries = manifest["entries"]
    payload_dict: dict[str, Any] = {"version": 1, "entries": entries}
    if policy is not None:
        payload_dict["policy_sha256"] = policy.policy_sha256
    canonical_bytes = canonical_json_str(payload_dict).encode("utf-8")
    dir_witness = DirTreeStat.capture(root)
    scheme = policy.scheme if policy is not None else "sha256:manifest-v1"
    return (
        Digest(scheme, manifest["tree_hash"]),
        canonical_bytes,
        len(entries),
        dir_witness.total_bytes,
        dir_witness,
    )


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
    int,
    int,
    int,
    tuple[tuple[str, tuple[int, int, int | None, int | None]], ...],
    tuple[tuple[str, int], ...],
]:
    return (
        st.file_count,
        st.total_bytes,
        st.max_mtime_ns,
        tuple((rel, _stat_tuple(fs)) for rel, fs in st.entries),
        st.subdirs,
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


def _load_stored_digest_policy(
    ledger: StoreLedger | None,
    policy_sha256: str,
) -> DigestPolicy | None:
    if ledger is None:
        return None
    with ledger.exec_cursor() as cur:
        row = cur.execute(
            "SELECT policy_json FROM main.digest_policy WHERE policy_sha256 = ?",
            [policy_sha256],
        ).fetchone()
    if row is None or row[0] is None:
        return None
    raw = json.loads(str(row[0])) if isinstance(row[0], str) else dict(row[0])
    return DigestPolicy(
        include=tuple(raw.get("include") or ()),
        exclude=tuple(raw.get("exclude") or ()),
    )


def _record_digest_policy_and_manifest(
    ledger: StoreLedger,
    *,
    digest: Digest,
    entry_count: int,
    total_bytes: int,
    body_bytes: bytes,
    manifest_policy: DigestPolicy | None,
) -> None:
    if ledger.read_only:
        return
    policy_sha = manifest_policy.policy_sha256 if manifest_policy is not None else None
    if manifest_policy is not None:
        with ledger.exec_cursor() as cur:
            cur.execute(
                "INSERT INTO main.digest_policy "
                "(policy_sha256, policy_json, created_at) "
                "VALUES (?, ?, ?) ON CONFLICT (policy_sha256) DO NOTHING",
                [
                    policy_sha,
                    manifest_policy.policy_json,
                    datetime.now(timezone.utc),
                ],
            )
    ledger.record_content(digest, "tree")
    with ledger.exec_cursor() as cur:
        cur.execute(
            "INSERT INTO main.content_manifest "
            "(anchor, format, policy_sha256, entry_count, total_bytes, body) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (anchor) DO UPDATE SET "
            "policy_sha256 = EXCLUDED.policy_sha256, "
            "entry_count = EXCLUDED.entry_count, "
            "total_bytes = EXCLUDED.total_bytes, "
            "body = EXCLUDED.body",
            [
                str(digest),
                "manifest-v1",
                policy_sha,
                int(entry_count),
                int(total_bytes),
                bytes(body_bytes),
            ],
        )


def compute_scheme_digest(
    path: Path,
    scheme: str,
    *,
    driver: str = "file",
    member: str | None = None,
    ledger: StoreLedger | None = None,
    max_workers: int = 4,
    policy: Any = "preserve_canonical",
    executor: Executor | None = None,
) -> tuple[Digest, Witness]:
    """Compute a descriptor-pinned `(Digest, Witness)` for `path` under `scheme`."""
    canonical_scheme = SchemeRegistry.get(scheme).name
    if member is not None and canonical_scheme not in _MEMBER_SCOPED_SCHEMES:
        raise ValueError(
            f"Scheme {canonical_scheme!r} cannot be used for sub-container member "
            f"{member!r}; expected one of {sorted(_MEMBER_SCOPED_SCHEMES)}"
        )

    if canonical_scheme == "sha256:file":
        digest, fstat = _hash_file_descriptor_pinned(path)
        return (digest, fstat)

    if canonical_scheme == "blake3:file":
        try:
            blake3_mod = importlib.import_module("blake3")
        except ImportError as exc:
            raise ValueError(
                "Optional dependency 'blake3' is required to compute 'blake3:file' digests"
            ) from exc

        def _digest_blake3_fd(fd: int) -> Digest:
            hasher = blake3_mod.blake3()
            while chunk := os.read(fd, _STREAM_CHUNK_BYTES):
                hasher.update(chunk)
            return Digest("blake3:file", hasher.hexdigest())

        return _run_fd_pinned(path, _digest_blake3_fd)

    if canonical_scheme == "sha256:manifest-v1" or canonical_scheme.startswith(
        "sha256:manifest-v1+p:"
    ):
        if (
            canonical_scheme == "sha256:manifest-v1"
            and driver.strip().lower() == "shapefile"
            and path.is_file()
        ):
            from consist.core.directory_artifacts import build_shapefile_bundle_manifest

            shp_manifest = build_shapefile_bundle_manifest(path)
            shp_digest = Digest("sha256:manifest-v1", str(shp_manifest["tree_hash"]))
            _, stat_after = _run_fd_pinned(path, lambda _fd: shp_digest)
            return (shp_digest, stat_after)

        manifest_policy: DigestPolicy | None = (
            policy if isinstance(policy, DigestPolicy) else None
        )
        if canonical_scheme.startswith("sha256:manifest-v1+p:"):
            expected_sha = canonical_scheme.split("+p:", 1)[1]
            if manifest_policy is None:
                manifest_policy = _load_stored_digest_policy(ledger, expected_sha)
            if manifest_policy is None or manifest_policy.policy_sha256 != expected_sha:
                raise ValueError(
                    f"DigestPolicy with policy_sha256={expected_sha!r} is required "
                    f"to compute {canonical_scheme!r}"
                )
        digest, body_bytes, entry_count, total_bytes, dir_stat = (
            _compute_manifest_v1_and_witness(
                path,
                max_workers=max_workers,
                policy=manifest_policy,
                executor=executor,
            )
        )
        if ledger is not None and not ledger.read_only:
            _record_digest_policy_and_manifest(
                ledger,
                digest=digest,
                entry_count=entry_count,
                total_bytes=total_bytes,
                body_bytes=body_bytes,
                manifest_policy=manifest_policy,
            )
        return (digest, dir_stat)

    effective_policy = (
        policy
        if (policy is not None and not isinstance(policy, DigestPolicy))
        else "preserve_canonical"
    )

    if canonical_scheme == "ctd1:sha256":
        digest, stat_after = _run_fd_pinned(
            path,
            lambda _fd: compute_ctd1_digest(path, member=member),
        )
        witness: Witness = (
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

        h5_policy = resolve_canonicalization_policy(effective_policy)
        if member is not None and member.strip("/"):
            h5_policy = replace(h5_policy, h5_data_group=f"/{member.strip('/')}")
        digest, stat_after = _run_fd_pinned(
            path,
            lambda _fd: Digest(
                "sha256:logical_matrices:v1",
                compute_omx_logical_matrices_sha256(
                    path,
                    policy=h5_policy,
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


def _non_racy_file_stat(fs: FileStat, verify_started_at_ns: int) -> FileStat:
    is_racy = (verify_started_at_ns - fs.mtime_ns) < _RACY_WINDOW_NS
    return replace(fs, racy=is_racy) if fs.racy != is_racy else fs


def _non_racy_witness(
    witness: Witness | None,
    verify_started_at_ns: int | None = None,
) -> Witness | None:
    """Update `racy` on a verified witness: clear `racy=False` only when `(verify_started_at_ns - mtime_ns) >= _RACY_WINDOW_NS`."""
    ref_ns = time.time_ns() if verify_started_at_ns is None else verify_started_at_ns
    if isinstance(witness, FileStat):
        return _non_racy_file_stat(witness, ref_ns)
    if isinstance(witness, MemberOf):
        new_parent = _non_racy_file_stat(witness.parent, ref_ns)
        return (
            replace(witness, parent=new_parent)
            if new_parent != witness.parent
            else witness
        )
    if isinstance(witness, DirTreeStat):
        new_entries = tuple(
            (rel, _non_racy_file_stat(fs, ref_ns)) for rel, fs in witness.entries
        )
        subdir_racy = any((ref_ns - sm) < _RACY_WINDOW_NS for _, sm in witness.subdirs)
        tree_racy = (
            (ref_ns - witness.max_mtime_ns) < _RACY_WINDOW_NS
            or subdir_racy
            or any(fs.racy for _, fs in new_entries)
        )
        if new_entries == witness.entries and witness.racy == tree_racy:
            return witness
        return replace(
            witness,
            entries=new_entries,
            racy=tree_racy,
        )
    return witness


def _resolve_active_ledger(
    artifact: Artifact,
    explicit_ledger: StoreLedger | HintLedger | None,
) -> StoreLedger | HintLedger:
    if explicit_ledger is not None and not (
        _is_store_ledger(explicit_ledger)
        and explicit_ledger._closed
        and not explicit_ledger._suspended_needs_reopen
    ):
        return explicit_ledger
    live = lookup_live_ledger(artifact.origin)
    if isinstance(live, HintLedger) or _is_store_ledger(live):
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
    default_site: str = "local",
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
    observed_at_ns: int | None = None,
) -> Observation:
    uri, matched_mount = virtualize_path(path, mounts)
    site = site_context.site if site_context is not None else default_site
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
    memo_site = site_context.site if site_context is not None else "local"

    # Fast-path: check memoized ResolvedLocation when no custom mounts override resolution
    if not mounts:
        hint_members: list[str | None] = []
        for h_obs in artifact.hints:
            if h_obs.location.member not in hint_members:
                hint_members.append(h_obs.location.member)
        if not hint_members:
            hint_members.append(None)
        for hint_member in hint_members:
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

    if _is_store_ledger(active_ledger) and primary_anchor.kind is Kind.HISTORICAL:
        promo = active_ledger.get_promotion(primary_anchor)
        if promo is not None:
            ver_anchor, _basis, _ev = promo
            promoted_verified_anchor = ver_anchor
            if ver_anchor not in anchors_to_search:
                anchors_to_search.append(ver_anchor)
    elif primary_anchor.kind is Kind.HISTORICAL and artifact.hints:
        for hint_obs in artifact.hints:
            if hint_obs.identity.anchor not in anchors_to_search:
                anchors_to_search.append(hint_obs.identity.anchor)

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

        row_site = str(row.get("site") or "local")
        row_scope_key = (
            str(row["scope_key"]) if row.get("scope_key") is not None else ""
        )
        row_mount_inst = (
            str(row["mount_instance_id"])
            if row.get("mount_instance_id") is not None
            else ""
        )
        effective_site = site_context.site if site_context is not None else row_site
        if freshness is Freshness.FRESH:
            resolved_loc = ResolvedLocation(
                path=local_path,
                uri=uri,
                member=cand_member,
                observation_id=row.get("observation_id"),
                freshness=Freshness.FRESH,
                witness=witness,
                site=row_site,
                scope_key=row_scope_key,
                mount_instance_id=row_mount_inst,
            )
            active_ledger.memoize_location(
                primary_anchor,
                resolved_loc,
                member=cand_member,
                site=memo_site,
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
                site=row_site,
                scope_key=row_scope_key,
                mount_instance_id=row_mount_inst,
            )

        verify_started_at_ns = time.time_ns()
        try:
            matched, _actual, new_witness = verify_path_digest(
                local_path,
                expected_digest,
                driver=artifact.driver,
                member=cand_member,
                ledger=active_ledger if _is_store_ledger(active_ledger) else None,
            )
        except ConsistError:
            raise
        except Exception:
            matched = False
            new_witness = None

        if not matched:
            continue

        refreshed_witness = _non_racy_witness(new_witness, verify_started_at_ns)
        new_obs_id = row.get("observation_id")
        res_site = row_site
        res_scope_key = row_scope_key
        res_mount_inst = row_mount_inst
        if _is_store_ledger(active_ledger):
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
            eff_scope_opt = (
                site_context.scope_key_for(matched_mount)
                if (site_context is not None and matched_mount is not None)
                else (
                    str(row["scope_key"]) if row.get("scope_key") is not None else None
                )
            )
            eff_mount_opt = (
                matched_mount.resolve_instance_id()
                if matched_mount is not None
                else (
                    str(row["mount_instance_id"])
                    if row.get("mount_instance_id") is not None
                    else None
                )
            )
            refreshed_obs = Observation(
                location=Location(uri=uri, member=cand_member),
                identity=obs_identity,
                access_driver=str(row.get("access_driver") or artifact.driver),
                witness=refreshed_witness,
                basis="computed",
                site=effective_site,
                scope_key=eff_scope_opt,
                mount_instance_id=eff_mount_opt,
                observed_at_ns=time.time_ns(),
            )
            new_obs_id = active_ledger.record_observation(
                refreshed_obs,
                repr_kind=repr_kind,
            )
            res_site = effective_site
            res_scope_key = eff_scope_opt or ""
            res_mount_inst = eff_mount_opt or ""
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
            site=res_site,
            scope_key=res_scope_key,
            mount_instance_id=res_mount_inst,
        )
        active_ledger.memoize_location(
            primary_anchor,
            resolved_loc,
            member=cand_member,
            site=memo_site,
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
        effective_mounts = _effective_mounts(active_ledger, mounts)
        for h_obs in artifact.hints:
            try:
                h_path, _ = resolve_uri_to_path(h_obs.location.uri, effective_mounts)
            except ValueError:
                continue
            if h_path is not None and h_path == target_path:
                return (target_path, h_obs.location.member)
        return (target_path, artifact.member)
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
        store_ledger = active_ledger if _is_store_ledger(active_ledger) else None
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
    store_ledger = active_ledger if _is_store_ledger(active_ledger) else None

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
    if isinstance(policy, DigestPolicy):
        if canonical_scheme == "sha256:manifest-v1":
            effective_scheme = policy.scheme
        elif (
            canonical_scheme.startswith("sha256:manifest-v1+p:")
            and canonical_scheme != policy.scheme
        ):
            raise ValueError(
                f"Scheme {canonical_scheme!r} conflicts with DigestPolicy "
                f"policy_sha256={policy.policy_sha256!r}"
            )
        else:
            effective_scheme = canonical_scheme
    else:
        effective_scheme = canonical_scheme

    if artifact.member is not None and effective_scheme not in _MEMBER_SCOPED_SCHEMES:
        raise ValueError(
            f"Scheme {effective_scheme!r} cannot be used for sub-container member "
            f"{artifact.member!r}; expected one of {sorted(_MEMBER_SCOPED_SCHEMES)}"
        )

    active_ledger = _resolve_active_ledger(artifact, ledger)
    store_ledger = active_ledger if _is_store_ledger(active_ledger) else None

    is_manifest_scheme = (
        effective_scheme == "sha256:manifest-v1"
        or effective_scheme.startswith("sha256:manifest-v1+p:")
    )
    expected_policy_sha: str | None = (
        policy.policy_sha256
        if isinstance(policy, DigestPolicy)
        else (
            effective_scheme.split("+p:", 1)[1]
            if effective_scheme.startswith("sha256:manifest-v1+p:")
            else None
        )
    )

    def _manifest_claim_policy_matches(claim: Digest) -> bool:
        if not is_manifest_scheme:
            return policy is None
        if store_ledger is None:
            return True
        with store_ledger.exec_cursor() as cur:
            row = cur.execute(
                "SELECT policy_sha256 FROM main.content_manifest WHERE anchor = ?",
                [str(claim)],
            ).fetchone()
        if row is None:
            return expected_policy_sha is None
        stored_sha = str(row[0]) if row[0] is not None else None
        return stored_sha == expected_policy_sha

    if (
        existing := artifact.identity.get(effective_scheme)
    ) is not None and _manifest_claim_policy_matches(existing):
        return (artifact, existing)

    if store_ledger is not None and (
        policy is None or isinstance(policy, DigestPolicy)
    ):
        stored_identity = store_ledger.get_identity(artifact.identity.anchor)
        if (
            stored_claim := stored_identity.get(effective_scheme)
        ) is not None and _manifest_claim_policy_matches(stored_claim):
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
                f"Cannot compute {effective_scheme!r} for artifact {artifact.key!r}: "
                f"bytes at {target_path} no longer match anchor {artifact.identity.anchor}"
            )

    new_digest, _witness = compute_scheme_digest(
        target_path,
        effective_scheme,
        driver=artifact.driver,
        member=member,
        ledger=store_ledger,
        policy=policy,
    )
    if (
        (policy is None or isinstance(policy, DigestPolicy))
        and store_ledger is not None
        and not store_ledger.read_only
        and (
            new_digest.scheme != artifact.identity.anchor.scheme
            or new_digest.hex == artifact.identity.anchor.hex
        )
    ):
        store_ledger.record_content_digest(
            artifact.identity.anchor,
            new_digest,
            basis="computed",
        )
    if (
        new_digest.scheme == artifact.identity.anchor.scheme
        and new_digest.hex != artifact.identity.anchor.hex
    ):
        updated_identity = Identity(
            anchor=new_digest,
            claims=tuple(
                c for c in artifact.identity.claims if c.scheme != new_digest.scheme
            ),
        )
    else:
        updated_identity = artifact.identity.with_claim(new_digest)
    updated_artifact = replace(
        artifact,
        identity=updated_identity,
    )
    return (updated_artifact, new_digest)


def _try_reflink_clone(src: Path, dst: Path) -> bool:
    """Attempt a single-entry copy-on-write clone (`clonefile` on macOS, `FICLONE` on Linux regular files)."""
    if sys.platform == "darwin":
        try:
            libc = ctypes.CDLL(None, use_errno=True)
            clonefile_fn = libc["clonefile"]
            clonefile_fn.argtypes = [
                ctypes.c_char_p,
                ctypes.c_char_p,
                ctypes.c_uint32,
            ]
            clonefile_fn.restype = ctypes.c_int
            rc = clonefile_fn(os.fsencode(src), os.fsencode(dst), 0)
            if rc == 0:
                return True
        except (OSError, KeyError):
            pass
        _remove_path(dst)
        return False

    if sys.platform.startswith("linux"):
        if src.is_dir():
            return False
        fcntl_mod = importlib.import_module("fcntl")
        ficlone_op = 0x40049409
        try:
            s_fd = os.open(src, os.O_RDONLY)
            try:
                s_st = os.fstat(s_fd)
                d_fd = os.open(
                    dst,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    s_st.st_mode & 0o777,
                )
                try:
                    fcntl_mod.ioctl(d_fd, ficlone_op, s_fd)
                finally:
                    os.close(d_fd)
            finally:
                os.close(s_fd)
            shutil.copystat(src, dst, follow_symlinks=False)
            return True
        except OSError:
            _remove_path(dst)
            return False

    return False


def _reflink_or_raise(
    src: str | os.PathLike[str],
    dst: str | os.PathLike[str],
) -> str:
    if not _try_reflink_clone(Path(src), Path(dst)):
        raise OSError(
            errno.ENOTSUP,
            f"Copy-on-write reflink clone is not supported from {src} to {dst}",
        )
    return os.fsdecode(dst)


def _reflink_or_copy2(
    src: str | os.PathLike[str],
    dst: str | os.PathLike[str],
) -> str:
    if _try_reflink_clone(Path(src), Path(dst)):
        return os.fsdecode(dst)
    return os.fsdecode(shutil.copy2(src, dst))


def iter_artifact_candidates(
    artifact: Artifact,
    *,
    ledger: StoreLedger | HintLedger | None = None,
    site_context: SiteContext | None = None,
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
) -> Iterator[tuple[Path, str | None, Artifact, bool]]:
    """Yield `(local_path, member, candidate_artifact, is_verified)` for each visible candidate."""
    active_ledger = _resolve_active_ledger(artifact, ledger)
    effective_mounts = _effective_mounts(active_ledger, mounts)
    primary_anchor = artifact.identity.anchor
    memo_site = site_context.site if site_context is not None else "local"

    anchors_to_search: list[Digest] = [primary_anchor]
    promoted_verified_anchor: Digest | None = None

    if _is_store_ledger(active_ledger) and primary_anchor.kind is Kind.HISTORICAL:
        promo = active_ledger.get_promotion(primary_anchor)
        if promo is not None:
            ver_anchor, _basis, _ev = promo
            promoted_verified_anchor = ver_anchor
            if ver_anchor not in anchors_to_search:
                anchors_to_search.append(ver_anchor)
    elif primary_anchor.kind is Kind.HISTORICAL and artifact.hints:
        for hint_obs in artifact.hints:
            if hint_obs.identity.anchor not in anchors_to_search:
                anchors_to_search.append(hint_obs.identity.anchor)

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

    def _enrich_from_ledger(base_art: Artifact, anchor_for_id: Digest) -> Artifact:
        if not _is_store_ledger(active_ledger):
            return base_art
        stored_id = active_ledger.get_identity(anchor_for_id)
        merged_id = base_art.identity
        if merged_id.anchor.kind is Kind.HISTORICAL and stored_id.verifiable:
            merged_id = stored_id
        else:
            for d in stored_id.claims:
                if merged_id.get(d.scheme) is None:
                    merged_id = merged_id.with_claim(d)
        return (
            replace(base_art, identity=merged_id)
            if merged_id != base_art.identity
            else base_art
        )

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

        row_site = str(row.get("site") or "local")
        row_scope_key = (
            str(row["scope_key"]) if row.get("scope_key") is not None else ""
        )
        row_mount_inst = (
            str(row["mount_instance_id"])
            if row.get("mount_instance_id") is not None
            else ""
        )
        effective_site = site_context.site if site_context is not None else row_site
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

        if freshness is Freshness.FRESH and expected_digest is not None:
            resolved_loc = ResolvedLocation(
                path=local_path,
                uri=uri,
                member=cand_member,
                observation_id=row.get("observation_id"),
                freshness=Freshness.FRESH,
                witness=witness,
                site=row_site,
                scope_key=row_scope_key,
                mount_instance_id=row_mount_inst,
            )
            active_ledger.memoize_location(
                primary_anchor,
                resolved_loc,
                member=cand_member,
                site=memo_site,
            )
            yield (
                local_path,
                cand_member,
                _enrich_from_ledger(artifact, expected_digest),
                True,
            )
            continue

        if expected_digest is None:
            yield (local_path, cand_member, artifact, False)
            continue

        verify_started_at_ns = time.time_ns()
        actual_digest: Digest | None = None
        try:
            matched, actual_digest, new_witness = verify_path_digest(
                local_path,
                expected_digest,
                driver=artifact.driver,
                member=cand_member,
                ledger=active_ledger if _is_store_ledger(active_ledger) else None,
            )
        except ConsistError:
            raise
        except Exception:
            matched = False
            new_witness = None

        if matched:
            refreshed_witness = _non_racy_witness(new_witness, verify_started_at_ns)
            new_obs_id = row.get("observation_id")
            res_site = row_site
            res_scope_key = row_scope_key
            res_mount_inst = row_mount_inst
            if _is_store_ledger(active_ledger):
                obs_anchor = (
                    primary_anchor
                    if primary_anchor.kind is not Kind.HISTORICAL
                    else expected_digest
                )
                obs_identity = active_ledger.get_identity(obs_anchor)
                repr_kind = (
                    "member"
                    if cand_member is not None
                    else (
                        "tree" if isinstance(refreshed_witness, DirTreeStat) else "file"
                    )
                )
                eff_scope_opt = (
                    site_context.scope_key_for(matched_mount)
                    if (site_context is not None and matched_mount is not None)
                    else (
                        str(row["scope_key"])
                        if row.get("scope_key") is not None
                        else None
                    )
                )
                eff_mount_opt = (
                    matched_mount.resolve_instance_id()
                    if matched_mount is not None
                    else (
                        str(row["mount_instance_id"])
                        if row.get("mount_instance_id") is not None
                        else None
                    )
                )
                refreshed_obs = Observation(
                    location=Location(uri=uri, member=cand_member),
                    identity=obs_identity,
                    access_driver=str(row.get("access_driver") or artifact.driver),
                    witness=refreshed_witness,
                    basis="computed",
                    site=effective_site,
                    scope_key=eff_scope_opt,
                    mount_instance_id=eff_mount_opt,
                    observed_at_ns=time.time_ns(),
                )
                new_obs_id = active_ledger.record_observation(
                    refreshed_obs,
                    repr_kind=repr_kind,
                )
                res_site = effective_site
                res_scope_key = eff_scope_opt or ""
                res_mount_inst = eff_mount_opt or ""
            resolved_loc = ResolvedLocation(
                path=local_path,
                uri=uri,
                member=cand_member,
                observation_id=new_obs_id,
                freshness=Freshness.FRESH,
                witness=refreshed_witness,
                site=res_site,
                scope_key=res_scope_key,
                mount_instance_id=res_mount_inst,
            )
            active_ledger.memoize_location(
                primary_anchor,
                resolved_loc,
                member=cand_member,
                site=memo_site,
            )
            yield (
                local_path,
                cand_member,
                _enrich_from_ledger(artifact, expected_digest),
                True,
            )
        elif actual_digest is not None:
            yield (
                local_path,
                cand_member,
                replace(artifact, identity=Identity(anchor=actual_digest)),
                True,
            )
        else:
            yield (local_path, cand_member, artifact, False)


def stage_verified_source(
    artifact: Artifact,
    src_path: Path,
    dest_path: Path | str,
    *,
    src_member: str | None = None,
    mode: Literal["auto", "hardlink", "reflink", "symlink", "copy"] = "auto",
    contract: Contract = Contract(),
    ledger: StoreLedger | HintLedger | None = None,
    site_context: SiteContext | None = None,
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
) -> Path:
    """Atomically stage an already-located source path to `dest_path` without re-hashing."""
    if mode not in ("auto", "hardlink", "reflink", "symlink", "copy"):
        raise ValueError(f"Unsupported stage mode: {mode!r}")
    if contract.mutates and mode in ("hardlink", "symlink"):
        raise ValueError(
            f"Cannot stage artifact with mode={mode!r} when contract.mutates=True"
        )

    active_ledger = _resolve_active_ledger(artifact, ledger)
    effective_mounts = _effective_mounts(active_ledger, mounts)
    src = src_path.expanduser().resolve()
    raw_dest = Path(dest_path).expanduser().absolute()
    raw_dest.parent.mkdir(parents=True, exist_ok=True)
    dest = raw_dest.parent.resolve() / raw_dest.name

    if dest == src and not dest.is_symlink():
        if contract.mutates:
            raise ValueError(
                f"Cannot stage artifact onto its own source path {src} when contract.mutates=True"
            )
        return dest

    src_is_dir = src.is_dir()
    tmp_dest = dest.parent / f".consist-tmp-{uuid.uuid4().hex}"
    try:
        if mode == "symlink":
            os.symlink(src, tmp_dest, target_is_directory=src_is_dir)
        elif mode == "hardlink":
            if src_is_dir:
                shutil.copytree(src, tmp_dest, symlinks=True, copy_function=os.link)
            else:
                os.link(src, tmp_dest)
        elif mode == "reflink":
            if not _try_reflink_clone(src, tmp_dest):
                if src_is_dir:
                    shutil.copytree(
                        src,
                        tmp_dest,
                        symlinks=False,
                        copy_function=_reflink_or_raise,
                    )
                else:
                    raise OSError(
                        errno.ENOTSUP,
                        f"Copy-on-write reflink clone is not supported from {src} to {dest}",
                    )
        elif mode == "copy":
            if src_is_dir:
                shutil.copytree(
                    src, tmp_dest, symlinks=True, copy_function=shutil.copy2
                )
            else:
                shutil.copy2(src, tmp_dest)
        else:
            # mode == "auto": CoW clone when supported, else byte copy (never os.link)
            if not _try_reflink_clone(src, tmp_dest):
                if src_is_dir:
                    shutil.copytree(
                        src,
                        tmp_dest,
                        symlinks=False,
                        copy_function=_reflink_or_copy2,
                    )
                else:
                    shutil.copy2(src, tmp_dest)

        _atomic_replace_path(tmp_dest, dest)
    except BaseException:
        _remove_path(tmp_dest)
        raise

    if _is_store_ledger(active_ledger) and not contract.mutates:
        now_ns = time.time_ns()
        dest_witness, repr_kind = _capture_path_witness(
            dest,
            is_dir=src_is_dir,
            member=src_member,
            observed_at_ns=now_ns,
        )
        staged_obs = _build_path_observation(
            dest,
            identity=artifact.identity,
            driver=artifact.driver,
            witness=dest_witness,
            basis="derived",
            member=src_member,
            site_context=site_context,
            mounts=effective_mounts,
            observed_at_ns=now_ns,
        )
        active_ledger.record_observation(staged_obs, repr_kind=repr_kind)

    return dest


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
    return stage_verified_source(
        artifact,
        resolved.path,
        dest_path,
        src_member=resolved.member,
        mode=mode,
        contract=contract,
        ledger=active_ledger,
        site_context=site_context,
        mounts=effective_mounts,
    )


def _infer_driver(po: PendingOutput, resolved_path: Path) -> str:
    if po.driver is not None and po.driver.strip():
        return po.driver.strip()
    return infer_path_driver(resolved_path, is_dir=po.is_dir, member=po.member)


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


def _atomic_copy_file(src_path: Path, dst_path: Path) -> None:
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_dst = dst_path.parent / f".consist-tmp-{uuid.uuid4().hex}"
    try:
        src_st = os.stat(src_path)
        with src_path.open("rb") as src_f, tmp_dst.open("wb") as dst_f:
            while chunk := src_f.read(_STREAM_CHUNK_BYTES):
                dst_f.write(chunk)
            dst_f.flush()
            os.fsync(dst_f.fileno())
        try:
            os.utime(tmp_dst, ns=(src_st.st_mtime_ns, src_st.st_mtime_ns))
        except OSError:
            pass
        _atomic_replace_path(tmp_dst, dst_path)
    except BaseException:
        _remove_path(tmp_dst)
        raise


def _atomic_copy_dir(
    src_dir: Path,
    dst_dir: Path,
    *,
    symlinks: bool = True,
) -> None:
    dst_dir.parent.mkdir(parents=True, exist_ok=True)
    tmp_dst = dst_dir.parent / f".consist-tmp-{uuid.uuid4().hex}"
    try:
        shutil.copytree(
            src_dir,
            tmp_dst,
            symlinks=symlinks,
            copy_function=shutil.copy2,
        )
        _atomic_replace_path(tmp_dst, dst_dir)
    except BaseException:
        _remove_path(tmp_dst)
        raise


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

    # Step 1: Validate unique keys, FrozenMeta construction, member rules, and path existence up front.
    seen_roles: set[str] = set()
    seen_art_keys: set[str] = set()
    prevalidated_meta: dict[int, FrozenMeta] = {}
    for idx, po in enumerate(items):
        if po.role in seen_roles:
            raise ValueError(f"Duplicate output role {po.role!r} in seal()")
        seen_roles.add(po.role)
        eff_key = po.key.strip() if (po.key is not None and po.key.strip()) else po.role
        if eff_key in seen_art_keys:
            raise ValueError(f"Duplicate output artifact key {eff_key!r} in seal()")
        seen_art_keys.add(eff_key)
        prevalidated_meta[idx] = FrozenMeta(po.meta) if po.meta else EMPTY_META
        if po.member is not None and po.driver is not None:
            drv_norm = po.driver.strip().lower()
            if drv_norm in ("sha256:file", "blake3:file", "sha256:manifest-v1"):
                raise ValueError(
                    f"Scheme {drv_norm!r} cannot be used for sub-container member {po.member!r}"
                )

    for po in items:
        expanded = po.path.expanduser()
        if not expanded.exists() or (po.is_dir and not expanded.is_dir()):
            raise MissingOutputError(role=po.role, path=po.path)

    mount_map: dict[str, Mount] = {
        **(ledger.mounts if ledger is not None else {}),
        **_normalize_mounts(mounts),
    }
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

    def _process_leaf(idx: int, po: PendingOutput) -> _SealedItemWork:
        resolved_path = po.path.expanduser().resolve()
        drv = _infer_driver(po, resolved_path)
        target_arch_path = archive_dests.get(idx)

        if po.member is not None:
            drv_lower = drv.lower()
            if drv_lower in ("matrix_set", "logical_matrices"):
                scheme = "sha256:logical_matrices:v1"
            elif drv_lower in ("omx", "openmatrix", "matrix", "zarr", "npy", "npz"):
                scheme = "sha256:logical_matrix:v1"
            else:
                scheme = "ctd1:sha256"
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

    def _process_dir(
        idx: int,
        po: PendingOutput,
        *,
        executor: Executor | None,
    ) -> _SealedItemWork:
        resolved_path = po.path.expanduser().resolve()
        drv = _infer_driver(po, resolved_path)
        target_arch_path = archive_dests.get(idx)
        anchor, body_bytes, entry_count, total_bytes, dir_stat = (
            _compute_manifest_v1_and_witness(
                resolved_path,
                max_workers=1,
                executor=executor,
            )
        )
        arch_path: Path | None = None
        arch_witness: Witness | None = None
        if target_arch_path is not None:
            arch_path = target_arch_path
            if arch_path != resolved_path:
                _atomic_copy_dir(resolved_path, arch_path, symlinks=False)
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

    dir_items: list[tuple[int, PendingOutput]] = []
    leaf_items: list[tuple[int, PendingOutput]] = []
    for idx, po in enumerate(items):
        if po.member is None and (po.is_dir or po.path.expanduser().is_dir()):
            dir_items.append((idx, po))
        else:
            leaf_items.append((idx, po))

    bounded_workers = max(1, int(max_workers))
    results_by_idx: dict[int, _SealedItemWork] = {}
    if bounded_workers > 1 and (len(leaf_items) > 1 or bool(dir_items)):
        with ThreadPoolExecutor(max_workers=bounded_workers) as shared_pool:
            leaf_futures: dict[int, Future[_SealedItemWork]] = {
                idx: shared_pool.submit(_process_leaf, idx, po)
                for idx, po in leaf_items
            }
            for idx, po in dir_items:
                results_by_idx[idx] = _process_dir(
                    idx,
                    po,
                    executor=shared_pool,
                )
            for idx, fut in leaf_futures.items():
                results_by_idx[idx] = fut.result()
    else:
        for idx, po in leaf_items:
            results_by_idx[idx] = _process_leaf(idx, po)
        for idx, po in dir_items:
            results_by_idx[idx] = _process_dir(idx, po, executor=None)

    completed_work = [results_by_idx[i] for i in range(len(items))]

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
                    _atomic_copy_file(work.resolved_path, arch_parent)
                parent_stat = FileStat.capture(arch_parent)
                archived_member_parents[work.resolved_path] = (
                    arch_parent,
                    parent_stat,
                )

    sealed_artifacts: dict[str, Artifact] = {}
    tx_cm = (
        ledger.transaction()
        if (ledger is not None and not ledger.read_only)
        else nullcontext()
    )

    with tx_cm:
        next_occurrence_idx = 0
        if ledger is not None and run_id is not None and not ledger.read_only:
            with ledger.exec_cursor() as cur:
                occ_rows = cur.execute(
                    "SELECT occurrence_index "
                    "FROM main.run_binding WHERE run_id = ? AND direction = 'output'",
                    [run_id],
                ).fetchall()
            next_occurrence_idx = (
                max((int(r[0]) for r in occ_rows if r[0] is not None), default=-1) + 1
            )

        for idx, work in enumerate(completed_work):
            po = work.po
            art_key = (
                po.key.strip() if (po.key is not None and po.key.strip()) else po.role
            )
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

            frozen_meta = prevalidated_meta[idx]

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
    src_is_dir = src.is_dir()
    raw_dest = Path(dest_path).expanduser().absolute()
    if (
        os.path.lexists(raw_dest)
        and raw_dest.is_dir()
        and not raw_dest.is_symlink()
        and not src_is_dir
    ):
        dest = raw_dest.resolve() / src.name
    else:
        raw_dest.parent.mkdir(parents=True, exist_ok=True)
        dest = raw_dest.parent.resolve() / raw_dest.name

    if archive_mount_name not in effective_mounts:
        effective_mounts[archive_mount_name] = Mount(
            name=archive_mount_name,
            root=dest.parent,
        )

    store_ledger = active_ledger if _is_store_ledger(active_ledger) else None
    src_site = (
        site_context.site if site_context is not None else (resolved.site or "local")
    )
    src_scope_key = resolved.scope_key
    src_mount_instance_id = resolved.mount_instance_id

    anchor = artifact.identity.anchor
    anchor_scheme = anchor.scheme
    is_verifiable_scheme = artifact.identity.verifiable and (
        anchor_scheme in _VERIFIABLE_SURVIVOR_SCHEMES
        or anchor_scheme.startswith("sha256:manifest-v1+p:")
    )

    dest_verified = False
    if (
        dest != src
        and not src_is_dir
        and resolved.member is None
        and artifact.identity.verifiable
        and anchor_scheme == "sha256:file"
    ):
        verify_started_at_ns = time.time_ns()
        _computed_d, _src_stat, arch_stat = _tee_hash_and_copy_file(
            src,
            dest,
            expected_digest=anchor,
        )
        dest_witness = _non_racy_witness(arch_stat, verify_started_at_ns)
        repr_kind = "file"
        now_ns = time.time_ns()
        dest_verified = True
    else:
        if dest != src:
            if src_is_dir:
                _atomic_copy_dir(src, dest, symlinks=True)
            else:
                _atomic_copy_file(src, dest)

        if is_verifiable_scheme:
            verify_started_at_ns = time.time_ns()
            matched, actual_d, raw_dest_witness = verify_path_digest(
                dest,
                anchor,
                driver=artifact.driver,
                member=resolved.member,
                ledger=store_ledger,
            )
            if not matched or raw_dest_witness is None:
                raise ValueError(
                    f"Archived copy at {dest} failed digest verification against "
                    f"anchor {anchor} (actual={actual_d})"
                )
            dest_witness = _non_racy_witness(raw_dest_witness, verify_started_at_ns)
            repr_kind = (
                "tree"
                if src_is_dir
                else ("member" if resolved.member is not None else "file")
            )
            now_ns = time.time_ns()
            dest_verified = True
        else:
            now_ns = time.time_ns()
            dest_witness, repr_kind = _capture_path_witness(
                dest,
                is_dir=src_is_dir,
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
        default_site=src_site,
        mounts=effective_mounts,
        observed_at_ns=now_ns,
    )
    src_place = (src_site, src_scope_key, src_mount_instance_id, resolved.uri)
    arch_place = (
        arch_obs.site,
        arch_obs.scope_key or "",
        arch_obs.mount_instance_id or "",
        arch_obs.location.uri,
    )

    if mode == "move" and dest != src:
        arch_is_survivor = arch_place != src_place and (
            dest_verified
            or _surviving_row_is_fresh_on_disk(
                {
                    "uri": arch_obs.location.uri,
                    "member": arch_obs.location.member,
                    "access_driver": arch_obs.access_driver,
                    "site": arch_obs.site,
                    "scope_key": arch_obs.scope_key,
                    "mount_instance_id": arch_obs.mount_instance_id,
                    "witness": arch_obs.witness,
                },
                artifact=artifact,
                effective_mounts=effective_mounts,
                store_ledger=store_ledger,
                site_context=site_context,
            )
        )
        if not arch_is_survivor:
            existing_rows = active_ledger.locate_current(
                [str(anchor)],
                site=(site_context.site if site_context is not None else None),
                include_evicted=False,
            )
            has_other_survivor = any(
                (
                    str(r.get("site") or "local"),
                    str(r.get("scope_key") or ""),
                    str(r.get("mount_instance_id") or ""),
                    str(r["uri"]),
                )
                != src_place
                and _surviving_row_is_fresh_on_disk(
                    r,
                    artifact=artifact,
                    effective_mounts=effective_mounts,
                    store_ledger=store_ledger,
                    site_context=site_context,
                )
                for r in existing_rows
            )
            if not has_other_survivor:
                raise ConsistError(
                    f"Refusing to evict last active copy of artifact {artifact.key!r} "
                    f"(anchor={anchor})"
                )

    tx_cm = (
        store_ledger.transaction()
        if (store_ledger is not None and not store_ledger.read_only)
        else nullcontext()
    )
    with tx_cm:
        sub_src_uris: list[str] = []
        if store_ledger is not None:
            store_ledger.record_observation(arch_obs, repr_kind=repr_kind)
            if arch_place != src_place and resolved.member is None:
                store_ledger.record_archive_copy(
                    src_uri=resolved.uri,
                    dst_uri=arch_obs.location.uri,
                    site=arch_obs.site,
                    src_site=src_site,
                    evict_scope_key=src_scope_key,
                    evict_mount_instance_id=src_mount_instance_id,
                    dst_path=dest,
                    scope_key=arch_obs.scope_key,
                    mount_instance_id=arch_obs.mount_instance_id,
                    exclude_anchor_member=(
                        str(anchor),
                        resolved.member,
                    ),
                )
                if src_is_dir and not resolved.uri.endswith("://"):
                    sub_prefix = f"{resolved.uri.rstrip('/')}/"
                    with store_ledger.exec_cursor() as cur:
                        sub_rows = cur.execute(
                            "SELECT DISTINCT uri FROM main.artifact_location "
                            "WHERE site = ? AND starts_with(uri, ?)",
                            [src_site, sub_prefix],
                        ).fetchall()
                    sub_src_uris = [str(r[0]) for r in sub_rows]
                    if store_ledger.read_only:
                        for ov in store_ledger._overlay_observations.values():
                            ov_u = str(ov["uri"])
                            if (
                                str(ov["site"]) == src_site
                                and ov_u.startswith(sub_prefix)
                                and ov_u not in sub_src_uris
                            ):
                                sub_src_uris.append(ov_u)
                    for sub_src_u in sub_src_uris:
                        rel_sub = sub_src_u[len(sub_prefix) :]
                        sub_dst_u = f"{arch_obs.location.uri.rstrip('/')}/{rel_sub}"
                        sub_dst_p = dest / rel_sub
                        store_ledger.record_archive_copy(
                            src_uri=sub_src_u,
                            dst_uri=sub_dst_u,
                            site=arch_obs.site,
                            src_site=src_site,
                            evict_scope_key=src_scope_key,
                            evict_mount_instance_id=src_mount_instance_id,
                            dst_path=sub_dst_p,
                            scope_key=arch_obs.scope_key,
                            mount_instance_id=arch_obs.mount_instance_id,
                        )

        updated_hints = (arch_obs, *[h for h in artifact.hints if h != arch_obs])[:4]
        updated_artifact = replace(artifact, hints=tuple(updated_hints))

        if mode == "move" and dest != src:
            if resolved.member is None and store_ledger is not None:
                uris_to_evict_children = [resolved.uri, *sub_src_uris]
                for target_u in uris_to_evict_children:
                    is_sub_u = target_u != resolved.uri
                    child_rows = [
                        c_row
                        for c_row in store_ledger.query_location_current(
                            site=src_site,
                            uri=target_u,
                            include_evicted=False,
                        )
                        if (is_sub_u or c_row.get("member") is not None)
                        and str(c_row.get("status", "active")) == "active"
                        and str(c_row.get("scope_key") or "") == src_scope_key
                        and str(c_row.get("mount_instance_id") or "")
                        == src_mount_instance_id
                    ]
                    for c_row in child_rows:
                        raw_child_obs = c_row.get("observation_id")
                        if raw_child_obs is not None:
                            child_uuid = (
                                raw_child_obs
                                if isinstance(raw_child_obs, uuid.UUID)
                                else uuid.UUID(str(raw_child_obs))
                            )
                            store_ledger.record_location_event(
                                observation_id=child_uuid,
                                status="evicted",
                            )
            evict(
                updated_artifact,
                uri=resolved.uri,
                path=src,
                delete_files=False,
                require_surviving_copy=False,
                ledger=active_ledger,
                site_context=site_context,
                mounts=effective_mounts,
                target_site=src_site,
                evict_scope_key=src_scope_key,
                evict_mount_instance_id=src_mount_instance_id,
            )

    if mode == "move" and dest != src:
        if resolved.member is None:
            shared_container = False
            if store_ledger is not None:
                remaining_for_uri = [
                    rem
                    for rem in store_ledger.query_location_current(
                        site=src_site,
                        uri=resolved.uri,
                        include_evicted=False,
                    )
                    if str(rem.get("scope_key") or "") == src_scope_key
                    and str(rem.get("mount_instance_id") or "") == src_mount_instance_id
                ]
                if remaining_for_uri:
                    shared_container = True
            if not shared_container and os.path.lexists(src):
                _remove_path(src)

        updated_artifact = replace(
            updated_artifact,
            hints=tuple(
                h
                for h in updated_artifact.hints
                if (
                    h.site,
                    h.scope_key or "",
                    h.mount_instance_id or "",
                    h.location.uri,
                )
                != src_place
            ),
        )

    return updated_artifact


def _surviving_row_is_fresh_on_disk(
    row: Mapping[str, Any],
    *,
    artifact: Artifact,
    effective_mounts: Mapping[str, Mount],
    store_ledger: StoreLedger | None,
    site_context: SiteContext | None = None,
) -> bool:
    try:
        surv_path, matched_mount = resolve_uri_to_path(
            str(row["uri"]),
            mounts=effective_mounts,
        )
    except ValueError:
        return False
    if surv_path is None or not surv_path.exists():
        return False
    if not _is_candidate_visible(
        row,
        site_context=site_context,
        matched_mount=matched_mount,
    ):
        return False

    witness = row.get("witness")
    if not isinstance(witness, (FileStat, DirTreeStat, MemberOf)):
        if isinstance(witness, (str, Mapping)):
            witness = deserialize_witness_json(witness)
        else:
            witness = deserialize_witness_json(row.get("witness_json"))
    if isinstance(witness, (FileStat, DirTreeStat, MemberOf)):
        if witness.check(surv_path) is Freshness.FRESH:
            return True

    if not artifact.identity.verifiable:
        return False

    cand_member = str(row["member"]) if row.get("member") is not None else None
    drv = str(row.get("access_driver") or artifact.driver)
    digests_to_try: list[Digest] = [artifact.identity.anchor]
    for clm in artifact.identity.claims:
        if clm not in digests_to_try:
            digests_to_try.append(clm)
    if store_ledger is not None:
        stored_id = store_ledger.get_identity(artifact.identity.anchor)
        for clm in stored_id.claims:
            if clm not in digests_to_try:
                digests_to_try.append(clm)

    for candidate_digest in digests_to_try:
        if not (
            candidate_digest.scheme in _VERIFIABLE_SURVIVOR_SCHEMES
            or candidate_digest.scheme.startswith("sha256:manifest-v1+p:")
        ):
            continue
        try:
            matched, _actual, _w = verify_path_digest(
                surv_path,
                candidate_digest,
                driver=drv,
                member=cand_member,
                ledger=store_ledger,
            )
        except (OSError, ValueError, KeyError, ConsistError):
            continue
        if matched:
            return True
    return False


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
    target_site: str | None = None,
    evict_scope_key: str | None = None,
    evict_mount_instance_id: str | None = None,
) -> list[uuid.UUID]:
    """Evict a physical copy of `artifact`, optionally requiring at least one surviving copy."""
    active_ledger = _resolve_active_ledger(artifact, ledger)
    store_ledger = active_ledger if _is_store_ledger(active_ledger) else None
    effective_mounts = _effective_mounts(active_ledger, mounts)
    effective_site = (
        target_site
        if target_site is not None
        else (site_context.site if site_context is not None else None)
    )
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
        try:
            row_path, row_mount = resolve_uri_to_path(row_uri, mounts=effective_mounts)
        except ValueError:
            row_path = None
            row_mount = None

        row_visible = _is_candidate_visible(
            row,
            site_context=site_context,
            matched_mount=row_mount,
        )
        scope_matches = (
            evict_scope_key is None
            or str(row.get("scope_key") or "") == evict_scope_key
        ) and (
            evict_mount_instance_id is None
            or str(row.get("mount_instance_id") or "") == evict_mount_instance_id
        )

        is_match = False
        if row_visible and scope_matches:
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
                site_context=site_context,
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
            row_scope_str = str(row.get("scope_key") or "")
            row_mount_inst_str = str(row.get("mount_instance_id") or "")
            shared_container = False
            if store_ledger is not None:
                remaining_for_uri = [
                    rem
                    for rem in store_ledger.query_location_current(
                        site=row_site_str,
                        uri=row_uri_str,
                        include_evicted=False,
                    )
                    if str(rem.get("scope_key") or "") == row_scope_str
                    and str(rem.get("mount_instance_id") or "") == row_mount_inst_str
                ]
                if remaining_for_uri:
                    shared_container = True
            if not shared_container:
                row_path, _ = resolve_uri_to_path(
                    row_uri_str,
                    mounts=effective_mounts,
                )
                if row_path is not None and os.path.lexists(row_path):
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
