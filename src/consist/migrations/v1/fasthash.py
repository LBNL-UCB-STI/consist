"""
Consist v1 fast-stat file hash formulas, isolated strictly inside
`consist.migrations` for migration evidence classification (`migrated_fast_match`
vs. `racy_fast_stat`).

Never import this module outside `consist.migrations`.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import stat as stat_mod
import time

_RACY_WINDOW_NS: int = 2_000_000_000
RACY_STAT_WINDOW_NS: int = _RACY_WINDOW_NS


@dataclass(frozen=True, slots=True)
class V1FastDirectoryStat:
    """Replayed v1 fast directory hashes and aggregate tree stat without following inner symlinks."""

    canonical_root: Path
    legacy_fast_hex: str
    stat_v2_observation: str
    entry_count: int
    total_bytes: int
    max_mtime_ns: int
    is_racy: bool


def _is_mtime_racy(
    mtime_ns: int,
    *,
    now_ns: int,
    v1_recorded_ns: int | None = None,
) -> bool:
    if abs(int(now_ns) - int(mtime_ns)) < _RACY_WINDOW_NS:
        return True
    if v1_recorded_ns is not None:
        return abs(int(v1_recorded_ns) - int(mtime_ns)) < _RACY_WINDOW_NS
    return False


def compute_v1_fast_file_hash(size: int, mtime_ns: int) -> str:
    """Compute Consist v1's `hashing_strategy='fast'` file checksum.

    In v1 (`IdentityManager.compute_file_checksum`), fast file hashes were
    computed as `sha256(f"{stat.st_size}_{stat.st_mtime_ns}")` without `st_ino`.
    """
    if size < 0:
        raise ValueError(f"size must be non-negative, got {size}")
    return hashlib.sha256(f"{size}_{mtime_ns}".encode("utf-8")).hexdigest()


def stat_v1_fast_file_hash(
    path: Path | str,
    *,
    v1_recorded_ns: int | None = None,
    now_ns: int | None = None,
) -> tuple[str, os.stat_result, bool]:
    """Stat `path`, compute its v1 fast hash, and check the symmetric Lustre/NFS racy window.

    Returns `(hex_digest, stat_result, is_racy)`, where `is_racy` is `True` when
    `abs(now_ns - st.st_mtime_ns) < 2_000_000_000` (or symmetrically relative to
    `v1_recorded_ns` when provided).
    """
    st = Path(path).stat()
    probe_ns = int(now_ns) if now_ns is not None else time.time_ns()
    digest = compute_v1_fast_file_hash(st.st_size, st.st_mtime_ns)
    is_racy = _is_mtime_racy(
        st.st_mtime_ns,
        now_ns=probe_ns,
        v1_recorded_ns=v1_recorded_ns,
    )
    return digest, st, is_racy


def stat_v1_fast_directory_hash(
    root: Path | str,
    *,
    v1_recorded_ns: int | None = None,
    now_ns: int | None = None,
) -> V1FastDirectoryStat:
    """Replay Consist v1 fast directory hashes without following symlinks inside the tree.

    Resolves OS parent symlinks (`root.parent.resolve() / root.name`) consistently with
    `consist.digest.build_manifest_v1` while rejecting symlinked roots or inner symlinks.
    """
    raw_root = Path(root)
    if raw_root.is_symlink():
        raise ValueError(
            f"Directory fast-hash replay refuses symlinked root: {raw_root}"
        )
    if not raw_root.exists():
        raise FileNotFoundError(f"Directory not found: {raw_root}")
    if not raw_root.is_dir():
        raise NotADirectoryError(f"Expected directory, got non-directory: {raw_root}")

    canonical_root = (
        raw_root.parent.resolve() / raw_root.name
        if raw_root.name
        else raw_root.resolve()
    )
    probe_ns = int(now_ns) if now_ns is not None else time.time_ns()
    root_st = os.lstat(canonical_root)
    is_racy = _is_mtime_racy(
        root_st.st_mtime_ns,
        now_ns=probe_ns,
        v1_recorded_ns=v1_recorded_ns,
    )

    leaves: list[tuple[tuple[str, ...], str, str, int, int]] = []
    max_mtime_ns = int(root_st.st_mtime_ns)
    total_bytes = 0

    def _scan_dir(current_dir: Path, rel_parts: tuple[str, ...]) -> None:
        nonlocal is_racy, max_mtime_ns, total_bytes
        with os.scandir(current_dir) as scan:
            entries = sorted(scan, key=lambda e: e.name)
        for entry in entries:
            entry_path = Path(entry.path)
            if entry.is_symlink():
                raise ValueError(
                    f"Symlink encountered inside directory fast-hash replay: {entry_path}"
                )
            st = entry.stat(follow_symlinks=False)
            if stat_mod.S_ISLNK(st.st_mode):
                raise ValueError(
                    f"Symlink encountered inside directory fast-hash replay: {entry_path}"
                )
            child_parts = (*rel_parts, entry.name)
            if entry.is_dir(follow_symlinks=False):
                if st.st_mtime_ns > max_mtime_ns:
                    max_mtime_ns = int(st.st_mtime_ns)
                if _is_mtime_racy(
                    st.st_mtime_ns,
                    now_ns=probe_ns,
                    v1_recorded_ns=v1_recorded_ns,
                ):
                    is_racy = True
                _scan_dir(entry_path, child_parts)
            elif stat_mod.S_ISREG(st.st_mode):
                rel_posix = "/".join(child_parts)
                size = int(st.st_size)
                mtime_ns = int(st.st_mtime_ns)
                total_bytes += size
                if mtime_ns > max_mtime_ns:
                    max_mtime_ns = mtime_ns
                if _is_mtime_racy(
                    mtime_ns,
                    now_ns=probe_ns,
                    v1_recorded_ns=v1_recorded_ns,
                ):
                    is_racy = True
                leaves.append((child_parts, rel_posix, entry.name, size, mtime_ns))

    _scan_dir(canonical_root, ())
    leaves.sort(key=lambda item: item[0])

    legacy_hasher = hashlib.sha256()
    stat_v2_hasher = hashlib.sha256()
    for _parts, rel_posix, fname, size, mtime_ns in leaves:
        legacy_hasher.update(f"{fname}:{size}_{mtime_ns}|".encode("utf-8"))
        stat_v2_hasher.update(f"{rel_posix}:{size}:{mtime_ns}|".encode("utf-8"))

    return V1FastDirectoryStat(
        canonical_root=canonical_root,
        legacy_fast_hex=legacy_hasher.hexdigest(),
        stat_v2_observation=f"stat-v2:directory:{stat_v2_hasher.hexdigest()}",
        entry_count=len(leaves),
        total_bytes=total_bytes,
        max_mtime_ns=max_mtime_ns,
        is_racy=is_racy,
    )


__all__ = [
    "RACY_STAT_WINDOW_NS",
    "V1FastDirectoryStat",
    "_RACY_WINDOW_NS",
    "compute_v1_fast_file_hash",
    "stat_v1_fast_directory_hash",
    "stat_v1_fast_file_hash",
]
