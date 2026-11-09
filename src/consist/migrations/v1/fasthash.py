"""
Consist v1 fast-stat file hash formulas, isolated strictly inside
`consist.migrations` for migration evidence classification (`migrated_fast_match`
vs. `racy_fast_stat`).

Never import this module outside `consist.migrations`.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

RACY_STAT_WINDOW_NS: int = 2_000_000_000


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
) -> tuple[str, os.stat_result, bool]:
    """Stat `path`, compute its v1 fast hash, and check the Lustre/NFS racy-stat window.

    Returns `(hex_digest, stat_result, is_racy)`, where `is_racy` is `True` when
    `v1_recorded_ns` is provided and `(v1_recorded_ns - st.st_mtime_ns) < 2_000_000_000`.
    """
    st = Path(path).stat()
    digest = compute_v1_fast_file_hash(st.st_size, st.st_mtime_ns)
    is_racy = (
        (int(v1_recorded_ns) - st.st_mtime_ns) < RACY_STAT_WINDOW_NS
        if v1_recorded_ns is not None
        else False
    )
    return digest, st, is_racy


__all__ = [
    "RACY_STAT_WINDOW_NS",
    "compute_v1_fast_file_hash",
    "stat_v1_fast_file_hash",
]
