from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import shutil
from typing import TYPE_CHECKING, Any, Dict, Optional
import uuid

if TYPE_CHECKING:
    from consist.core.persistence import DatabaseManager


def snapshot_sidecar_path(destination: Path) -> Path:
    """Return the metadata sidecar path paired with a snapshot database file."""
    base_name = destination.stem if destination.suffix else destination.name
    return destination.with_name(f"{base_name}.snapshot_meta.json")


def _paths_alias(candidate: Path, target: Path) -> bool:
    """Return True if ``candidate`` resolves to, symlinks to, or hardlinks ``target``."""
    target_resolved = target.resolve(strict=False)
    if candidate.resolve(strict=False) == target_resolved:
        return True
    if candidate.is_symlink():
        try:
            link_target = Path(
                os.path.abspath(
                    os.path.join(str(candidate.parent), os.readlink(candidate))
                )
            )
            if link_target.resolve(strict=False) == target_resolved:
                return True
        except OSError:
            pass
    if (candidate.exists() or candidate.is_symlink()) and target.exists():
        try:
            if os.path.samefile(candidate, target):
                return True
        except OSError:
            pass
    return False


class DatabaseSnapshotOps:
    """Snapshot and atomic file-write helpers backed by ``DatabaseManager``."""

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    def _unlink_temp_path(self, temp_path: Path) -> None:
        try:
            if temp_path.exists() or temp_path.is_symlink():
                temp_path.unlink()
        except OSError:
            return

    def _atomic_copy_file(self, src: Path, dest: Path) -> None:
        temp_path = dest.parent / f".{dest.name}.{uuid.uuid4().hex}.tmp"
        try:
            shutil.copy2(src, temp_path)
            temp_path.replace(dest)
        finally:
            self._unlink_temp_path(temp_path)

    def _atomic_write_json_file(self, payload: Dict[str, Any], dest: Path) -> None:
        temp_path = dest.parent / f".{dest.name}.{uuid.uuid4().hex}.tmp"
        try:
            temp_path.write_text(
                json.dumps(payload, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            temp_path.replace(dest)
        finally:
            self._unlink_temp_path(temp_path)

    def _snapshot_sidecar_path(self, destination: Path) -> Path:
        return snapshot_sidecar_path(destination)

    @staticmethod
    def _reject_source_aliases(
        *,
        source_db_path: Path,
        source_wal_path: Path,
        destination: Path,
        destination_wal_path: Path,
        sidecar_path: Optional[Path],
    ) -> None:
        candidates = [destination, destination_wal_path]
        if sidecar_path is not None:
            candidates.append(sidecar_path)
        for candidate in candidates:
            for protected in (source_db_path, source_wal_path):
                if _paths_alias(candidate, protected):
                    raise ValueError(
                        f"Snapshot destination '{destination}' aliases live database file '{protected}'."
                    )

    def snapshot_to(
        self,
        dest_path: str | os.PathLike[str],
        checkpoint: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Path:
        if self._db.db_path == ":memory:":
            raise ValueError("Cannot snapshot an in-memory DuckDB database.")

        source_db_path = Path(self._db.db_path)
        source_wal_path = Path(f"{source_db_path}.wal")
        destination = Path(dest_path)
        destination_wal_path = Path(f"{destination}.wal")
        sidecar_path = (
            self._snapshot_sidecar_path(destination) if metadata is not None else None
        )

        self._reject_source_aliases(
            source_db_path=source_db_path,
            source_wal_path=source_wal_path,
            destination=destination,
            destination_wal_path=destination_wal_path,
            sidecar_path=sidecar_path,
        )

        with self._db._lock:
            self._db._wait_until_active_locked()
            destination.parent.mkdir(parents=True, exist_ok=True)
            self._reject_source_aliases(
                source_db_path=source_db_path,
                source_wal_path=source_wal_path,
                destination=destination,
                destination_wal_path=destination_wal_path,
                sidecar_path=sidecar_path,
            )

            if checkpoint:
                self._db.checkpoint()

            self._atomic_copy_file(source_db_path, destination)

            if checkpoint:
                try:
                    if (
                        destination_wal_path.exists()
                        or destination_wal_path.is_symlink()
                    ):
                        destination_wal_path.unlink()
                except OSError as exc:
                    logging.warning(
                        "Failed to remove stale snapshot WAL at %s: %s",
                        destination_wal_path,
                        exc,
                    )
            elif source_wal_path.exists():
                self._atomic_copy_file(source_wal_path, destination_wal_path)

            if metadata is not None and sidecar_path is not None:
                snapshot_metadata = dict(metadata)
                snapshot_metadata["snapshot_ts_utc"] = datetime.now(
                    timezone.utc
                ).isoformat()
                snapshot_metadata["source_db_path"] = str(source_db_path)
                self._atomic_write_json_file(
                    snapshot_metadata,
                    sidecar_path,
                )

        return destination
