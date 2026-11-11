from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import shutil
from typing import TYPE_CHECKING, Any, Dict, Optional
import uuid

from consist.engine import _try_reflink_clone

if TYPE_CHECKING:
    from consist.core.persistence import DatabaseManager


def snapshot_sidecar_path(destination: Path) -> Path:
    """Return the metadata sidecar path paired with a snapshot database file."""
    base_name = destination.stem if destination.suffix else destination.name
    return destination.with_name(f"{base_name}.snapshot_meta.json")


class DatabaseSnapshotOps:
    """
    Snapshot and atomic file-write helpers for ``DatabaseManager`` over ``StoreLedger``.
    """

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    def _unlink_temp_path(self, temp_path: Path) -> None:
        try:
            if temp_path.exists():
                temp_path.unlink()
        except OSError:
            return

    def _atomic_copy_file(self, src: Path, dest: Path) -> None:
        temp_path = dest.parent / f".{dest.name}.{uuid.uuid4().hex}.tmp"
        try:
            if not _try_reflink_clone(src, temp_path):
                shutil.copy2(src, temp_path)
            os.replace(temp_path, dest)
        finally:
            self._unlink_temp_path(temp_path)

    def _atomic_write_json_file(self, payload: Dict[str, Any], dest: Path) -> None:
        temp_path = dest.parent / f".{dest.name}.{uuid.uuid4().hex}.tmp"
        try:
            temp_path.write_text(
                json.dumps(payload, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            os.replace(temp_path, dest)
        finally:
            self._unlink_temp_path(temp_path)

    def _snapshot_sidecar_path(self, destination: Path) -> Path:
        return snapshot_sidecar_path(destination)

    def snapshot_to(
        self,
        dest_path: str | os.PathLike[str],
        checkpoint: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Path:
        if self._db.db_path == ":memory:" or self._db.ledger.db_path is None:
            raise ValueError("Cannot snapshot an in-memory DuckDB database.")

        source_db_path = self._db.ledger.db_path
        destination = Path(dest_path)
        destination.parent.mkdir(parents=True, exist_ok=True)

        ledger = self._db.ledger
        with ledger._lock:
            checkpointed = False
            if checkpoint and not ledger.read_only and ledger._tx_depth == 0:
                ledger.connection.execute("FORCE CHECKPOINT")
                checkpointed = True

            self._atomic_copy_file(source_db_path, destination)

            source_wal_path = Path(f"{source_db_path}.wal")
            destination_wal_path = Path(f"{destination}.wal")
            if checkpointed:
                try:
                    if destination_wal_path.exists():
                        destination_wal_path.unlink()
                except OSError as exc:
                    logging.warning(
                        "Failed to remove stale snapshot WAL at %s: %s",
                        destination_wal_path,
                        exc,
                    )
            elif source_wal_path.exists():
                self._atomic_copy_file(source_wal_path, destination_wal_path)

        if metadata is not None:
            snapshot_metadata = dict(metadata)
            snapshot_metadata["snapshot_ts_utc"] = datetime.now(
                timezone.utc
            ).isoformat()
            snapshot_metadata["source_db_path"] = str(source_db_path)
            self._atomic_write_json_file(
                snapshot_metadata,
                self._snapshot_sidecar_path(destination),
            )

        return destination
