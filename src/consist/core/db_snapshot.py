from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
import shutil
import uuid
from typing import Any, Dict, Optional

from consist.core._db_ops_base import _DatabaseOpsBase


class DatabaseSnapshotOps(_DatabaseOpsBase):
    """
    Snapshot and atomic file-write helpers extracted from ``DatabaseManager``.

    The implementation still delegates through the concrete owning
    ``DatabaseManager`` for engine access and retry behavior. The split is meant
    to isolate snapshot concerns, not to define a finalized independent
    persistence layer yet.
    """

    def _unlink_temp_path(self, temp_path: Path) -> None:
        try:
            if temp_path.exists():
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
        base_name = destination.stem if destination.suffix else destination.name
        return destination.with_name(f"{base_name}.snapshot_meta.json")

    def snapshot_to(
        self,
        dest_path: str | os.PathLike[str],
        checkpoint: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Path:
        if self.db_path == ":memory:":
            raise ValueError("Cannot snapshot an in-memory DuckDB database.")

        source_db_path = Path(self.db_path)
        destination = Path(dest_path)
        destination.parent.mkdir(parents=True, exist_ok=True)

        if checkpoint:

            def _checkpoint() -> None:
                with self.engine.begin() as conn:
                    conn.exec_driver_sql("CHECKPOINT")

            self.execute_with_retry(_checkpoint, operation_name="snapshot_checkpoint")

        self._atomic_copy_file(source_db_path, destination)

        source_wal_path = Path(f"{source_db_path}.wal")
        destination_wal_path = Path(f"{destination}.wal")
        if checkpoint:
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
