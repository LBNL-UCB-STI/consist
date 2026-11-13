from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import tempfile
import time
from typing import TYPE_CHECKING, Any, Literal, Optional
from urllib.parse import unquote, urlsplit
import uuid

import duckdb

from consist.core._performance_attribution import _track_maintenance_merge_phase
from consist.core.facet_common import flatten_facet_values
from consist.core.fs import FileSystemManager, build_verified_location_row
from consist.core.persistence import (
    _dump_json,
    _dump_optional_json,
    lookup_live_db_by_path,
)
from consist.core.run_resolution import format_input_link_role
from consist.digest import canonical_json_sha256
from consist.location import ArtifactLocationRow, LocationSite
from consist.models.artifact import Artifact
from consist.models.artifact_kv import ArtifactKV
from consist.models.artifact_schema import ArtifactSchema, ArtifactSchemaObservation
from consist.models.run import (
    ConsistRecord,
    Run,
    resolve_canonical_run_meta_field,
)
from consist.models.run_config_kv import RunConfigKV

if TYPE_CHECKING:
    from consist.core.persistence import DatabaseManager

GlobalTableMode = Literal["run_scoped", "run_link", "unscoped_cache"]
UnsafeDeleteTargetPolicy = Literal["fail", "skip"]
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
UTC = timezone.utc


@dataclass(slots=True)
class PurgePlan:
    """Deletion preview for a run purge request."""

    run_ids: list[str]
    child_run_ids: list[str]
    orphaned_artifact_ids: list[uuid.UUID]
    json_files: list[Path]
    disk_files: list[Path]
    unsafe_json_files: list[Path]
    unsafe_disk_files: list[Path]
    ingested_data: dict[str, int]
    ingested_table_modes: dict[str, GlobalTableMode]


@dataclass(slots=True)
class PurgeResult:
    """Outcome metadata for a purge operation."""

    plan: PurgePlan
    executed: bool
    ingested_data_skipped: bool
    skipped_unsafe_json_files: list[Path] = field(default_factory=list)
    skipped_unsafe_disk_files: list[Path] = field(default_factory=list)


@dataclass(slots=True)
class ExportResult:
    """Outcome metadata for shard export operations."""

    run_ids: list[str]
    artifact_count: int
    out_path: Path
    ingested_rows: dict[str, int]
    ingested_table_modes: dict[str, GlobalTableMode]
    unscoped_cache_tables_skipped: list[str]
    snapshots_copied: int


@dataclass(slots=True)
class MergeResult:
    """Outcome metadata for shard merge operations."""

    shard_path: Path
    runs_merged: list[str]
    runs_skipped: list[str]
    artifacts_merged: int
    ingested_tables_merged: list[str]
    unscoped_cache_tables_skipped: list[str]
    conflicts_detected: list[str]
    snapshots_merged: int
    incompatible_global_tables_skipped: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class InspectReport:
    """Read-only database health summary."""

    total_runs: int
    runs_by_status: dict[str, int]
    total_artifacts: int
    orphaned_artifact_count: int
    zombie_run_ids: list[str]
    global_table_sizes: dict[str, int]
    db_file_size_mb: float
    json_snapshot_count: int
    json_db_parity: bool


@dataclass(slots=True)
class DoctorReport:
    """Read-only invariant diagnostics for maintenance checks."""

    zombie_run_ids: list[str]
    completed_without_end_time: list[str]
    dangling_parent_run_ids: list[str]
    artifacts_with_missing_producing_run: list[uuid.UUID]
    global_table_schema_drift: dict[str, str]


@dataclass(slots=True)
class RebuildResult:
    """Result payload for JSON-to-DB rebuild operations."""

    json_files_scanned: int
    runs_inserted: int
    runs_already_present: int
    artifacts_inserted: int
    errors: list[str] = field(default_factory=list)
    dry_run: bool = False


class DatabaseMaintenance:
    """
    Maintenance service for Consist provenance databases.

    This class intentionally avoids depending on `Tracker` so maintenance routines
    can be tested and executed in isolation using only `DatabaseManager` and a
    run directory root.
    """

    def __init__(
        self,
        db: DatabaseManager,
        run_dir: Path,
        *,
        mounts: Optional[Mapping[str, str | Path]] = None,
    ) -> None:
        self.db = db
        self.run_dir = Path(run_dir)
        self._mounts: dict[str, str] = (
            {str(k): str(v) for k, v in mounts.items()} if mounts else {}
        )

    @staticmethod
    def _artifact_location_digest_match_sql(*, loc_alias: str, art_alias: str) -> str:
        return (
            f'{art_alias}."digest" = {loc_alias}."digest" '
            f"AND (({loc_alias}.\"site\" IN ('archive', 'advisory') AND {loc_alias}.\"member\" = '') "
            f'OR COALESCE({art_alias}."member", \'\') = {loc_alias}."member")'
        )

    def _deletion_allowed_roots(self) -> tuple[Path, ...]:
        roots: list[Path] = []
        seen: set[str] = set()
        raw_roots: list[Path] = [self.run_dir]
        for mount_root in self._mounts.values():
            if mount_root:
                raw_roots.append(Path(mount_root))
        for raw_root in raw_roots:
            try:
                resolved = raw_root.resolve()
            except OSError:
                resolved = raw_root
            key = str(resolved)
            if key not in seen:
                seen.add(key)
                roots.append(resolved)
        return tuple(roots)

    def _resolve_default_run_directory(self) -> Path:
        try:
            return self.run_dir.resolve()
        except OSError:
            return self.run_dir

    def _ensure_distinct_from_source_db(self, target_path: Path) -> None:
        db_path_raw = self.db.db_path
        if (
            not isinstance(db_path_raw, str)
            or not db_path_raw
            or db_path_raw == ":memory:"
        ):
            return
        src_db = Path(db_path_raw)
        src_wal = Path(f"{db_path_raw}.wal")
        dst_db = Path(target_path)
        dst_wal = Path(f"{dst_db}.wal")

        for dst in (dst_db, dst_wal):
            try:
                dst_resolved = dst.resolve()
            except OSError:
                dst_resolved = dst
            dst_real = Path(os.path.realpath(dst))
            for src in (src_db, src_wal):
                try:
                    src_resolved = src.resolve()
                except OSError:
                    src_resolved = src
                src_real = Path(os.path.realpath(src))
                if dst_resolved == src_resolved or dst_real == src_real:
                    raise ValueError(
                        f"Refusing to overwrite active source database path: {target_path}"
                    )
                if dst.exists() and src.exists():
                    try:
                        if os.path.samefile(dst, src):
                            raise ValueError(
                                f"Refusing to overwrite active source database path: {target_path}"
                            )
                    except OSError:
                        pass

    @contextmanager
    def _attached_database(
        self,
        db_path: Path,
        alias: str,
        *,
        read_only: bool = False,
        mutate_main: bool = False,
    ) -> Iterator[duckdb.DuckDBPyConnection]:
        quoted_alias = self._quote_ident(alias)
        quoted_path = self._quote_sql_string_literal(str(db_path))
        attach_opts = " (READ_ONLY)" if read_only else " (READ_WRITE)"
        with self.db._lock:
            self.db._wait_until_active_locked()
            if self.db._tx_depth > 0:
                raise RuntimeError(
                    "Cannot ATTACH shard database inside an active transaction."
                )
            con = self.db.connection
            con.execute(f"ATTACH {quoted_path} AS {quoted_alias}{attach_opts}")
            try:
                if mutate_main:
                    with self.db.transaction() as tx_con:
                        yield tx_con
                else:
                    cur = con.cursor()
                    try:
                        cur.execute("BEGIN TRANSACTION")
                        try:
                            yield cur
                            cur.execute("COMMIT")
                        except BaseException:
                            try:
                                cur.execute("ROLLBACK")
                            except duckdb.Error:
                                pass
                            raise
                    finally:
                        try:
                            cur.close()
                        except duckdb.Error:
                            pass
            finally:
                self._detach_temporary_database(con, alias)

    def _detach_temporary_database(
        self, conn: duckdb.DuckDBPyConnection, alias: str
    ) -> None:
        quoted_alias = self._quote_ident(alias)
        try:
            conn.execute(f"DETACH {quoted_alias}")
        except duckdb.Error:
            try:
                conn.execute("ROLLBACK")
            except duckdb.Error:
                pass
            conn.execute(f"DETACH {quoted_alias}")

    @contextmanager
    def _mountable_shard_path(self, shard_path: Path) -> Iterator[Path]:
        live_shard = lookup_live_db_by_path(shard_path)
        if live_shard is not None and live_shard is not self.db:
            with tempfile.TemporaryDirectory(prefix="consist_merge_live_") as tmp_dir:
                tmp_snap = Path(tmp_dir) / shard_path.name
                live_shard.snapshot_to(tmp_snap, checkpoint=True)
                yield tmp_snap
                return
        yield shard_path

    def _classify_delete_targets(
        self, paths: Iterable[Path]
    ) -> tuple[list[Path], list[Path]]:
        allowed_roots = self._deletion_allowed_roots()
        safe_paths: list[Path] = []
        unsafe_paths: list[Path] = []
        seen_safe: set[str] = set()
        seen_unsafe: set[str] = set()

        for raw_path in paths:
            try:
                resolved = raw_path.resolve()
            except OSError:
                resolved = raw_path

            is_safe = False
            for root in allowed_roots:
                try:
                    resolved.relative_to(root)
                    is_safe = True
                    break
                except ValueError:
                    continue

            path_key = str(resolved)
            if is_safe:
                if path_key not in seen_safe:
                    seen_safe.add(path_key)
                    safe_paths.append(resolved)
            else:
                if path_key not in seen_unsafe:
                    seen_unsafe.add(path_key)
                    unsafe_paths.append(resolved)

        return safe_paths, unsafe_paths

    def inspect(self) -> InspectReport:
        total_runs_row = self.db.fetchone("SELECT COUNT(*) FROM run")
        total_runs = int(total_runs_row[0] if total_runs_row else 0)

        status_rows = self.db.fetchall(
            """
            SELECT status, COUNT(*) AS count
            FROM run
            GROUP BY status
            ORDER BY status
            """
        )
        runs_by_status = {
            str(status): int(count) for status, count in status_rows if status
        }

        total_artifacts_row = self.db.fetchone("SELECT COUNT(*) FROM artifact")
        total_artifacts = int(total_artifacts_row[0] if total_artifacts_row else 0)

        orphaned_row = self.db.fetchone(
            """
            SELECT COUNT(*)
            FROM artifact a
            WHERE NOT EXISTS (
                SELECT 1
                FROM run_artifact_link ral
                WHERE ral.artifact_id = a.id
            )
            """
        )
        orphaned_artifact_count = int(orphaned_row[0] if orphaned_row else 0)

        zombie_rows = self.db.fetchall(
            """
            SELECT id
            FROM run
            WHERE status = 'running'
              AND ended_at IS NOT NULL
            ORDER BY id
            """
        )
        zombie_run_ids = [str(row[0]) for row in zombie_rows]

        db_run_rows = self.db.fetchall("SELECT id FROM run ORDER BY id")
        db_run_ids = [str(row[0]) for row in db_run_rows]

        global_tables = self._discover_global_tables()
        global_table_sizes = self._global_table_row_counts(global_tables, [])

        db_file_size_mb = 0.0
        db_path_raw = self.db.db_path
        if isinstance(db_path_raw, str) and db_path_raw and db_path_raw != ":memory:":
            db_path = Path(db_path_raw)
            if db_path.exists() and db_path.is_file():
                db_file_size_mb = round(db_path.stat().st_size / (1024 * 1024), 6)

        snapshot_dir = self.run_dir / "consist_runs"
        snapshot_files = (
            sorted(
                p
                for p in snapshot_dir.glob("*.json")
                if not p.name.endswith(".locations.json")
            )
            if snapshot_dir.exists()
            else []
        )
        snapshot_run_ids: set[str] = set()
        for snapshot_file in snapshot_files:
            try:
                payload = json.loads(snapshot_file.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            run_payload = payload.get("run")
            if not isinstance(run_payload, dict):
                continue
            run_id = run_payload.get("id")
            if run_id is None:
                continue
            parsed_run_id = str(run_id).strip()
            if parsed_run_id:
                snapshot_run_ids.add(parsed_run_id)

        db_run_id_set = set(db_run_ids)

        return InspectReport(
            total_runs=total_runs,
            runs_by_status=runs_by_status,
            total_artifacts=total_artifacts,
            orphaned_artifact_count=orphaned_artifact_count,
            zombie_run_ids=zombie_run_ids,
            global_table_sizes=global_table_sizes,
            db_file_size_mb=db_file_size_mb,
            json_snapshot_count=len(snapshot_files),
            json_db_parity=db_run_id_set == snapshot_run_ids,
        )

    def doctor(self) -> DoctorReport:
        zombie_rows = self.db.fetchall(
            """
            SELECT id
            FROM run
            WHERE status = 'running'
              AND ended_at IS NOT NULL
            ORDER BY id
            """
        )
        zombie_run_ids = [str(row[0]) for row in zombie_rows]

        completed_without_end_rows = self.db.fetchall(
            """
            SELECT id
            FROM run
            WHERE status IN ('completed', 'failed')
              AND ended_at IS NULL
            ORDER BY id
            """
        )
        completed_without_end_time = [str(row[0]) for row in completed_without_end_rows]

        dangling_parent_rows = self.db.fetchall(
            """
            SELECT DISTINCT child.parent_run_id
            FROM run child
            LEFT JOIN run parent
              ON child.parent_run_id = parent.id
            WHERE child.parent_run_id IS NOT NULL
              AND parent.id IS NULL
            ORDER BY child.parent_run_id
            """
        )
        dangling_parent_run_ids = [
            str(row[0]) for row in dangling_parent_rows if row[0]
        ]

        missing_producer_rows = self.db.fetchall(
            """
            SELECT artifact.id
            FROM artifact
            LEFT JOIN run
              ON artifact.run_id = run.id
            WHERE artifact.run_id IS NOT NULL
              AND run.id IS NULL
            ORDER BY CAST(artifact.id AS VARCHAR)
            """
        )
        artifacts_with_missing_producing_run: list[uuid.UUID] = []
        for row in missing_producer_rows:
            parsed_uuid = self._coerce_uuid(row[0])
            if parsed_uuid is not None:
                artifacts_with_missing_producing_run.append(parsed_uuid)

        return DoctorReport(
            zombie_run_ids=zombie_run_ids,
            completed_without_end_time=completed_without_end_time,
            dangling_parent_run_ids=dangling_parent_run_ids,
            artifacts_with_missing_producing_run=artifacts_with_missing_producing_run,
            global_table_schema_drift={},
        )

    def _load_runs_by_ids(self, run_ids: Sequence[str]) -> dict[str, Run]:
        if not run_ids:
            return {}
        runs = self.db.find_runs(run_ids=list(run_ids))
        return {r.id: r for r in runs}

    def plan_purge(
        self, run_ids: Iterable[str] | str, *, include_children: bool
    ) -> PurgePlan:
        seed_run_ids = self._normalize_run_ids(run_ids)
        expanded_run_ids = (
            self._expand_run_ids(seed_run_ids) if include_children else seed_run_ids
        )
        seed_run_id_set = set(seed_run_ids)
        child_run_ids = [
            run_id for run_id in expanded_run_ids if run_id not in seed_run_id_set
        ]
        orphaned_artifact_ids = self._find_orphaned_artifacts(expanded_run_ids)
        runs_by_id = self._load_runs_by_ids(expanded_run_ids)

        json_candidates: list[Path] = []
        seen_json_paths: set[str] = set()
        for run_id in expanded_run_ids:
            snapshot_path = self._resolve_run_snapshot_path(
                run_id, runs_by_id.get(run_id)
            )
            if not snapshot_path.exists():
                continue
            path_key = str(snapshot_path)
            if path_key in seen_json_paths:
                continue
            seen_json_paths.add(path_key)
            json_candidates.append(snapshot_path)

        json_files, unsafe_json_files = self._classify_delete_targets(json_candidates)
        disk_files, unsafe_disk_files = self._classify_delete_targets(
            self._unshared_orphaned_disk_paths(orphaned_artifact_ids)
        )

        discovered_tables = self._discover_global_tables()
        if expanded_run_ids:
            ingested_data = self._global_table_row_counts(
                discovered_tables,
                expanded_run_ids,
            )
        else:
            ingested_data = {table: 0 for table in discovered_tables}

        ingested_table_modes = {
            table: self._classify_global_table(table) for table in discovered_tables
        }

        return PurgePlan(
            run_ids=expanded_run_ids,
            child_run_ids=child_run_ids,
            orphaned_artifact_ids=orphaned_artifact_ids,
            json_files=json_files,
            disk_files=disk_files,
            unsafe_json_files=unsafe_json_files,
            unsafe_disk_files=unsafe_disk_files,
            ingested_data=ingested_data,
            ingested_table_modes=ingested_table_modes,
        )

    def purge(
        self,
        run_ids: Iterable[str] | str,
        *,
        include_children: bool = True,
        delete_files: bool = False,
        delete_ingested_data: bool = False,
        dry_run: bool = False,
        prune_cache: bool = False,
        unsafe_delete_targets: UnsafeDeleteTargetPolicy = "fail",
    ) -> PurgeResult:
        plan = self.plan_purge(run_ids, include_children=include_children)
        ingested_candidates_exist = any(
            count > 0 for count in plan.ingested_data.values()
        )
        ingested_data_skipped = ingested_candidates_exist and not delete_ingested_data

        if dry_run:
            return PurgeResult(
                plan=plan,
                executed=False,
                ingested_data_skipped=ingested_data_skipped,
            )

        if unsafe_delete_targets not in {"fail", "skip"}:
            raise ValueError("unsafe_delete_targets must be either 'fail' or 'skip'.")

        blocked_paths = [*plan.unsafe_json_files, *plan.unsafe_disk_files]
        if blocked_paths and unsafe_delete_targets == "fail":
            blocked_display = ", ".join(str(path) for path in blocked_paths)
            raise ValueError(
                "Refusing purge filesystem deletion outside allowed roots "
                f"({', '.join(str(root) for root in self._deletion_allowed_roots())}): "
                f"{blocked_display}"
            )

        selected_run_ids = list(plan.run_ids)
        selected_orphaned_artifact_ids = [
            str(value) for value in plan.orphaned_artifact_ids
        ]

        with self.db.transaction():
            if self.db.has_table("run_coupler_snapshot"):
                if selected_run_ids and selected_orphaned_artifact_ids:
                    self.db.execute(
                        """
                        DELETE FROM run_coupler_snapshot
                        WHERE run_id IN (SELECT UNNEST(?))
                           OR scenario_run_id IN (SELECT UNNEST(?))
                           OR CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                        """,
                        [
                            selected_run_ids,
                            selected_run_ids,
                            selected_orphaned_artifact_ids,
                        ],
                    )
                elif selected_run_ids:
                    self.db.execute(
                        """
                        DELETE FROM run_coupler_snapshot
                        WHERE run_id IN (SELECT UNNEST(?))
                           OR scenario_run_id IN (SELECT UNNEST(?))
                        """,
                        [selected_run_ids, selected_run_ids],
                    )
                elif selected_orphaned_artifact_ids:
                    self.db.execute(
                        """
                        DELETE FROM run_coupler_snapshot
                        WHERE CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                        """,
                        [selected_orphaned_artifact_ids],
                    )
                if selected_run_ids:
                    self.db.execute(
                        """
                        UPDATE run_coupler_snapshot
                        SET producer_run_id = NULL
                        WHERE producer_run_id IN (SELECT UNNEST(?))
                        """,
                        [selected_run_ids],
                    )

            if selected_run_ids and self.db.has_table("run_binding_invocation"):
                self.db.execute(
                    """
                    DELETE FROM run_binding_invocation
                    WHERE requested_run_id IN (SELECT UNNEST(?))
                       OR execution_run_id IN (SELECT UNNEST(?))
                    """,
                    [selected_run_ids, selected_run_ids],
                )
                self.db.execute(
                    """
                    UPDATE run_binding_invocation
                    SET cache_source_run_id = NULL
                    WHERE cache_source_run_id IN (SELECT UNNEST(?))
                    """,
                    [selected_run_ids],
                )

            if selected_run_ids and selected_orphaned_artifact_ids:
                self.db.execute(
                    """
                    DELETE FROM artifact_schema_observation
                    WHERE run_id IN (SELECT UNNEST(?))
                       OR CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                    """,
                    [selected_run_ids, selected_orphaned_artifact_ids],
                )
            elif selected_run_ids:
                self.db.execute(
                    """
                    DELETE FROM artifact_schema_observation
                    WHERE run_id IN (SELECT UNNEST(?))
                    """,
                    [selected_run_ids],
                )
            elif selected_orphaned_artifact_ids:
                self.db.execute(
                    """
                    DELETE FROM artifact_schema_observation
                    WHERE CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                    """,
                    [selected_orphaned_artifact_ids],
                )

            if selected_run_ids:
                self.db.execute(
                    "DELETE FROM run_config_kv WHERE run_id IN (SELECT UNNEST(?))",
                    [selected_run_ids],
                )

            if selected_orphaned_artifact_ids:
                self.db.execute(
                    """
                    DELETE FROM artifact_kv
                    WHERE CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                    """,
                    [selected_orphaned_artifact_ids],
                )

            if selected_run_ids and selected_orphaned_artifact_ids:
                self.db.execute(
                    """
                    DELETE FROM run_artifact_link
                    WHERE run_id IN (SELECT UNNEST(?))
                       OR CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                    """,
                    [selected_run_ids, selected_orphaned_artifact_ids],
                )
            elif selected_run_ids:
                self.db.execute(
                    "DELETE FROM run_artifact_link WHERE run_id IN (SELECT UNNEST(?))",
                    [selected_run_ids],
                )
            elif selected_orphaned_artifact_ids:
                self.db.execute(
                    """
                    DELETE FROM run_artifact_link
                    WHERE CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                    """,
                    [selected_orphaned_artifact_ids],
                )

            if selected_orphaned_artifact_ids:
                self.db.execute(
                    """
                    UPDATE artifact
                    SET parent_artifact_id = NULL
                    WHERE CAST(parent_artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                      AND CAST(id AS VARCHAR) NOT IN (SELECT UNNEST(?))
                    """,
                    [selected_orphaned_artifact_ids, selected_orphaned_artifact_ids],
                )

            if selected_run_ids:
                if selected_orphaned_artifact_ids:
                    self.db.execute(
                        """
                        UPDATE artifact
                        SET run_id = NULL
                        WHERE run_id IN (SELECT UNNEST(?))
                          AND CAST(id AS VARCHAR) NOT IN (SELECT UNNEST(?))
                        """,
                        [selected_run_ids, selected_orphaned_artifact_ids],
                    )
                else:
                    self.db.execute(
                        "UPDATE artifact SET run_id = NULL WHERE run_id IN (SELECT UNNEST(?))",
                        [selected_run_ids],
                    )
                self.db.execute(
                    "DELETE FROM run WHERE id IN (SELECT UNNEST(?))",
                    [selected_run_ids],
                )

            if selected_orphaned_artifact_ids:
                self.db.execute(
                    "DELETE FROM artifact WHERE CAST(id AS VARCHAR) IN (SELECT UNNEST(?))",
                    [selected_orphaned_artifact_ids],
                )

            if self.db.has_artifact_location_table():
                loc_art_match = self._artifact_location_digest_match_sql(
                    loc_alias="al", art_alias="a"
                )
                if selected_orphaned_artifact_ids:
                    self.db.execute(
                        f"""
                        UPDATE artifact_location AS al
                        SET artifact_id = (
                            SELECT a.id FROM main.artifact AS a
                            WHERE {loc_art_match}
                            LIMIT 1
                        )
                        WHERE al.site IN ('workspace', 'external')
                          AND al.digest IS NOT NULL
                          AND CAST(al.artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                          AND EXISTS (
                              SELECT 1 FROM main.artifact AS a WHERE {loc_art_match}
                          )
                        """,
                        [selected_orphaned_artifact_ids],
                    )
                    self.db.execute(
                        """
                        DELETE FROM artifact_location
                        WHERE CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                        """,
                        [selected_orphaned_artifact_ids],
                    )
                self.db.execute(
                    f"""
                    DELETE FROM artifact_location AS al
                    WHERE (
                        al.artifact_id IS NULL
                        OR NOT EXISTS (
                            SELECT 1 FROM main.artifact AS a WHERE a.id = al.artifact_id
                        )
                    )
                    AND (
                        al.digest IS NULL
                        OR NOT EXISTS (
                            SELECT 1 FROM main.artifact AS a WHERE {loc_art_match}
                        )
                    )
                    """
                )
                self.db.bump_location_epoch()

            if delete_ingested_data and selected_run_ids:
                derivable_run_link_tables: list[str] = []
                prunable_cache_tables: list[str] = []
                purged_content_hashes: set[str] = set()

                if prune_cache:
                    for table in plan.ingested_data:
                        safe_table = self._validate_identifier(table, label="table")
                        mode = plan.ingested_table_modes.get(table)
                        if mode not in {"run_link", "unscoped_cache"}:
                            continue
                        column_rows = self.db.fetchall(
                            """
                            SELECT column_name
                            FROM information_schema.columns
                            WHERE table_schema = 'global_tables'
                              AND table_name = ?
                            """,
                            [safe_table],
                        )
                        columns = {str(row[0]) for row in column_rows}
                        if mode == "run_link" and {
                            "run_id",
                            "content_hash",
                        }.issubset(columns):
                            derivable_run_link_tables.append(safe_table)
                        if mode == "unscoped_cache" and "content_hash" in columns:
                            prunable_cache_tables.append(safe_table)

                    if derivable_run_link_tables and prunable_cache_tables:
                        for table in derivable_run_link_tables:
                            quoted_table = self._quote_ident(table)
                            hash_rows = self.db.fetchall(
                                f"""
                                SELECT DISTINCT CAST("content_hash" AS VARCHAR)
                                FROM global_tables.{quoted_table}
                                WHERE "run_id" IN (SELECT UNNEST(?))
                                  AND "content_hash" IS NOT NULL
                                """,
                                [selected_run_ids],
                            )
                            purged_content_hashes.update(
                                str(row[0]) for row in hash_rows if row[0] is not None
                            )

                for table in plan.ingested_data:
                    mode = plan.ingested_table_modes.get(table)
                    if mode not in {"run_scoped", "run_link"}:
                        continue
                    safe_table = self._validate_identifier(table, label="table")
                    quoted_table = self._quote_ident(safe_table)
                    run_column = (
                        self._quote_ident("consist_run_id")
                        if mode == "run_scoped"
                        else self._quote_ident("run_id")
                    )
                    self.db.execute(
                        f"""
                        DELETE FROM global_tables.{quoted_table}
                        WHERE {run_column} IN (SELECT UNNEST(?))
                        """,
                        [selected_run_ids],
                    )

                if (
                    prune_cache
                    and derivable_run_link_tables
                    and prunable_cache_tables
                    and purged_content_hashes
                ):
                    sorted_purged_hashes = sorted(purged_content_hashes)
                    surviving_content_hashes: set[str] = set()
                    for table in derivable_run_link_tables:
                        quoted_table = self._quote_ident(table)
                        surviving_hash_rows = self.db.fetchall(
                            f"""
                            SELECT DISTINCT CAST("content_hash" AS VARCHAR)
                            FROM global_tables.{quoted_table}
                            WHERE CAST("content_hash" AS VARCHAR) IN (SELECT UNNEST(?))
                              AND "content_hash" IS NOT NULL
                            """,
                            [sorted_purged_hashes],
                        )
                        surviving_content_hashes.update(
                            str(row[0])
                            for row in surviving_hash_rows
                            if row[0] is not None
                        )

                    unreferenced_hashes = sorted(
                        purged_content_hashes - surviving_content_hashes
                    )
                    if unreferenced_hashes:
                        for table in prunable_cache_tables:
                            quoted_table = self._quote_ident(table)
                            self.db.execute(
                                f"""
                                DELETE FROM global_tables.{quoted_table}
                                WHERE CAST("content_hash" AS VARCHAR) IN (SELECT UNNEST(?))
                                """,
                                [unreferenced_hashes],
                            )

        for json_path in plan.json_files:
            try:
                json_path.unlink(missing_ok=True)
                json_path.with_name(f"{json_path.stem}.locations.json").unlink(
                    missing_ok=True
                )
            except OSError:
                continue

        if delete_files and plan.disk_files:
            safe_unshared_files = self._filter_paths_against_surviving_artifacts(
                plan.disk_files
            )
            for disk_path in safe_unshared_files:
                try:
                    if not disk_path.exists() and not disk_path.is_symlink():
                        continue
                    if disk_path.is_dir() and not disk_path.is_symlink():
                        shutil.rmtree(disk_path, ignore_errors=True)
                    else:
                        disk_path.unlink(missing_ok=True)
                except OSError:
                    continue

        self._log_audit(
            "purge",
            (
                f"runs={len(plan.run_ids)} "
                f"children={len(plan.child_run_ids)} "
                f"orphaned_artifacts={len(plan.orphaned_artifact_ids)} "
                f"json_files={len(plan.json_files)} "
                f"delete_ingested_data={delete_ingested_data} "
                f"prune_cache={prune_cache} "
                f"delete_files={delete_files}"
            ),
        )

        return PurgeResult(
            plan=plan,
            executed=True,
            ingested_data_skipped=ingested_data_skipped,
            skipped_unsafe_json_files=(
                list(plan.unsafe_json_files) if unsafe_delete_targets == "skip" else []
            ),
            skipped_unsafe_disk_files=(
                list(plan.unsafe_disk_files) if unsafe_delete_targets == "skip" else []
            ),
        )

    def fix_status(
        self,
        run_id: str,
        new_status: str,
        *,
        reason: Optional[str],
        force: bool = False,
    ) -> Run:
        normalized_run_id = str(run_id).strip()
        normalized_status = str(new_status).strip().lower()
        allowed_statuses = {"running", "completed", "failed"}
        if normalized_status not in allowed_statuses:
            allowed_values = ", ".join(sorted(allowed_statuses))
            raise ValueError(
                f"Invalid status {new_status!r}. Expected one of: {allowed_values}."
            )

        now = datetime.now(UTC)

        with self.db.transaction():
            run = self.db.get_run(normalized_run_id)
            if run is None:
                raise ValueError(f"Run not found: {normalized_run_id}")

            previous_status = str(run.status).strip().lower()
            if (
                normalized_status == "running"
                and previous_status in {"completed", "failed"}
                and not force
            ):
                raise ValueError(
                    "Use --force to transition a terminal run back to running."
                )

            run.status = normalized_status
            run.updated_at = now
            if normalized_status in {"completed", "failed"}:
                if run.ended_at is None:
                    run.ended_at = now
            else:
                run.ended_at = None

            if reason is not None:
                run_meta = dict(run.meta) if isinstance(run.meta, Mapping) else {}
                run_meta["status_fix_reason"] = reason
                run.meta = run_meta

            self.db.upsert_run(run)
            updated_run = self.db.get_run(normalized_run_id) or run

        self._log_audit(
            "fix_status",
            (
                f"run_id={updated_run.id} "
                f"status={updated_run.status} "
                f"reason={reason or ''}"
            ),
        )
        return updated_run

    def export(
        self,
        run_ids: Iterable[str] | str,
        out_path: Path,
        *,
        include_data: bool,
        include_snapshots: bool,
        include_children: bool = True,
        dry_run: bool = False,
    ) -> ExportResult:
        out_path = Path(out_path)
        self._ensure_distinct_from_source_db(out_path)

        normalized_run_ids = self._normalize_run_ids(run_ids)
        expanded_run_ids = (
            self._expand_run_ids(normalized_run_ids)
            if include_children
            else normalized_run_ids
        )

        runs_by_id = self._load_runs_by_ids(expanded_run_ids)
        selected_run_ids = [
            run_id for run_id in expanded_run_ids if run_id in runs_by_id
        ]

        discovered_tables = self._discover_global_tables()
        ingested_table_modes = {
            table: self._classify_global_table(table) for table in discovered_tables
        }
        ingested_rows: dict[str, int] = {}
        unscoped_cache_tables_skipped: list[str] = []
        artifact_count = 0

        if dry_run:
            if selected_run_ids:
                artifact_rows = self.db.fetchall(
                    """
                    SELECT DISTINCT CAST(artifact_id AS VARCHAR)
                    FROM run_artifact_link
                    WHERE run_id IN (SELECT UNNEST(?))
                    ORDER BY 1
                    """,
                    [selected_run_ids],
                )
                artifact_count = len(artifact_rows)

            if include_data and selected_run_ids:
                candidate_rows = self._global_table_row_counts(
                    discovered_tables,
                    selected_run_ids,
                )
                for table in discovered_tables:
                    mode = ingested_table_modes.get(table)
                    if mode == "unscoped_cache":
                        unscoped_cache_tables_skipped.append(table)
                        continue
                    ingested_rows[table] = candidate_rows.get(table, 0)

            return ExportResult(
                run_ids=selected_run_ids,
                artifact_count=artifact_count,
                out_path=out_path,
                ingested_rows=ingested_rows,
                ingested_table_modes=ingested_table_modes,
                unscoped_cache_tables_skipped=unscoped_cache_tables_skipped,
                snapshots_copied=0,
            )

        self._init_shard_schema(out_path)

        alias = "shard_export"
        with self._attached_database(
            out_path, alias, read_only=False, mutate_main=False
        ) as cur:
            cur.execute(
                f"CREATE SCHEMA IF NOT EXISTS {self._quote_ident(alias)}.global_tables"
            )
            source_schema = "main"
            target_schema = "main"

            def _core_columns(table_name: str) -> list[str]:
                src_cols = set(
                    self._list_table_columns_cur(cur, table_name, schema=source_schema)
                )
                dst_cols = self._list_table_columns_cur(
                    cur, table_name, schema=target_schema, catalog=alias
                )
                return [
                    self._quote_ident(col_name)
                    for col_name in dst_cols
                    if col_name in src_cols
                ]

            if selected_run_ids:
                run_columns = _core_columns("run")
                if run_columns:
                    col_sql = ", ".join(run_columns)
                    cur.execute(
                        f"""
                        INSERT INTO {self._qualified_table_sql("run", catalog=alias)} ({col_sql})
                        SELECT {col_sql}
                        FROM {self._qualified_table_sql("run")}
                        WHERE id IN (SELECT UNNEST(?))
                        """,
                        [selected_run_ids],
                    )

                link_columns = _core_columns("run_artifact_link")
                if link_columns:
                    col_sql = ", ".join(link_columns)
                    cur.execute(
                        f"""
                        INSERT INTO {self._qualified_table_sql("run_artifact_link", catalog=alias)} ({col_sql})
                        SELECT {col_sql}
                        FROM {self._qualified_table_sql("run_artifact_link")}
                        WHERE run_id IN (SELECT UNNEST(?))
                        """,
                        [selected_run_ids],
                    )

                artifact_rows = cur.execute(
                    """
                    SELECT DISTINCT CAST(artifact_id AS VARCHAR)
                    FROM main.run_artifact_link
                    WHERE run_id IN (SELECT UNNEST(?))
                    ORDER BY 1
                    """,
                    [selected_run_ids],
                ).fetchall()
                artifact_ids = [str(row[0]) for row in artifact_rows]
                artifact_count = len(artifact_ids)

                facet_cols = _core_columns("config_facet")
                if facet_cols:
                    col_sql = ", ".join(facet_cols)
                    cur.execute(
                        f"""
                        INSERT INTO {self._qualified_table_sql("config_facet", catalog=alias)} ({col_sql})
                        SELECT {col_sql}
                        FROM {self._qualified_table_sql("config_facet")}
                        WHERE id IN (
                            SELECT DISTINCT facet_id FROM main.run
                            WHERE id IN (SELECT UNNEST(?)) AND facet_id IS NOT NULL
                            UNION
                            SELECT DISTINCT facet_id FROM main.run_config_kv
                            WHERE run_id IN (SELECT UNNEST(?)) AND facet_id IS NOT NULL
                        )
                        ON CONFLICT (id) DO NOTHING
                        """,
                        [selected_run_ids, selected_run_ids],
                    )

                run_kv_cols = _core_columns("run_config_kv")
                if run_kv_cols:
                    col_sql = ", ".join(run_kv_cols)
                    cur.execute(
                        f"""
                        INSERT INTO {self._qualified_table_sql("run_config_kv", catalog=alias)} ({col_sql})
                        SELECT {col_sql}
                        FROM {self._qualified_table_sql("run_config_kv")}
                        WHERE run_id IN (SELECT UNNEST(?))
                        """,
                        [selected_run_ids],
                    )

                if artifact_ids:
                    art_cols = _core_columns("artifact")
                    if art_cols:
                        col_sql = ", ".join(art_cols)
                        cur.execute(
                            f"""
                            INSERT INTO {self._qualified_table_sql("artifact", catalog=alias)} ({col_sql})
                            SELECT {col_sql}
                            FROM {self._qualified_table_sql("artifact")}
                            WHERE CAST(id AS VARCHAR) IN (SELECT UNNEST(?))
                            """,
                            [artifact_ids],
                        )

                    art_facet_cols = _core_columns("artifact_facet")
                    if art_facet_cols:
                        col_sql = ", ".join(art_facet_cols)
                        cur.execute(
                            f"""
                            INSERT INTO {self._qualified_table_sql("artifact_facet", catalog=alias)} ({col_sql})
                            SELECT {col_sql}
                            FROM {self._qualified_table_sql("artifact_facet")}
                            WHERE id IN (
                                SELECT DISTINCT facet_id FROM main.artifact
                                WHERE CAST(id AS VARCHAR) IN (SELECT UNNEST(?))
                                  AND facet_id IS NOT NULL
                                UNION
                                SELECT DISTINCT facet_id FROM main.artifact_kv
                                WHERE CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                                  AND facet_id IS NOT NULL
                            )
                            ON CONFLICT (id) DO NOTHING
                            """,
                            [artifact_ids, artifact_ids],
                        )

                    art_kv_cols = _core_columns("artifact_kv")
                    if art_kv_cols:
                        col_sql = ", ".join(art_kv_cols)
                        cur.execute(
                            f"""
                            INSERT INTO {self._qualified_table_sql("artifact_kv", catalog=alias)} ({col_sql})
                            SELECT {col_sql}
                            FROM {self._qualified_table_sql("artifact_kv")}
                            WHERE CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                            """,
                            [artifact_ids],
                        )

                    loc_cols = _core_columns("artifact_location")
                    if loc_cols:
                        col_sql = ", ".join(loc_cols)
                        sel_exprs: list[str] = []
                        sel_params: list[Any] = []
                        for col in loc_cols:
                            if col == '"artifact_id"':
                                sel_exprs.append(
                                    'CASE WHEN CAST("al"."artifact_id" AS VARCHAR) IN (SELECT UNNEST(?)) '
                                    'THEN "al"."artifact_id" ELSE NULL END AS "artifact_id"'
                                )
                                sel_params.append(artifact_ids)
                            else:
                                sel_exprs.append(f'"al".{col}')
                        export_match_sql = self._artifact_location_digest_match_sql(
                            loc_alias='"al"', art_alias='"a"'
                        )
                        cur.execute(
                            f"""
                            INSERT INTO {self._qualified_table_sql("artifact_location", catalog=alias)} ({col_sql})
                            SELECT {", ".join(sel_exprs)}
                            FROM {self._qualified_table_sql("artifact_location")} AS "al"
                            WHERE CAST("al"."artifact_id" AS VARCHAR) IN (SELECT UNNEST(?))
                               OR (
                                   "al"."digest" IS NOT NULL
                                   AND EXISTS (
                                       SELECT 1 FROM main."artifact" AS "a"
                                       WHERE CAST("a"."id" AS VARCHAR) IN (SELECT UNNEST(?))
                                         AND {export_match_sql}
                                   )
                               )
                            ON CONFLICT (location_id) DO NOTHING
                            """,
                            [*sel_params, artifact_ids, artifact_ids],
                        )

                obs_cols = _core_columns("artifact_schema_observation")
                if obs_cols:
                    obs_params: list[Any] = [selected_run_ids]
                    obs_where = "run_id IN (SELECT UNNEST(?))"
                    if artifact_ids:
                        obs_where = f"({obs_where} OR CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?)))"
                        obs_params.append(artifact_ids)
                    schema_rows = cur.execute(
                        f"""
                        SELECT DISTINCT CAST(schema_id AS VARCHAR)
                        FROM main.artifact_schema_observation
                        WHERE {obs_where} AND schema_id IS NOT NULL
                        ORDER BY 1
                        """,
                        obs_params,
                    ).fetchall()
                    schema_ids = [str(row[0]) for row in schema_rows]
                    if schema_ids:
                        for schema_tbl in (
                            "artifact_schema",
                            "artifact_schema_field",
                            "artifact_schema_relation",
                        ):
                            st_cols = _core_columns(schema_tbl)
                            if st_cols:
                                key_col = (
                                    "id"
                                    if schema_tbl == "artifact_schema"
                                    else "schema_id"
                                )
                                col_sql = ", ".join(st_cols)
                                cur.execute(
                                    f"""
                                    INSERT INTO {self._qualified_table_sql(schema_tbl, catalog=alias)} ({col_sql})
                                    SELECT {col_sql}
                                    FROM {self._qualified_table_sql(schema_tbl)}
                                    WHERE CAST({self._quote_ident(key_col)} AS VARCHAR) IN (SELECT UNNEST(?))
                                    """,
                                    [schema_ids],
                                )
                    col_sql = ", ".join(obs_cols)
                    cur.execute(
                        f"""
                        INSERT INTO {self._qualified_table_sql("artifact_schema_observation", catalog=alias)} ({col_sql})
                        SELECT {col_sql}
                        FROM {self._qualified_table_sql("artifact_schema_observation")}
                        WHERE {obs_where}
                        """,
                        obs_params,
                    )

            if include_data and selected_run_ids:
                for table in discovered_tables:
                    mode = ingested_table_modes.get(table)
                    if mode == "unscoped_cache":
                        unscoped_cache_tables_skipped.append(table)
                        continue
                    safe_table = self._validate_identifier(table, label="table")
                    quoted_table = self._quote_ident(safe_table)
                    filter_sql = self._resolve_global_table_filter_sql(
                        safe_table,
                        selected_run_ids,
                        table_alias="src_gt",
                    )
                    if filter_sql is None:
                        continue
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS
                            {self._quote_ident(alias)}.global_tables.{quoted_table}
                        AS
                        SELECT *
                        FROM global_tables.{quoted_table}
                        WHERE 1 = 0
                        """
                    )
                    count_row = cur.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM global_tables.{quoted_table} AS "src_gt"
                        WHERE {filter_sql}
                        """
                    ).fetchone()
                    row_count = int(count_row[0] if count_row else 0)
                    ingested_rows[safe_table] = row_count
                    if row_count <= 0:
                        continue
                    cur.execute(
                        f"""
                        INSERT INTO {self._quote_ident(alias)}.global_tables.{quoted_table}
                        SELECT *
                        FROM global_tables.{quoted_table} AS "src_gt"
                        WHERE {filter_sql}
                        """
                    )

        snapshots_copied = 0
        if include_snapshots:
            shard_snapshot_dir = out_path.parent / "shard_snapshots"
            shard_snapshot_dir.mkdir(parents=True, exist_ok=True)
            for run_id in selected_run_ids:
                source_snapshot = self._resolve_run_snapshot_path(
                    run_id, runs_by_id.get(run_id)
                )
                if not source_snapshot.exists():
                    continue
                destination_snapshot = shard_snapshot_dir / source_snapshot.name
                shutil.copy2(source_snapshot, destination_snapshot)
                sidecar_src = source_snapshot.with_name(
                    f"{source_snapshot.stem}.locations.json"
                )
                if sidecar_src.is_file():
                    shutil.copy2(sidecar_src, shard_snapshot_dir / sidecar_src.name)
                snapshots_copied += 1

        self._log_audit(
            "export",
            (
                f"out_path={out_path} "
                f"runs={len(selected_run_ids)} "
                f"artifacts={artifact_count} "
                f"include_data={include_data} "
                f"include_snapshots={include_snapshots}"
            ),
        )

        return ExportResult(
            run_ids=selected_run_ids,
            artifact_count=artifact_count,
            out_path=out_path,
            ingested_rows=ingested_rows,
            ingested_table_modes=ingested_table_modes,
            unscoped_cache_tables_skipped=unscoped_cache_tables_skipped,
            snapshots_copied=snapshots_copied,
        )

    def merge(
        self,
        shard_path: Path,
        *,
        conflict: str,
        include_snapshots: bool,
        dry_run: bool = False,
    ) -> MergeResult:
        shard_path = Path(shard_path)
        if not shard_path.exists():
            raise ValueError(f"Shard database does not exist: {shard_path}")

        conflict_mode = str(conflict).strip().lower()
        if conflict_mode not in {"error", "skip"}:
            raise ValueError("conflict must be one of: error, skip")

        with self._mountable_shard_path(shard_path) as mount_path:
            shard_db = self.db.__class__(str(mount_path), read_only=True)
            try:
                with _track_maintenance_merge_phase("merge.load_shard_run_ids"):
                    shard_run_columns = self._list_table_columns("run", db=shard_db)
                    if shard_run_columns and "id" in shard_run_columns:
                        shard_run_rows = shard_db.fetchall(
                            'SELECT "id" FROM "run" ORDER BY "id"'
                        )
                        shard_run_ids = [str(row[0]) for row in shard_run_rows]
                    else:
                        shard_run_ids = []
                with _track_maintenance_merge_phase("merge.discover_global_tables"):
                    shard_global_tables = self._discover_global_tables(db=shard_db)
                with _track_maintenance_merge_phase("merge.classify_global_tables"):
                    shard_table_modes: dict[str, GlobalTableMode] = {
                        table: self._classify_global_table(table, db=shard_db)
                        for table in shard_global_tables
                    }

                with _track_maintenance_merge_phase("merge.detect_run_conflicts"):
                    if shard_run_ids:
                        existing_rows = self.db.fetchall(
                            """
                            SELECT CAST("id" AS VARCHAR)
                            FROM main."run"
                            WHERE "id" IN (SELECT UNNEST(?))
                            ORDER BY 1
                            """,
                            [shard_run_ids],
                        )
                        existing_run_ids = {str(row[0]) for row in existing_rows}
                    else:
                        existing_run_ids = set()

                conflicts_detected = sorted(
                    existing_run_ids.intersection(shard_run_ids)
                )
                conflict_set = set(conflicts_detected)

                if conflicts_detected and conflict_mode == "error":
                    raise ValueError(
                        "Run ID conflicts detected: " + ", ".join(conflicts_detected)
                    )

                if conflict_mode == "skip":
                    runs_skipped = conflicts_detected
                    runs_to_merge = [
                        run_id for run_id in shard_run_ids if run_id not in conflict_set
                    ]
                else:
                    runs_skipped = []
                    runs_to_merge = list(shard_run_ids)

                incompatible_global_tables_skipped: dict[str, str] = {}
                if runs_to_merge:
                    compatibility_issues: dict[str, str] = {}
                    with _track_maintenance_merge_phase(
                        "merge.check_global_table_compatibility"
                    ):
                        for table, mode in shard_table_modes.items():
                            if mode == "unscoped_cache":
                                continue
                            compatibility_issue = (
                                self._global_table_merge_compatibility_issue(
                                    table,
                                    mode=mode,
                                    source_db=shard_db,
                                    target_db=self.db,
                                )
                            )
                            if compatibility_issue:
                                compatibility_issues[table] = compatibility_issue

                    if compatibility_issues:
                        incompatible_global_tables_skipped = {
                            table: compatibility_issues[table]
                            for table in sorted(compatibility_issues)
                        }
                        if conflict_mode == "error":
                            details = "; ".join(
                                f"{table}: {reason}"
                                for table, reason in incompatible_global_tables_skipped.items()
                            )
                            raise ValueError(
                                "Global table schema compatibility check failed: "
                                + details
                            )
                shard_link_columns = self._list_table_columns(
                    "run_artifact_link", db=shard_db
                )
            finally:
                shard_db.close()

            artifacts_merged = 0
            ingested_tables_merged: list[str] = []
            unscoped_cache_tables_skipped: list[str] = []

            if dry_run:
                if runs_to_merge:
                    dry_run_alias = "shard_merge_dry_run"
                    with self._attached_database(
                        mount_path, dry_run_alias, read_only=True, mutate_main=False
                    ) as cur:
                        with _track_maintenance_merge_phase(
                            "merge.dry_run.select_artifacts"
                        ):
                            if {"run_id", "artifact_id"}.issubset(
                                set(shard_link_columns)
                            ):
                                row = cur.execute(
                                    f"""
                                    SELECT COUNT(DISTINCT CAST("src_link"."artifact_id" AS VARCHAR))
                                    FROM {self._qualified_table_sql("run_artifact_link", catalog=dry_run_alias)} AS "src_link"
                                    WHERE "src_link"."run_id" IN (SELECT UNNEST(?))
                                      AND "src_link"."artifact_id" IS NOT NULL
                                      AND NOT EXISTS (
                                          SELECT 1
                                          FROM main."artifact" AS "dst_art"
                                          WHERE CAST("dst_art"."id" AS VARCHAR)
                                                = CAST("src_link"."artifact_id" AS VARCHAR)
                                      )
                                    """,
                                    [runs_to_merge],
                                ).fetchone()
                                artifacts_merged = int(row[0] if row else 0)

                        with _track_maintenance_merge_phase(
                            "merge.dry_run.count_global_tables"
                        ):
                            for table in shard_global_tables:
                                mode = shard_table_modes.get(table)
                                if mode == "unscoped_cache":
                                    unscoped_cache_tables_skipped.append(table)
                                    continue
                                if table in incompatible_global_tables_skipped:
                                    continue
                                run_col = (
                                    "consist_run_id"
                                    if mode == "run_scoped"
                                    else "run_id"
                                )
                                quoted_table = self._quote_ident(
                                    self._validate_identifier(table, label="table")
                                )
                                row = cur.execute(
                                    f"""
                                    SELECT COUNT(*)
                                    FROM {self._quote_ident(dry_run_alias)}.global_tables.{quoted_table}
                                    WHERE {self._quote_ident(run_col)} IN (SELECT UNNEST(?))
                                    """,
                                    [runs_to_merge],
                                ).fetchone()
                                if int(row[0] if row else 0) > 0:
                                    ingested_tables_merged.append(table)

                snapshots_merged = 0
                with _track_maintenance_merge_phase("merge.dry_run.count_snapshots"):
                    if include_snapshots and runs_to_merge:
                        source_snapshot_dir = shard_path.parent / "shard_snapshots"
                        for run_id in runs_to_merge:
                            source_name = self._resolve_run_snapshot_path(
                                run_id, None
                            ).name
                            source_snapshot = source_snapshot_dir / source_name
                            if not source_snapshot.exists():
                                continue
                            destination_snapshot = self._resolve_run_snapshot_path(
                                run_id, None
                            )
                            if destination_snapshot.exists():
                                continue
                            snapshots_merged += 1

                return MergeResult(
                    shard_path=shard_path,
                    runs_merged=runs_to_merge,
                    runs_skipped=runs_skipped,
                    artifacts_merged=artifacts_merged,
                    ingested_tables_merged=sorted(set(ingested_tables_merged)),
                    unscoped_cache_tables_skipped=sorted(
                        set(unscoped_cache_tables_skipped)
                    ),
                    conflicts_detected=conflicts_detected,
                    snapshots_merged=snapshots_merged,
                    incompatible_global_tables_skipped=incompatible_global_tables_skipped,
                )

            alias = "shard_merge"
            with _track_maintenance_merge_phase("merge.write.total"):
                with self._attached_database(
                    mount_path, alias, read_only=True, mutate_main=True
                ) as cur:
                    with _track_maintenance_merge_phase("merge.write.setup_schemas"):
                        cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")

                    if runs_to_merge:
                        source_schema = "main"
                        target_schema = "main"

                        def _core_columns(table_name: str) -> list[str]:
                            src_cols = set(
                                self._list_table_columns_cur(
                                    cur, table_name, schema=source_schema, catalog=alias
                                )
                            )
                            dst_cols = self._list_table_columns_cur(
                                cur, table_name, schema=target_schema
                            )
                            return [
                                self._quote_ident(col_name)
                                for col_name in dst_cols
                                if col_name in src_cols
                            ]

                        def _source_select_columns(
                            table_alias: str, columns: list[str]
                        ) -> str:
                            safe_alias = self._validate_identifier(
                                table_alias, label="table_alias"
                            )
                            return ", ".join(
                                f"{self._quote_ident(safe_alias)}.{column}"
                                for column in columns
                            )

                        run_columns = _core_columns("run")
                        if run_columns:
                            col_sql = ", ".join(run_columns)
                            sel_sql = _source_select_columns("src_run", run_columns)
                            cur.execute(
                                f"""
                                INSERT INTO main."run" ({col_sql})
                                SELECT {sel_sql}
                                FROM {self._qualified_table_sql("run", catalog=alias)} AS "src_run"
                                WHERE "src_run"."id" IN (SELECT UNNEST(?))
                                ON CONFLICT ("id") DO NOTHING
                                """,
                                [runs_to_merge],
                            )

                        artifact_ids: list[str] = []
                        if {"run_id", "artifact_id"}.issubset(set(shard_link_columns)):
                            art_id_rows = cur.execute(
                                f"""
                                SELECT DISTINCT CAST("artifact_id" AS VARCHAR)
                                FROM {self._qualified_table_sql("run_artifact_link", catalog=alias)}
                                WHERE "run_id" IN (SELECT UNNEST(?))
                                  AND "artifact_id" IS NOT NULL
                                ORDER BY 1
                                """,
                                [runs_to_merge],
                            ).fetchall()
                            artifact_ids = [str(r[0]) for r in art_id_rows]

                        link_columns = _core_columns("run_artifact_link")
                        if link_columns:
                            col_sql = ", ".join(link_columns)
                            sel_sql = _source_select_columns("src_link", link_columns)
                            cur.execute(
                                f"""
                                INSERT INTO main."run_artifact_link" ({col_sql})
                                SELECT {sel_sql}
                                FROM {self._qualified_table_sql("run_artifact_link", catalog=alias)} AS "src_link"
                                WHERE "src_link"."run_id" IN (SELECT UNNEST(?))
                                ON CONFLICT DO NOTHING
                                """,
                                [runs_to_merge],
                            )

                        if artifact_ids:
                            new_art_row = cur.execute(
                                f"""
                                SELECT COUNT(*)
                                FROM {self._qualified_table_sql("artifact", catalog=alias)} AS "src_art"
                                WHERE CAST("src_art"."id" AS VARCHAR) IN (SELECT UNNEST(?))
                                  AND NOT EXISTS (
                                      SELECT 1 FROM main."artifact" AS "dst_art"
                                      WHERE CAST("dst_art"."id" AS VARCHAR)
                                            = CAST("src_art"."id" AS VARCHAR)
                                  )
                                """,
                                [artifact_ids],
                            ).fetchone()
                            artifacts_merged = int(new_art_row[0] if new_art_row else 0)
                            art_columns = _core_columns("artifact")
                            if art_columns:
                                col_sql = ", ".join(art_columns)
                                sel_sql = _source_select_columns("src_art", art_columns)
                                cur.execute(
                                    f"""
                                    INSERT INTO main."artifact" ({col_sql})
                                    SELECT {sel_sql}
                                    FROM {self._qualified_table_sql("artifact", catalog=alias)} AS "src_art"
                                    WHERE CAST("src_art"."id" AS VARCHAR) IN (SELECT UNNEST(?))
                                    ON CONFLICT ("id") DO NOTHING
                                    """,
                                    [artifact_ids],
                                )

                            art_facet_cols = _core_columns("artifact_facet")
                            if art_facet_cols:
                                col_sql = ", ".join(art_facet_cols)
                                sel_sql = _source_select_columns(
                                    "src_af", art_facet_cols
                                )
                                cur.execute(
                                    f"""
                                    INSERT INTO main."artifact_facet" ({col_sql})
                                    SELECT {sel_sql}
                                    FROM {self._qualified_table_sql("artifact_facet", catalog=alias)} AS "src_af"
                                    ON CONFLICT ("id") DO NOTHING
                                    """
                                )

                            art_kv_cols = _core_columns("artifact_kv")
                            if art_kv_cols:
                                col_sql = ", ".join(art_kv_cols)
                                sel_sql = _source_select_columns("src_akv", art_kv_cols)
                                cur.execute(
                                    f"""
                                    INSERT INTO main."artifact_kv" ({col_sql})
                                    SELECT {sel_sql}
                                    FROM {self._qualified_table_sql("artifact_kv", catalog=alias)} AS "src_akv"
                                    WHERE CAST("src_akv"."artifact_id" AS VARCHAR) IN (SELECT UNNEST(?))
                                    ON CONFLICT DO NOTHING
                                    """,
                                    [artifact_ids],
                                )

                            loc_cols = _core_columns("artifact_location")
                            if loc_cols:
                                col_sql = ", ".join(loc_cols)
                                sel_exprs: list[str] = []
                                sel_params: list[Any] = []
                                for col in loc_cols:
                                    if col == '"artifact_id"':
                                        sel_exprs.append(
                                            'CASE WHEN CAST("src_loc"."artifact_id" AS VARCHAR) IN (SELECT UNNEST(?)) '
                                            'THEN "src_loc"."artifact_id" ELSE NULL END AS "artifact_id"'
                                        )
                                        sel_params.append(artifact_ids)
                                    else:
                                        sel_exprs.append(f'"src_loc".{col}')
                                upd_cols = [
                                    (
                                        '"artifact_id" = COALESCE(EXCLUDED."artifact_id", main."artifact_location"."artifact_id")'
                                        if c == '"artifact_id"'
                                        else f"{c} = EXCLUDED.{c}"
                                    )
                                    for c in loc_cols
                                    if c != '"location_id"'
                                ]
                                conflict_sql = (
                                    f'ON CONFLICT ("location_id") DO UPDATE SET {", ".join(upd_cols)} '
                                    'WHERE EXCLUDED."observed_at_ns" >= main."artifact_location"."observed_at_ns"'
                                    if '"observed_at_ns"' in loc_cols and upd_cols
                                    else 'ON CONFLICT ("location_id") DO NOTHING'
                                )
                                merge_match_sql = (
                                    self._artifact_location_digest_match_sql(
                                        loc_alias='"src_loc"', art_alias='"src_art"'
                                    )
                                    if {'"digest"', '"site"', '"member"'}.issubset(
                                        loc_cols
                                    )
                                    else '"src_art"."digest" = "src_loc"."digest"'
                                )
                                cur.execute(
                                    f"""
                                    INSERT INTO main."artifact_location" ({col_sql})
                                    SELECT {", ".join(sel_exprs)}
                                    FROM {self._qualified_table_sql("artifact_location", catalog=alias)} AS "src_loc"
                                    WHERE CAST("src_loc"."artifact_id" AS VARCHAR) IN (SELECT UNNEST(?))
                                       OR (
                                           "src_loc"."digest" IS NOT NULL
                                           AND EXISTS (
                                               SELECT 1
                                               FROM {self._qualified_table_sql("artifact", catalog=alias)} AS "src_art"
                                               WHERE CAST("src_art"."id" AS VARCHAR) IN (SELECT UNNEST(?))
                                                 AND {merge_match_sql}
                                           )
                                       )
                                    {conflict_sql}
                                    """,
                                    [*sel_params, artifact_ids, artifact_ids],
                                )
                                cur.execute(
                                    """
                                    UPDATE main."artifact_location" AS al
                                    SET status = 'stale'
                                    WHERE al.status = 'active'
                                      AND al.site != 'advisory'
                                      AND EXISTS (
                                          SELECT 1 FROM main."artifact_location" AS newer
                                          WHERE newer.status = 'active'
                                            AND newer.site = al.site
                                            AND newer.mount_name = al.mount_name
                                            AND newer.mount_instance_id = al.mount_instance_id
                                            AND newer.uri = al.uri
                                            AND newer.member = al.member
                                            AND newer.location_id != al.location_id
                                            AND (
                                                al.site IN ('workspace', 'external')
                                                OR newer.digest IS DISTINCT FROM al.digest
                                                OR (
                                                    (
                                                        al.digest LIKE 'stat_file_v1:%'
                                                        OR al.digest LIKE 'stat_dir_v1:%'
                                                    )
                                                    AND newer.artifact_id IS DISTINCT FROM al.artifact_id
                                                )
                                            )
                                            AND (
                                                newer.observed_at_ns > al.observed_at_ns
                                                OR (
                                                    newer.observed_at_ns = al.observed_at_ns
                                                    AND CAST(newer.location_id AS VARCHAR) > CAST(al.location_id AS VARCHAR)
                                                )
                                            )
                                      )
                                    """
                                )
                                self.db.bump_location_epoch()

                        cfg_facet_cols = _core_columns("config_facet")
                        if cfg_facet_cols:
                            col_sql = ", ".join(cfg_facet_cols)
                            sel_sql = _source_select_columns("src_cf", cfg_facet_cols)
                            cur.execute(
                                f"""
                                INSERT INTO main."config_facet" ({col_sql})
                                SELECT {sel_sql}
                                FROM {self._qualified_table_sql("config_facet", catalog=alias)} AS "src_cf"
                                ON CONFLICT ("id") DO NOTHING
                                """
                            )

                        run_kv_cols = _core_columns("run_config_kv")
                        if run_kv_cols:
                            col_sql = ", ".join(run_kv_cols)
                            sel_sql = _source_select_columns("src_cfg", run_kv_cols)
                            cur.execute(
                                f"""
                                INSERT INTO main."run_config_kv" ({col_sql})
                                SELECT {sel_sql}
                                FROM {self._qualified_table_sql("run_config_kv", catalog=alias)} AS "src_cfg"
                                WHERE "src_cfg"."run_id" IN (SELECT UNNEST(?))
                                ON CONFLICT DO NOTHING
                                """,
                                [runs_to_merge],
                            )

                        obs_cols = _core_columns("artifact_schema_observation")
                        if obs_cols:
                            obs_params: list[Any] = [runs_to_merge]
                            obs_where = '"src_obs"."run_id" IN (SELECT UNNEST(?))'
                            if artifact_ids:
                                obs_where = f'({obs_where} OR CAST("src_obs"."artifact_id" AS VARCHAR) IN (SELECT UNNEST(?)))'
                                obs_params.append(artifact_ids)
                            schema_rows = cur.execute(
                                f"""
                                SELECT DISTINCT CAST("src_obs"."schema_id" AS VARCHAR)
                                FROM {self._qualified_table_sql("artifact_schema_observation", catalog=alias)} AS "src_obs"
                                WHERE {obs_where} AND "src_obs"."schema_id" IS NOT NULL
                                """,
                                obs_params,
                            ).fetchall()
                            schema_ids = [str(r[0]) for r in schema_rows]
                            if schema_ids:
                                for schema_tbl in (
                                    "artifact_schema",
                                    "artifact_schema_field",
                                    "artifact_schema_relation",
                                ):
                                    st_cols = _core_columns(schema_tbl)
                                    if st_cols:
                                        key_col = (
                                            "id"
                                            if schema_tbl == "artifact_schema"
                                            else "schema_id"
                                        )
                                        col_sql = ", ".join(st_cols)
                                        sel_sql = _source_select_columns(
                                            "src_s", st_cols
                                        )
                                        cur.execute(
                                            f"""
                                            INSERT INTO {self._qualified_table_sql(schema_tbl)} ({col_sql})
                                            SELECT {sel_sql}
                                            FROM {self._qualified_table_sql(schema_tbl, catalog=alias)} AS "src_s"
                                            WHERE CAST("src_s".{self._quote_ident(key_col)} AS VARCHAR) IN (SELECT UNNEST(?))
                                            ON CONFLICT DO NOTHING
                                            """,
                                            [schema_ids],
                                        )
                            col_sql = ", ".join(obs_cols)
                            sel_sql = _source_select_columns("src_obs", obs_cols)
                            cur.execute(
                                f"""
                                INSERT INTO main."artifact_schema_observation" ({col_sql})
                                SELECT {sel_sql}
                                FROM {self._qualified_table_sql("artifact_schema_observation", catalog=alias)} AS "src_obs"
                                WHERE {obs_where}
                                ON CONFLICT DO NOTHING
                                """,
                                obs_params,
                            )

                        with _track_maintenance_merge_phase(
                            "merge.write.global_tables"
                        ):
                            for table in shard_global_tables:
                                mode = shard_table_modes.get(table)
                                if mode == "unscoped_cache":
                                    unscoped_cache_tables_skipped.append(table)
                                    continue
                                if table in incompatible_global_tables_skipped:
                                    continue
                                safe_table = self._validate_identifier(
                                    table, label="table"
                                )
                                quoted_table = self._quote_ident(safe_table)
                                cur.execute(
                                    f"""
                                    CREATE TABLE IF NOT EXISTS global_tables.{quoted_table}
                                    AS
                                    SELECT *
                                    FROM {self._quote_ident(alias)}.global_tables.{quoted_table}
                                    WHERE 1 = 0
                                    """
                                )
                                run_col = (
                                    "consist_run_id"
                                    if mode == "run_scoped"
                                    else "run_id"
                                )
                                count_row = cur.execute(
                                    f"""
                                    SELECT COUNT(*)
                                    FROM {self._quote_ident(alias)}.global_tables.{quoted_table}
                                    WHERE {self._quote_ident(run_col)} IN (SELECT UNNEST(?))
                                    """,
                                    [runs_to_merge],
                                ).fetchone()
                                if int(count_row[0] if count_row else 0) <= 0:
                                    continue
                                src_cols = set(
                                    self._list_table_columns_cur(
                                        cur,
                                        safe_table,
                                        schema="global_tables",
                                        catalog=alias,
                                    )
                                )
                                dst_cols = self._list_table_columns_cur(
                                    cur, safe_table, schema="global_tables"
                                )
                                shared_cols = [
                                    self._quote_ident(c)
                                    for c in dst_cols
                                    if c in src_cols
                                ]
                                if not shared_cols:
                                    continue
                                col_sql = ", ".join(shared_cols)
                                cur.execute(
                                    f"""
                                    INSERT INTO global_tables.{quoted_table} ({col_sql})
                                    SELECT {col_sql}
                                    FROM {self._quote_ident(alias)}.global_tables.{quoted_table}
                                    WHERE {self._quote_ident(run_col)} IN (SELECT UNNEST(?))
                                    """,
                                    [runs_to_merge],
                                )
                                ingested_tables_merged.append(safe_table)

            snapshots_merged = 0
            with _track_maintenance_merge_phase("merge.copy_snapshots"):
                if include_snapshots and runs_to_merge:
                    source_snapshot_dir = shard_path.parent / "shard_snapshots"
                    for run_id in runs_to_merge:
                        source_name = self._resolve_run_snapshot_path(run_id, None).name
                        source_snapshot = source_snapshot_dir / source_name
                        if not source_snapshot.exists():
                            continue
                        destination_snapshot = self._resolve_run_snapshot_path(
                            run_id, None
                        )
                        if destination_snapshot.exists():
                            continue
                        destination_snapshot.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(source_snapshot, destination_snapshot)
                        sidecar_src = source_snapshot.with_name(
                            f"{source_snapshot.stem}.locations.json"
                        )
                        if sidecar_src.is_file():
                            shutil.copy2(
                                sidecar_src,
                                destination_snapshot.with_name(sidecar_src.name),
                            )
                        snapshots_merged += 1

            with _track_maintenance_merge_phase("merge.audit_log"):
                self._log_audit(
                    "merge",
                    (
                        f"shard_path={shard_path} "
                        f"runs_merged={len(runs_to_merge)} "
                        f"conflicts={len(conflicts_detected)} "
                        f"conflict_mode={conflict_mode} "
                        f"include_snapshots={include_snapshots}"
                    ),
                )

            return MergeResult(
                shard_path=shard_path,
                runs_merged=runs_to_merge,
                runs_skipped=runs_skipped,
                artifacts_merged=artifacts_merged,
                ingested_tables_merged=sorted(set(ingested_tables_merged)),
                unscoped_cache_tables_skipped=sorted(
                    set(unscoped_cache_tables_skipped)
                ),
                conflicts_detected=conflicts_detected,
                snapshots_merged=snapshots_merged,
                incompatible_global_tables_skipped=incompatible_global_tables_skipped,
            )

    def compact(self) -> None:
        self.db.execute("VACUUM")
        self._log_audit("compact", "operation=vacuum")

    def rebuild_from_json(
        self,
        json_dir: Path,
        *,
        dry_run: bool,
        mode: Literal["minimal", "full"] = "minimal",
    ) -> RebuildResult:
        snapshot_dir = Path(json_dir)
        errors: list[str] = []
        runs_inserted = 0
        runs_already_present = 0
        artifacts_inserted = 0
        run_exists_cache: dict[str, bool] = {}
        artifact_exists_cache: dict[str, bool] = {}
        rebuild_mode = str(mode).strip().lower()
        if rebuild_mode not in {"minimal", "full"}:
            raise ValueError("mode must be one of: minimal, full")
        supports_full_restore = (
            self._build_rebuild_full_table_support()
            if rebuild_mode == "full" and not dry_run
            else {}
        )

        if not snapshot_dir.exists() or not snapshot_dir.is_dir():
            errors.append(
                f"{snapshot_dir}: JSON directory does not exist or is not a directory."
            )
            return RebuildResult(
                json_files_scanned=0,
                runs_inserted=0,
                runs_already_present=0,
                artifacts_inserted=0,
                errors=errors,
                dry_run=dry_run,
            )

        snapshot_files = sorted(
            [
                path
                for path in snapshot_dir.glob("*.json")
                if path.is_file() and not path.name.endswith(".locations.json")
            ],
            key=lambda path: path.name,
        )

        for snapshot_path in snapshot_files:
            try:
                payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                errors.append(
                    f"{snapshot_path.name}: failed to read/parse JSON ({exc})"
                )
                continue

            try:
                record = ConsistRecord.model_validate(payload)
            except Exception as exc:
                errors.append(f"{snapshot_path.name}: invalid snapshot payload ({exc})")
                continue

            run = record.run
            run_id = str(run.id).strip()
            if not run_id:
                errors.append(f"{snapshot_path.name}: missing run.id")
                continue

            artifact_rows: dict[str, Artifact] = {}
            artifact_legacy_roots: dict[str, list[str]] = {}
            snapshot_artifact_meta: dict[str, tuple[uuid.UUID, dict[str, Any]]] = {}
            output_ids_in_record: set[str] = set()
            rebuild_fs = FileSystemManager(
                run_dir=self.run_dir,
                mounts=self._mounts,
                create_run_dir=False,
            )
            sidecar_roots_by_aid: dict[str, list[str]] = {}
            sidecar_path = snapshot_path.with_name(
                f"{snapshot_path.stem}.locations.json"
            )
            if sidecar_path.is_file():
                try:
                    sidecar_payload = json.loads(
                        sidecar_path.read_text(encoding="utf-8")
                    )
                    raw_map = (
                        sidecar_payload.get("recovery_roots_by_artifact_id")
                        if isinstance(sidecar_payload, Mapping)
                        else None
                    )
                    if isinstance(raw_map, Mapping):
                        for raw_aid, raw_roots in raw_map.items():
                            coerced = self._coerce_uuid(raw_aid)
                            if coerced is not None:
                                try:
                                    sidecar_roots_by_aid[str(coerced)] = (
                                        rebuild_fs.normalize_recovery_roots(raw_roots)
                                    )
                                except ValueError:
                                    pass
                except (OSError, json.JSONDecodeError):
                    pass

            for artifact in record.outputs:
                coerced_out = self._coerce_uuid(artifact.id)
                if coerced_out is not None:
                    output_ids_in_record.add(str(coerced_out))

            for artifact in [*record.outputs, *record.inputs]:
                parsed_uuid = self._coerce_uuid(artifact.id)
                if parsed_uuid is None:
                    errors.append(
                        f"{snapshot_path.name}: invalid artifact id {artifact.id!r}"
                    )
                    continue
                artifact_id = str(parsed_uuid)
                raw_meta = (
                    dict(artifact.meta) if isinstance(artifact.meta, Mapping) else {}
                )
                legacy_roots_raw = raw_meta.pop("recovery_roots", None)
                combined_roots: list[str] = list(
                    artifact_legacy_roots.get(artifact_id, [])
                )
                if legacy_roots_raw is not None:
                    try:
                        for r in rebuild_fs.normalize_recovery_roots(legacy_roots_raw):
                            if r not in combined_roots:
                                combined_roots.append(r)
                    except ValueError:
                        pass
                for r in sidecar_roots_by_aid.get(artifact_id, []):
                    if r not in combined_roots:
                        combined_roots.append(r)
                if combined_roots:
                    artifact_legacy_roots[artifact_id] = combined_roots
                if artifact_id not in artifact_rows:
                    artifact_rows[artifact_id] = Artifact(
                        id=parsed_uuid,
                        key=artifact.key,
                        uri=artifact.uri,
                        member=artifact.member,
                        driver=artifact.driver,
                        digest=artifact.digest,
                        facet_id=artifact.facet_id,
                        parent_artifact_id=artifact.parent_artifact_id,
                        run_id=artifact.run_id,
                        meta=raw_meta,
                        created_at=artifact.created_at,
                    )
                if artifact_id not in snapshot_artifact_meta:
                    snapshot_artifact_meta[artifact_id] = (
                        parsed_uuid,
                        dict(raw_meta),
                    )

            run_meta_map = run.meta if isinstance(run.meta, Mapping) else {}
            roles_by_input_aid: dict[str, list[str]] = {}
            raw_ib = run_meta_map.get("input_binding")
            raw_ii = run_meta_map.get("input_identity")
            raw_sc = (
                raw_ii.get("strict_input_count", 0)
                if isinstance(raw_ii, Mapping)
                else 0
            )
            strict_cnt = (
                int(raw_sc)
                if isinstance(raw_sc, int)
                and not isinstance(raw_sc, bool)
                and raw_sc > 0
                else 0
            )
            for blk_map, offset in ((raw_ib, 0), (raw_ii, strict_cnt)):
                if not isinstance(blk_map, Mapping) or not isinstance(
                    blk_map.get("bindings"), list
                ):
                    continue
                for idx, entry in enumerate(blk_map["bindings"]):
                    if not isinstance(entry, Mapping) or entry.get("role") is None:
                        continue
                    kind_raw = entry.get("kind")
                    kind_str = kind_raw if isinstance(kind_raw, str) else None
                    role_val = format_input_link_role(kind_str, entry.get("role"))
                    if not role_val:
                        continue
                    aid_u = self._coerce_uuid(entry.get("artifact_id"))
                    eff_idx = idx + offset
                    if aid_u is None and eff_idx < len(record.inputs):
                        aid_u = self._coerce_uuid(record.inputs[eff_idx].id)
                    if aid_u is not None:
                        aid_s = str(aid_u)
                        cur_roles = roles_by_input_aid.setdefault(aid_s, [])
                        if role_val not in cur_roles:
                            cur_roles.append(role_val)

            planned_links: list[tuple[uuid.UUID, str, str]] = []
            seen_links: set[tuple[uuid.UUID, str, str]] = set()
            for artifact in record.outputs:
                parsed_uuid = self._coerce_uuid(artifact.id)
                if parsed_uuid is not None:
                    triple = (parsed_uuid, "output", str(artifact.key or ""))
                    if triple not in seen_links:
                        seen_links.add(triple)
                        planned_links.append(triple)
            for artifact in record.inputs:
                parsed_uuid = self._coerce_uuid(artifact.id)
                if parsed_uuid is None or str(parsed_uuid) in output_ids_in_record:
                    continue
                aid_s = str(parsed_uuid)
                roles = roles_by_input_aid.get(aid_s) or [str(artifact.key or "")]
                for role_str in roles:
                    triple = (parsed_uuid, "input", role_str)
                    if triple not in seen_links:
                        seen_links.add(triple)
                        planned_links.append(triple)

            if dry_run:
                if run_id not in run_exists_cache:
                    run_exists_cache[run_id] = self.db.get_run(run_id) is not None
                if run_exists_cache[run_id]:
                    runs_already_present += 1
                else:
                    runs_inserted += 1
                    run_exists_cache[run_id] = True

                for artifact_id, artifact in artifact_rows.items():
                    if artifact_id not in artifact_exists_cache:
                        artifact_exists_cache[artifact_id] = (
                            self.db.get_artifact(artifact.id) is not None
                        )
                    if not artifact_exists_cache[artifact_id]:
                        artifacts_inserted += 1
                        artifact_exists_cache[artifact_id] = True
                continue

            try:
                with self.db.transaction():
                    run_exists = self.db.get_run(run_id) is not None
                    if not run_exists:
                        stage = resolve_canonical_run_meta_field(run, "stage")
                        phase = resolve_canonical_run_meta_field(run, "phase")
                        meta_payload = (
                            dict(run.meta) if isinstance(run.meta, Mapping) else {}
                        )
                        if stage is not None:
                            meta_payload["stage"] = stage
                        if phase is not None:
                            meta_payload["phase"] = phase
                        self.db.upsert_run(
                            Run(
                                id=run_id,
                                parent_run_id=run.parent_run_id,
                                status=run.status,
                                model_name=run.model_name,
                                description=run.description,
                                year=run.year,
                                iteration=run.iteration,
                                stage=stage,
                                phase=phase,
                                tags=list(run.tags or []),
                                config_hash=run.config_hash,
                                git_hash=run.git_hash,
                                input_hash=run.input_hash,
                                signature=run.signature,
                                cacheable=run.cacheable,
                                meta=meta_payload,
                                started_at=run.started_at,
                                ended_at=run.ended_at,
                                created_at=run.created_at,
                                updated_at=run.updated_at,
                            )
                        )

                    inserted_artifacts_for_file = 0
                    for artifact_id_str, artifact in artifact_rows.items():
                        already_present = self.db.get_artifact(artifact.id) is not None
                        is_producer = artifact_id_str in output_ids_in_record or (
                            artifact.run_id is not None
                            and str(artifact.run_id) == run_id
                        )
                        extra_roots = artifact_legacy_roots.get(artifact_id_str, [])
                        if already_present and not is_producer and not extra_roots:
                            continue
                        rebuilt_locs = self._build_rebuilt_artifact_locations(
                            rebuild_fs=rebuild_fs,
                            artifact=artifact,
                            run=run,
                            legacy_roots=extra_roots,
                        )
                        if not already_present or is_producer:
                            self.db.upsert_artifact(
                                artifact, locations=rebuilt_locs or None
                            )
                            if not already_present:
                                inserted_artifacts_for_file += 1
                        elif rebuilt_locs:
                            self.db.upsert_artifact_locations(rebuilt_locs)

                    for parsed_uuid, direction, role_str in planned_links:
                        existing_link = self.db.fetchone(
                            """
                            SELECT 1 FROM run_artifact_link
                            WHERE run_id = ? AND artifact_id = ? AND direction = ? AND role = ?
                            LIMIT 1
                            """,
                            [run_id, parsed_uuid, direction, role_str],
                        )
                        if existing_link is not None:
                            continue
                        self.db.link_run_artifact(
                            run_id=run_id,
                            artifact_id=parsed_uuid,
                            direction=direction,
                            role=role_str,
                        )
            except Exception as exc:
                errors.append(f"{snapshot_path.name}: rebuild write failed ({exc})")
                continue

            if run_exists:
                runs_already_present += 1
            else:
                runs_inserted += 1
            artifacts_inserted += inserted_artifacts_for_file

            if rebuild_mode == "full":
                try:
                    self._restore_full_snapshot_metadata(
                        run_id=run_id,
                        run=run,
                        run_facet=record.facet,
                        snapshot_artifact_meta=snapshot_artifact_meta,
                        supports_full_restore=supports_full_restore,
                    )
                except Exception as exc:
                    errors.append(
                        f"{snapshot_path.name}: optional full restore failed ({exc})"
                    )
                    continue

        result = RebuildResult(
            json_files_scanned=len(snapshot_files),
            runs_inserted=runs_inserted,
            runs_already_present=runs_already_present,
            artifacts_inserted=artifacts_inserted,
            errors=errors,
            dry_run=dry_run,
        )

        if not dry_run:
            self._log_audit(
                "rebuild_from_json",
                (
                    f"json_dir={snapshot_dir} "
                    f"json_files_scanned={result.json_files_scanned} "
                    f"runs_inserted={result.runs_inserted} "
                    f"runs_already_present={result.runs_already_present} "
                    f"artifacts_inserted={result.artifacts_inserted} "
                    f"mode={rebuild_mode} "
                    f"errors={len(result.errors)}"
                ),
            )

        return result

    def _build_rebuilt_artifact_locations(
        self,
        *,
        rebuild_fs: FileSystemManager,
        artifact: Artifact,
        run: Run,
        legacy_roots: Sequence[str],
    ) -> list[ArtifactLocationRow]:
        rows: list[ArtifactLocationRow] = []
        run_meta = run.meta if isinstance(run.meta, Mapping) else {}
        hist_run_dir = run_meta.get("_physical_run_dir")
        raw_rm = run_meta.get("mounts")
        run_mounts = (
            {
                k: v
                for k, v in raw_rm.items()
                if isinstance(k, str) and isinstance(v, str)
            }
            if isinstance(raw_rm, Mapping)
            else {}
        )
        effective_fs = (
            FileSystemManager(
                run_dir=hist_run_dir
                if isinstance(hist_run_dir, str) and hist_run_dir
                else self.run_dir,
                mounts={**run_mounts, **(self._mounts or {})},
                create_run_dir=False,
            )
            if (isinstance(hist_run_dir, str) and hist_run_dir) or run_mounts
            else rebuild_fs
        )
        m_name, m_inst, canonical_uri = effective_fs.location_coordinate_for_uri(
            artifact.uri
        )
        site: LocationSite = (
            "workspace" if m_name in ("workspace", "runs") else "external"
        )
        candidate_path = effective_fs.resolve_location_coordinate(
            m_name, m_inst, canonical_uri
        )
        cand_exists = candidate_path is not None and candidate_path.exists()
        obs_ns = time.time_ns()
        if cand_exists or (
            isinstance(artifact.meta, Mapping)
            and artifact.meta.get("is_ingested") is not True
        ):
            rows.append(
                build_verified_location_row(
                    effective_fs,
                    artifact,
                    local_path=candidate_path if cand_exists else None,
                    uri=artifact.uri,
                    site=site,
                    capture_local_witness=cand_exists,
                    observed_at_ns=obs_ns,
                )
            )
        for idx, root in enumerate(legacy_roots, start=1):
            _, _, _, target_p = effective_fs.archive_coordinate_for_root(
                artifact.uri, root
            )
            rec_exists = (
                not str(target_p).startswith("/__unmounted__/") and target_p.exists()
            )
            rows.append(
                build_verified_location_row(
                    effective_fs,
                    artifact,
                    local_path=target_p if rec_exists else None,
                    uri=artifact.uri,
                    archive_root=root,
                    site="archive" if rec_exists else "advisory",
                    capture_local_witness=rec_exists,
                    observed_at_ns=obs_ns + idx,
                )
            )
        return rows

    def _build_rebuild_full_table_support(self) -> dict[str, bool]:
        return {
            "run_config_kv": self._table_supports_model("run_config_kv", RunConfigKV),
            "artifact_kv": self._table_supports_model("artifact_kv", ArtifactKV),
            "artifact_schema": self._table_supports_model(
                "artifact_schema", ArtifactSchema
            ),
            "artifact_schema_observation": self._table_supports_model(
                "artifact_schema_observation", ArtifactSchemaObservation
            ),
        }

    def _restore_full_snapshot_metadata(
        self,
        *,
        run_id: str,
        run: Run,
        run_facet: Any,
        snapshot_artifact_meta: dict[str, tuple[uuid.UUID, dict[str, Any]]],
        supports_full_restore: dict[str, bool],
    ) -> None:
        with self.db.transaction():
            run_meta = dict(run.meta) if isinstance(run.meta, Mapping) else {}
            run_namespace = (
                self._coerce_non_empty_str(run_meta.get("config_facet_namespace"))
                or run.model_name
            )

            if supports_full_restore.get("run_config_kv"):
                self._restore_run_config_kv_rows(
                    run_id=run_id,
                    run_facet=run_facet,
                    run_meta=run_meta,
                    namespace=run_namespace,
                )

            restore_schema = bool(
                supports_full_restore.get("artifact_schema")
                and supports_full_restore.get("artifact_schema_observation")
            )
            for artifact_id, artifact_meta in snapshot_artifact_meta.values():
                if supports_full_restore.get("artifact_kv"):
                    self._restore_artifact_kv_rows(
                        artifact_id=artifact_id,
                        artifact_meta=artifact_meta,
                        default_namespace=run.model_name,
                    )

                if restore_schema:
                    self._restore_artifact_schema_rows(
                        artifact_id=artifact_id,
                        artifact_meta=artifact_meta,
                        run_id=run_id,
                    )

    def _restore_run_config_kv_rows(
        self,
        *,
        run_id: str,
        run_facet: Any,
        run_meta: Mapping[str, Any],
        namespace: str,
    ) -> None:
        if not isinstance(run_facet, Mapping) or not run_facet:
            return

        facet_id = self._coerce_non_empty_str(run_meta.get("config_facet_id"))
        if facet_id is None:
            facet_id = canonical_json_sha256(run_facet)

        flattened = flatten_facet_values(
            facet_dict=run_facet,
            include_json_leaves=True,
        )
        now = datetime.now(UTC)
        rows = [
            RunConfigKV(
                run_id=run_id,
                facet_id=facet_id,
                namespace=namespace,
                key=row.key_path,
                value_type=row.value_type,
                value_str=row.value_str,
                value_num=row.value_num,
                value_bool=row.value_bool,
                value_json=row.value_json,
                created_at=now,
            )
            for row in flattened
        ]
        if rows:
            self.db.insert_run_config_kv_bulk(rows)

    def _restore_artifact_kv_rows(
        self,
        *,
        artifact_id: uuid.UUID,
        artifact_meta: Mapping[str, Any],
        default_namespace: str,
    ) -> None:
        facet_payload = artifact_meta.get("artifact_facet")
        if not isinstance(facet_payload, Mapping) or not facet_payload:
            fallback_payload = artifact_meta.get("facet")
            if isinstance(fallback_payload, Mapping) and fallback_payload:
                facet_payload = fallback_payload
            else:
                return

        facet_id = self._coerce_non_empty_str(artifact_meta.get("artifact_facet_id"))
        if facet_id is None:
            facet_id = canonical_json_sha256(facet_payload)
        namespace = (
            self._coerce_non_empty_str(artifact_meta.get("artifact_facet_namespace"))
            or default_namespace
        )

        flattened = flatten_facet_values(
            facet_dict=facet_payload,
            include_json_leaves=False,
        )
        now = datetime.now(UTC)
        rows = [
            ArtifactKV(
                artifact_id=artifact_id,
                facet_id=facet_id,
                key_path=row.key_path,
                namespace=namespace,
                value_type=row.value_type,
                value_str=row.value_str,
                value_num=row.value_num,
                value_bool=row.value_bool,
                created_at=now,
            )
            for row in flattened
        ]
        if rows:
            self.db.insert_artifact_kv_bulk(rows)

    def _restore_artifact_schema_rows(
        self,
        *,
        artifact_id: uuid.UUID,
        artifact_meta: Mapping[str, Any],
        run_id: str,
    ) -> None:
        schema_id = self._coerce_non_empty_str(artifact_meta.get("schema_id"))
        if schema_id is None:
            return

        summary = artifact_meta.get("schema_summary")
        summary_json = dict(summary) if isinstance(summary, Mapping) else {}

        profile = artifact_meta.get("schema_profile")
        profile_json = dict(profile) if isinstance(profile, Mapping) else None

        existing_schema_row = self.db.fetchone(
            "SELECT summary_json, profile_json FROM artifact_schema WHERE id = ?",
            [schema_id],
        )
        now = datetime.now(UTC)
        if existing_schema_row is None:
            profile_version = summary_json.get("profile_version")
            if not isinstance(profile_version, int) or profile_version <= 0:
                profile_version = 1
            self.db.execute(
                """
                INSERT INTO artifact_schema (
                    id, profile_version, summary_json, profile_json, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [
                    schema_id,
                    profile_version,
                    _dump_json(summary_json),
                    _dump_optional_json(profile_json),
                    now,
                ],
            )
        else:
            existing_summary = existing_schema_row[0]
            existing_profile = existing_schema_row[1]
            has_summary = bool(
                json.loads(existing_summary)
                if isinstance(existing_summary, str) and existing_summary
                else existing_summary
            )
            if not has_summary and summary_json:
                self.db.execute(
                    "UPDATE artifact_schema SET summary_json = ? WHERE id = ?",
                    [_dump_json(summary_json), schema_id],
                )
            if existing_profile is None and profile_json is not None:
                self.db.execute(
                    "UPDATE artifact_schema SET profile_json = ? WHERE id = ?",
                    [_dump_optional_json(profile_json), schema_id],
                )

        source = self._schema_observation_source(
            summary_json=summary_json,
            artifact_meta=artifact_meta,
        )
        sample_rows = self._schema_observation_sample_rows(
            summary_json=summary_json,
            artifact_meta=artifact_meta,
        )

        existing_obs = self.db.fetchone(
            """
            SELECT 1 FROM artifact_schema_observation
            WHERE run_id = ? AND artifact_id = ? AND schema_id = ?
              AND source = ? AND sample_rows IS NOT DISTINCT FROM ?
            LIMIT 1
            """,
            [run_id, artifact_id, schema_id, source, sample_rows],
        )
        if existing_obs is not None:
            return

        self.db.execute(
            """
            INSERT INTO artifact_schema_observation (
                id, artifact_id, schema_id, run_id, source, sample_rows, observed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [uuid.uuid4(), artifact_id, schema_id, run_id, source, sample_rows, now],
        )

    def _table_supports_model(self, table_name: str, model: type[Any]) -> bool:
        table_columns = set(self._list_table_columns(table_name))
        if not table_columns:
            return False
        model_columns = set(model.model_fields.keys())
        return model_columns.issubset(table_columns)

    @staticmethod
    def _coerce_non_empty_str(value: Any) -> Optional[str]:
        if not isinstance(value, str):
            return None
        candidate = value.strip()
        return candidate if candidate else None

    @classmethod
    def _schema_observation_source(
        cls, *, summary_json: Mapping[str, Any], artifact_meta: Mapping[str, Any]
    ) -> str:
        for raw in (
            summary_json.get("source"),
            artifact_meta.get("schema_source"),
            artifact_meta.get("source"),
        ):
            coerced = cls._coerce_non_empty_str(raw)
            if coerced is not None:
                return coerced
        return "snapshot_rebuild"

    @staticmethod
    def _schema_observation_sample_rows(
        *, summary_json: Mapping[str, Any], artifact_meta: Mapping[str, Any]
    ) -> Optional[int]:
        for raw in (
            summary_json.get("sample_rows"),
            artifact_meta.get("sample_rows"),
            artifact_meta.get("schema_sample_rows"),
        ):
            if isinstance(raw, int) and not isinstance(raw, bool):
                return raw
        return None

    def snapshot(
        self,
        dest_path: Path,
        *,
        checkpoint: bool = True,
        metadata: Optional[dict] = None,
    ) -> Path:
        snapshot_metadata: dict = {"operation": "maintenance_snapshot"}
        if metadata:
            snapshot_metadata.update(metadata)
        snapshot_path = self.db.snapshot_to(
            dest_path=dest_path,
            checkpoint=checkpoint,
            metadata=snapshot_metadata,
        )
        self._log_audit(
            "snapshot",
            f"dest_path={snapshot_path} checkpoint={checkpoint}",
        )
        return snapshot_path

    def _expand_run_ids(self, run_ids: Iterable[str] | str) -> list[str]:
        """Expand run IDs to include all descendant runs (breadth-first, cycle-safe)."""
        seed_ids = self._normalize_run_ids(run_ids)
        if not seed_ids:
            return []

        rows = self.db.fetchall(
            "SELECT id, parent_run_id FROM run WHERE parent_run_id IS NOT NULL"
        )
        children_by_parent: dict[str, list[str]] = {}
        for child_id, parent_id in rows:
            if parent_id is None:
                continue
            children_by_parent.setdefault(str(parent_id), []).append(str(child_id))

        for children in children_by_parent.values():
            children.sort()

        expanded: list[str] = []
        seen: set[str] = set()
        queue: deque[str] = deque(seed_ids)

        while queue:
            current = queue.popleft()
            if current in seen:
                continue
            seen.add(current)
            expanded.append(current)
            for child_id in children_by_parent.get(current, []):
                if child_id not in seen:
                    queue.append(child_id)

        return expanded

    def _find_orphaned_artifacts(self, run_ids: Iterable[str] | str) -> list[uuid.UUID]:
        """
        Return artifact IDs linked exclusively to the selected run IDs.

        Considers both `run_artifact_link` and `run_coupler_snapshot` so artifacts
        still referenced by surviving scenarios are never marked orphaned.
        """
        selected_ids = set(self._normalize_run_ids(run_ids))
        if not selected_ids:
            return []

        links = list(
            self.db.fetchall("SELECT artifact_id, run_id FROM run_artifact_link")
        )
        if self.db.has_table("run_coupler_snapshot"):
            links.extend(
                self.db.fetchall(
                    """
                    SELECT artifact_id, run_id FROM run_coupler_snapshot
                    UNION ALL
                    SELECT artifact_id, scenario_run_id FROM run_coupler_snapshot
                    """
                )
            )

        linked_runs_by_artifact: dict[uuid.UUID, set[str]] = {}
        for artifact_id, run_id in links:
            parsed_id = self._coerce_uuid(artifact_id)
            if parsed_id is None or run_id is None:
                continue
            linked_runs_by_artifact.setdefault(parsed_id, set()).add(str(run_id))

        orphaned = [
            artifact_id
            for artifact_id, linked_runs in linked_runs_by_artifact.items()
            if linked_runs and linked_runs.issubset(selected_ids)
        ]
        orphaned.sort(key=str)
        return orphaned

    def _discover_global_tables(
        self, db: Optional[DatabaseManager] = None
    ) -> list[str]:
        active_db = db or self.db
        rows = active_db.fetchall(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'global_tables'
            ORDER BY table_name
            """
        )
        return [str(row[0]) for row in rows]

    def _classify_global_table(
        self, table: str, db: Optional[DatabaseManager] = None
    ) -> GlobalTableMode:
        safe_table = self._validate_identifier(table, label="table")
        active_db = db or self.db
        rows = active_db.fetchall(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'global_tables'
              AND table_name = ?
            """,
            [safe_table],
        )
        column_names = {str(row[0]) for row in rows}
        if "consist_run_id" in column_names:
            return "run_scoped"
        if "run_id" in column_names:
            return "run_link"
        return "unscoped_cache"

    def _global_table_row_counts(
        self,
        tables: Iterable[str],
        run_ids: Iterable[str] | str,
        db: Optional[DatabaseManager] = None,
    ) -> dict[str, int]:
        safe_tables = sorted(
            {self._validate_identifier(table, label="table") for table in tables}
        )
        normalized_run_ids = self._normalize_run_ids(run_ids)
        active_db = db or self.db

        counts: dict[str, int] = {}
        for table in safe_tables:
            quoted_table = self._quote_ident(table)
            if not normalized_run_ids:
                row = active_db.fetchone(
                    f"SELECT COUNT(*) FROM global_tables.{quoted_table}"
                )
                counts[table] = int(row[0] if row else 0)
                continue

            filter_sql = self._resolve_global_table_filter_sql(
                table,
                normalized_run_ids,
                table_alias="gt",
                db=active_db,
            )
            if filter_sql is None:
                counts[table] = 0
                continue

            row = active_db.fetchone(
                f"""
                SELECT COUNT(*)
                FROM global_tables.{quoted_table} AS "gt"
                WHERE {filter_sql}
                """
            )
            counts[table] = int(row[0] if row else 0)

        return {table: counts[table] for table in safe_tables}

    def _resolve_global_table_filter_sql(
        self,
        table: str,
        run_ids: Iterable[str] | str,
        *,
        table_alias: str = "",
        db: Optional[DatabaseManager] = None,
    ) -> Optional[str]:
        safe_table = self._validate_identifier(table, label="table")
        mode = self._classify_global_table(safe_table, db=db)
        if mode == "unscoped_cache":
            return None

        normalized_run_ids = self._normalize_run_ids(run_ids)
        if not normalized_run_ids:
            return "1 = 0"

        run_column = "consist_run_id" if mode == "run_scoped" else "run_id"
        quoted_column = self._quote_ident(run_column)
        if table_alias:
            safe_alias = self._validate_identifier(table_alias, label="table_alias")
            column_ref = f"{self._quote_ident(safe_alias)}.{quoted_column}"
        else:
            column_ref = quoted_column

        run_id_literals = ", ".join(
            self._quote_sql_string_literal(run_id) for run_id in normalized_run_ids
        )
        return f"{column_ref} = ANY([{run_id_literals}])"

    @staticmethod
    def _required_global_table_column(mode: GlobalTableMode) -> Optional[str]:
        if mode == "run_scoped":
            return "consist_run_id"
        if mode == "run_link":
            return "run_id"
        return None

    @staticmethod
    def _normalize_sql_type(type_name: str) -> str:
        return " ".join(str(type_name).strip().upper().split())

    def _global_table_merge_compatibility_issue(
        self,
        table: str,
        *,
        mode: GlobalTableMode,
        source_db: DatabaseManager,
        target_db: DatabaseManager,
    ) -> Optional[str]:
        safe_table = self._validate_identifier(table, label="table")
        source_column_types = self._list_table_columns_with_types(
            safe_table,
            schema="global_tables",
            db=source_db,
        )
        if not source_column_types:
            return "source table missing in global_tables schema"

        target_column_types = self._list_table_columns_with_types(
            safe_table,
            schema="global_tables",
            db=target_db,
        )
        target_table_exists = bool(target_column_types)

        required_column = self._required_global_table_column(mode)
        if required_column and required_column not in source_column_types:
            return (
                f"missing required source column '{required_column}' for mode '{mode}'"
            )
        if (
            required_column
            and target_table_exists
            and required_column not in target_column_types
        ):
            return (
                f"missing required target column '{required_column}' for mode '{mode}'"
            )

        shared_columns = sorted(
            set(source_column_types).intersection(target_column_types)
        )
        incompatible_columns: list[str] = []
        for column_name in shared_columns:
            source_type = source_column_types[column_name]
            target_type = target_column_types[column_name]
            if not self._types_compatible_for_merge(
                source_type, target_type, db=target_db
            ):
                incompatible_columns.append(
                    f"{column_name} (source={source_type}, target={target_type})"
                )

        if incompatible_columns:
            return "incompatible shared column type(s): " + ", ".join(
                incompatible_columns
            )
        return None

    def _types_compatible_for_merge(
        self,
        source_type: str,
        target_type: str,
        *,
        db: Optional[DatabaseManager] = None,
    ) -> bool:
        normalized_source = self._normalize_sql_type(source_type)
        normalized_target = self._normalize_sql_type(target_type)
        if not normalized_source or not normalized_target:
            return False
        if normalized_source == normalized_target:
            return True

        active_db = db or self.db
        try:
            row = active_db.fetchone(
                f"""
                SELECT can_cast_implicitly(
                    NULL::{normalized_source},
                    NULL::{normalized_target}
                )
                """
            )
        except duckdb.Error:
            return False
        return bool(row[0]) if row else False

    def _list_table_columns_with_types_cur(
        self,
        cur: duckdb.DuckDBPyConnection,
        table: str,
        *,
        schema: str = "main",
        catalog: Optional[str] = None,
    ) -> dict[str, str]:
        safe_table = self._validate_identifier(table, label="table")
        safe_schema = self._validate_identifier(schema, label="schema")
        safe_catalog = (
            self._validate_identifier(catalog, label="catalog")
            if catalog is not None
            else None
        )
        lookup_name = self._qualified_table_lookup_name(
            safe_table, schema=safe_schema, catalog=safe_catalog
        )
        try:
            rows = cur.execute(
                f"""
                SELECT name, type
                FROM pragma_table_info({self._quote_sql_string_literal(lookup_name)})
                ORDER BY cid
                """
            ).fetchall()
        except duckdb.Error:
            return {}
        column_types: dict[str, str] = {}
        for row in rows:
            column_name = str(row[0])
            if column_name not in column_types:
                column_types[column_name] = str(row[1] or "").strip()
        return column_types

    def _list_table_columns_cur(
        self,
        cur: duckdb.DuckDBPyConnection,
        table: str,
        *,
        schema: str = "main",
        catalog: Optional[str] = None,
    ) -> list[str]:
        return list(
            self._list_table_columns_with_types_cur(
                cur, table, schema=schema, catalog=catalog
            ).keys()
        )

    def _list_table_columns_with_types(
        self,
        table: str,
        *,
        schema: str = "main",
        catalog: Optional[str] = None,
        db: Optional[DatabaseManager] = None,
    ) -> dict[str, str]:
        safe_table = self._validate_identifier(table, label="table")
        safe_schema = self._validate_identifier(schema, label="schema")
        safe_catalog = (
            self._validate_identifier(catalog, label="catalog")
            if catalog is not None
            else None
        )
        active_db = db or self.db
        lookup_name = self._qualified_table_lookup_name(
            safe_table, schema=safe_schema, catalog=safe_catalog
        )
        try:
            rows = active_db.fetchall(
                f"""
                SELECT name, type
                FROM pragma_table_info({self._quote_sql_string_literal(lookup_name)})
                ORDER BY cid
                """
            )
        except duckdb.Error:
            return {}
        column_types: dict[str, str] = {}
        for row in rows:
            column_name = str(row[0])
            if column_name not in column_types:
                column_types[column_name] = str(row[1] or "").strip()
        return column_types

    def _list_table_columns(
        self,
        table: str,
        *,
        schema: str = "main",
        catalog: Optional[str] = None,
        db: Optional[DatabaseManager] = None,
    ) -> list[str]:
        return list(
            self._list_table_columns_with_types(
                table, schema=schema, catalog=catalog, db=db
            ).keys()
        )

    def _build_intersection_column_list(
        self,
        table: str,
        *,
        source_schema: str,
        target_schema: str,
        db: Optional[DatabaseManager] = None,
    ) -> list[str]:
        safe_table = self._validate_identifier(table, label="table")
        safe_source_schema = self._validate_identifier(
            source_schema, label="source_schema"
        )
        safe_target_schema = self._validate_identifier(
            target_schema, label="target_schema"
        )
        active_db = db or self.db
        rows = active_db.fetchall(
            """
            SELECT table_schema, column_name, ordinal_position
            FROM information_schema.columns
            WHERE table_name = ?
              AND table_schema IN (?, ?)
            ORDER BY table_schema, ordinal_position
            """,
            [safe_table, safe_source_schema, safe_target_schema],
        )

        source_columns: set[str] = set()
        target_columns_in_order: list[str] = []
        seen_target_columns: set[str] = set()

        for schema_name, column_name, _ in rows:
            schema_value = str(schema_name)
            column_value = str(column_name)
            if schema_value == safe_source_schema:
                source_columns.add(column_value)
            if (
                schema_value == safe_target_schema
                and column_value not in seen_target_columns
            ):
                target_columns_in_order.append(column_value)
                seen_target_columns.add(column_value)

        if not source_columns or not target_columns_in_order:
            return []

        return [
            self._quote_ident(column_name)
            for column_name in target_columns_in_order
            if column_name in source_columns
        ]

    @staticmethod
    def _custom_mount_key(uri: str) -> Optional[tuple[str, str]]:
        raw = str(uri or "").strip()
        if "://" not in raw:
            return None
        scheme, rel_path = raw.split("://", 1)
        norm_scheme = scheme.strip()
        if not norm_scheme or norm_scheme in {"file", "workspace", "db"}:
            return None
        norm_rel = DatabaseMaintenance._normalize_relative_uri_path(rel_path)
        if norm_rel is None:
            return None
        return (norm_scheme, norm_rel)

    @staticmethod
    def _paths_conflict_for_delete(candidate: Path, protected: Path) -> bool:
        try:
            cand_res = candidate.resolve()
        except OSError:
            cand_res = candidate
        try:
            prot_res = protected.resolve()
        except OSError:
            prot_res = protected
        if cand_res == prot_res:
            return True
        try:
            prot_res.relative_to(cand_res)
            return True
        except ValueError:
            pass
        if candidate.exists() and protected.exists():
            try:
                if os.path.samefile(candidate, protected):
                    return True
            except OSError:
                pass
        return False

    def _collect_known_mount_roots_from_db(self) -> dict[str, list[Path]]:
        roots_by_scheme: dict[str, list[Path]] = {}
        seen: set[tuple[str, str]] = set()

        def _add(scheme: str, root_str: str) -> None:
            norm_root = self._normalize_mount_root_path(root_str)
            if norm_root is None:
                return
            key = (scheme, str(norm_root))
            if key not in seen:
                seen.add(key)
                roots_by_scheme.setdefault(scheme, []).append(norm_root)

        for scheme, root_str in self._mounts.items():
            if scheme and root_str:
                _add(scheme, root_str)

        run_meta_rows = self.db.fetchall("SELECT meta FROM run WHERE meta IS NOT NULL")
        for (raw_meta,) in run_meta_rows:
            meta = (
                json.loads(raw_meta)
                if isinstance(raw_meta, str) and raw_meta
                else (raw_meta if isinstance(raw_meta, Mapping) else {})
            )
            if isinstance(meta, Mapping):
                mounts_raw = meta.get("mounts")
                if isinstance(mounts_raw, Mapping):
                    for scheme, root_val in mounts_raw.items():
                        if isinstance(scheme, str) and isinstance(root_val, str):
                            _add(scheme, root_val)
        return roots_by_scheme

    def _load_artifacts_with_runs(
        self,
        *,
        include_ids: Optional[Sequence[uuid.UUID]] = None,
        exclude_ids: Optional[Sequence[uuid.UUID]] = None,
    ) -> tuple[list[Artifact], dict[uuid.UUID, Optional[Run]]]:
        if include_ids is not None:
            if not include_ids:
                return [], {}
            artifacts = self.db.get_artifacts_by_ids(include_ids)
        elif exclude_ids:
            excl_strs = [str(uid) for uid in exclude_ids]
            id_rows = self.db.fetchall(
                """
                SELECT id FROM artifact
                WHERE CAST(id AS VARCHAR) NOT IN (SELECT UNNEST(?))
                ORDER BY CAST(id AS VARCHAR)
                """,
                [excl_strs],
            )
            surv_ids = [
                uid
                for (raw_id,) in id_rows
                if (uid := self._coerce_uuid(raw_id)) is not None
            ]
            artifacts = self.db.get_artifacts_by_ids(surv_ids) if surv_ids else []
        else:
            id_rows = self.db.fetchall(
                "SELECT id FROM artifact ORDER BY CAST(id AS VARCHAR)"
            )
            all_ids = [
                uid
                for (raw_id,) in id_rows
                if (uid := self._coerce_uuid(raw_id)) is not None
            ]
            artifacts = self.db.get_artifacts_by_ids(all_ids) if all_ids else []

        if not artifacts:
            return [], {}

        run_id_by_artifact: dict[uuid.UUID, str] = {}
        run_ids: set[str] = set()
        missing_run_aids: list[str] = []
        for art in artifacts:
            if art.run_id:
                run_id_by_artifact[art.id] = art.run_id
                run_ids.add(art.run_id)
            else:
                missing_run_aids.append(str(art.id))

        if missing_run_aids:
            out_rows = self.db.fetchall(
                """
                SELECT artifact_id, run_id
                FROM run_artifact_link
                WHERE CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                  AND direction = 'output'
                ORDER BY run_id
                """,
                [missing_run_aids],
            )
            for aid_raw, rid_raw in out_rows:
                parsed = self._coerce_uuid(aid_raw)
                if parsed is not None and parsed not in run_id_by_artifact and rid_raw:
                    rid = str(rid_raw).strip()
                    if rid:
                        run_id_by_artifact[parsed] = rid
                        run_ids.add(rid)

        runs_by_id = self._load_runs_by_ids(list(run_ids))
        run_for_artifact: dict[uuid.UUID, Optional[Run]] = {
            art.id: runs_by_id.get(run_id_by_artifact[art.id])
            if art.id in run_id_by_artifact
            else None
            for art in artifacts
        }
        return artifacts, run_for_artifact

    def _resolve_artifact_disk_paths(
        self, artifact_ids: Iterable[uuid.UUID]
    ) -> list[Path]:
        normalized_artifact_ids: list[uuid.UUID] = []
        seen_artifact_ids: set[uuid.UUID] = set()
        for value in artifact_ids:
            parsed_uuid = self._coerce_uuid(value)
            if parsed_uuid is None or parsed_uuid in seen_artifact_ids:
                continue
            seen_artifact_ids.add(parsed_uuid)
            normalized_artifact_ids.append(parsed_uuid)

        if not normalized_artifact_ids:
            return []

        artifacts, run_for_artifact = self._load_artifacts_with_runs(
            include_ids=normalized_artifact_ids
        )
        artifacts_by_id = {art.id: art for art in artifacts}

        resolved_paths: list[Path] = []
        seen_paths: set[str] = set()
        for artifact_id in normalized_artifact_ids:
            artifact = artifacts_by_id.get(artifact_id)
            if artifact is None:
                continue
            run = run_for_artifact.get(artifact_id)
            for candidate in self._resolve_artifact_uri_candidates(artifact, run):
                if not candidate.exists() and not candidate.is_symlink():
                    continue
                path_key = str(candidate)
                if path_key in seen_paths:
                    continue
                seen_paths.add(path_key)
                resolved_paths.append(candidate)

        return resolved_paths

    def _collect_surviving_artifact_protections(
        self, *, exclude_ids: Optional[Sequence[uuid.UUID]] = None
    ) -> tuple[list[Path], set[tuple[str, str]]]:
        surviving_artifacts, run_for_surviving = self._load_artifacts_with_runs(
            exclude_ids=exclude_ids
        )
        if not surviving_artifacts:
            return [], set()

        known_mounts = self._collect_known_mount_roots_from_db()
        protected_paths: list[Path] = []
        seen_paths: set[str] = set()
        protected_mount_keys: set[tuple[str, str]] = set()

        surv_locs_by_aid = (
            self.db.get_active_artifact_locations_bulk(surviving_artifacts)
            if self.db.has_artifact_location_table()
            else {}
        )
        base_fs = FileSystemManager(
            run_dir=self.run_dir, mounts=self._mounts, create_run_dir=False
        )

        for surv_art in surviving_artifacts:
            mkey = self._custom_mount_key(surv_art.uri)
            if mkey is not None:
                protected_mount_keys.add(mkey)
                scheme, rel = mkey
                for extra_root in known_mounts.get(scheme, ()):
                    extra_cand = self._resolve_relative_path_under_root(rel, extra_root)
                    if extra_cand is not None:
                        pkey = str(extra_cand)
                        if pkey not in seen_paths:
                            seen_paths.add(pkey)
                            protected_paths.append(extra_cand)
            surv_run = run_for_surviving.get(surv_art.id)
            for surv_cand in self._resolve_artifact_uri_candidates(
                surv_art, surv_run, fallback_mounts=known_mounts
            ):
                pkey = str(surv_cand)
                if pkey not in seen_paths:
                    seen_paths.add(pkey)
                    protected_paths.append(surv_cand)
            hist_rd: Optional[str] = None
            if surv_run is not None and isinstance(surv_run.meta, Mapping):
                rrd = surv_run.meta.get("_physical_run_dir")
                if isinstance(rrd, str) and rrd:
                    hist_rd = rrd
            for loc in surv_locs_by_aid.get(surv_art.id, ()):
                loc_mkey = self._custom_mount_key(loc.uri)
                if loc_mkey is not None:
                    protected_mount_keys.add(loc_mkey)
                    loc_scheme, loc_rel = loc_mkey
                    for extra_root in known_mounts.get(loc_scheme, ()):
                        extra_cand = self._resolve_relative_path_under_root(
                            loc_rel, extra_root
                        )
                        if extra_cand is not None:
                            pkey = str(extra_cand)
                            if pkey not in seen_paths:
                                seen_paths.add(pkey)
                                protected_paths.append(extra_cand)
                loc_cand = base_fs.resolve_location_row_path(
                    loc, historical_run_dir=hist_rd
                )
                if loc_cand is not None:
                    pkey = str(loc_cand)
                    if pkey not in seen_paths:
                        seen_paths.add(pkey)
                        protected_paths.append(loc_cand)

        return protected_paths, protected_mount_keys

    def _unshared_orphaned_disk_paths(
        self, orphaned_artifact_ids: Iterable[uuid.UUID]
    ) -> list[Path]:
        normalized_ids: list[uuid.UUID] = []
        seen_ids: set[uuid.UUID] = set()
        for value in orphaned_artifact_ids:
            parsed = self._coerce_uuid(value)
            if parsed is not None and parsed not in seen_ids:
                seen_ids.add(parsed)
                normalized_ids.append(parsed)
        if not normalized_ids:
            return []

        orphaned_artifacts, run_for_orphaned = self._load_artifacts_with_runs(
            include_ids=normalized_ids
        )
        if not orphaned_artifacts:
            return []

        protected_paths, protected_mount_keys = (
            self._collect_surviving_artifact_protections(exclude_ids=normalized_ids)
        )
        known_mounts = self._collect_known_mount_roots_from_db()
        artifacts_by_id = {art.id: art for art in orphaned_artifacts}

        unshared_paths: list[Path] = []
        seen_paths: set[str] = set()
        for aid in normalized_ids:
            artifact = artifacts_by_id.get(aid)
            if artifact is None:
                continue
            mkey = self._custom_mount_key(artifact.uri)
            if mkey is not None and mkey in protected_mount_keys:
                continue
            run = run_for_orphaned.get(aid)
            for candidate in self._resolve_artifact_uri_candidates(
                artifact, run, fallback_mounts=known_mounts
            ):
                if not candidate.exists() and not candidate.is_symlink():
                    continue
                if any(
                    self._paths_conflict_for_delete(candidate, prot)
                    for prot in protected_paths
                ):
                    continue
                path_key = str(candidate)
                if path_key not in seen_paths:
                    seen_paths.add(path_key)
                    unshared_paths.append(candidate)

        return unshared_paths

    def _filter_paths_against_surviving_artifacts(
        self, candidate_paths: Sequence[Path]
    ) -> list[Path]:
        if not candidate_paths:
            return []
        protected_paths, _ = self._collect_surviving_artifact_protections()
        if not protected_paths:
            return list(candidate_paths)
        return [
            cand
            for cand in candidate_paths
            if not any(
                self._paths_conflict_for_delete(cand, prot) for prot in protected_paths
            )
        ]

    def _resolve_artifact_uri_candidates(
        self,
        artifact: Artifact,
        run: Optional[Run],
        *,
        fallback_mounts: Optional[Mapping[str, Sequence[Path]]] = None,
    ) -> list[Path]:
        uri = str(artifact.uri or "").strip()
        if not uri:
            return []

        run_dir = self._resolve_run_directory_for_artifact(run)
        run_meta = run.meta if run and isinstance(run.meta, Mapping) else {}
        artifact_meta = artifact.meta if isinstance(artifact.meta, Mapping) else {}
        candidate_paths: list[Path] = []
        seen_candidates: set[str] = set()

        def _append_candidate(path: Optional[Path]) -> None:
            if path is None:
                return
            path_key = str(path)
            if path_key not in seen_candidates:
                seen_candidates.add(path_key)
                candidate_paths.append(path)

        if uri.startswith("./"):
            _append_candidate(self._resolve_relative_path_under_root(uri[2:], run_dir))
            return candidate_paths

        if "://" in uri:
            scheme, rel_path = uri.split("://", 1)
            normalized_scheme = scheme.strip()

            if normalized_scheme == "file":
                _append_candidate(self._resolve_file_uri(uri))
                return candidate_paths

            if normalized_scheme == "workspace":
                _append_candidate(
                    self._resolve_relative_path_under_root(rel_path, run_dir)
                )
                return candidate_paths

            safe_relative = self._normalize_relative_uri_path(rel_path)
            if safe_relative is None:
                return candidate_paths

            for root in self._mount_roots_for_uri(
                normalized_scheme,
                run_meta=run_meta,
                artifact_meta=artifact_meta,
                fallback_mounts=fallback_mounts,
            ):
                _append_candidate(
                    self._resolve_relative_path_under_root(safe_relative, root)
                )
            return candidate_paths

        raw_path = Path(uri)
        if raw_path.is_absolute():
            try:
                _append_candidate(raw_path.resolve())
            except OSError:
                return candidate_paths

        return candidate_paths

    def _resolve_run_directory_for_artifact(self, run: Optional[Run]) -> Path:
        if run and isinstance(run.meta, Mapping):
            physical_run_dir = run.meta.get("_physical_run_dir")
            if isinstance(physical_run_dir, str) and physical_run_dir:
                try:
                    return Path(physical_run_dir).resolve()
                except OSError:
                    return self._resolve_default_run_directory()
        return self._resolve_default_run_directory()

    def _mount_roots_for_uri(
        self,
        scheme: str,
        *,
        run_meta: Mapping[str, Any],
        artifact_meta: Mapping[str, Any],
        fallback_mounts: Optional[Mapping[str, Sequence[Path]]] = None,
    ) -> list[Path]:
        roots: list[Path] = []
        seen_roots: set[str] = set()

        def _add_path(p: Optional[Path]) -> None:
            if p is None:
                return
            key = str(p)
            if key not in seen_roots:
                seen_roots.add(key)
                roots.append(p)

        if scheme in self._mounts:
            _add_path(self._normalize_mount_root_path(self._mounts[scheme]))

        mounts_raw = run_meta.get("mounts")
        if isinstance(mounts_raw, Mapping):
            mount_root = mounts_raw.get(scheme)
            if isinstance(mount_root, str) and mount_root:
                _add_path(self._normalize_mount_root_path(mount_root))

        artifact_mount_root = artifact_meta.get("mount_root")
        if isinstance(artifact_mount_root, str) and artifact_mount_root:
            _add_path(self._normalize_mount_root_path(artifact_mount_root))

        if fallback_mounts and scheme in fallback_mounts:
            for extra_root in fallback_mounts[scheme]:
                _add_path(extra_root)

        return roots

    @staticmethod
    def _normalize_mount_root_path(root: str) -> Optional[Path]:
        try:
            root_path = Path(root)
            if not root_path.is_absolute():
                return None
            return root_path.resolve()
        except OSError:
            return None

    @staticmethod
    def _normalize_relative_uri_path(value: str) -> Optional[str]:
        rel_path = PurePosixPath(value.strip())
        if not rel_path.parts or rel_path.is_absolute():
            return None
        if any(part == ".." for part in rel_path.parts):
            return None
        return rel_path.as_posix()

    @staticmethod
    def _resolve_relative_path_under_root(
        relative_path: str, root: Path
    ) -> Optional[Path]:
        safe_rel_path = DatabaseMaintenance._normalize_relative_uri_path(relative_path)
        if safe_rel_path is None:
            return None
        try:
            root_resolved = root.resolve()
            resolved_path = (root_resolved / safe_rel_path).resolve()
            resolved_path.relative_to(root_resolved)
        except (OSError, ValueError):
            return None
        return resolved_path

    @staticmethod
    def _resolve_file_uri(uri: str) -> Optional[Path]:
        parsed = urlsplit(uri)
        if parsed.scheme != "file":
            return None
        if parsed.netloc not in {"", "localhost"}:
            return None
        path_text = unquote(parsed.path or "")
        if not path_text:
            return None
        path = Path(path_text)
        if not path.is_absolute():
            return None
        try:
            return path.resolve()
        except OSError:
            return None

    def _init_shard_schema(self, shard_path: Path) -> None:
        target_path = Path(shard_path)
        self._ensure_distinct_from_source_db(target_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        for path in (target_path, Path(f"{target_path}.wal")):
            if path.exists() or path.is_symlink():
                path.unlink()
        shard_db = self.db.__class__(str(target_path))
        try:
            with shard_db.transaction():
                shard_db.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        finally:
            shard_db.close()

    def _resolve_run_snapshot_path(self, run_id: str, run: Optional[Run]) -> Path:
        run_dir = self.run_dir
        if run and isinstance(run.meta, Mapping):
            physical_run_dir = run.meta.get("_physical_run_dir")
            if isinstance(physical_run_dir, str) and physical_run_dir:
                run_dir = Path(physical_run_dir)
        safe_run_id = "".join(
            c if (c.isalnum() or c in ("-", "_", ".")) else "_" for c in run_id
        )
        return run_dir / "consist_runs" / f"{safe_run_id}.json"

    def _log_audit(self, operation: str, summary: str) -> None:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        timestamp = (
            datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
        )
        audit_path = self.run_dir / ".consist_audit.log"
        with audit_path.open("a", encoding="utf-8") as file:
            file.write(f"{timestamp}\t{operation}\t{summary}\n")

    @staticmethod
    def _normalize_run_ids(run_ids: Iterable[str] | str) -> list[str]:
        items = [run_ids] if isinstance(run_ids, str) else list(run_ids)
        normalized: list[str] = []
        seen: set[str] = set()
        for run_id in items:
            value = str(run_id).strip()
            if not value or value in seen:
                continue
            seen.add(value)
            normalized.append(value)
        return normalized

    @staticmethod
    def _coerce_uuid(value: object) -> Optional[uuid.UUID]:
        if isinstance(value, uuid.UUID):
            return value
        try:
            return uuid.UUID(str(value))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _validate_identifier(identifier: str, *, label: str) -> str:
        value = str(identifier).strip()
        if not _SAFE_IDENTIFIER_RE.fullmatch(value):
            raise ValueError(
                f"Invalid {label}. Only letters, numbers, and underscores are allowed, "
                "and the identifier must not start with a number."
            )
        return value

    @staticmethod
    def _quote_ident(identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    @staticmethod
    def _quote_sql_string_literal(value: str) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    @staticmethod
    def _qualified_table_lookup_name(
        table: str, *, schema: str = "main", catalog: Optional[str] = None
    ) -> str:
        parts = [schema, table]
        if catalog:
            parts.insert(0, catalog)
        return ".".join(parts)

    @staticmethod
    def _qualified_table_sql(
        table: str, *, schema: str = "main", catalog: Optional[str] = None
    ) -> str:
        parts = [
            DatabaseMaintenance._quote_ident(schema),
            DatabaseMaintenance._quote_ident(table),
        ]
        if catalog:
            parts.insert(0, DatabaseMaintenance._quote_ident(catalog))
        return ".".join(parts)
