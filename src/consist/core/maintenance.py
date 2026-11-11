from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path, PurePosixPath
import shutil
import tempfile
from typing import TYPE_CHECKING, Any, Iterator, Literal, Optional
from urllib.parse import unquote, urlsplit
import uuid

import duckdb

from consist.artifact import Artifact
from consist.core._performance_attribution import _track_maintenance_merge_phase
from consist.core.views import (
    _SAFE_IDENTIFIER_RE,
    _quote_ident as _shared_quote_ident,
    _validate_identifier as _shared_validate_identifier,
)
from consist.engine import (
    _surviving_row_is_fresh_on_disk,
    resolve_uri_to_path,
)
from consist.location import (
    Mount,
    SiteContext,
    lookup_live_ledger_by_path,
)
from consist.migrations.v1.migrator import migrate_v1_to_v2
from consist.models.run import (
    ConsistRecord,
    Run,
    resolve_canonical_run_meta_field,
)
from consist.store.detect import (
    ConsistError,
    IncompatibleSchemaError,
    SchemaState,
    detect_schema,
    inspect_v1_summary,
)
from consist.store.ledger import StoreLedger

if TYPE_CHECKING:
    from consist.core.persistence import DatabaseManager

GlobalTableMode = Literal["run_scoped", "run_link", "unscoped_cache"]
UnsafeDeleteTargetPolicy = Literal["fail", "skip"]
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
    """Read-only invariant diagnostics for Schema v2 maintenance checks."""

    zombie_run_ids: list[str]
    completed_without_end_time: list[str]
    dangling_parent_run_ids: list[str]
    artifacts_with_missing_producing_run: list[uuid.UUID]
    global_table_schema_drift: dict[str, str]
    orphaned_run_binding_count: int = 0
    artifacts_with_missing_content: list[uuid.UUID] = field(default_factory=list)
    digest_conflict_count: int = 0


@dataclass(slots=True)
class RebuildResult:
    """Result payload for JSON-to-DB rebuild operations."""

    json_files_scanned: int
    runs_inserted: int
    runs_already_present: int
    artifacts_inserted: int
    errors: list[str] = field(default_factory=list)
    dry_run: bool = False


@dataclass(frozen=True, slots=True)
class _PruneCandidate:
    path: Path
    artifact: Artifact
    obs_row: dict[str, Any]


class DatabaseMaintenance:
    """
    Maintenance service for Consist Schema v2 provenance databases (`StoreLedger`).

    This class avoids depending on `Tracker` so maintenance routines can be
    tested and executed in isolation using only `DatabaseManager` and a run
    directory root.
    """

    def __init__(self, db: DatabaseManager, run_dir: Path) -> None:
        self.db = db
        self.run_dir = Path(run_dir)

    @property
    def ledger(self) -> StoreLedger:
        return self.db.ledger

    def _effective_mounts(self) -> dict[str, Mount]:
        run_root = self._resolve_default_run_directory()
        mounts: dict[str, Mount] = {"workspace": Mount("workspace", run_root)}
        mounts.update(self.ledger.mounts)
        return mounts

    def _deletion_allowed_roots(self) -> tuple[Path, ...]:
        roots: list[Path] = []
        seen: set[str] = set()
        for raw_root in (
            self.run_dir,
            *(mount.root for mount in self.ledger.mounts.values()),
        ):
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
        ledger = self.ledger
        with ledger._lock:
            if ledger._tx_depth > 0:
                raise ConsistError(
                    "Cannot ATTACH shard database inside an active transaction."
                )
            if mutate_main:
                ledger._require_writable("attach_mutate_main")
            con = ledger.connection
            con.execute(f"ATTACH {quoted_path} AS {quoted_alias}{attach_opts}")
            try:
                if mutate_main:
                    with ledger.transaction() as cur:
                        yield cur
                else:
                    with ledger.exec_cursor() as cur:
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
                    con.execute(f"DETACH {quoted_alias}")
                except duckdb.Error:
                    pass

    @contextmanager
    def _mountable_shard_path(
        self,
        shard_path: Path,
        *,
        auto_migrate: bool | Literal["quick", "full"] = False,
    ) -> Iterator[Path]:
        state = detect_schema(shard_path)
        if state == SchemaState.V1:
            if not auto_migrate or auto_migrate == "off":
                raise inspect_v1_summary(shard_path)
            mig_mode: Literal["quick", "full"] = (
                "quick" if auto_migrate == "quick" else "full"
            )
            with tempfile.TemporaryDirectory(prefix="consist_merge_v1_") as tmp_dir:
                tmp_db = Path(tmp_dir) / shard_path.name
                shutil.copy2(shard_path, tmp_db)
                wal_src = Path(f"{shard_path}.wal")
                if wal_src.exists():
                    shutil.copy2(wal_src, Path(f"{tmp_db}.wal"))
                migrate_v1_to_v2(
                    tmp_db,
                    quick=(mig_mode == "quick"),
                    mounts=self.ledger.mounts,
                )
                yield tmp_db
                return

        if state != SchemaState.V2:
            raise IncompatibleSchemaError(
                f"Cannot merge database {shard_path}: unsupported schema state {state.value!r}."
            )

        live_shard = lookup_live_ledger_by_path(shard_path)
        if live_shard is not None and not live_shard.read_only:
            with tempfile.TemporaryDirectory(prefix="consist_merge_live_") as tmp_dir:
                tmp_snap = Path(tmp_dir) / shard_path.name
                with live_shard._lock:
                    if live_shard._tx_depth == 0:
                        live_shard.connection.execute("FORCE CHECKPOINT")
                    shutil.copy2(shard_path, tmp_snap)
                    wal_src = Path(f"{shard_path}.wal")
                    if live_shard._tx_depth > 0 and wal_src.exists():
                        shutil.copy2(wal_src, Path(f"{tmp_snap}.wal"))
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
        def _query() -> tuple[int, dict[str, int], int, int, list[str], list[str]]:
            with self.ledger.exec_cursor() as cur:
                total_runs_row = cur.execute("SELECT COUNT(*) FROM main.run").fetchone()
                total_runs = int(total_runs_row[0] if total_runs_row else 0)

                status_rows = cur.execute(
                    """
                    SELECT status, COUNT(*) AS count
                    FROM main.run
                    GROUP BY status
                    ORDER BY status
                    """
                ).fetchall()
                runs_by_status = {
                    str(status): int(count) for status, count in status_rows if status
                }

                total_artifacts_row = cur.execute(
                    "SELECT COUNT(*) FROM main.artifact"
                ).fetchone()
                total_artifacts = int(
                    total_artifacts_row[0] if total_artifacts_row else 0
                )

                orphaned_row = cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM main.artifact a
                    WHERE NOT EXISTS (
                        SELECT 1 FROM main.run_binding rb
                        WHERE rb.artifact_id = a.id
                    )
                    """
                ).fetchone()
                orphaned_artifact_count = int(orphaned_row[0] if orphaned_row else 0)

                zombie_rows = cur.execute(
                    """
                    SELECT id
                    FROM main.run
                    WHERE status = 'running'
                      AND ended_at IS NOT NULL
                    ORDER BY id
                    """
                ).fetchall()
                zombie_run_ids = [str(row[0]) for row in zombie_rows]

                db_run_rows = cur.execute(
                    """
                    SELECT id
                    FROM main.run
                    ORDER BY id
                    """
                ).fetchall()
                db_run_ids = [str(row[0]) for row in db_run_rows]

            return (
                total_runs,
                runs_by_status,
                total_artifacts,
                orphaned_artifact_count,
                zombie_run_ids,
                db_run_ids,
            )

        (
            total_runs,
            runs_by_status,
            total_artifacts,
            orphaned_artifact_count,
            zombie_run_ids,
            db_run_ids,
        ) = self.db.execute_with_retry(_query, operation_name="maintenance_inspect")

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
            sorted(snapshot_dir.glob("*.json")) if snapshot_dir.exists() else []
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
        def _query() -> tuple[
            list[str],
            list[str],
            list[str],
            list[uuid.UUID],
            int,
            list[uuid.UUID],
            int,
            dict[str, str],
        ]:
            with self.ledger.exec_cursor() as cur:
                zombie_rows = cur.execute(
                    """
                    SELECT id
                    FROM main.run
                    WHERE status = 'running'
                      AND ended_at IS NOT NULL
                    ORDER BY id
                    """
                ).fetchall()
                zombie_run_ids = [str(row[0]) for row in zombie_rows]

                completed_without_end_rows = cur.execute(
                    """
                    SELECT id
                    FROM main.run
                    WHERE status IN ('completed', 'failed')
                      AND ended_at IS NULL
                    ORDER BY id
                    """
                ).fetchall()
                completed_without_end_time = [
                    str(row[0]) for row in completed_without_end_rows
                ]

                dangling_parent_rows = cur.execute(
                    """
                    SELECT DISTINCT child.parent_run_id
                    FROM main.run child
                    LEFT JOIN main.run parent
                      ON child.parent_run_id = parent.id
                    WHERE child.parent_run_id IS NOT NULL
                      AND parent.id IS NULL
                    ORDER BY child.parent_run_id
                    """
                ).fetchall()
                dangling_parent_run_ids = [
                    str(row[0]) for row in dangling_parent_rows if row[0]
                ]

                missing_producer_rows = cur.execute(
                    """
                    SELECT a.id
                    FROM main.artifact a
                    LEFT JOIN main.run r
                      ON a.run_id = r.id
                    WHERE a.run_id IS NOT NULL
                      AND r.id IS NULL
                    ORDER BY CAST(a.id AS VARCHAR)
                    """
                ).fetchall()
                artifacts_with_missing_producing_run: list[uuid.UUID] = []
                for row in missing_producer_rows:
                    parsed_uuid = self._coerce_uuid(row[0])
                    if parsed_uuid is not None:
                        artifacts_with_missing_producing_run.append(parsed_uuid)

                orphaned_binding_row = cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM main.run_binding rb
                    LEFT JOIN main.run r ON rb.run_id = r.id
                    LEFT JOIN main.artifact a ON rb.artifact_id = a.id
                    WHERE r.id IS NULL OR a.id IS NULL
                    """
                ).fetchone()
                orphaned_run_binding_count = int(
                    orphaned_binding_row[0] if orphaned_binding_row else 0
                )

                missing_content_rows = cur.execute(
                    """
                    SELECT a.id
                    FROM main.artifact a
                    LEFT JOIN main.content c ON a.anchor = c.anchor
                    WHERE c.anchor IS NULL
                    ORDER BY CAST(a.id AS VARCHAR)
                    """
                ).fetchall()
                artifacts_with_missing_content: list[uuid.UUID] = []
                for row in missing_content_rows:
                    parsed_uuid = self._coerce_uuid(row[0])
                    if parsed_uuid is not None:
                        artifacts_with_missing_content.append(parsed_uuid)

                conflict_row = cur.execute(
                    "SELECT COUNT(*) FROM main.content_digest_conflict"
                ).fetchone()
                digest_conflict_count = int(conflict_row[0] if conflict_row else 0)

                drift: dict[str, str] = {}
                gt_rows = cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'global_tables'
                      AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                    """
                ).fetchall()
                for (tname_raw,) in gt_rows:
                    tname = str(tname_raw)
                    if not _SAFE_IDENTIFIER_RE.fullmatch(tname) or tname.startswith(
                        "_dlt_"
                    ):
                        continue
                    col_rows = cur.execute(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'global_tables' AND table_name = ?
                        """,
                        [tname],
                    ).fetchall()
                    cols = {str(r[0]) for r in col_rows}
                    quoted_t = self._quote_ident(tname)
                    if "consist_run_id" in cols:
                        orphan_gt = cur.execute(
                            f"""
                            SELECT COUNT(*)
                            FROM global_tables.{quoted_t} gt
                            LEFT JOIN main.run r
                              ON CAST(gt."consist_run_id" AS VARCHAR) = r.id
                            WHERE gt."consist_run_id" IS NOT NULL AND r.id IS NULL
                            """
                        ).fetchone()
                        cnt = int(orphan_gt[0] if orphan_gt else 0)
                        if cnt > 0:
                            drift[tname] = (
                                f"{cnt} row(s) reference missing consist_run_id"
                            )

            return (
                zombie_run_ids,
                completed_without_end_time,
                dangling_parent_run_ids,
                artifacts_with_missing_producing_run,
                orphaned_run_binding_count,
                artifacts_with_missing_content,
                digest_conflict_count,
                drift,
            )

        (
            zombie_run_ids,
            completed_without_end_time,
            dangling_parent_run_ids,
            artifacts_with_missing_producing_run,
            orphaned_run_binding_count,
            artifacts_with_missing_content,
            digest_conflict_count,
            drift,
        ) = self.db.execute_with_retry(_query, operation_name="maintenance_doctor")

        return DoctorReport(
            zombie_run_ids=zombie_run_ids,
            completed_without_end_time=completed_without_end_time,
            dangling_parent_run_ids=dangling_parent_run_ids,
            artifacts_with_missing_producing_run=artifacts_with_missing_producing_run,
            global_table_schema_drift=drift,
            orphaned_run_binding_count=orphaned_run_binding_count,
            artifacts_with_missing_content=artifacts_with_missing_content,
            digest_conflict_count=digest_conflict_count,
        )

    def _load_runs_by_ids(self, run_ids: Sequence[str]) -> dict[str, Run]:
        if not run_ids:
            return {}
        with self.ledger.exec_cursor() as cur:
            rows = cur.execute(
                f"SELECT {StoreLedger._RUN_SELECT_COLUMNS} FROM main.run "
                "WHERE id IN (SELECT UNNEST(?))",
                [list(run_ids)],
            ).fetchall()
        runs = [StoreLedger._hydrate_run_row(r) for r in rows]
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
        prune_candidates = self._collect_prune_candidates(orphaned_artifact_ids)
        disk_files, unsafe_disk_files = self._classify_delete_targets(
            [cand.path for cand in prune_candidates]
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
        prune_files: Optional[bool] = None,
        unsafe_delete_targets: UnsafeDeleteTargetPolicy = "fail",
    ) -> PurgeResult:
        effective_delete_files = (
            bool(prune_files) if prune_files is not None else bool(delete_files)
        )
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
        selected_orphaned_ids = [str(value) for value in plan.orphaned_artifact_ids]
        prune_candidates = (
            self._collect_prune_candidates(plan.orphaned_artifact_ids)
            if effective_delete_files
            else []
        )

        def _execute() -> None:
            with self.ledger.transaction() as cur:
                # 1. Operational run/binding tables (FK-safe order, base tables ONLY)
                if selected_run_ids and selected_orphaned_ids:
                    cur.execute(
                        """
                        DELETE FROM main.run_coupler_snapshot
                        WHERE run_id IN (SELECT UNNEST(?))
                           OR scenario_run_id IN (SELECT UNNEST(?))
                           OR CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                        """,
                        [selected_run_ids, selected_run_ids, selected_orphaned_ids],
                    )
                elif selected_run_ids:
                    cur.execute(
                        """
                        DELETE FROM main.run_coupler_snapshot
                        WHERE run_id IN (SELECT UNNEST(?))
                           OR scenario_run_id IN (SELECT UNNEST(?))
                        """,
                        [selected_run_ids, selected_run_ids],
                    )
                elif selected_orphaned_ids:
                    cur.execute(
                        """
                        DELETE FROM main.run_coupler_snapshot
                        WHERE CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                        """,
                        [selected_orphaned_ids],
                    )

                if selected_run_ids:
                    cur.execute(
                        """
                        UPDATE main.run_coupler_snapshot
                        SET producer_run_id = NULL
                        WHERE producer_run_id IN (SELECT UNNEST(?))
                        """,
                        [selected_run_ids],
                    )
                    cur.execute(
                        """
                        DELETE FROM main.run_binding_invocation
                        WHERE requested_run_id IN (SELECT UNNEST(?))
                           OR execution_run_id IN (SELECT UNNEST(?))
                        """,
                        [selected_run_ids, selected_run_ids],
                    )
                    cur.execute(
                        """
                        UPDATE main.run_binding_invocation
                        SET cache_source_run_id = NULL
                        WHERE cache_source_run_id IN (SELECT UNNEST(?))
                        """,
                        [selected_run_ids],
                    )

                if selected_run_ids and selected_orphaned_ids:
                    cur.execute(
                        """
                        DELETE FROM main.artifact_schema_observation
                        WHERE run_id IN (SELECT UNNEST(?))
                           OR CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                        """,
                        [selected_run_ids, selected_orphaned_ids],
                    )
                    cur.execute(
                        """
                        DELETE FROM main.run_binding
                        WHERE run_id IN (SELECT UNNEST(?))
                           OR CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                        """,
                        [selected_run_ids, selected_orphaned_ids],
                    )
                elif selected_run_ids:
                    cur.execute(
                        """
                        DELETE FROM main.artifact_schema_observation
                        WHERE run_id IN (SELECT UNNEST(?))
                        """,
                        [selected_run_ids],
                    )
                    cur.execute(
                        """
                        DELETE FROM main.run_binding
                        WHERE run_id IN (SELECT UNNEST(?))
                        """,
                        [selected_run_ids],
                    )
                elif selected_orphaned_ids:
                    cur.execute(
                        """
                        DELETE FROM main.artifact_schema_observation
                        WHERE CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                        """,
                        [selected_orphaned_ids],
                    )
                    cur.execute(
                        """
                        DELETE FROM main.run_binding
                        WHERE CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                        """,
                        [selected_orphaned_ids],
                    )

                # 2. Collect candidate anchors before deleting orphaned artifacts
                candidate_anchors: list[str] = []
                if selected_orphaned_ids:
                    anchor_rows = cur.execute(
                        """
                        SELECT DISTINCT anchor
                        FROM main.artifact
                        WHERE CAST(id AS VARCHAR) IN (SELECT UNNEST(?))
                        """,
                        [selected_orphaned_ids],
                    ).fetchall()
                    candidate_anchors = [str(r[0]) for r in anchor_rows if r[0]]

                    cur.execute(
                        """
                        UPDATE main.artifact
                        SET parent_artifact_id = NULL
                        WHERE CAST(parent_artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                          AND CAST(id AS VARCHAR) NOT IN (SELECT UNNEST(?))
                        """,
                        [selected_orphaned_ids, selected_orphaned_ids],
                    )

                if selected_run_ids:
                    if selected_orphaned_ids:
                        cur.execute(
                            """
                            UPDATE main.artifact
                            SET run_id = NULL
                            WHERE run_id IN (SELECT UNNEST(?))
                              AND CAST(id AS VARCHAR) NOT IN (SELECT UNNEST(?))
                            """,
                            [selected_run_ids, selected_orphaned_ids],
                        )
                    else:
                        cur.execute(
                            """
                            UPDATE main.artifact
                            SET run_id = NULL
                            WHERE run_id IN (SELECT UNNEST(?))
                            """,
                            [selected_run_ids],
                        )
                    cur.execute(
                        """
                        UPDATE main.run
                        SET parent_run_id = NULL
                        WHERE parent_run_id IN (SELECT UNNEST(?))
                          AND id NOT IN (SELECT UNNEST(?))
                        """,
                        [selected_run_ids, selected_run_ids],
                    )
                    cur.execute(
                        "DELETE FROM main.run WHERE id IN (SELECT UNNEST(?))",
                        [selected_run_ids],
                    )

                if selected_orphaned_ids:
                    cur.execute(
                        """
                        DELETE FROM main.artifact_migration_audit
                        WHERE CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                        """,
                        [selected_orphaned_ids],
                    )
                    cur.execute(
                        """
                        DELETE FROM main.artifact
                        WHERE CAST(id AS VARCHAR) IN (SELECT UNNEST(?))
                        """,
                        [selected_orphaned_ids],
                    )

                # 3. Clean up content/location/manifest/promotion rows for anchors with
                # ZERO remaining artifact or binding references
                if candidate_anchors:
                    cur.execute(
                        """
                        DELETE FROM main.anchor_promotion
                        WHERE historical_anchor IN (SELECT UNNEST(?))
                          AND NOT EXISTS (
                              SELECT 1 FROM main.artifact a
                              WHERE a.anchor = main.anchor_promotion.historical_anchor
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM main.run_binding rb
                              WHERE rb.anchor = main.anchor_promotion.historical_anchor
                          )
                        """,
                        [candidate_anchors],
                    )
                    unref_rows = cur.execute(
                        """
                        SELECT c.anchor
                        FROM (SELECT DISTINCT UNNEST(?) AS anchor) c
                        WHERE NOT EXISTS (
                            SELECT 1 FROM main.artifact a WHERE a.anchor = c.anchor
                        )
                        AND NOT EXISTS (
                            SELECT 1 FROM main.run_binding rb WHERE rb.anchor = c.anchor
                        )
                        AND NOT EXISTS (
                            SELECT 1 FROM main.anchor_promotion ap
                            WHERE ap.historical_anchor = c.anchor
                               OR ap.verified_anchor = c.anchor
                        )
                        """,
                        [candidate_anchors],
                    ).fetchall()
                    unref_anchors = [str(r[0]) for r in unref_rows if r[0]]
                    if unref_anchors:
                        cur.execute(
                            """
                            DELETE FROM main.location_event
                            WHERE observation_id IN (
                                SELECT observation_id
                                FROM main.artifact_location
                                WHERE anchor IN (SELECT UNNEST(?))
                            )
                            """,
                            [unref_anchors],
                        )
                        cur.execute(
                            """
                            DELETE FROM main.artifact_location
                            WHERE anchor IN (SELECT UNNEST(?))
                            """,
                            [unref_anchors],
                        )
                        cur.execute(
                            """
                            DELETE FROM main.content_derivation
                            WHERE src_anchor IN (SELECT UNNEST(?))
                               OR dst_anchor IN (SELECT UNNEST(?))
                            """,
                            [unref_anchors, unref_anchors],
                        )
                        cur.execute(
                            """
                            DELETE FROM main.content_manifest
                            WHERE anchor IN (SELECT UNNEST(?))
                            """,
                            [unref_anchors],
                        )
                        cur.execute(
                            """
                            DELETE FROM main.content_digest_conflict
                            WHERE anchor IN (SELECT UNNEST(?))
                            """,
                            [unref_anchors],
                        )
                        cur.execute(
                            """
                            DELETE FROM main.content_digest
                            WHERE anchor IN (SELECT UNNEST(?))
                            """,
                            [unref_anchors],
                        )
                        cur.execute(
                            """
                            DELETE FROM main.content
                            WHERE anchor IN (SELECT UNNEST(?))
                            """,
                            [unref_anchors],
                        )

                # 4. Clean up unreferenced artifact_schema* rows
                unref_schema_rows = cur.execute(
                    """
                    SELECT s.id
                    FROM main.artifact_schema s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM main.artifact a WHERE a.schema_id = s.id
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM main.artifact_schema_observation o
                        WHERE o.schema_id = s.id
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM main.content c WHERE c.schema_id = s.id
                    )
                    """
                ).fetchall()
                unref_schema_ids = [str(r[0]) for r in unref_schema_rows if r[0]]
                if unref_schema_ids:
                    cur.execute(
                        """
                        DELETE FROM main.artifact_schema_relation
                        WHERE schema_id IN (SELECT UNNEST(?))
                        """,
                        [unref_schema_ids],
                    )
                    cur.execute(
                        """
                        DELETE FROM main.artifact_schema_field
                        WHERE schema_id IN (SELECT UNNEST(?))
                        """,
                        [unref_schema_ids],
                    )
                    cur.execute(
                        """
                        DELETE FROM main.artifact_schema
                        WHERE id IN (SELECT UNNEST(?))
                        """,
                        [unref_schema_ids],
                    )

                # 5. Global tables cleanup when delete_ingested_data=True
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
                            column_rows = cur.execute(
                                """
                                SELECT column_name
                                FROM information_schema.columns
                                WHERE table_schema = 'global_tables'
                                  AND table_name = ?
                                """,
                                [safe_table],
                            ).fetchall()
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
                                hash_rows = cur.execute(
                                    f"""
                                    SELECT DISTINCT CAST("content_hash" AS VARCHAR)
                                    FROM global_tables.{quoted_table}
                                    WHERE "run_id" IN (SELECT UNNEST(?))
                                      AND "content_hash" IS NOT NULL
                                    """,
                                    [selected_run_ids],
                                ).fetchall()
                                purged_content_hashes.update(
                                    str(row[0])
                                    for row in hash_rows
                                    if row[0] is not None
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
                        cur.execute(
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
                            surviving_hash_rows = cur.execute(
                                f"""
                                SELECT DISTINCT CAST("content_hash" AS VARCHAR)
                                FROM global_tables.{quoted_table}
                                WHERE CAST("content_hash" AS VARCHAR) IN (SELECT UNNEST(?))
                                  AND "content_hash" IS NOT NULL
                                """,
                                [sorted_purged_hashes],
                            ).fetchall()
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
                                cur.execute(
                                    f"""
                                    DELETE FROM global_tables.{quoted_table}
                                    WHERE CAST("content_hash" AS VARCHAR) IN (SELECT UNNEST(?))
                                    """,
                                    [unreferenced_hashes],
                                )

        self.db.execute_with_retry(_execute, operation_name="maintenance_purge")
        with self.ledger._lock:
            self.ledger._memoized_observations.clear()
            self.ledger._memoized_locations.clear()

        for json_path in plan.json_files:
            try:
                json_path.unlink(missing_ok=True)
            except OSError:
                continue

        if effective_delete_files and prune_candidates:
            safe_disk_set = {str(p) for p in plan.disk_files}
            self._unlink_verified_prune_candidates(
                prune_candidates,
                safe_disk_set=safe_disk_set,
            )

        self._log_audit(
            "purge",
            (
                f"runs={len(plan.run_ids)} "
                f"children={len(plan.child_run_ids)} "
                f"orphaned_artifacts={len(plan.orphaned_artifact_ids)} "
                f"json_files={len(plan.json_files)} "
                f"delete_ingested_data={delete_ingested_data} "
                f"prune_cache={prune_cache} "
                f"delete_files={effective_delete_files}"
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

    def _unlink_verified_prune_candidates(
        self,
        candidates: list[_PruneCandidate],
        *,
        safe_disk_set: set[str],
    ) -> None:
        effective_mounts = self._effective_mounts()
        site_ctx = SiteContext.detect()

        with self.ledger.exec_cursor() as cur:
            surviving_loc_rows = cur.execute(
                """
                SELECT DISTINCT l.uri
                FROM main.artifact_location_current l
                JOIN main.artifact a
                  ON (
                      a.anchor = l.anchor
                      OR EXISTS (
                          SELECT 1 FROM main.anchor_promotion ap
                          WHERE ap.historical_anchor = a.anchor
                            AND ap.verified_anchor = l.anchor
                      )
                      OR EXISTS (
                          SELECT 1 FROM main.anchor_promotion ap2
                          WHERE ap2.verified_anchor = a.anchor
                            AND ap2.historical_anchor = l.anchor
                      )
                  )
                WHERE l.status = 'active'
                  AND l.uri NOT LIKE 'db://%'
                """
            ).fetchall()

        surviving_disk_paths: set[str] = set()
        for (surv_uri_raw,) in surviving_loc_rows:
            try:
                surv_path, _ = resolve_uri_to_path(
                    str(surv_uri_raw), mounts=effective_mounts
                )
            except ValueError:
                continue
            if surv_path is not None:
                surviving_disk_paths.add(str(surv_path))

        for cand in candidates:
            disk_path = cand.path
            if str(disk_path) not in safe_disk_set:
                continue
            if str(disk_path) in surviving_disk_paths:
                continue
            if not disk_path.exists() and not disk_path.is_symlink():
                continue

            check_anchors = [str(cand.artifact.identity.anchor)]
            if cand.artifact.recorded_anchor is not None:
                rec_str = str(cand.artifact.recorded_anchor)
                if rec_str not in check_anchors:
                    check_anchors.append(rec_str)

            with self.ledger.exec_cursor() as cur:
                remaining_row = cur.execute(
                    """
                    SELECT (
                        (SELECT COUNT(*) FROM main.artifact WHERE anchor IN (SELECT UNNEST(?)))
                        + (SELECT COUNT(*) FROM main.run_binding WHERE anchor IN (SELECT UNNEST(?)))
                        + (
                            SELECT COUNT(*)
                            FROM main.anchor_promotion ap
                            JOIN main.artifact a
                              ON a.anchor = ap.historical_anchor OR a.anchor = ap.verified_anchor
                            WHERE ap.verified_anchor IN (SELECT UNNEST(?))
                               OR ap.historical_anchor IN (SELECT UNNEST(?))
                        )
                        + (
                            SELECT COUNT(*)
                            FROM main.anchor_promotion ap
                            JOIN main.run_binding rb
                              ON rb.anchor = ap.historical_anchor OR rb.anchor = ap.verified_anchor
                            WHERE ap.verified_anchor IN (SELECT UNNEST(?))
                               OR ap.historical_anchor IN (SELECT UNNEST(?))
                        )
                    )
                    """,
                    [
                        check_anchors,
                        check_anchors,
                        check_anchors,
                        check_anchors,
                        check_anchors,
                        check_anchors,
                    ],
                ).fetchone()
            if int(remaining_row[0] if remaining_row else 0) > 0:
                continue

            verify_row = dict(cand.obs_row)
            if cand.artifact.identity.verifiable:
                verify_row["witness"] = None
                verify_row["witness_json"] = None

            row_site_raw = verify_row.get("site")
            check_ctx = (
                site_ctx
                if row_site_raw is None or str(row_site_raw) == site_ctx.site
                else None
            )
            if not _surviving_row_is_fresh_on_disk(
                verify_row,
                artifact=cand.artifact,
                effective_mounts=effective_mounts,
                store_ledger=self.ledger,
                site_context=check_ctx,
            ):
                continue

            try:
                if disk_path.is_dir() and not disk_path.is_symlink():
                    shutil.rmtree(disk_path, ignore_errors=True)
                else:
                    disk_path.unlink(missing_ok=True)
            except OSError:
                continue

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

        def _update() -> Run:
            with self.ledger.transaction():
                run = self.ledger.get_run(normalized_run_id)
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

                self.ledger.upsert_run(run)
                refreshed = self.ledger.get_run(normalized_run_id)
                return refreshed if refreshed is not None else run

        updated_run = self.db.execute_with_retry(
            _update, operation_name="maintenance_fix_status"
        )
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
        normalized_run_ids = self._normalize_run_ids(run_ids)
        expanded_run_ids = (
            self._expand_run_ids(normalized_run_ids)
            if include_children
            else normalized_run_ids
        )
        out_path = Path(out_path)

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
                with self.ledger.exec_cursor() as cur:
                    artifact_rows = cur.execute(
                        """
                        SELECT DISTINCT CAST(id AS VARCHAR)
                        FROM (
                            SELECT artifact_id AS id
                            FROM main.run_binding
                            WHERE run_id IN (SELECT UNNEST(?))
                            UNION
                            SELECT id
                            FROM main.artifact
                            WHERE run_id IN (SELECT UNNEST(?))
                            UNION
                            SELECT artifact_id AS id
                            FROM main.run_coupler_snapshot
                            WHERE run_id IN (SELECT UNNEST(?))
                               OR scenario_run_id IN (SELECT UNNEST(?))
                        )
                        ORDER BY 1
                        """,
                        [
                            selected_run_ids,
                            selected_run_ids,
                            selected_run_ids,
                            selected_run_ids,
                        ],
                    ).fetchall()
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

        def _write() -> None:
            nonlocal artifact_count, ingested_rows, unscoped_cache_tables_skipped
            alias = "shard_export"
            q_alias = self._quote_ident(alias)
            with self._attached_database(
                out_path, alias, read_only=False, mutate_main=False
            ) as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {q_alias}.global_tables")

                if selected_run_ids:
                    cur.execute(
                        f"""
                        INSERT INTO {q_alias}.main.run
                        SELECT * FROM main.run
                        WHERE id IN (SELECT UNNEST(?))
                        ON CONFLICT (id) DO NOTHING
                        """,
                        [selected_run_ids],
                    )
                    cur.execute(
                        f"""
                        INSERT INTO {q_alias}.main.run_binding
                        SELECT * FROM main.run_binding
                        WHERE run_id IN (SELECT UNNEST(?))
                        ON CONFLICT DO NOTHING
                        """,
                        [selected_run_ids],
                    )
                    cur.execute(
                        f"""
                        INSERT INTO {q_alias}.main.run_binding_invocation
                        SELECT * FROM main.run_binding_invocation
                        WHERE requested_run_id IN (SELECT UNNEST(?))
                           OR execution_run_id IN (SELECT UNNEST(?))
                        ON CONFLICT (id) DO NOTHING
                        """,
                        [selected_run_ids, selected_run_ids],
                    )
                    cur.execute(
                        f"""
                        INSERT INTO {q_alias}.main.run_coupler_snapshot
                        SELECT * FROM main.run_coupler_snapshot
                        WHERE run_id IN (SELECT UNNEST(?))
                           OR scenario_run_id IN (SELECT UNNEST(?))
                        ON CONFLICT DO NOTHING
                        """,
                        [selected_run_ids, selected_run_ids],
                    )

                    art_rows = cur.execute(
                        """
                        SELECT DISTINCT CAST(id AS VARCHAR)
                        FROM (
                            SELECT artifact_id AS id
                            FROM main.run_binding
                            WHERE run_id IN (SELECT UNNEST(?))
                            UNION
                            SELECT id
                            FROM main.artifact
                            WHERE run_id IN (SELECT UNNEST(?))
                            UNION
                            SELECT artifact_id AS id
                            FROM main.run_coupler_snapshot
                            WHERE run_id IN (SELECT UNNEST(?))
                               OR scenario_run_id IN (SELECT UNNEST(?))
                        )
                        ORDER BY 1
                        """,
                        [
                            selected_run_ids,
                            selected_run_ids,
                            selected_run_ids,
                            selected_run_ids,
                        ],
                    ).fetchall()
                    artifact_ids = [str(r[0]) for r in art_rows if r[0]]
                    artifact_count = len(artifact_ids)

                    if artifact_ids:
                        cur.execute(
                            f"""
                            INSERT INTO {q_alias}.main.artifact
                            SELECT * FROM main.artifact
                            WHERE CAST(id AS VARCHAR) IN (SELECT UNNEST(?))
                            ON CONFLICT (id) DO NOTHING
                            """,
                            [artifact_ids],
                        )
                        cur.execute(
                            f"""
                            INSERT INTO {q_alias}.main.artifact_migration_audit
                            SELECT * FROM main.artifact_migration_audit
                            WHERE CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                            ON CONFLICT (artifact_id) DO NOTHING
                            """,
                            [artifact_ids],
                        )

                    anchor_rows = cur.execute(
                        f"""
                        SELECT DISTINCT anchor
                        FROM (
                            SELECT anchor FROM {q_alias}.main.artifact
                            UNION
                            SELECT anchor FROM {q_alias}.main.run_binding
                        )
                        WHERE anchor IS NOT NULL
                        """
                    ).fetchall()
                    base_anchors = [str(r[0]) for r in anchor_rows if r[0]]

                    if base_anchors:
                        extra_anchor_rows = cur.execute(
                            """
                            SELECT DISTINCT a
                            FROM (
                                SELECT UNNEST(?) AS a
                                UNION
                                SELECT historical_anchor AS a FROM main.anchor_promotion
                                WHERE historical_anchor IN (SELECT UNNEST(?))
                                   OR verified_anchor IN (SELECT UNNEST(?))
                                UNION
                                SELECT verified_anchor AS a FROM main.anchor_promotion
                                WHERE historical_anchor IN (SELECT UNNEST(?))
                                   OR verified_anchor IN (SELECT UNNEST(?))
                                UNION
                                SELECT src_anchor AS a FROM main.content_derivation
                                WHERE src_anchor IN (SELECT UNNEST(?))
                                   OR dst_anchor IN (SELECT UNNEST(?))
                                UNION
                                SELECT dst_anchor AS a FROM main.content_derivation
                                WHERE src_anchor IN (SELECT UNNEST(?))
                                   OR dst_anchor IN (SELECT UNNEST(?))
                            )
                            WHERE a IS NOT NULL
                            """,
                            [
                                base_anchors,
                                base_anchors,
                                base_anchors,
                                base_anchors,
                                base_anchors,
                                base_anchors,
                                base_anchors,
                                base_anchors,
                                base_anchors,
                            ],
                        ).fetchall()
                        export_anchors = [str(r[0]) for r in extra_anchor_rows if r[0]]

                        cur.execute(
                            f"""
                            INSERT INTO {q_alias}.main.content
                            SELECT * FROM main.content
                            WHERE anchor IN (SELECT UNNEST(?))
                            ON CONFLICT (anchor) DO NOTHING
                            """,
                            [export_anchors],
                        )
                        cur.execute(
                            f"""
                            INSERT INTO {q_alias}.main.content_digest
                            SELECT * FROM main.content_digest
                            WHERE anchor IN (SELECT UNNEST(?))
                            ON CONFLICT DO NOTHING
                            """,
                            [export_anchors],
                        )
                        cur.execute(
                            f"""
                            INSERT INTO {q_alias}.main.content_digest_conflict
                            SELECT * FROM main.content_digest_conflict
                            WHERE anchor IN (SELECT UNNEST(?))
                            """,
                            [export_anchors],
                        )
                        cur.execute(
                            f"""
                            INSERT INTO {q_alias}.main.content_manifest
                            SELECT * FROM main.content_manifest
                            WHERE anchor IN (SELECT UNNEST(?))
                            ON CONFLICT (anchor) DO NOTHING
                            """,
                            [export_anchors],
                        )
                        cur.execute(
                            f"""
                            INSERT INTO {q_alias}.main.digest_policy
                            SELECT dp.* FROM main.digest_policy dp
                            WHERE dp.policy_sha256 IN (
                                SELECT DISTINCT policy_sha256
                                FROM {q_alias}.main.content_manifest
                                WHERE policy_sha256 IS NOT NULL
                            )
                            ON CONFLICT (policy_sha256) DO NOTHING
                            """
                        )
                        cur.execute(
                            f"""
                            INSERT INTO {q_alias}.main.content_derivation
                            SELECT * FROM main.content_derivation
                            WHERE src_anchor IN (SELECT UNNEST(?))
                               OR dst_anchor IN (SELECT UNNEST(?))
                            ON CONFLICT DO NOTHING
                            """,
                            [export_anchors, export_anchors],
                        )
                        cur.execute(
                            f"""
                            INSERT INTO {q_alias}.main.anchor_promotion
                            SELECT * FROM main.anchor_promotion
                            WHERE historical_anchor IN (SELECT UNNEST(?))
                               OR verified_anchor IN (SELECT UNNEST(?))
                            ON CONFLICT (historical_anchor) DO NOTHING
                            """,
                            [export_anchors, export_anchors],
                        )
                        cur.execute(
                            f"""
                            INSERT INTO {q_alias}.main.artifact_location
                            SELECT * FROM main.artifact_location
                            WHERE anchor IN (SELECT UNNEST(?))
                            ON CONFLICT (observation_id) DO NOTHING
                            """,
                            [export_anchors],
                        )
                        cur.execute(
                            f"""
                            INSERT INTO {q_alias}.main.location_event
                            SELECT le.* FROM main.location_event le
                            JOIN {q_alias}.main.artifact_location al
                              ON le.observation_id = al.observation_id
                            ON CONFLICT (event_id) DO NOTHING
                            """
                        )

                    # Schema metadata
                    obs_params: list[Any] = [selected_run_ids]
                    obs_where = "run_id IN (SELECT UNNEST(?))"
                    if artifact_ids:
                        obs_where += (
                            " OR CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?))"
                        )
                        obs_params.append(artifact_ids)

                    schema_rows = cur.execute(
                        f"""
                        SELECT DISTINCT sid
                        FROM (
                            SELECT schema_id AS sid
                            FROM main.artifact_schema_observation
                            WHERE ({obs_where}) AND schema_id IS NOT NULL
                            UNION
                            SELECT schema_id AS sid
                            FROM {q_alias}.main.artifact
                            WHERE schema_id IS NOT NULL
                            UNION
                            SELECT schema_id AS sid
                            FROM {q_alias}.main.content
                            WHERE schema_id IS NOT NULL
                        )
                        """,
                        obs_params,
                    ).fetchall()
                    schema_ids = [str(r[0]) for r in schema_rows if r[0]]
                    if schema_ids:
                        cur.execute(
                            f"""
                            INSERT INTO {q_alias}.main.artifact_schema
                            SELECT * FROM main.artifact_schema
                            WHERE id IN (SELECT UNNEST(?))
                            ON CONFLICT (id) DO NOTHING
                            """,
                            [schema_ids],
                        )
                        cur.execute(
                            f"""
                            INSERT INTO {q_alias}.main.artifact_schema_field
                            SELECT * FROM main.artifact_schema_field
                            WHERE schema_id IN (SELECT UNNEST(?))
                            ON CONFLICT DO NOTHING
                            """,
                            [schema_ids],
                        )
                        cur.execute(
                            f"""
                            INSERT INTO {q_alias}.main.artifact_schema_relation
                            SELECT * FROM main.artifact_schema_relation
                            WHERE schema_id IN (SELECT UNNEST(?))
                            ON CONFLICT DO NOTHING
                            """,
                            [schema_ids],
                        )

                    cur.execute(
                        f"""
                        INSERT INTO {q_alias}.main.artifact_schema_observation
                        SELECT * FROM main.artifact_schema_observation
                        WHERE {obs_where}
                        ON CONFLICT (id) DO NOTHING
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
                            CREATE TABLE IF NOT EXISTS {q_alias}.global_tables.{quoted_table}
                            AS SELECT * FROM global_tables.{quoted_table} WHERE 1 = 0
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
                            INSERT INTO {q_alias}.global_tables.{quoted_table}
                            SELECT *
                            FROM global_tables.{quoted_table} AS "src_gt"
                            WHERE {filter_sql}
                            """
                        )

        self.db.execute_with_retry(_write, operation_name="maintenance_export")

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
        conflict: str = "error",
        include_snapshots: bool = False,
        dry_run: bool = False,
        auto_migrate: bool | Literal["quick", "full"] = False,
    ) -> MergeResult:
        shard_path = Path(shard_path)
        if not shard_path.exists():
            raise ValueError(f"Shard database does not exist: {shard_path}")

        conflict_mode = str(conflict).strip().lower()
        if conflict_mode not in {"error", "skip"}:
            raise ValueError("conflict must be one of: error, skip")

        with self._mountable_shard_path(
            shard_path, auto_migrate=auto_migrate
        ) as effective_shard:
            return self._merge_v2_shard(
                shard_path=shard_path,
                effective_shard=effective_shard,
                conflict_mode=conflict_mode,
                include_snapshots=include_snapshots,
                dry_run=dry_run,
            )

    def _merge_v2_shard(
        self,
        *,
        shard_path: Path,
        effective_shard: Path,
        conflict_mode: str,
        include_snapshots: bool,
        dry_run: bool,
    ) -> MergeResult:
        artifacts_merged = 0
        ingested_tables_merged: list[str] = []
        unscoped_cache_tables_skipped: list[str] = []
        conflicts_detected: list[str] = []
        runs_skipped: list[str] = []
        runs_to_merge: list[str] = []
        incompatible_global_tables_skipped: dict[str, str] = {}

        def _execute() -> None:
            nonlocal \
                artifacts_merged, \
                ingested_tables_merged, \
                unscoped_cache_tables_skipped, \
                conflicts_detected, \
                runs_skipped, \
                runs_to_merge, \
                incompatible_global_tables_skipped

            alias = "shard_merge"
            q_alias = self._quote_ident(alias)
            with _track_maintenance_merge_phase("merge.total"):
                with self._attached_database(
                    effective_shard, alias, read_only=True, mutate_main=True
                ) as cur:
                    shard_run_rows = cur.execute(
                        f"SELECT id FROM {q_alias}.main.run ORDER BY id"
                    ).fetchall()
                    shard_run_ids = [str(r[0]) for r in shard_run_rows if r[0]]

                    if shard_run_ids:
                        existing_rows = cur.execute(
                            "SELECT id FROM main.run WHERE id IN (SELECT UNNEST(?)) ORDER BY id",
                            [shard_run_ids],
                        ).fetchall()
                        existing_set = {str(r[0]) for r in existing_rows}
                    else:
                        existing_set = set()

                    conflicts_detected = sorted(
                        existing_set.intersection(shard_run_ids)
                    )
                    if conflicts_detected and conflict_mode == "error":
                        raise ValueError(
                            "Run ID conflicts detected: "
                            + ", ".join(conflicts_detected)
                        )

                    conflict_set = set(conflicts_detected)
                    if conflict_mode == "skip":
                        runs_skipped = list(conflicts_detected)
                        runs_to_merge = [
                            rid for rid in shard_run_ids if rid not in conflict_set
                        ]
                    else:
                        runs_skipped = []
                        runs_to_merge = list(shard_run_ids)

                    shard_global_tables = self._discover_global_tables_in_catalog(
                        cur, catalog=alias
                    )
                    shard_table_modes: dict[str, GlobalTableMode] = {
                        table: self._classify_global_table_in_catalog(
                            cur, table, catalog=alias
                        )
                        for table in shard_global_tables
                    }

                    if runs_to_merge:
                        compatibility_issues: dict[str, str] = {}
                        for table, mode in shard_table_modes.items():
                            if mode == "unscoped_cache":
                                continue
                            issue = self._global_table_merge_compatibility_issue_cur(
                                cur,
                                table,
                                mode=mode,
                                source_catalog=alias,
                            )
                            if issue:
                                compatibility_issues[table] = issue
                        if compatibility_issues:
                            incompatible_global_tables_skipped = {
                                t: compatibility_issues[t]
                                for t in sorted(compatibility_issues)
                            }
                            if conflict_mode == "error":
                                details = "; ".join(
                                    f"{t}: {reason}"
                                    for t, reason in incompatible_global_tables_skipped.items()
                                )
                                raise ValueError(
                                    "Global table schema compatibility check failed: "
                                    + details
                                )

                    if not runs_to_merge:
                        artifacts_merged = 0
                        return

                    art_rows = cur.execute(
                        f"""
                        SELECT DISTINCT CAST(id AS VARCHAR)
                        FROM (
                            SELECT artifact_id AS id
                            FROM {q_alias}.main.run_binding
                            WHERE run_id IN (SELECT UNNEST(?))
                            UNION
                            SELECT id
                            FROM {q_alias}.main.artifact
                            WHERE run_id IN (SELECT UNNEST(?))
                            UNION
                            SELECT artifact_id AS id
                            FROM {q_alias}.main.run_coupler_snapshot
                            WHERE run_id IN (SELECT UNNEST(?))
                               OR scenario_run_id IN (SELECT UNNEST(?))
                        )
                        WHERE id IS NOT NULL
                        ORDER BY 1
                        """,
                        [runs_to_merge, runs_to_merge, runs_to_merge, runs_to_merge],
                    ).fetchall()
                    merge_artifact_ids = [str(r[0]) for r in art_rows if r[0]]

                    if merge_artifact_ids:
                        new_art_row = cur.execute(
                            f"""
                            SELECT COUNT(*)
                            FROM {q_alias}.main.artifact src
                            WHERE CAST(src.id AS VARCHAR) IN (SELECT UNNEST(?))
                              AND NOT EXISTS (
                                  SELECT 1 FROM main.artifact dst
                                  WHERE dst.id = src.id
                              )
                            """,
                            [merge_artifact_ids],
                        ).fetchone()
                        artifacts_merged = int(new_art_row[0] if new_art_row else 0)
                    else:
                        artifacts_merged = 0

                    if dry_run:
                        for table in shard_global_tables:
                            mode = shard_table_modes.get(table)
                            if mode == "unscoped_cache":
                                unscoped_cache_tables_skipped.append(table)
                                continue
                            if table in incompatible_global_tables_skipped:
                                continue
                            run_col = (
                                self._quote_ident("consist_run_id")
                                if mode == "run_scoped"
                                else self._quote_ident("run_id")
                            )
                            quoted_t = self._quote_ident(
                                self._validate_identifier(table, label="table")
                            )
                            cnt_row = cur.execute(
                                f"""
                                SELECT COUNT(*)
                                FROM {q_alias}.global_tables.{quoted_t}
                                WHERE {run_col} IN (SELECT UNNEST(?))
                                """,
                                [runs_to_merge],
                            ).fetchone()
                            if int(cnt_row[0] if cnt_row else 0) > 0:
                                ingested_tables_merged.append(table)
                        return

                    # Perform actual merge of Schema v2 base tables
                    cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")

                    # Collect referenced anchors in source
                    anchor_params: list[Any] = [runs_to_merge]
                    anchor_art_sql = ""
                    if merge_artifact_ids:
                        anchor_art_sql = (
                            f"UNION SELECT anchor FROM {q_alias}.main.artifact "
                            "WHERE CAST(id AS VARCHAR) IN (SELECT UNNEST(?))"
                        )
                        anchor_params.append(merge_artifact_ids)

                    base_anchor_rows = cur.execute(
                        f"""
                        SELECT DISTINCT anchor
                        FROM (
                            SELECT anchor FROM {q_alias}.main.run_binding
                            WHERE run_id IN (SELECT UNNEST(?))
                            {anchor_art_sql}
                        )
                        WHERE anchor IS NOT NULL
                        """,
                        anchor_params,
                    ).fetchall()
                    base_anchors = [str(r[0]) for r in base_anchor_rows if r[0]]
                    merge_anchors: list[str] = []

                    if base_anchors:
                        all_anchor_rows = cur.execute(
                            f"""
                            SELECT DISTINCT a
                            FROM (
                                SELECT UNNEST(?) AS a
                                UNION
                                SELECT historical_anchor AS a
                                FROM {q_alias}.main.anchor_promotion
                                WHERE historical_anchor IN (SELECT UNNEST(?))
                                   OR verified_anchor IN (SELECT UNNEST(?))
                                UNION
                                SELECT verified_anchor AS a
                                FROM {q_alias}.main.anchor_promotion
                                WHERE historical_anchor IN (SELECT UNNEST(?))
                                   OR verified_anchor IN (SELECT UNNEST(?))
                                UNION
                                SELECT src_anchor AS a
                                FROM {q_alias}.main.content_derivation
                                WHERE src_anchor IN (SELECT UNNEST(?))
                                   OR dst_anchor IN (SELECT UNNEST(?))
                                UNION
                                SELECT dst_anchor AS a
                                FROM {q_alias}.main.content_derivation
                                WHERE src_anchor IN (SELECT UNNEST(?))
                                   OR dst_anchor IN (SELECT UNNEST(?))
                            )
                            WHERE a IS NOT NULL
                            """,
                            [
                                base_anchors,
                                base_anchors,
                                base_anchors,
                                base_anchors,
                                base_anchors,
                                base_anchors,
                                base_anchors,
                                base_anchors,
                                base_anchors,
                            ],
                        ).fetchall()
                        merge_anchors = [str(r[0]) for r in all_anchor_rows if r[0]]

                        cur.execute(
                            f"""
                            INSERT INTO main.digest_policy
                            SELECT * FROM {q_alias}.main.digest_policy
                            ON CONFLICT (policy_sha256) DO NOTHING
                            """
                        )
                        cur.execute(
                            f"""
                            INSERT INTO main.content
                            SELECT * FROM {q_alias}.main.content
                            WHERE anchor IN (SELECT UNNEST(?))
                            ON CONFLICT (anchor) DO NOTHING
                            """,
                            [merge_anchors],
                        )
                        cur.execute(
                            f"""
                            INSERT INTO main.content_digest
                            SELECT * FROM {q_alias}.main.content_digest
                            WHERE anchor IN (SELECT UNNEST(?))
                            ON CONFLICT DO NOTHING
                            """,
                            [merge_anchors],
                        )
                        cur.execute(
                            f"""
                            INSERT INTO main.content_digest_conflict
                            SELECT src.* FROM {q_alias}.main.content_digest_conflict src
                            WHERE src.anchor IN (SELECT UNNEST(?))
                              AND NOT EXISTS (
                                  SELECT 1 FROM main.content_digest_conflict dst
                                  WHERE dst.anchor = src.anchor
                                    AND dst.scheme = src.scheme
                                    AND dst.rejected_value = src.rejected_value
                              )
                            """,
                            [merge_anchors],
                        )
                        cur.execute(
                            f"""
                            INSERT INTO main.content_manifest
                            SELECT * FROM {q_alias}.main.content_manifest
                            WHERE anchor IN (SELECT UNNEST(?))
                            ON CONFLICT (anchor) DO NOTHING
                            """,
                            [merge_anchors],
                        )
                        cur.execute(
                            f"""
                            INSERT INTO main.content_derivation
                            SELECT * FROM {q_alias}.main.content_derivation
                            WHERE src_anchor IN (SELECT UNNEST(?))
                               OR dst_anchor IN (SELECT UNNEST(?))
                            ON CONFLICT DO NOTHING
                            """,
                            [merge_anchors, merge_anchors],
                        )
                        cur.execute(
                            f"""
                            INSERT INTO main.anchor_promotion
                            SELECT * FROM {q_alias}.main.anchor_promotion
                            WHERE historical_anchor IN (SELECT UNNEST(?))
                               OR verified_anchor IN (SELECT UNNEST(?))
                            ON CONFLICT (historical_anchor) DO NOTHING
                            """,
                            [merge_anchors, merge_anchors],
                        )
                        cur.execute(
                            f"""
                            INSERT INTO main.artifact_location
                            SELECT * FROM {q_alias}.main.artifact_location
                            WHERE anchor IN (SELECT UNNEST(?))
                            ON CONFLICT (observation_id) DO NOTHING
                            """,
                            [merge_anchors],
                        )
                        cur.execute(
                            f"""
                            INSERT INTO main.location_event
                            SELECT src.* FROM {q_alias}.main.location_event src
                            JOIN {q_alias}.main.artifact_location al
                              ON src.observation_id = al.observation_id
                            WHERE al.anchor IN (SELECT UNNEST(?))
                            ON CONFLICT (event_id) DO NOTHING
                            """,
                            [merge_anchors],
                        )

                    cur.execute(
                        f"""
                        INSERT INTO main.run
                        SELECT * FROM {q_alias}.main.run
                        WHERE id IN (SELECT UNNEST(?))
                        ON CONFLICT (id) DO NOTHING
                        """,
                        [runs_to_merge],
                    )

                    if merge_artifact_ids:
                        cur.execute(
                            f"""
                            INSERT INTO main.artifact
                            SELECT * FROM {q_alias}.main.artifact
                            WHERE CAST(id AS VARCHAR) IN (SELECT UNNEST(?))
                            ON CONFLICT (id) DO NOTHING
                            """,
                            [merge_artifact_ids],
                        )
                        cur.execute(
                            f"""
                            INSERT INTO main.artifact_migration_audit
                            SELECT * FROM {q_alias}.main.artifact_migration_audit
                            WHERE CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                            ON CONFLICT (artifact_id) DO NOTHING
                            """,
                            [merge_artifact_ids],
                        )

                    cur.execute(
                        f"""
                        INSERT INTO main.run_binding
                        SELECT * FROM {q_alias}.main.run_binding
                        WHERE run_id IN (SELECT UNNEST(?))
                        ON CONFLICT DO NOTHING
                        """,
                        [runs_to_merge],
                    )
                    cur.execute(
                        f"""
                        INSERT INTO main.run_binding_invocation
                        SELECT * FROM {q_alias}.main.run_binding_invocation
                        WHERE requested_run_id IN (SELECT UNNEST(?))
                           OR execution_run_id IN (SELECT UNNEST(?))
                        ON CONFLICT (id) DO NOTHING
                        """,
                        [runs_to_merge, runs_to_merge],
                    )
                    cur.execute(
                        f"""
                        INSERT INTO main.run_coupler_snapshot
                        SELECT * FROM {q_alias}.main.run_coupler_snapshot
                        WHERE run_id IN (SELECT UNNEST(?))
                           OR scenario_run_id IN (SELECT UNNEST(?))
                        ON CONFLICT DO NOTHING
                        """,
                        [runs_to_merge, runs_to_merge],
                    )

                    # Schemas & observations
                    obs_params: list[Any] = [runs_to_merge]
                    obs_where = "run_id IN (SELECT UNNEST(?))"
                    if merge_artifact_ids:
                        obs_where += (
                            " OR CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?))"
                        )
                        obs_params.append(merge_artifact_ids)

                    schema_sql_parts = [
                        f"SELECT schema_id AS sid FROM {q_alias}.main.artifact_schema_observation "
                        f"WHERE ({obs_where}) AND schema_id IS NOT NULL"
                    ]
                    schema_params: list[Any] = list(obs_params)
                    if merge_artifact_ids:
                        schema_sql_parts.append(
                            f"SELECT schema_id AS sid FROM {q_alias}.main.artifact "
                            "WHERE CAST(id AS VARCHAR) IN (SELECT UNNEST(?)) "
                            "AND schema_id IS NOT NULL"
                        )
                        schema_params.append(merge_artifact_ids)
                    if merge_anchors:
                        schema_sql_parts.append(
                            f"SELECT schema_id AS sid FROM {q_alias}.main.content "
                            "WHERE anchor IN (SELECT UNNEST(?)) "
                            "AND schema_id IS NOT NULL"
                        )
                        schema_params.append(merge_anchors)

                    schema_rows = cur.execute(
                        f"SELECT DISTINCT sid FROM ({' UNION '.join(schema_sql_parts)})",
                        schema_params,
                    ).fetchall()
                    schema_ids = [str(r[0]) for r in schema_rows if r[0]]
                    if schema_ids:
                        cur.execute(
                            f"""
                            INSERT INTO main.artifact_schema
                            SELECT * FROM {q_alias}.main.artifact_schema
                            WHERE id IN (SELECT UNNEST(?))
                            ON CONFLICT (id) DO NOTHING
                            """,
                            [schema_ids],
                        )
                        cur.execute(
                            f"""
                            INSERT INTO main.artifact_schema_field
                            SELECT * FROM {q_alias}.main.artifact_schema_field
                            WHERE schema_id IN (SELECT UNNEST(?))
                            ON CONFLICT DO NOTHING
                            """,
                            [schema_ids],
                        )
                        cur.execute(
                            f"""
                            INSERT INTO main.artifact_schema_relation
                            SELECT * FROM {q_alias}.main.artifact_schema_relation
                            WHERE schema_id IN (SELECT UNNEST(?))
                            ON CONFLICT DO NOTHING
                            """,
                            [schema_ids],
                        )

                    cur.execute(
                        f"""
                        INSERT INTO main.artifact_schema_observation
                        SELECT * FROM {q_alias}.main.artifact_schema_observation
                        WHERE {obs_where}
                        ON CONFLICT (id) DO NOTHING
                        """,
                        obs_params,
                    )

                    # Global tables
                    for table in shard_global_tables:
                        mode = shard_table_modes.get(table)
                        if mode == "unscoped_cache":
                            unscoped_cache_tables_skipped.append(table)
                            continue
                        if table in incompatible_global_tables_skipped:
                            continue
                        safe_table = self._validate_identifier(table, label="table")
                        quoted_table = self._quote_ident(safe_table)
                        run_col = (
                            self._quote_ident("consist_run_id")
                            if mode == "run_scoped"
                            else self._quote_ident("run_id")
                        )
                        cnt_row = cur.execute(
                            f"""
                            SELECT COUNT(*)
                            FROM {q_alias}.global_tables.{quoted_table}
                            WHERE {run_col} IN (SELECT UNNEST(?))
                            """,
                            [runs_to_merge],
                        ).fetchone()
                        if int(cnt_row[0] if cnt_row else 0) <= 0:
                            continue

                        cur.execute(
                            f"""
                            CREATE TABLE IF NOT EXISTS global_tables.{quoted_table}
                            AS SELECT * FROM {q_alias}.global_tables.{quoted_table} WHERE 1 = 0
                            """
                        )
                        src_cols = set(
                            self._list_table_columns_cur(
                                cur,
                                safe_table,
                                schema="global_tables",
                                catalog=alias,
                            )
                        )
                        dst_cols = self._list_table_columns_cur(
                            cur,
                            safe_table,
                            schema="global_tables",
                            catalog=None,
                        )
                        shared_cols = [
                            self._quote_ident(c) for c in dst_cols if c in src_cols
                        ]
                        if not shared_cols:
                            continue
                        col_sql = ", ".join(shared_cols)
                        cur.execute(
                            f"""
                            INSERT INTO global_tables.{quoted_table} ({col_sql})
                            SELECT {col_sql}
                            FROM {q_alias}.global_tables.{quoted_table}
                            WHERE {run_col} IN (SELECT UNNEST(?))
                            """,
                            [runs_to_merge],
                        )
                        ingested_tables_merged.append(safe_table)

        self.db.execute_with_retry(_execute, operation_name="maintenance_merge")
        if not dry_run:
            with self.ledger._lock:
                self.ledger._memoized_observations.clear()
                self.ledger._memoized_locations.clear()

        snapshots_merged = 0
        if include_snapshots and runs_to_merge:
            source_snapshot_dir = shard_path.parent / "shard_snapshots"
            for run_id in runs_to_merge:
                source_name = self._resolve_run_snapshot_path(run_id, None).name
                source_snapshot = source_snapshot_dir / source_name
                if not source_snapshot.exists():
                    continue
                destination_snapshot = self._resolve_run_snapshot_path(run_id, None)
                if destination_snapshot.exists():
                    continue
                if not dry_run:
                    destination_snapshot.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source_snapshot, destination_snapshot)
                snapshots_merged += 1

        if not dry_run:
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
            unscoped_cache_tables_skipped=sorted(set(unscoped_cache_tables_skipped)),
            conflicts_detected=conflicts_detected,
            snapshots_merged=snapshots_merged,
            incompatible_global_tables_skipped=incompatible_global_tables_skipped,
        )

    def compact(self) -> None:
        def _vacuum() -> None:
            ledger = self.ledger
            with ledger._lock:
                if ledger._tx_depth == 0:
                    ledger.connection.execute("VACUUM")
                    if not ledger.read_only and ledger.db_path is not None:
                        ledger.connection.execute("FORCE CHECKPOINT")

        self.db.execute_with_retry(_vacuum, operation_name="maintenance_compact")
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
            [path for path in snapshot_dir.glob("*.json") if path.is_file()],
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

            parsed_record = self._parse_snapshot_payload(payload)
            if isinstance(parsed_record, str):
                errors.append(
                    f"{snapshot_path.name}: invalid snapshot payload ({parsed_record})"
                )
                continue
            (
                run,
                input_artifacts,
                output_artifacts,
                facet_dict,
                is_historical_snapshot,
                raw_meta_by_artifact_id,
            ) = parsed_record

            run_id = str(run.id).strip()
            if not run_id:
                errors.append(f"{snapshot_path.name}: missing run.id")
                continue

            artifact_rows: dict[str, Artifact] = {}
            snapshot_artifact_meta: dict[str, tuple[uuid.UUID, dict[str, Any]]] = {}
            for artifact in [*input_artifacts, *output_artifacts]:
                parsed_uuid = self._coerce_uuid(artifact.id)
                if parsed_uuid is None:
                    errors.append(
                        f"{snapshot_path.name}: invalid artifact id {artifact.id!r}"
                    )
                    continue
                artifact_id = str(parsed_uuid)
                if artifact_id not in artifact_rows:
                    artifact_rows[artifact_id] = artifact
                if artifact_id not in snapshot_artifact_meta:
                    merged_art_meta: dict[str, Any] = {}
                    raw_m = raw_meta_by_artifact_id.get(artifact_id)
                    if isinstance(raw_m, Mapping):
                        merged_art_meta.update(raw_m)
                    if isinstance(artifact.meta, Mapping):
                        merged_art_meta.update(artifact.meta)
                    if artifact.schema_id and "schema_id" not in merged_art_meta:
                        merged_art_meta["schema_id"] = artifact.schema_id
                    snapshot_artifact_meta[artifact_id] = (
                        parsed_uuid,
                        merged_art_meta,
                    )

            link_rows: dict[str, str] = {}
            for artifact in input_artifacts:
                parsed_uuid = self._coerce_uuid(artifact.id)
                if parsed_uuid is not None:
                    link_rows.setdefault(str(parsed_uuid), "input")
            for artifact in output_artifacts:
                parsed_uuid = self._coerce_uuid(artifact.id)
                if parsed_uuid is not None:
                    link_rows[str(parsed_uuid)] = "output"

            if dry_run:
                if run_id not in run_exists_cache:
                    run_exists_cache[run_id] = self.ledger.get_run(run_id) is not None
                if run_exists_cache[run_id]:
                    runs_already_present += 1
                else:
                    runs_inserted += 1
                    run_exists_cache[run_id] = True

                for artifact_id, artifact in artifact_rows.items():
                    if artifact_id not in artifact_exists_cache:
                        artifact_exists_cache[artifact_id] = (
                            self.ledger.get_artifact(artifact.id) is not None
                        )
                    if not artifact_exists_cache[artifact_id]:
                        artifacts_inserted += 1
                        artifact_exists_cache[artifact_id] = True
                continue

            def _upsert_snapshot() -> tuple[bool, int]:
                with self.ledger.transaction() as cur:
                    existing_run = self.ledger.get_run(run_id)
                    run_exists = existing_run is not None
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
                        if rebuild_mode == "full" and facet_dict:
                            meta_payload["facet"] = dict(facet_dict)
                        run_to_save = Run(
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
                            meta=meta_payload,
                            started_at=run.started_at,
                            ended_at=run.ended_at,
                            created_at=run.created_at,
                            updated_at=run.updated_at,
                        )
                        self.ledger.upsert_run(
                            run_to_save,
                            cache_status=(
                                "historical" if is_historical_snapshot else None
                            ),
                            signature_scheme=(
                                "legacy-v1" if is_historical_snapshot else None
                            ),
                            facet_index=(rebuild_mode == "full"),
                        )
                    elif (
                        rebuild_mode == "full"
                        and facet_dict
                        and existing_run is not None
                    ):
                        existing_facet_row = cur.execute(
                            "SELECT facet_kv, cache_status, signature_scheme "
                            "FROM main.run WHERE id = ?",
                            [run_id],
                        ).fetchone()
                        has_facet_kv = (
                            existing_facet_row is not None
                            and existing_facet_row[0] is not None
                        )
                        if not has_facet_kv:
                            prev_cs_raw = (
                                str(existing_facet_row[1])
                                if existing_facet_row is not None
                                and existing_facet_row[1] is not None
                                else None
                            )
                            prev_cache_status: (
                                Literal["live", "historical", "ineligible"] | None
                            ) = (
                                prev_cs_raw
                                if prev_cs_raw in ("live", "historical", "ineligible")
                                else None
                            )
                            prev_sig_scheme = (
                                str(existing_facet_row[2])
                                if existing_facet_row is not None
                                and existing_facet_row[2] is not None
                                else None
                            )
                            merged_meta = (
                                dict(existing_run.meta)
                                if isinstance(existing_run.meta, Mapping)
                                else {}
                            )
                            merged_meta["facet"] = dict(facet_dict)
                            updated_existing = existing_run.model_copy(
                                update={"meta": merged_meta}
                            )
                            self.ledger.upsert_run(
                                updated_existing,
                                cache_status=prev_cache_status,
                                signature_scheme=prev_sig_scheme,
                                facet_index=True,
                            )

                    inserted_artifacts_for_file = 0
                    for artifact in artifact_rows.values():
                        existing_art = self.ledger.get_artifact(artifact.id)
                        if existing_art is None:
                            self.db._persist_artifact_and_hints(
                                artifact,
                                facet_index=(rebuild_mode == "full"),
                            )
                            inserted_artifacts_for_file += 1
                        elif rebuild_mode == "full":
                            self.db._persist_artifact_and_hints(
                                artifact,
                                facet_index=True,
                            )

                    for artifact_id_str, direction in link_rows.items():
                        parsed_uuid = self._coerce_uuid(artifact_id_str)
                        if parsed_uuid is None:
                            continue
                        art_obj = artifact_rows.get(artifact_id_str)
                        if art_obj is None:
                            continue
                        self.db._insert_run_binding_checked(
                            cur,
                            run_id=run_id,
                            artifact_id=parsed_uuid,
                            anchor=art_obj.identity.anchor,
                            artifact_key=art_obj.key,
                            direction=direction,
                        )

                    if rebuild_mode == "full":
                        for aid_uuid, art_meta in snapshot_artifact_meta.values():
                            self._restore_artifact_schema_rows_cur(
                                cur=cur,
                                artifact_id=aid_uuid,
                                artifact_meta=art_meta,
                                run_id=run_id,
                            )

                    return run_exists, inserted_artifacts_for_file

            try:
                run_exists, inserted_artifacts_for_file = self.db.execute_with_retry(
                    _upsert_snapshot,
                    operation_name="maintenance_rebuild_from_json_upsert",
                )
            except Exception as exc:
                errors.append(f"{snapshot_path.name}: rebuild write failed ({exc})")
                continue

            if run_exists:
                runs_already_present += 1
            else:
                runs_inserted += 1
            artifacts_inserted += inserted_artifacts_for_file

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

    def _parse_snapshot_payload(
        self, payload: Any
    ) -> (
        tuple[
            Run,
            list[Artifact],
            list[Artifact],
            dict[str, Any],
            bool,
            dict[str, dict[str, Any]],
        ]
        | str
    ):
        if not isinstance(payload, dict):
            return "expected JSON object"

        raw_run_val = payload.get("run")
        raw_run: Mapping[str, Any] = (
            raw_run_val if isinstance(raw_run_val, Mapping) else {}
        )
        raw_meta_val = raw_run.get("meta")
        raw_run_meta: Mapping[str, Any] = (
            raw_meta_val if isinstance(raw_meta_val, Mapping) else {}
        )
        raw_art_dicts = [
            d
            for d in [*(payload.get("inputs") or []), *(payload.get("outputs") or [])]
            if isinstance(d, Mapping)
        ]
        raw_meta_by_artifact_id: dict[str, dict[str, Any]] = {}
        for d in raw_art_dicts:
            aid_uuid = self._coerce_uuid(d.get("id"))
            if aid_uuid is not None:
                m_raw = d.get("meta")
                m_copy = dict(m_raw) if isinstance(m_raw, Mapping) else {}
                if d.get("schema_id") and "schema_id" not in m_copy:
                    m_copy["schema_id"] = d["schema_id"]
                raw_meta_by_artifact_id.setdefault(str(aid_uuid), m_copy)

        raw_lacked_v2_identity = (
            any("anchor" not in d and "identity" not in d for d in raw_art_dicts)
            if raw_art_dicts
            else ("input_binding" not in raw_run_meta)
        )

        def _finalize(
            run_obj: Run,
            in_arts: list[Artifact],
            out_arts: list[Artifact],
            f_dict: dict[str, Any],
        ) -> tuple[
            Run,
            list[Artifact],
            list[Artifact],
            dict[str, Any],
            bool,
            dict[str, dict[str, Any]],
        ]:
            is_hist = any(not a.identity.verifiable for a in [*in_arts, *out_arts]) or (
                raw_lacked_v2_identity and "signature_scheme" not in raw_run_meta
            )
            return (
                run_obj,
                in_arts,
                out_arts,
                f_dict,
                is_hist,
                raw_meta_by_artifact_id,
            )

        try:
            record = ConsistRecord.model_validate(payload)
            facet_dict = dict(record.facet) if isinstance(record.facet, Mapping) else {}
            return _finalize(
                record.run,
                list(record.inputs),
                list(record.outputs),
                facet_dict,
            )
        except Exception as primary_exc:
            # Support legacy v1 JSON snapshots where artifacts had `container_uri` / `hash`
            try:
                run_raw = payload.get("run")
                if not isinstance(run_raw, dict):
                    return str(primary_exc)
                run = Run.model_validate(run_raw)
                inputs_raw = payload.get("inputs") or []
                outputs_raw = payload.get("outputs") or []
                inputs = [
                    self._parse_legacy_artifact_dict(item)
                    for item in inputs_raw
                    if isinstance(item, dict)
                ]
                outputs = [
                    self._parse_legacy_artifact_dict(item)
                    for item in outputs_raw
                    if isinstance(item, dict)
                ]
                facet_raw = payload.get("facet")
                facet_dict = dict(facet_raw) if isinstance(facet_raw, dict) else {}
                return _finalize(run, inputs, outputs, facet_dict)
            except Exception:
                return str(primary_exc)

    def _parse_legacy_artifact_dict(self, item: dict[str, Any]) -> Artifact:
        normalized = dict(item)
        normalized.setdefault("key", "")
        return Artifact._pydantic_validate(normalized)

    def _restore_artifact_schema_rows_cur(
        self,
        *,
        cur: duckdb.DuckDBPyConnection,
        artifact_id: uuid.UUID,
        artifact_meta: dict[str, Any],
        run_id: str,
    ) -> None:
        schema_id = self._coerce_non_empty_str(artifact_meta.get("schema_id"))
        if schema_id is None:
            return

        summary = artifact_meta.get("schema_summary")
        summary_json = summary if isinstance(summary, dict) else {}
        profile = artifact_meta.get("schema_profile")
        profile_json = profile if isinstance(profile, dict) else None
        profile_version = summary_json.get("profile_version")
        if not isinstance(profile_version, int) or profile_version <= 0:
            profile_version = 1

        existing = cur.execute(
            "SELECT summary_json, profile_json FROM main.artifact_schema WHERE id = ?",
            [schema_id],
        ).fetchone()
        if existing is None:
            cur.execute(
                """
                INSERT INTO main.artifact_schema (
                    id, profile_version, summary_json, profile_json, created_at
                ) VALUES (?, ?, ?::JSON, ?::JSON, ?)
                """,
                [
                    schema_id,
                    profile_version,
                    json.dumps(summary_json),
                    json.dumps(profile_json) if profile_json is not None else None,
                    datetime.now(UTC),
                ],
            )

        source = self._schema_observation_source(
            summary_json=summary_json,
            artifact_meta=artifact_meta,
        )
        sample_rows = self._schema_observation_sample_rows(
            summary_json=summary_json,
            artifact_meta=artifact_meta,
        )

        dup = cur.execute(
            """
            SELECT 1 FROM main.artifact_schema_observation
            WHERE run_id = ? AND artifact_id = ? AND schema_id = ?
              AND source = ? AND sample_rows IS NOT DISTINCT FROM ?
            LIMIT 1
            """,
            [run_id, artifact_id, schema_id, source, sample_rows],
        ).fetchone()
        if dup is not None:
            return

        cur.execute(
            """
            INSERT INTO main.artifact_schema_observation (
                id, artifact_id, schema_id, run_id, source, sample_rows, observed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                uuid.uuid4(),
                artifact_id,
                schema_id,
                run_id,
                source,
                sample_rows,
                datetime.now(UTC),
            ],
        )

    @staticmethod
    def _coerce_non_empty_str(value: Any) -> Optional[str]:
        if not isinstance(value, str):
            return None
        candidate = value.strip()
        return candidate if candidate else None

    @classmethod
    def _schema_observation_source(
        cls, *, summary_json: dict[str, Any], artifact_meta: dict[str, Any]
    ) -> str:
        source = cls._coerce_non_empty_str(summary_json.get("source"))
        if source is not None:
            return source
        source = cls._coerce_non_empty_str(artifact_meta.get("schema_source"))
        if source is not None:
            return source
        source = cls._coerce_non_empty_str(artifact_meta.get("source"))
        if source is not None:
            return source
        return "snapshot_rebuild"

    @staticmethod
    def _schema_observation_sample_rows(
        *, summary_json: dict[str, Any], artifact_meta: dict[str, Any]
    ) -> Optional[int]:
        summary_rows = summary_json.get("sample_rows")
        if isinstance(summary_rows, int):
            return summary_rows
        meta_rows = artifact_meta.get("sample_rows")
        if isinstance(meta_rows, int):
            return meta_rows
        schema_meta_rows = artifact_meta.get("schema_sample_rows")
        if isinstance(schema_meta_rows, int):
            return schema_meta_rows
        return None

    def snapshot(
        self,
        dest_path: Path,
        *,
        checkpoint: bool = True,
        metadata: Optional[dict[str, Any]] = None,
    ) -> Path:
        snapshot_metadata: dict[str, Any] = {"operation": "maintenance_snapshot"}
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
        """
        Expand run IDs to include all descendant runs.

        Traversal is breadth-first and cycle-safe, preserving user-provided seed
        order and deduplicating repeated IDs.
        """
        seed_ids = self._normalize_run_ids(run_ids)
        if not seed_ids:
            return []

        with self.ledger.exec_cursor() as cur:
            rows = cur.execute(
                """
                SELECT id, parent_run_id
                FROM main.run
                WHERE parent_run_id IS NOT NULL
                """
            ).fetchall()

        children_by_parent: dict[str, list[str]] = {}
        for child_id_raw, parent_id_raw in rows:
            if parent_id_raw is None:
                continue
            children_by_parent.setdefault(str(parent_id_raw), []).append(
                str(child_id_raw)
            )

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

        An artifact is considered orphaned by purge when it has at least one
        binding (or `artifact.run_id`) and every linked run belongs to `run_ids`.
        """
        selected_ids = set(self._normalize_run_ids(run_ids))
        if not selected_ids:
            return []

        with self.ledger.exec_cursor() as cur:
            binding_rows = cur.execute(
                "SELECT artifact_id, run_id FROM main.run_binding"
            ).fetchall()
            producer_rows = cur.execute(
                "SELECT id, run_id FROM main.artifact WHERE run_id IS NOT NULL"
            ).fetchall()

        linked_runs_by_artifact: dict[uuid.UUID, set[str]] = {}
        for artifact_id_raw, run_id_raw in binding_rows:
            parsed_id = self._coerce_uuid(artifact_id_raw)
            if parsed_id is None or run_id_raw is None:
                continue
            linked_runs_by_artifact.setdefault(parsed_id, set()).add(str(run_id_raw))

        for artifact_id_raw, run_id_raw in producer_rows:
            parsed_id = self._coerce_uuid(artifact_id_raw)
            if parsed_id is None or run_id_raw is None:
                continue
            linked_runs_by_artifact.setdefault(parsed_id, set()).add(str(run_id_raw))

        orphaned = [
            artifact_id
            for artifact_id, linked_runs in linked_runs_by_artifact.items()
            if linked_runs and linked_runs.issubset(selected_ids)
        ]
        orphaned.sort(key=str)
        return orphaned

    def _discover_global_tables(self) -> list[str]:
        def _query() -> list[str]:
            with self.ledger.exec_cursor() as cur:
                return self._discover_global_tables_in_catalog(cur, catalog=None)

        return self.db.execute_with_retry(
            _query, operation_name="maintenance_discover_global_tables"
        )

    def _discover_global_tables_in_catalog(
        self, cur: duckdb.DuckDBPyConnection, *, catalog: Optional[str] = None
    ) -> list[str]:
        if catalog is not None:
            rows = cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_catalog = ?
                  AND table_schema = 'global_tables'
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """,
                [catalog],
            ).fetchall()
        else:
            rows = cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'global_tables'
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """
            ).fetchall()
        return [str(row[0]) for row in rows]

    def _classify_global_table(self, table: str) -> GlobalTableMode:
        safe_table = self._validate_identifier(table, label="table")

        def _query() -> GlobalTableMode:
            with self.ledger.exec_cursor() as cur:
                return self._classify_global_table_in_catalog(
                    cur, safe_table, catalog=None
                )

        return self.db.execute_with_retry(
            _query, operation_name="maintenance_classify_global_table"
        )

    def _classify_global_table_in_catalog(
        self,
        cur: duckdb.DuckDBPyConnection,
        table: str,
        *,
        catalog: Optional[str] = None,
    ) -> GlobalTableMode:
        safe_table = self._validate_identifier(table, label="table")
        cols = set(
            self._list_table_columns_cur(
                cur, safe_table, schema="global_tables", catalog=catalog
            )
        )
        if "consist_run_id" in cols:
            return "run_scoped"
        if "run_id" in cols:
            return "run_link"
        return "unscoped_cache"

    def _global_table_row_counts(
        self,
        tables: Iterable[str],
        run_ids: Iterable[str] | str,
    ) -> dict[str, int]:
        safe_tables = sorted(
            {self._validate_identifier(table, label="table") for table in tables}
        )
        normalized_run_ids = self._normalize_run_ids(run_ids)

        def _query() -> dict[str, int]:
            counts: dict[str, int] = {}
            with self.ledger.exec_cursor() as cur:
                for table in safe_tables:
                    quoted_table = self._quote_ident(table)
                    if not normalized_run_ids:
                        row = cur.execute(
                            f"SELECT COUNT(*) FROM global_tables.{quoted_table}"
                        ).fetchone()
                        counts[table] = int(row[0] if row else 0)
                        continue

                    filter_sql = self._resolve_global_table_filter_sql(
                        table,
                        normalized_run_ids,
                        table_alias="gt",
                    )
                    if filter_sql is None:
                        counts[table] = 0
                        continue

                    row = cur.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM global_tables.{quoted_table} AS "gt"
                        WHERE {filter_sql}
                        """
                    ).fetchone()
                    counts[table] = int(row[0] if row else 0)

            return {table: counts[table] for table in safe_tables}

        return self.db.execute_with_retry(
            _query, operation_name="maintenance_global_table_row_counts"
        )

    def _resolve_global_table_filter_sql(
        self,
        table: str,
        run_ids: Iterable[str] | str,
        *,
        table_alias: str = "",
    ) -> Optional[str]:
        safe_table = self._validate_identifier(table, label="table")
        mode = self._classify_global_table(safe_table)
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

    def _global_table_merge_compatibility_issue_cur(
        self,
        cur: duckdb.DuckDBPyConnection,
        table: str,
        *,
        mode: GlobalTableMode,
        source_catalog: str,
    ) -> Optional[str]:
        safe_table = self._validate_identifier(table, label="table")
        source_column_types = self._list_table_columns_with_types_cur(
            cur,
            safe_table,
            schema="global_tables",
            catalog=source_catalog,
        )
        if not source_column_types:
            return "source table missing in global_tables schema"

        target_column_types = self._list_table_columns_with_types_cur(
            cur,
            safe_table,
            schema="global_tables",
            catalog=None,
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
            if not self._types_compatible_for_merge_cur(cur, source_type, target_type):
                incompatible_columns.append(
                    f"{column_name} (source={source_type}, target={target_type})"
                )

        if incompatible_columns:
            return "incompatible shared column type(s): " + ", ".join(
                incompatible_columns
            )
        return None

    def _types_compatible_for_merge_cur(
        self,
        cur: duckdb.DuckDBPyConnection,
        source_type: str,
        target_type: str,
    ) -> bool:
        normalized_source = self._normalize_sql_type(source_type)
        normalized_target = self._normalize_sql_type(target_type)
        if not normalized_source or not normalized_target:
            return False
        if normalized_source == normalized_target:
            return True
        try:
            row = cur.execute(
                f"""
                SELECT can_cast_implicitly(
                    NULL::{normalized_source},
                    NULL::{normalized_target}
                )
                """
            ).fetchone()
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

    def _collect_prune_candidates(
        self, artifact_ids: Iterable[uuid.UUID]
    ) -> list[_PruneCandidate]:
        normalized_ids: list[uuid.UUID] = []
        seen_ids: set[uuid.UUID] = set()
        for value in artifact_ids:
            parsed_uuid = self._coerce_uuid(value)
            if parsed_uuid is None or parsed_uuid in seen_ids:
                continue
            seen_ids.add(parsed_uuid)
            normalized_ids.append(parsed_uuid)

        if not normalized_ids:
            return []

        artifacts: list[Artifact] = []
        for aid in normalized_ids:
            art = self.ledger.get_artifact(aid)
            if art is not None:
                artifacts.append(art)
        if not artifacts:
            return []

        unique_anchors = list(dict.fromkeys(art.identity.anchor for art in artifacts))
        loc_rows = self.ledger.locate_current(unique_anchors, include_evicted=False)
        rows_by_anchor: dict[str, list[dict[str, Any]]] = {}
        for row in loc_rows:
            rows_by_anchor.setdefault(str(row["anchor"]), []).append(dict(row))

        run_ids: set[str] = set()
        run_id_by_artifact: dict[uuid.UUID, str] = {}
        for art in artifacts:
            if art.run_id:
                run_id_by_artifact[art.id] = art.run_id
                run_ids.add(art.run_id)

        missing_run_aids = [
            str(art.id) for art in artifacts if art.id not in run_id_by_artifact
        ]
        if missing_run_aids:
            with self.ledger.exec_cursor() as cur:
                out_rows = cur.execute(
                    """
                    SELECT artifact_id, run_id
                    FROM main.run_binding
                    WHERE CAST(artifact_id AS VARCHAR) IN (SELECT UNNEST(?))
                      AND direction = 'output'
                    ORDER BY run_id
                    """,
                    [missing_run_aids],
                ).fetchall()
            for aid_raw, rid_raw in out_rows:
                parsed = self._coerce_uuid(aid_raw)
                if parsed is not None and parsed not in run_id_by_artifact and rid_raw:
                    rid = str(rid_raw).strip()
                    if rid:
                        run_id_by_artifact[parsed] = rid
                        run_ids.add(rid)

        runs_by_id = self._load_runs_by_ids(list(run_ids))

        candidates: list[_PruneCandidate] = []
        seen_paths: set[str] = set()
        for art in artifacts:
            anchor_str = str(art.identity.anchor)
            run = runs_by_id.get(run_id_by_artifact.get(art.id, ""))
            anchor_obs = rows_by_anchor.get(anchor_str, [])
            if not anchor_obs and art.hints:
                for hint in art.hints:
                    anchor_obs.append(
                        {
                            "anchor": anchor_str,
                            "uri": hint.location.uri,
                            "member": hint.location.member,
                            "access_driver": hint.access_driver,
                            "witness": hint.witness,
                            "witness_json": None,
                            "site": hint.site,
                            "scope_key": hint.scope_key,
                            "mount_instance_id": hint.mount_instance_id,
                            "status": "active",
                        }
                    )

            for obs_row in anchor_obs:
                uri = str(obs_row.get("uri") or "").strip()
                if not uri or uri.startswith("db://"):
                    continue
                for cand_path in self._resolve_uri_disk_candidates(uri, art, run):
                    if not cand_path.exists() and not cand_path.is_symlink():
                        continue
                    pkey = str(cand_path)
                    if pkey in seen_paths:
                        continue
                    seen_paths.add(pkey)
                    candidates.append(
                        _PruneCandidate(
                            path=cand_path,
                            artifact=art,
                            obs_row=obs_row,
                        )
                    )

        return candidates

    def _resolve_uri_disk_candidates(
        self, uri: str, artifact: Artifact, run: Optional[Run]
    ) -> list[Path]:
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
            if path_key in seen_candidates:
                return
            seen_candidates.add(path_key)
            candidate_paths.append(path)

        # First check active ledger mounts
        if "://" in uri and not uri.startswith(("file://", "workspace://")):
            try:
                resolved, _ = resolve_uri_to_path(uri, mounts=self.ledger.mounts)
                if resolved is not None:
                    _append_candidate(resolved)
            except ValueError:
                return []

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
                normalized_scheme, run_meta=run_meta, artifact_meta=artifact_meta
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
    ) -> list[Path]:
        roots: list[Path] = []
        seen_roots: set[str] = set()
        mounts_raw = run_meta.get("mounts")
        if isinstance(mounts_raw, Mapping):
            mount_root = mounts_raw.get(scheme)
            if isinstance(mount_root, str) and mount_root:
                normalized_root = self._normalize_mount_root_path(mount_root)
                if normalized_root is not None:
                    root_key = str(normalized_root)
                    seen_roots.add(root_key)
                    roots.append(normalized_root)

        artifact_mount_root = artifact_meta.get("mount_root")
        if isinstance(artifact_mount_root, str) and artifact_mount_root:
            normalized_root = self._normalize_mount_root_path(artifact_mount_root)
            if normalized_root is not None:
                root_key = str(normalized_root)
                if root_key not in seen_roots:
                    seen_roots.add(root_key)
                    roots.append(normalized_root)

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
        target_path.parent.mkdir(parents=True, exist_ok=True)
        for path in (target_path, Path(f"{target_path}.wal")):
            if path.exists():
                path.unlink()
        shard_ledger = StoreLedger(target_path)
        try:
            with shard_ledger.transaction() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        finally:
            shard_ledger.close()

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
        """Normalize run IDs into a de-duplicated list while preserving order."""
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
        """Coerce mixed UUID/string DB values into UUID objects."""
        if isinstance(value, uuid.UUID):
            return value
        try:
            return uuid.UUID(str(value))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _validate_identifier(identifier: str, *, label: str) -> str:
        return _shared_validate_identifier(str(identifier).strip(), label=label)

    @staticmethod
    def _quote_ident(identifier: str) -> str:
        return _shared_quote_ident(identifier)

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
