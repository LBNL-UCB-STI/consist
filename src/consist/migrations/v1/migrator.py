"""
Consist v1-to-v2 offline and automatic migration engine (`consist migrate`,
`auto_migrate`, and `consist promote`).

Implements the Overwritten-Path Proof Classifier, Slurm/HPC pre-flight checks
and lockfile coordination, selective `dlt` (`global_tables` + `_dlt_*`) table
copy, hard 8-point `verify_migration` gate, atomic `.v1-backup-YYYY-MM-DD.duckdb`
swap with `undo_migration`, and `promote_historical_artifact`.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import time
from typing import Any, Literal
import uuid

import duckdb

from consist.digest import (
    Digest,
    Identity,
    Kind,
    canonical_json_str,
    compute_ctd1_digest,
    digest_bytes,
    open_h5py_read_only,
)
from consist.engine import (
    _build_manifest_v1_parallel,
    _hash_file_descriptor_pinned,
    _normalize_mounts,
    compute_scheme_digest,
    resolve_uri_to_path,
    virtualize_path,
)
from consist.location import (
    DbTableChangeToken,
    DirTreeStat,
    Location,
    Mount,
    Observation,
    Witness,
)
from consist.migrations.v1.fasthash import (
    RACY_STAT_WINDOW_NS,
    stat_v1_fast_file_hash,
)
from consist.migrations.v1.reader import (
    V1ArtifactRecord,
    V1DatabaseReader,
    V1DirEntryRecord,
    V1DltTableInfo,
    V1LinkRecord,
    V1RunRecord,
)
from consist.store.detect import (
    ConsistError,
    IncompatibleSchemaError,
    SchemaState,
    _resolve_auto_migrate,
    detect_schema,
    inspect_v1_summary,
)
from consist.store.ledger import StoreLedger

ClassificationOutcome = Literal["verified", "historical"]
ClassificationBasis = Literal[
    "directory_manifest",
    "dlt_hot_table",
    "v1_full",
    "v1_full_reverified",
    "migrated_fast_match",
    "racy_fast_stat",
    "overwritten_cold",
    "missing_cold",
    "quick_deferred",
    "unhashed_v1",
]

_HEX_CHARS = frozenset("0123456789abcdefABCDEF")
_STRIPPED_META_KEYS = frozenset(
    {
        "recovery_roots",
        "hash_semantics",
        "hashing_strategy",
        "directory_manifest",
        "directory_artifact",
        "is_dir",
    }
)


def _is_64_hex(val: str | None) -> bool:
    return bool(val and len(val) == 64 and all(c in _HEX_CHARS for c in val))


def _quote_ident(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


@dataclass(frozen=True, slots=True)
class ArtifactClassification:
    """Result of classifying a single v1 artifact row during migration planning."""

    artifact_id: uuid.UUID
    outcome: ClassificationOutcome
    basis: ClassificationBasis
    anchor: Digest
    repr_kind: str
    resolved_path: Path | None = None
    witness: Witness | None = None
    manifest_payload: tuple[bytes, int, int] | None = None
    dlt_member: str | None = None
    v1_row_json: str = "{}"
    cold_path_overwritten: bool = False


@dataclass(frozen=True, slots=True)
class MigrationPlan:
    """Complete pre-commit classification plan for a v1-to-v2 migration."""

    v1_path: Path
    run_count: int
    artifact_count: int
    link_count: int
    dlt_tables: tuple[V1DltTableInfo, ...]
    classifications: tuple[ArtifactClassification, ...]
    counts_by_basis: Mapping[str, int]
    counts_by_outcome: Mapping[str, int]
    runs: tuple[V1RunRecord, ...] = ()
    artifacts: tuple[V1ArtifactRecord, ...] = ()
    links: tuple[V1LinkRecord, ...] = ()


@dataclass(frozen=True, slots=True)
class MigrationVerificationReport:
    """Hard verification report comparing the migrated v2 store against the v1 source."""

    ok: bool
    v1_run_count: int = 0
    v2_run_count: int = 0
    v1_artifact_count: int = 0
    v2_artifact_count: int = 0
    v2_audit_count: int = 0
    link_triples_match: bool = True
    referential_integrity_ok: bool = True
    historical_isolation_ok: bool = True
    manifests_verified: int = 0
    dlt_table_counts_match: bool = True
    errors: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class MigrationResult:
    """Completed (or dry-run) v1-to-v2 migration result."""

    v1_path: Path
    v2_path: Path
    backup_path: Path | None
    plan: MigrationPlan
    verification: MigrationVerificationReport
    already_v2: bool = False


def default_migration_workers() -> int:
    """Determine bounded worker count respecting Slurm/HPC CPU allocations."""
    slurm_cpus = os.environ.get("SLURM_CPUS_PER_TASK")
    if slurm_cpus is not None:
        try:
            parsed = int(slurm_cpus.strip())
            if parsed > 0:
                return parsed
        except ValueError:
            pass

    sched_fn = os.__dict__.get("sched_getaffinity")
    if callable(sched_fn):
        try:
            affinity = sched_fn(0)
            if len(affinity) > 0:
                return len(affinity)
        except OSError:
            pass

    return min(8, os.cpu_count() or 4)


def _lookup_owner_username(path: Path) -> str:
    try:
        uid = path.stat().st_uid
    except OSError:
        return "unknown"
    try:
        import pwd

        return pwd.getpwuid(uid).pw_name
    except (ImportError, KeyError, OSError):
        return str(uid)


def preflight_check_migration(
    db_path: Path | str,
    *,
    checkpoint: bool = False,
) -> SchemaState:
    """Run HPC/filesystem pre-flight checks on `db_path` before migration."""
    target = Path(db_path).expanduser().resolve()
    if not target.exists():
        raise FileNotFoundError(f"Database file does not exist: {target}")

    parent_dir = target.parent
    if not os.access(parent_dir, os.W_OK):
        owner = _lookup_owner_username(parent_dir)
        raise ConsistError(
            f"Parent directory '{parent_dir}' is not writable by current user "
            f"(directory owner: {owner}). Migration requires write access to "
            f"'{parent_dir}' to create the backup and temporary v2 database."
        )

    wal_path = Path(f"{target}.wal")
    if wal_path.exists():
        try:
            wal_size = os.path.getsize(wal_path)
        except OSError:
            wal_size = 0
        if wal_size > 0:
            if checkpoint:
                con = duckdb.connect(str(target))
                try:
                    con.execute("CHECKPOINT")
                finally:
                    con.close()
            else:
                raise ConsistError(
                    f"Unflushed DuckDB WAL file found at '{wal_path}' ({wal_size} bytes). "
                    "Close any active v1 writers or pass --checkpoint (checkpoint=True) "
                    "to flush the WAL before migrating."
                )

    state = detect_schema(target)
    if state is SchemaState.V2:
        return SchemaState.V2
    if state is SchemaState.LOCKED:
        raise ConsistError(
            f"Database '{target}' is currently locked by another DuckDB process."
        )
    if state is SchemaState.FUTURE:
        raise IncompatibleSchemaError(
            f"Database '{target}' uses a newer schema version than this Consist release supports."
        )
    if state in (SchemaState.UNREADABLE, SchemaState.UNKNOWN, SchemaState.ABSENT):
        raise ConsistError(
            f"Database '{target}' cannot be migrated from schema state {state.value!r}."
        )

    usage = shutil.disk_usage(parent_dir)
    required_bytes = 2 * target.stat().st_size
    if usage.free < required_bytes:
        raise ConsistError(
            f"Insufficient free disk space in '{parent_dir}': "
            f"{usage.free} bytes available, but at least {required_bytes} bytes "
            f"(2x '{target.name}') are required for safe backup and migration."
        )

    return SchemaState.V1


def _is_local_pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False


@contextmanager
def _migration_lock_context(
    db_path: Path | str,
    *,
    wait_seconds: float | None = None,
) -> Iterator[Path]:
    """Acquire an atomic filesystem lockfile around `db_path` for concurrent Slurm/rank safety."""
    target = Path(db_path).expanduser().resolve()
    lock_path = target.with_name(f".{target.name}.migrate.lock")
    timeout = (
        float(wait_seconds)
        if wait_seconds is not None
        else float(os.environ.get("CONSIST_MIGRATE_WAIT", "300"))
    )
    deadline = time.monotonic() + max(0.0, timeout)
    my_host = socket.gethostname()

    acquired = False
    while not acquired:
        try:
            fd = os.open(
                str(lock_path),
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o644,
            )
            try:
                payload = json.dumps(
                    {
                        "hostname": my_host,
                        "pid": os.getpid(),
                        "started_at_ns": time.time_ns(),
                    },
                    sort_keys=True,
                ).encode("utf-8")
                os.write(fd, payload)
            finally:
                os.close(fd)
            acquired = True
        except FileExistsError:
            stale = False
            try:
                raw_lock = lock_path.read_text(encoding="utf-8").strip()
                if raw_lock:
                    info = json.loads(raw_lock)
                    if isinstance(info, Mapping):
                        lock_host = info.get("hostname")
                        lock_pid = info.get("pid")
                        if (
                            lock_host == my_host
                            and isinstance(lock_pid, int)
                            and not _is_local_pid_alive(lock_pid)
                        ):
                            stale = True
            except (OSError, ValueError):
                pass

            if stale:
                try:
                    lock_path.unlink(missing_ok=True)
                except OSError:
                    pass
                continue

            if time.monotonic() >= deadline:
                raise ConsistError(
                    f"Timed out after {timeout:.1f}s waiting for migration lock '{lock_path}'."
                )
            time.sleep(0.1)

    try:
        yield lock_path
    finally:
        try:
            lock_path.unlink(missing_ok=True)
        except OSError:
            pass


def _serialize_v1_artifact_row(art: V1ArtifactRecord) -> str:
    payload: dict[str, Any] = {
        "id": str(art.id),
        "key": art.key,
        "container_uri": art.container_uri,
        "driver": art.driver,
        "hash": art.hash,
        "hash_algorithm": art.hash_algorithm,
        "hash_semantics": art.hash_semantics,
        "table_path": art.table_path,
        "array_path": art.array_path,
        "is_dir": art.is_dir,
        "content_id": str(art.content_id) if art.content_id is not None else None,
        "parent_artifact_id": (
            str(art.parent_artifact_id) if art.parent_artifact_id is not None else None
        ),
        "run_id": art.run_id,
        "schema_id": art.schema_id,
        "recovery_roots": art.recovery_roots,
        "facet": art.facet,
        "facet_kv": art.facet_kv,
        "meta": art.meta,
        "created_at": (
            art.created_at.isoformat() if art.created_at is not None else None
        ),
        "content_hash": art.content_hash,
        "content_meta": art.content_meta,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _has_v1_full_file_hash_evidence(art: V1ArtifactRecord) -> bool:
    if art.is_dir:
        return False
    for sem in (art.hash_semantics, art.meta.get("hash_semantics")):
        if isinstance(sem, str) and sem.strip().lower() in (
            "full",
            "computed_full",
            "raw_file_bytes",
            "sha256:file",
        ):
            return True
        if isinstance(sem, Mapping):
            src = str(sem.get("source") or "").strip().lower()
            contract = str(sem.get("digest_contract") or "").strip().lower()
            if src in ("full", "computed_full") or contract == "raw_file_bytes":
                return True
    strat = art.meta.get("hashing_strategy")
    if isinstance(strat, str) and strat.strip().lower() == "full":
        return True
    return False


def _is_v1_full_hash_semantics(art: V1ArtifactRecord) -> bool:
    return (
        not art.table_path
        and not art.array_path
        and _has_v1_full_file_hash_evidence(art)
    )


def _resolve_h5_dataset_for_v1_hash(obj: Any, h5py_mod: Any) -> Any | None:
    if isinstance(obj, h5py_mod.Dataset):
        return obj
    if isinstance(obj, h5py_mod.Group):
        for key in ("table", "block0_values", "values"):
            if key in obj:
                candidate = obj[key]
                if isinstance(candidate, h5py_mod.Dataset):
                    return candidate
        for key in obj.keys():
            candidate = obj[key]
            if isinstance(candidate, h5py_mod.Dataset):
                return candidate
    return None


def _verify_v1_h5_table_hash(
    path: Path,
    table_path: str,
    expected_hex: str,
    chunk_rows: int | None = None,
) -> bool:
    """Verify whether an HDF5 dataset at `table_path` matches v1's `_hash_h5_dataset` SHA-256."""
    try:
        import h5py
        import numpy as np
    except ImportError:
        return False

    with open_h5py_read_only(str(path), h5py_module=h5py) as h5_file:
        if table_path in h5_file:
            target_obj = h5_file[table_path]
        else:
            alt_path = f"/{table_path.lstrip('/')}"
            if alt_path in h5_file:
                target_obj = h5_file[alt_path]
            else:
                return False

        resolved = _resolve_h5_dataset_for_v1_hash(target_obj, h5py)
        if resolved is None:
            return False

        hasher = hashlib.sha256()
        hasher.update(str(resolved.dtype).encode("utf-8"))
        hasher.update(str(resolved.shape).encode("utf-8"))

        shape = resolved.shape
        if shape == ():
            data = resolved[()]
            hasher.update(np.asarray(data).tobytes(order="C"))
            return hasher.hexdigest().lower() == expected_hex.lower()

        total_rows = int(shape[0]) if len(shape) else 0
        if total_rows == 0:
            return hasher.hexdigest().lower() == expected_hex.lower()

        rows_per_chunk = chunk_rows
        if rows_per_chunk is None:
            if resolved.chunks and resolved.chunks[0]:
                rows_per_chunk = int(resolved.chunks[0])
            else:
                rows_per_chunk = min(1024, total_rows)

        rows_per_chunk = max(1, rows_per_chunk)
        for start in range(0, total_rows, rows_per_chunk):
            end = min(total_rows, start + rows_per_chunk)
            chunk = resolved[start:end]
            hasher.update(np.asarray(chunk).tobytes(order="C"))

        return hasher.hexdigest().lower() == expected_hex.lower()


def _resolve_candidate_path(
    art: V1ArtifactRecord,
    *,
    v1_db_dir: Path,
    run_record: V1RunRecord | None,
    mounts: Mapping[str, Mount],
    snapshot_dirs: Sequence[Path],
) -> Path | None:
    uri = art.container_uri.strip()
    if not uri or uri.startswith("db://") or art.driver.strip().lower() == "dlt":
        return None

    direct_path, _ = resolve_uri_to_path(uri, mounts)
    if direct_path is not None and direct_path.exists():
        return direct_path.resolve()

    if "://" in uri:
        scheme, tail = uri.split("://", 1)
        if scheme.lower() == "file":
            rel_tail = tail.lstrip("/")
        else:
            rel_tail = tail.lstrip("/\\")
    else:
        rel_tail = uri
    while rel_tail.startswith("./"):
        rel_tail = rel_tail[2:]

    search_roots: list[Path] = []
    if run_record is not None and isinstance(run_record.meta, Mapping):
        for r_key in ("_consist_run_dir", "run_dir", "output_dir", "working_dir"):
            val = run_record.meta.get(r_key)
            if isinstance(val, str) and val.strip():
                search_roots.append(Path(val).expanduser())
    search_roots.append(v1_db_dir)

    if art.recovery_roots:
        for root_str in art.recovery_roots:
            if root_str.strip():
                search_roots.append(Path(root_str).expanduser())
    raw_meta_roots = art.meta.get("recovery_roots")
    if isinstance(raw_meta_roots, Sequence) and not isinstance(raw_meta_roots, str):
        for item in raw_meta_roots:
            if isinstance(item, str) and item.strip():
                search_roots.append(Path(item).expanduser())

    search_roots.extend(snapshot_dirs)

    rel_candidate = Path(rel_tail) if rel_tail else None
    basename = rel_candidate.name if rel_candidate is not None else ""

    for root in search_roots:
        if rel_candidate is not None and not rel_candidate.is_absolute():
            joined = root / rel_candidate
            if joined.exists():
                return joined.resolve()
        if basename:
            by_name = root / basename
            if by_name.exists():
                return by_name.resolve()

    return None


def _build_manifest_payload_from_v1_records(
    art: V1ArtifactRecord,
    dir_entries: Sequence[V1DirEntryRecord],
) -> tuple[Digest, tuple[bytes, int, int]] | None:
    raw_tuples: list[tuple[Any, Any, Any, Any]] = []
    if dir_entries:
        raw_tuples = [
            (rec.rel_path, rec.kind, rec.sha256, rec.size) for rec in dir_entries
        ]
    else:
        raw_manifest: Any = art.meta.get("directory_manifest")
        if not isinstance(raw_manifest, Mapping) and isinstance(
            art.content_meta, Mapping
        ):
            raw_manifest = art.content_meta.get("directory_manifest")
        if not isinstance(raw_manifest, Mapping):
            return None
        raw_entries = raw_manifest.get("entries")
        if not isinstance(raw_entries, Sequence) or isinstance(raw_entries, str):
            return None
        for item in raw_entries:
            if not isinstance(item, Mapping):
                return None
            raw_tuples.append(
                (
                    item.get("path") or item.get("rel_path"),
                    item.get("kind"),
                    item.get("sha256"),
                    item.get("size"),
                )
            )

    entries_by_path: dict[str, dict[str, Any]] = {}
    for raw_p, raw_kind, raw_sha, raw_sz in raw_tuples:
        if not isinstance(raw_p, str) or not raw_p.strip():
            if dir_entries:
                continue
            return None
        norm_path = Path(raw_p).as_posix().lstrip("./")
        if not norm_path or norm_path == ".":
            continue
        kind = "directory" if raw_kind == "directory" else "file"
        if kind == "file":
            if (
                not isinstance(raw_sha, str)
                or not _is_64_hex(raw_sha)
                or raw_sz is None
                or int(raw_sz) < 0
            ):
                return None
            entries_by_path[norm_path] = {
                "kind": "file",
                "path": norm_path,
                "sha256": raw_sha.lower(),
                "size": int(raw_sz),
            }
        else:
            entries_by_path[norm_path] = {
                "kind": "directory",
                "path": norm_path,
            }

    if not entries_by_path:
        return None

    for p_str in list(entries_by_path.keys()):
        parent = Path(p_str).parent
        while parent != Path("."):
            parent_posix = parent.as_posix()
            if parent_posix not in entries_by_path:
                entries_by_path[parent_posix] = {
                    "kind": "directory",
                    "path": parent_posix,
                }
            parent = parent.parent

    sorted_entries = sorted(
        entries_by_path.values(),
        key=lambda e: (str(e["path"]), str(e["kind"])),
    )
    total_bytes = sum(int(e["size"]) for e in sorted_entries if e["kind"] == "file")
    body_bytes = canonical_json_str({"version": 1, "entries": sorted_entries}).encode(
        "utf-8"
    )
    tree_hash = digest_bytes(body_bytes)
    anchor = Digest("sha256:manifest-v1", tree_hash)
    return (anchor, (body_bytes, len(sorted_entries), total_bytes))


def _cursor_to_arrow_table(cursor: Any) -> Any:
    if "to_arrow_table" in dir(cursor):
        return cursor.to_arrow_table()
    return cursor.fetch_arrow_table()


def _try_classify_dlt_hot_table(
    art: V1ArtifactRecord,
    *,
    con: duckdb.DuckDBPyConnection,
    dlt_tables: Sequence[V1DltTableInfo],
    candidate_path: Path | None = None,
    v1_row_json: str,
) -> ArtifactClassification | None:
    if not dlt_tables:
        return None

    is_explicit_dlt = art.driver.strip().lower() in (
        "dlt",
        "duckdb",
        "table",
    ) or art.container_uri.strip().lower().startswith("db://")
    is_ingested = bool(art.meta.get("is_ingested"))

    if not is_explicit_dlt and not is_ingested and candidate_path is not None:
        return None
    if not is_explicit_dlt and not is_ingested and _is_v1_full_hash_semantics(art):
        return None

    cand_names: list[str] = []
    dlt_meta_name = art.meta.get("dlt_table_name")
    if isinstance(dlt_meta_name, str) and dlt_meta_name.strip():
        cand_names.append(dlt_meta_name.strip())
    if art.container_uri.strip().lower().startswith("db://"):
        tail = art.container_uri.strip()[5:].strip("/")
        if "." in tail:
            tail = tail.split(".")[-1]
        if "/" in tail:
            tail = tail.split("/")[-1]
        if tail and tail not in cand_names:
            cand_names.append(tail)
    if art.table_path:
        tp = art.table_path.strip().lstrip("/")
        if "." in tp:
            tp = tp.split(".")[-1]
        if tp and tp not in cand_names:
            cand_names.append(tp)
    if art.key and art.key not in cand_names:
        cand_names.append(art.key)

    non_internal = [t for t in dlt_tables if not t.is_dlt_internal]
    matched_info: V1DltTableInfo | None = None
    for cand in cand_names:
        cand_lower = cand.lower()
        for info in non_internal:
            if info.table_name.lower() == cand_lower:
                matched_info = info
                break
        if matched_info is not None:
            break

    if matched_info is None:
        return None

    q_schema = _quote_ident(matched_info.schema_name)
    q_table = _quote_ident(matched_info.table_name)
    user_cols = [
        col
        for col in matched_info.columns
        if col
        not in {
            "consist_artifact_id",
            "consist_run_id",
            "consist_year",
            "consist_iteration",
        }
        and not col.startswith("_dlt_")
    ]
    select_cols = user_cols if user_cols else list(matched_info.columns.keys())
    if not select_cols:
        return None
    cols_sql = ", ".join(_quote_ident(c) for c in select_cols)

    arrow_table: Any = None
    where_sql = ""
    params: list[Any] = []
    dlt_member = matched_info.table_name

    if matched_info.has_consist_artifact_id:
        cand_where = " WHERE CAST(consist_artifact_id AS VARCHAR) = ?"
        cand_params = [str(art.id)]
        cand_arrow = _cursor_to_arrow_table(
            con.execute(
                f"SELECT {cols_sql} FROM {q_schema}.{q_table}{cand_where}",
                cand_params,
            )
        )
        if int(cand_arrow.num_rows) > 0:
            arrow_table = cand_arrow
            where_sql = cand_where
            params = cand_params
            dlt_member = f"{matched_info.table_name}?consist_artifact_id={art.id}"

    if (
        arrow_table is None
        and (is_explicit_dlt or is_ingested)
        and matched_info.has_consist_run_id
        and art.run_id is not None
    ):
        cand_where = (
            " WHERE CAST(consist_run_id AS VARCHAR) = ? AND consist_artifact_id IS NULL"
            if matched_info.has_consist_artifact_id
            else " WHERE CAST(consist_run_id AS VARCHAR) = ?"
        )
        cand_params = [art.run_id]
        cand_arrow = _cursor_to_arrow_table(
            con.execute(
                f"SELECT {cols_sql} FROM {q_schema}.{q_table}{cand_where}",
                cand_params,
            )
        )
        if int(cand_arrow.num_rows) > 0:
            arrow_table = cand_arrow
            where_sql = cand_where
            params = cand_params
            dlt_member = f"{matched_info.table_name}?consist_run_id={art.run_id}"

    if arrow_table is None and is_explicit_dlt and matched_info.row_count > 0:
        cand_arrow = _cursor_to_arrow_table(
            con.execute(f"SELECT {cols_sql} FROM {q_schema}.{q_table}")
        )
        if int(cand_arrow.num_rows) > 0:
            arrow_table = cand_arrow
            where_sql = ""
            params = []
            dlt_member = matched_info.table_name

    if arrow_table is None:
        return None

    num_rows = int(arrow_table.num_rows)
    hex_val = compute_ctd1_digest(arrow_table, columns=select_cols).hex
    load_id = str(art.run_id or art.id)
    if "_dlt_load_id" in matched_info.columns:
        ld_row = con.execute(
            f'SELECT CAST(MAX("_dlt_load_id") AS VARCHAR) FROM {q_schema}.{q_table}{where_sql}',
            params,
        ).fetchone()
        if ld_row is not None and ld_row[0] is not None:
            load_id = str(ld_row[0])
    else:
        for t_info in dlt_tables:
            if (
                t_info.schema_name == matched_info.schema_name
                and t_info.table_name == "_dlt_loads"
                and "load_id" in t_info.columns
            ):
                ld_row = con.execute(
                    f'SELECT CAST(MAX("load_id") AS VARCHAR) FROM {q_schema}."_dlt_loads"'
                ).fetchone()
                if ld_row is not None and ld_row[0] is not None:
                    load_id = str(ld_row[0])
                break

    return ArtifactClassification(
        artifact_id=art.id,
        outcome="verified",
        basis="dlt_hot_table",
        anchor=Digest("ctd1:sha256", hex_val),
        repr_kind="table",
        resolved_path=None,
        witness=DbTableChangeToken(load_id=load_id, row_count=num_rows),
        manifest_payload=None,
        dlt_member=dlt_member,
        v1_row_json=v1_row_json,
    )


def _fallback_historical_hex(art: V1ArtifactRecord) -> str:
    if _is_64_hex(art.hash):
        assert art.hash is not None
        return art.hash.lower()
    if _is_64_hex(art.content_hash):
        assert art.content_hash is not None
        return art.content_hash.lower()
    return hashlib.sha256(f"v1-artifact:{art.id}".encode("utf-8")).hexdigest()


def _classify_cold_artifact_on_disk(
    art: V1ArtifactRecord,
    candidate_path: Path | None,
    *,
    quick: bool,
    v1_row_json: str,
    max_workers: int = 4,
) -> ArtifactClassification:
    raw_table_hash = art.meta.get("table_hash")
    meta_table_hash = (
        raw_table_hash
        if isinstance(raw_table_hash, str) and _is_64_hex(raw_table_hash)
        else None
    )
    effective_hash = (
        art.hash
        if _is_64_hex(art.hash)
        else (art.content_hash if _is_64_hex(art.content_hash) else meta_table_hash)
    )

    # Step 3: Explicit full SHA-256 recorded in v1
    if _is_v1_full_hash_semantics(art) and effective_hash is not None:
        lower_hash = effective_hash.lower()
        anchor = Digest("sha256:file", lower_hash)
        if not quick and candidate_path is not None and candidate_path.is_file():
            try:
                actual_digest, fstat = _hash_file_descriptor_pinned(candidate_path)
            except OSError:
                actual_digest = None
                fstat = None
            if actual_digest is not None and actual_digest.hex == lower_hash:
                return ArtifactClassification(
                    artifact_id=art.id,
                    outcome="verified",
                    basis="v1_full_reverified",
                    anchor=anchor,
                    repr_kind="file",
                    resolved_path=candidate_path,
                    witness=fstat,
                    v1_row_json=v1_row_json,
                )
            # File on disk was proven overwritten: preserve the verified v1_full anchor
            # and flag cold_path_overwritten=True so apply_migration marks its location evicted.
            return ArtifactClassification(
                artifact_id=art.id,
                outcome="verified",
                basis="v1_full",
                anchor=anchor,
                repr_kind="file",
                resolved_path=None,
                witness=None,
                v1_row_json=v1_row_json,
                cold_path_overwritten=True,
            )
        return ArtifactClassification(
            artifact_id=art.id,
            outcome="verified",
            basis="v1_full",
            anchor=anchor,
            repr_kind="file",
            resolved_path=None,
            witness=None,
            v1_row_json=v1_row_json,
        )

    # Step 4: Cold file or directory on disk when not quick
    if not quick and candidate_path is not None and candidate_path.exists():
        if candidate_path.is_dir():
            try:
                anchor_dir, body_bytes, entry_count, total_bytes, dir_stat = (
                    _build_manifest_v1_parallel(
                        candidate_path,
                        max_workers=max(1, max_workers),
                    )
                )
                if effective_hash is None or effective_hash.lower() == anchor_dir.hex:
                    return ArtifactClassification(
                        artifact_id=art.id,
                        outcome="verified",
                        basis="migrated_fast_match",
                        anchor=anchor_dir,
                        repr_kind="tree",
                        resolved_path=candidate_path,
                        witness=dir_stat,
                        manifest_payload=(body_bytes, entry_count, total_bytes),
                        v1_row_json=v1_row_json,
                    )
            except (OSError, ValueError):
                pass
            return ArtifactClassification(
                artifact_id=art.id,
                outcome="historical",
                basis="overwritten_cold",
                anchor=Digest("legacy-v1:unverified", _fallback_historical_hex(art)),
                repr_kind="historical",
                resolved_path=None,
                witness=None,
                v1_row_json=v1_row_json,
            )

        if candidate_path.is_file() and effective_hash is not None:
            lower_hash = effective_hash.lower()
            if art.table_path:
                scheme = "ctd1:sha256"
                member: str | None = art.table_path
            elif art.array_path:
                scheme = "sha256:logical_matrix:v1"
                member = art.array_path
            else:
                scheme = "sha256:file"
                member = None
            repr_kind = "member" if member is not None else "file"

            try:
                current_fast, st, _ = stat_v1_fast_file_hash(candidate_path)
                if current_fast == lower_hash:
                    if art.created_at is not None:
                        recorded_ns = int(art.created_at.timestamp() * 1_000_000_000)
                        mtime_ns = (
                            st.st_mtime_ns
                            if st.st_mtime_ns is not None
                            else int(st.st_mtime * 1_000_000_000)
                        )
                        if (recorded_ns - mtime_ns) < RACY_STAT_WINDOW_NS:
                            return ArtifactClassification(
                                artifact_id=art.id,
                                outcome="historical",
                                basis="racy_fast_stat",
                                anchor=Digest("legacy-v1:unverified", lower_hash),
                                repr_kind="historical",
                                resolved_path=None,
                                witness=None,
                                v1_row_json=v1_row_json,
                            )

                    anchor_ver, witness_ver = compute_scheme_digest(
                        candidate_path,
                        scheme,
                        driver=art.driver,
                        member=member,
                        max_workers=1,
                    )
                    return ArtifactClassification(
                        artifact_id=art.id,
                        outcome="verified",
                        basis="migrated_fast_match",
                        anchor=anchor_ver,
                        repr_kind=repr_kind,
                        resolved_path=candidate_path,
                        witness=witness_ver,
                        v1_row_json=v1_row_json,
                    )

                if art.table_path is not None or art.array_path is not None:
                    matched_member = False
                    if art.table_path is not None:
                        raw_chunk = art.meta.get("table_hash_chunk_rows")
                        chunk_rows = (
                            int(raw_chunk)
                            if isinstance(raw_chunk, int)
                            and not isinstance(raw_chunk, bool)
                            else None
                        )
                        expected_table_hash = (
                            meta_table_hash.lower()
                            if meta_table_hash is not None
                            else lower_hash
                        )
                        if _verify_v1_h5_table_hash(
                            candidate_path,
                            art.table_path,
                            expected_table_hash,
                            chunk_rows=chunk_rows,
                        ):
                            matched_member = True
                    if not matched_member and _has_v1_full_file_hash_evidence(art):
                        if (
                            _hash_file_descriptor_pinned(candidate_path)[0].hex
                            == lower_hash
                        ):
                            matched_member = True

                    if matched_member:
                        anchor_ver, witness_ver = compute_scheme_digest(
                            candidate_path,
                            scheme,
                            driver=art.driver,
                            member=member,
                            max_workers=1,
                        )
                        return ArtifactClassification(
                            artifact_id=art.id,
                            outcome="verified",
                            basis="v1_full_reverified",
                            anchor=anchor_ver,
                            repr_kind=repr_kind,
                            resolved_path=candidate_path,
                            witness=witness_ver,
                            v1_row_json=v1_row_json,
                        )
            except (OSError, KeyError, ValueError):
                pass

            return ArtifactClassification(
                artifact_id=art.id,
                outcome="historical",
                basis="overwritten_cold",
                anchor=Digest("legacy-v1:unverified", lower_hash),
                repr_kind="historical",
                resolved_path=None,
                witness=None,
                v1_row_json=v1_row_json,
            )

    # Step 5: Remaining cases (unhashed_v1, quick_deferred, missing_cold)
    fallback_hex = _fallback_historical_hex(art)
    if effective_hash is None:
        basis: ClassificationBasis = "unhashed_v1"
    elif quick:
        basis = "quick_deferred"
    else:
        basis = "missing_cold"

    return ArtifactClassification(
        artifact_id=art.id,
        outcome="historical",
        basis=basis,
        anchor=Digest("legacy-v1:unverified", fallback_hex),
        repr_kind="historical",
        resolved_path=None,
        witness=None,
        v1_row_json=v1_row_json,
    )


def _classify_db_backed_artifact(
    art: V1ArtifactRecord,
    *,
    con: duckdb.DuckDBPyConnection,
    dlt_tables: Sequence[V1DltTableInfo],
    dir_entries: Sequence[V1DirEntryRecord],
    candidate_path: Path | None,
    quick: bool,
    v1_row_json: str,
) -> ArtifactClassification | None:
    manifest_info = _build_manifest_payload_from_v1_records(art, dir_entries)
    if manifest_info is not None:
        anchor_tree, manifest_payload = manifest_info
        dir_witness: Witness | None = None
        res_dir: Path | None = None
        if not quick and candidate_path is not None and candidate_path.is_dir():
            try:
                dir_witness = DirTreeStat.capture(candidate_path)
                res_dir = candidate_path
            except OSError:
                dir_witness = None
                res_dir = None
        return ArtifactClassification(
            artifact_id=art.id,
            outcome="verified",
            basis="directory_manifest",
            anchor=anchor_tree,
            repr_kind="tree",
            resolved_path=res_dir,
            witness=dir_witness,
            manifest_payload=manifest_payload,
            v1_row_json=v1_row_json,
        )

    return _try_classify_dlt_hot_table(
        art,
        con=con,
        dlt_tables=dlt_tables,
        candidate_path=candidate_path,
        v1_row_json=v1_row_json,
    )


def classify_v1_artifact(
    art: V1ArtifactRecord,
    *,
    con: duckdb.DuckDBPyConnection,
    dlt_tables: Sequence[V1DltTableInfo] = (),
    dir_entries: Sequence[V1DirEntryRecord] = (),
    v1_db_dir: Path,
    run_record: V1RunRecord | None = None,
    mounts: Mapping[str, Mount] | None = None,
    snapshot_dirs: Sequence[Path] = (),
    quick: bool = False,
) -> ArtifactClassification:
    """Classify a single `V1ArtifactRecord` without deduplicating by v1 fast hash."""
    v1_row_json = _serialize_v1_artifact_row(art)
    mount_map = dict(mounts) if mounts is not None else {}
    candidate_path = (
        None
        if quick
        else _resolve_candidate_path(
            art,
            v1_db_dir=v1_db_dir,
            run_record=run_record,
            mounts=mount_map,
            snapshot_dirs=snapshot_dirs,
        )
    )

    db_cls = _classify_db_backed_artifact(
        art,
        con=con,
        dlt_tables=dlt_tables,
        dir_entries=dir_entries,
        candidate_path=candidate_path,
        quick=quick,
        v1_row_json=v1_row_json,
    )
    if db_cls is not None:
        return db_cls

    return _classify_cold_artifact_on_disk(
        art,
        candidate_path,
        quick=quick,
        v1_row_json=v1_row_json,
    )


def plan_migration(
    v1_path: Path | str,
    *,
    quick: bool = False,
    workers: int | None = None,
    snapshot_dirs: Sequence[Path | str] = (),
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
) -> MigrationPlan:
    """Build a complete `MigrationPlan` for `v1_path`."""
    resolved_v1 = Path(v1_path).expanduser().resolve()
    mount_map = _normalize_mounts(mounts)
    norm_snapshots = tuple(Path(p).expanduser().resolve() for p in snapshot_dirs)
    effective_workers = (
        max(1, int(workers)) if workers is not None else default_migration_workers()
    )

    with V1DatabaseReader(resolved_v1) as reader:
        dlt_tables = tuple(reader.list_dlt_tables())
        runs = tuple(reader.read_runs())
        artifacts = tuple(reader.read_artifacts())
        links = tuple(reader.read_links())
        all_dir_entries = reader.read_dir_entries()

        runs_by_id: dict[str, V1RunRecord] = {r.id: r for r in runs}
        dir_entries_by_art: dict[uuid.UUID, list[V1DirEntryRecord]] = {}
        for entry in all_dir_entries:
            if entry.artifact_id is not None:
                dir_entries_by_art.setdefault(entry.artifact_id, []).append(entry)

        classifications_by_idx: dict[int, ArtifactClassification] = {}
        cold_work: list[tuple[int, V1ArtifactRecord, Path | None, str]] = []

        for idx, art in enumerate(artifacts):
            v1_row_json = _serialize_v1_artifact_row(art)
            run_rec = runs_by_id.get(art.run_id) if art.run_id is not None else None
            candidate_path = (
                None
                if quick
                else _resolve_candidate_path(
                    art,
                    v1_db_dir=resolved_v1.parent,
                    run_record=run_rec,
                    mounts=mount_map,
                    snapshot_dirs=norm_snapshots,
                )
            )

            db_cls = _classify_db_backed_artifact(
                art,
                con=reader.connection,
                dlt_tables=dlt_tables,
                dir_entries=dir_entries_by_art.get(art.id, ()),
                candidate_path=candidate_path,
                quick=quick,
                v1_row_json=v1_row_json,
            )
            if db_cls is not None:
                classifications_by_idx[idx] = db_cls
                continue

            cold_work.append((idx, art, candidate_path, v1_row_json))

        def _run_cold_one(
            item: tuple[int, V1ArtifactRecord, Path | None, str],
        ) -> tuple[int, ArtifactClassification]:
            item_idx, item_art, item_path, item_json = item
            cls_res = _classify_cold_artifact_on_disk(
                item_art,
                item_path,
                quick=quick,
                v1_row_json=item_json,
                max_workers=1,
            )
            return (item_idx, cls_res)

        parallel_cold_work = [
            item
            for item in cold_work
            if item[1].table_path is None and item[1].array_path is None
        ]
        serial_cold_work = [
            item
            for item in cold_work
            if item[1].table_path is not None or item[1].array_path is not None
        ]

        if not quick and len(parallel_cold_work) > 1 and effective_workers > 1:
            with ThreadPoolExecutor(max_workers=effective_workers) as pool:
                for idx, cls_res in pool.map(_run_cold_one, parallel_cold_work):
                    classifications_by_idx[idx] = cls_res
        else:
            for item in parallel_cold_work:
                idx, cls_res = _run_cold_one(item)
                classifications_by_idx[idx] = cls_res

        for item in serial_cold_work:
            idx, cls_res = _run_cold_one(item)
            classifications_by_idx[idx] = cls_res

        ordered_cls = tuple(classifications_by_idx[i] for i in range(len(artifacts)))
        basis_counts: dict[str, int] = {
            str(k): int(v) for k, v in Counter(c.basis for c in ordered_cls).items()
        }
        outcome_counts: dict[str, int] = {
            str(k): int(v) for k, v in Counter(c.outcome for c in ordered_cls).items()
        }

        return MigrationPlan(
            v1_path=resolved_v1,
            run_count=len(runs),
            artifact_count=len(artifacts),
            link_count=len(links),
            dlt_tables=dlt_tables,
            classifications=ordered_cls,
            counts_by_basis=basis_counts,
            counts_by_outcome=outcome_counts,
            runs=runs,
            artifacts=artifacts,
            links=links,
        )


def _sanitize_meta_for_v2(
    raw_meta: Mapping[str, Any],
    *,
    strip_keys: frozenset[str] = _STRIPPED_META_KEYS,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for k, v in raw_meta.items():
        k_str = str(k)
        if k_str in strip_keys or k_str.startswith("_dlt_"):
            continue
        cleaned[k_str] = v
    if extra:
        for k, v in extra.items():
            k_str = str(k)
            if k_str not in cleaned and k_str not in strip_keys:
                cleaned[k_str] = v
    return json.loads(json.dumps(cleaned, default=str))


def apply_migration(
    v1_path: Path | str,
    output_v2_path: Path | str,
    plan: MigrationPlan,
    *,
    site: str = "local",
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
) -> None:
    """Materialize a v2 `StoreLedger` at `output_v2_path` from `v1_path` and `plan`."""
    resolved_v1 = Path(v1_path).expanduser().resolve()
    resolved_v2 = Path(output_v2_path).expanduser().resolve()
    if resolved_v2.exists():
        resolved_v2.unlink()
    wal_v2 = Path(f"{resolved_v2}.wal")
    if wal_v2.exists():
        wal_v2.unlink()

    mount_map = _normalize_mounts(mounts)

    if (
        plan.runs
        or plan.artifacts
        or plan.links
        or (plan.run_count == 0 and plan.artifact_count == 0 and plan.link_count == 0)
    ):
        runs: Sequence[V1RunRecord] = plan.runs
        artifacts: Sequence[V1ArtifactRecord] = plan.artifacts
        links: Sequence[V1LinkRecord] = plan.links
    else:
        with V1DatabaseReader(resolved_v1) as reader:
            runs = reader.read_runs()
            artifacts = reader.read_artifacts()
            links = reader.read_links()

    cls_by_art_id: dict[uuid.UUID, ArtifactClassification] = {
        c.artifact_id: c for c in plan.classifications
    }
    art_by_id: dict[uuid.UUID, V1ArtifactRecord] = {a.id: a for a in artifacts}

    with StoreLedger(resolved_v2, mounts=mount_map, writer="consist.migrate") as ledger:
        con = ledger.connection

        # 1. Selectively copy all non-main (dlt / global_tables) schemas & tables
        if plan.dlt_tables:
            escaped_v1 = str(resolved_v1).replace("'", "''")
            con.execute(f"ATTACH '{escaped_v1}' AS v1_src (READ_ONLY)")
            try:
                seen_schemas: set[str] = set()
                for t_info in plan.dlt_tables:
                    q_schema = _quote_ident(t_info.schema_name)
                    q_table = _quote_ident(t_info.table_name)
                    if t_info.schema_name not in seen_schemas:
                        con.execute(f"CREATE SCHEMA IF NOT EXISTS {q_schema}")
                        seen_schemas.add(t_info.schema_name)
                    con.execute(
                        f"CREATE TABLE {q_schema}.{q_table} AS "
                        f"SELECT * FROM v1_src.{q_schema}.{q_table}"
                    )
            finally:
                con.execute("DETACH v1_src")

        now_utc = datetime.now(timezone.utc)
        now_ns = time.time_ns()

        con.begin()
        try:
            # 2. Record content (which also records primary content_digest when verifiable),
            #    content_manifest, artifact, artifact_migration_audit, and observations.
            for idx, art in enumerate(artifacts):
                cls = cls_by_art_id[art.id]
                obs_ts_ns = (
                    int(art.created_at.timestamp() * 1_000_000_000)
                    if art.created_at is not None
                    else (now_ns + idx)
                )

                ledger.record_content(
                    cls.anchor,
                    cls.repr_kind,
                    schema_id=art.schema_id,
                    created_at=art.created_at or now_utc,
                )
                if cls.manifest_payload is not None:
                    body_bytes, entry_count, total_bytes = cls.manifest_payload
                    ledger.record_content_manifest(
                        cls.anchor,
                        format="manifest-v1",
                        entry_count=entry_count,
                        total_bytes=total_bytes,
                        body=body_bytes,
                    )

                extra_meta: dict[str, Any] = {}
                if art.facet_kv:
                    extra_meta["facet"] = art.facet_kv
                elif art.facet:
                    extra_meta["facet"] = art.facet
                if art.parent_artifact_id is not None:
                    extra_meta["parent_artifact_id"] = str(art.parent_artifact_id)

                v2_meta = _sanitize_meta_for_v2(art.meta, extra=extra_meta)
                ledger.record_artifact(
                    artifact_id=art.id,
                    key=art.key,
                    driver=art.driver,
                    anchor=cls.anchor,
                    run_id=art.run_id,
                    schema_id=art.schema_id,
                    meta=v2_meta,
                    created_at=art.created_at or now_utc,
                )

                con.execute(
                    "INSERT INTO artifact_migration_audit "
                    "(artifact_id, v1_row_json, outcome, basis) "
                    "VALUES (?, ?, ?, ?) "
                    "ON CONFLICT (artifact_id) DO UPDATE SET "
                    "v1_row_json = EXCLUDED.v1_row_json, "
                    "outcome = EXCLUDED.outcome, "
                    "basis = EXCLUDED.basis",
                    [
                        art.id,
                        cls.v1_row_json,
                        cls.outcome,
                        cls.basis,
                    ],
                )

                member = art.table_path or art.array_path
                identity = Identity(anchor=cls.anchor)

                if cls.outcome == "verified" and cls.dlt_member is not None:
                    db_uri = (
                        art.container_uri
                        if art.container_uri.startswith("db://")
                        else f"db://{cls.dlt_member.split('?', 1)[0]}"
                    )
                    ledger.record_observation(
                        Observation(
                            location=Location(uri=db_uri, member=cls.dlt_member),
                            identity=identity,
                            access_driver=art.driver or "dlt",
                            witness=cls.witness,
                            basis="computed",
                            site=site,
                            observed_at_ns=obs_ts_ns,
                        ),
                        repr_kind=None,
                        writer="consist.migrate",
                    )
                elif cls.outcome == "verified" and cls.resolved_path is not None:
                    if art.container_uri.strip() and "://" in art.container_uri:
                        uri = art.container_uri.strip()
                        _, matched_mount = resolve_uri_to_path(uri, mount_map)
                    else:
                        uri, matched_mount = virtualize_path(
                            cls.resolved_path,
                            mount_map,
                        )
                    mount_inst_id = (
                        matched_mount.resolve_instance_id()
                        if matched_mount is not None
                        else None
                    )
                    ledger.record_observation(
                        Observation(
                            location=Location(uri=uri, member=member),
                            identity=identity,
                            access_driver=art.driver,
                            witness=cls.witness,
                            basis="computed",
                            site=site,
                            mount_instance_id=mount_inst_id,
                            observed_at_ns=obs_ts_ns,
                        ),
                        repr_kind=None,
                        writer="consist.migrate",
                    )
                elif art.container_uri.strip():
                    obs_id = ledger.record_observation(
                        Observation(
                            location=Location(
                                uri=art.container_uri.strip(),
                                member=member,
                            ),
                            identity=identity,
                            access_driver=art.driver,
                            witness=None,
                            basis="declared",
                            site=site,
                            observed_at_ns=obs_ts_ns,
                        ),
                        repr_kind=None,
                        writer="consist.migrate",
                    )
                    if cls.cold_path_overwritten or cls.basis == "overwritten_cold":
                        ledger.record_location_event(
                            observation_id=obs_id,
                            status="evicted",
                            status_rank=2,
                            at_ns=obs_ts_ns + 1,
                            writer="consist.migrate",
                        )

            # 3. Group links by run_id and determine run cache_status
            links_by_run: dict[str, list[tuple[uuid.UUID, str]]] = {}
            for link in links:
                if link.artifact_id in cls_by_art_id:
                    links_by_run.setdefault(link.run_id, []).append(
                        (link.artifact_id, link.direction)
                    )

            arts_by_run: dict[str, list[uuid.UUID]] = {}
            for art in artifacts:
                if art.run_id is not None:
                    arts_by_run.setdefault(art.run_id, []).append(art.id)

            recorded_run_ids: set[str] = set()
            for run in runs:
                bound_art_ids = {
                    *(aid for aid, _ in links_by_run.get(run.id, ())),
                    *arts_by_run.get(run.id, ()),
                }
                all_verified = all(
                    cls_by_art_id[aid].outcome == "verified"
                    and cls_by_art_id[aid].anchor.kind is not Kind.HISTORICAL
                    for aid in bound_art_ids
                    if aid in cls_by_art_id
                )
                if all_verified and run.signature:
                    cache_status: Literal["live", "historical"] = "live"
                    sig_val: str | None = run.signature
                    sig_scheme: str | None = "legacy-v1"
                else:
                    cache_status = "historical"
                    sig_val = None
                    sig_scheme = None

                extra_run_meta: dict[str, Any] = {}
                if run.stage is not None:
                    extra_run_meta["stage"] = run.stage
                if run.phase is not None:
                    extra_run_meta["phase"] = run.phase
                if run.facet is not None:
                    extra_run_meta["facet"] = run.facet
                if run.signature is not None and sig_val is None:
                    extra_run_meta["v1_signature"] = run.signature

                v2_run_meta = _sanitize_meta_for_v2(
                    run.meta,
                    strip_keys=frozenset(),
                    extra=extra_run_meta,
                )
                ledger.record_run(
                    run_id=run.id,
                    model_name=run.model_name,
                    status=run.status,
                    cache_status=cache_status,
                    description=run.description,
                    year=run.year,
                    iteration=run.iteration,
                    parent_run_id=run.parent_run_id,
                    code_hash=run.git_hash,
                    config_hash=run.config_hash,
                    input_hash=run.input_hash,
                    signature=sig_val,
                    signature_scheme=sig_scheme,
                    tags=run.tags,
                    meta=v2_run_meta,
                    started_at=run.started_at,
                    ended_at=run.ended_at,
                    created_at=run.created_at or now_utc,
                )
                recorded_run_ids.add(run.id)

            for link_run_id in sorted(links_by_run.keys()):
                if link_run_id not in recorded_run_ids:
                    ledger.record_run(
                        run_id=link_run_id,
                        model_name="unknown",
                        status="completed",
                        cache_status="historical",
                        created_at=now_utc,
                    )
                    recorded_run_ids.add(link_run_id)

            # 4. Record run_binding rows preserving 1-for-1 run_artifact_link view parity
            #    and pass-through (input + output) roles.
            for run_id, run_links in sorted(links_by_run.items()):
                occ_by_dir: dict[str, int] = {"input": 0, "output": 0, "depends_on": 0}
                seen_dir_art: set[tuple[str, uuid.UUID]] = set()
                used_slots: dict[tuple[str, str], int] = {}

                def _insert_binding(
                    direction_str: Literal["input", "output", "depends_on"],
                    art_id: uuid.UUID,
                ) -> None:
                    if (direction_str, art_id) in seen_dir_art:
                        return
                    seen_dir_art.add((direction_str, art_id))
                    art_rec = art_by_id[art_id]
                    cls_rec = cls_by_art_id[art_id]
                    base_slot = (
                        art_rec.key.strip() if art_rec.key.strip() else str(art_id)
                    )
                    ordinal = used_slots.get((direction_str, base_slot), 0)
                    used_slots[(direction_str, base_slot)] = ordinal + 1
                    occ_idx = occ_by_dir[direction_str]
                    occ_by_dir[direction_str] = occ_idx + 1
                    ledger.record_run_binding(
                        run_id=run_id,
                        direction=direction_str,
                        occurrence_index=occ_idx,
                        slot=base_slot,
                        artifact_id=art_id,
                        anchor=cls_rec.anchor,
                        ordinal=ordinal,
                        role=art_rec.key.strip() or None,
                    )

                for art_id, raw_dir in run_links:
                    norm_dir: Literal["input", "output", "depends_on"] = (
                        "input" if raw_dir in ("input", "depends_on") else "output"
                    )
                    _insert_binding(norm_dir, art_id)
                    art_rec = art_by_id[art_id]
                    if norm_dir == "input" and art_rec.run_id == run_id:
                        _insert_binding("output", art_id)

            con.commit()
        except Exception:
            con.rollback()
            raise


def verify_migration(
    v1_path: Path | str,
    v2_path: Path | str,
    *,
    plan: MigrationPlan | None = None,
) -> MigrationVerificationReport:
    """Verify all 8 release gates comparing `v2_path` against `v1_path`.

    Raises `ConsistError` if any check fails.
    """
    resolved_v1 = Path(v1_path).expanduser().resolve()
    resolved_v2 = Path(v2_path).expanduser().resolve()
    errors: list[str] = []

    if plan is not None and (
        plan.runs
        or plan.artifacts
        or plan.links
        or (plan.run_count == 0 and plan.artifact_count == 0 and plan.link_count == 0)
    ):
        v1_run_count = plan.run_count
        v1_artifact_count = plan.artifact_count
        v1_dlt_tables = plan.dlt_tables
        v1_links: Sequence[V1LinkRecord] = plan.links
    else:
        with V1DatabaseReader(resolved_v1) as v1_reader:
            v1_inv = v1_reader.inventory()
            v1_run_count = v1_inv.run_count
            v1_artifact_count = v1_inv.artifact_count
            v1_dlt_tables = v1_inv.dlt_tables
            v1_links = v1_reader.read_links()

    v1_link_map: dict[tuple[str, str], str] = {}
    for lnk in v1_links:
        key = (lnk.run_id, str(lnk.artifact_id))
        direction = "input" if lnk.direction in ("input", "depends_on") else "output"
        prev = v1_link_map.get(key)
        if prev is None or direction == "input":
            v1_link_map[key] = direction
    v1_triples = {(r_id, a_id, d) for (r_id, a_id), d in v1_link_map.items()}

    with duckdb.connect(str(resolved_v2), read_only=True) as v2_con:
        v2_run_count = int(
            (v2_con.execute("SELECT COUNT(*) FROM main.run").fetchone() or (0,))[0]
        )
        v2_artifact_count = int(
            (v2_con.execute("SELECT COUNT(*) FROM main.artifact").fetchone() or (0,))[0]
        )
        v2_audit_count = int(
            (
                v2_con.execute(
                    "SELECT COUNT(*) FROM main.artifact_migration_audit"
                ).fetchone()
                or (0,)
            )[0]
        )

        if v2_run_count < v1_run_count:
            errors.append(f"Run count mismatch: v1={v1_run_count}, v2={v2_run_count}")
        if v2_artifact_count != v1_artifact_count:
            errors.append(
                f"Artifact count mismatch: v1={v1_artifact_count}, v2={v2_artifact_count}"
            )
        if v2_audit_count != v1_artifact_count:
            errors.append(
                f"Migration audit count mismatch: expected {v1_artifact_count}, got {v2_audit_count}"
            )

        v2_link_rows = v2_con.execute(
            "SELECT run_id, CAST(artifact_id AS VARCHAR), direction FROM main.run_artifact_link"
        ).fetchall()
        v2_triples = {(str(r[0]), str(r[1]), str(r[2])) for r in v2_link_rows}
        link_triples_match = v1_triples == v2_triples
        if not link_triples_match:
            missing_links = v1_triples - v2_triples
            extra_links = v2_triples - v1_triples
            errors.append(
                f"run_artifact_link triples mismatch: missing={len(missing_links)}, extra={len(extra_links)}"
            )

        orphan_anchors = int(
            (
                v2_con.execute(
                    "SELECT COUNT(*) FROM main.artifact a "
                    "LEFT JOIN main.content c ON a.anchor = c.anchor "
                    "WHERE c.anchor IS NULL"
                ).fetchone()
                or (0,)
            )[0]
        )
        orphan_bindings = int(
            (
                v2_con.execute(
                    "SELECT COUNT(*) FROM main.run_binding rb "
                    "LEFT JOIN main.artifact a ON rb.artifact_id = a.id "
                    "WHERE a.id IS NULL"
                ).fetchone()
                or (0,)
            )[0]
        )
        referential_integrity_ok = orphan_anchors == 0 and orphan_bindings == 0
        if not referential_integrity_ok:
            errors.append(
                f"Referential integrity failed: orphan_anchors={orphan_anchors}, orphan_bindings={orphan_bindings}"
            )

        hist_in_digest = int(
            (
                v2_con.execute(
                    "SELECT COUNT(*) FROM main.content_digest "
                    "WHERE anchor LIKE 'legacy-v1:unverified:%' OR scheme = 'legacy-v1:unverified'"
                ).fetchone()
                or (0,)
            )[0]
        )
        hist_in_live_runs = int(
            (
                v2_con.execute(
                    "SELECT COUNT(*) FROM main.run r "
                    "JOIN main.run_binding rb ON r.id = rb.run_id "
                    "WHERE r.cache_status = 'live' AND rb.anchor LIKE 'legacy-v1:unverified:%'"
                ).fetchone()
                or (0,)
            )[0]
        )
        historical_isolation_ok = hist_in_digest == 0 and hist_in_live_runs == 0
        if not historical_isolation_ok:
            errors.append(
                f"Historical isolation violated: hist_in_digest={hist_in_digest}, hist_in_live_runs={hist_in_live_runs}"
            )

        manifest_rows = v2_con.execute(
            "SELECT anchor, body FROM main.content_manifest"
        ).fetchall()
        manifests_verified = 0
        for raw_anchor, raw_body in manifest_rows:
            parsed_anchor = Digest.parse(str(raw_anchor))
            body_bytes = bytes(raw_body)
            actual_hex = digest_bytes(body_bytes)
            if actual_hex != parsed_anchor.hex:
                errors.append(
                    f"Manifest body hash mismatch for {raw_anchor}: got {actual_hex}"
                )
            else:
                manifests_verified += 1

        dlt_table_counts_match = True
        for t_info in v1_dlt_tables:
            q_schema = _quote_ident(t_info.schema_name)
            q_table = _quote_ident(t_info.table_name)
            try:
                cnt_row = v2_con.execute(
                    f"SELECT COUNT(*) FROM {q_schema}.{q_table}"
                ).fetchone()
                v2_cnt = int(cnt_row[0]) if cnt_row is not None else -1
            except duckdb.Error:
                v2_cnt = -1
            if v2_cnt != t_info.row_count:
                dlt_table_counts_match = False
                errors.append(
                    f"dlt table {t_info.schema_name}.{t_info.table_name} row count mismatch: "
                    f"v1={t_info.row_count}, v2={v2_cnt}"
                )

    report = MigrationVerificationReport(
        ok=len(errors) == 0,
        v1_run_count=v1_run_count,
        v2_run_count=v2_run_count,
        v1_artifact_count=v1_artifact_count,
        v2_artifact_count=v2_artifact_count,
        v2_audit_count=v2_audit_count,
        link_triples_match=link_triples_match,
        referential_integrity_ok=referential_integrity_ok,
        historical_isolation_ok=historical_isolation_ok,
        manifests_verified=manifests_verified,
        dlt_table_counts_match=dlt_table_counts_match,
        errors=tuple(errors),
    )
    if not report.ok:
        raise ConsistError(
            "Migration verification failed:\n  - " + "\n  - ".join(report.errors)
        )
    return report


def _choose_backup_path(v1_path: Path) -> Path:
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    base = v1_path.with_name(f"{v1_path.stem}.v1-backup-{date_str}{v1_path.suffix}")
    if not base.exists():
        return base
    counter = 1
    while True:
        cand = v1_path.with_name(
            f"{v1_path.stem}.v1-backup-{date_str}-{counter}{v1_path.suffix}"
        )
        if not cand.exists():
            return cand
        counter += 1


def activate_migration(v1_path: Path | str, v2_tmp_path: Path | str) -> Path:
    """Copy `v1_path` to `.v1-backup-YYYY-MM-DD.duckdb` and atomically swap `v2_tmp_path` into place."""
    resolved_v1 = Path(v1_path).expanduser().resolve()
    resolved_tmp = Path(v2_tmp_path).expanduser().resolve()
    backup_path = _choose_backup_path(resolved_v1)
    shutil.copy2(resolved_v1, backup_path)
    os.replace(resolved_tmp, resolved_v1)
    try:
        Path(f"{resolved_v1}.wal").unlink(missing_ok=True)
    except OSError:
        pass
    return backup_path


def migrate_v1_to_v2(
    db_path: Path | str,
    *,
    quick: bool = False,
    workers: int | None = None,
    checkpoint: bool = False,
    dry_run: bool = False,
    snapshot_dirs: Sequence[Path | str] = (),
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
    site: str = "local",
) -> MigrationResult:
    """Run the end-to-end v1-to-v2 migration under `_migration_lock_context`."""
    resolved_db = Path(db_path).expanduser().resolve()
    with _migration_lock_context(resolved_db):
        state = preflight_check_migration(resolved_db, checkpoint=checkpoint)
        if state is SchemaState.V2:
            with duckdb.connect(str(resolved_db), read_only=True) as con:
                r_cnt = int(
                    (con.execute("SELECT COUNT(*) FROM main.run").fetchone() or (0,))[0]
                )
                a_cnt = int(
                    (
                        con.execute("SELECT COUNT(*) FROM main.artifact").fetchone()
                        or (0,)
                    )[0]
                )
            empty_plan = MigrationPlan(
                v1_path=resolved_db,
                run_count=r_cnt,
                artifact_count=a_cnt,
                link_count=0,
                dlt_tables=(),
                classifications=(),
                counts_by_basis={},
                counts_by_outcome={},
            )
            empty_ver = MigrationVerificationReport(
                ok=True,
                v1_run_count=r_cnt,
                v2_run_count=r_cnt,
                v1_artifact_count=a_cnt,
                v2_artifact_count=a_cnt,
                v2_audit_count=a_cnt,
            )
            return MigrationResult(
                v1_path=resolved_db,
                v2_path=resolved_db,
                backup_path=None,
                plan=empty_plan,
                verification=empty_ver,
                already_v2=True,
            )

        plan = plan_migration(
            resolved_db,
            quick=quick,
            workers=workers,
            snapshot_dirs=snapshot_dirs,
            mounts=mounts,
        )
        if dry_run:
            dry_ver = MigrationVerificationReport(
                ok=True,
                v1_run_count=plan.run_count,
                v2_run_count=plan.run_count,
                v1_artifact_count=plan.artifact_count,
                v2_artifact_count=plan.artifact_count,
                v2_audit_count=plan.artifact_count,
                manifests_verified=sum(
                    1 for c in plan.classifications if c.manifest_payload is not None
                ),
            )
            return MigrationResult(
                v1_path=resolved_db,
                v2_path=resolved_db,
                backup_path=None,
                plan=plan,
                verification=dry_ver,
                already_v2=False,
            )

        v2_tmp_path = resolved_db.with_name(f"{resolved_db.name}.v2.tmp")
        try:
            apply_migration(
                resolved_db,
                v2_tmp_path,
                plan,
                site=site,
                mounts=mounts,
            )
            verification = verify_migration(resolved_db, v2_tmp_path, plan=plan)
            backup_path = activate_migration(resolved_db, v2_tmp_path)
        finally:
            for cleanup_path in (v2_tmp_path, Path(f"{v2_tmp_path}.wal")):
                try:
                    cleanup_path.unlink(missing_ok=True)
                except OSError:
                    pass

        return MigrationResult(
            v1_path=resolved_db,
            v2_path=resolved_db,
            backup_path=backup_path,
            plan=plan,
            verification=verification,
            already_v2=False,
        )


def undo_migration(
    db_path: Path | str,
    *,
    force: bool = False,
) -> Path:
    """Restore `db_path` from the most recent `.v1-backup-YYYY-MM-DD.duckdb` backup."""
    resolved_db = Path(db_path).expanduser().resolve()
    with _migration_lock_context(resolved_db):
        parent = resolved_db.parent
        pattern = f"{resolved_db.stem}.v1-backup-*{resolved_db.suffix}"
        candidates = sorted(
            parent.glob(pattern),
            key=lambda p: (p.stat().st_mtime_ns, p.name),
            reverse=True,
        )
        if not candidates:
            raise FileNotFoundError(
                f"No v1 backup matching '{pattern}' found in '{parent}'."
            )
        backup_path = candidates[0]

        if (
            resolved_db.exists()
            and not force
            and detect_schema(resolved_db) is SchemaState.V2
        ):
            with duckdb.connect(str(resolved_db), read_only=True) as v2_con:
                v2_runs = {
                    str(r[0])
                    for r in v2_con.execute("SELECT id FROM main.run").fetchall()
                }
            with V1DatabaseReader(backup_path) as v1_reader:
                v1_runs = {r.id for r in v1_reader.iter_runs()}
            new_runs = v2_runs - v1_runs
            if new_runs:
                raise ConsistError(
                    f"Refusing to undo migration on '{resolved_db}': {len(new_runs)} new v2 "
                    f"run(s) ({sorted(new_runs)[:5]}) were recorded since upgrade. "
                    "Pass --force (force=True) to overwrite the v2 database with the v1 backup."
                )

        tmp_restore = resolved_db.with_name(f"{resolved_db.name}.undo.tmp")
        shutil.copy2(backup_path, tmp_restore)
        os.replace(tmp_restore, resolved_db)
        try:
            Path(f"{resolved_db}.wal").unlink(missing_ok=True)
        except OSError:
            pass
        return backup_path


def _resolve_promotion_target(
    con: duckdb.DuckDBPyConnection,
    target: uuid.UUID | str,
) -> tuple[uuid.UUID, Digest, str, str | None, str | None, str | None]:
    """Return `(artifact_id, historical_anchor, driver, v1_hash, table_path, array_path)`."""
    raw_target = str(target).strip()
    art_row: tuple[Any, ...] | None = None

    if raw_target.startswith("legacy-v1:unverified:"):
        art_row = con.execute(
            "SELECT id, anchor, driver FROM main.artifact WHERE anchor = ? "
            "ORDER BY created_at DESC LIMIT 1",
            [raw_target],
        ).fetchone()
        if art_row is None:
            hist_digest = Digest.parse(raw_target)
            return (uuid.uuid4(), hist_digest, "file", hist_digest.hex, None, None)

    if art_row is None:
        try:
            target_uuid = (
                target if isinstance(target, uuid.UUID) else uuid.UUID(raw_target)
            )
            art_row = con.execute(
                "SELECT id, anchor, driver FROM main.artifact WHERE id = ?",
                [target_uuid],
            ).fetchone()
        except ValueError:
            art_row = None

    if art_row is None:
        art_row = con.execute(
            "SELECT id, anchor, driver FROM main.artifact "
            "WHERE key = ? AND anchor LIKE 'legacy-v1:unverified:%' "
            "AND anchor NOT IN (SELECT historical_anchor FROM main.anchor_promotion) "
            "ORDER BY created_at DESC LIMIT 1",
            [raw_target],
        ).fetchone()
        if art_row is None:
            art_row = con.execute(
                "SELECT id, anchor, driver FROM main.artifact "
                "WHERE key = ? AND anchor LIKE 'legacy-v1:unverified:%' "
                "ORDER BY created_at DESC LIMIT 1",
                [raw_target],
            ).fetchone()
        if art_row is None:
            art_row = con.execute(
                "SELECT id, anchor, driver FROM main.artifact "
                "WHERE key = ? ORDER BY created_at DESC LIMIT 1",
                [raw_target],
            ).fetchone()

    if art_row is None:
        raise ConsistError(
            f"Could not find historical artifact matching target {raw_target!r}."
        )

    art_id = (
        art_row[0] if isinstance(art_row[0], uuid.UUID) else uuid.UUID(str(art_row[0]))
    )
    anchor_digest = Digest.parse(str(art_row[1]))
    if anchor_digest.kind is not Kind.HISTORICAL:
        raise ConsistError(
            f"Artifact {art_id} already has a verified anchor ({anchor_digest}); "
            "only historical ('legacy-v1:unverified:*') anchors can be promoted."
        )
    driver = str(art_row[2] or "file")

    audit_row = con.execute(
        "SELECT v1_row_json FROM main.artifact_migration_audit WHERE artifact_id = ?",
        [art_id],
    ).fetchone()
    v1_hash: str | None = None
    table_path: str | None = None
    array_path: str | None = None
    if audit_row is not None and audit_row[0]:
        try:
            parsed_v1 = json.loads(str(audit_row[0]))
            if isinstance(parsed_v1, Mapping):
                if isinstance(parsed_v1.get("hash"), str):
                    v1_hash = parsed_v1["hash"]
                if isinstance(parsed_v1.get("table_path"), str):
                    table_path = parsed_v1["table_path"]
                if isinstance(parsed_v1.get("array_path"), str):
                    array_path = parsed_v1["array_path"]
        except ValueError:
            pass
    if v1_hash is None and _is_64_hex(anchor_digest.hex):
        v1_hash = anchor_digest.hex

    return (art_id, anchor_digest, driver, v1_hash, table_path, array_path)


def promote_historical_artifact(
    db_path_or_ledger: Path | str | StoreLedger,
    target: uuid.UUID | str,
    *,
    path: Path | str,
    asserted: bool = False,
    asserted_by: str | None = None,
    expected_digest: Digest | str | None = None,
    site: str = "local",
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
) -> tuple[Digest, Digest, str]:
    """Promote a `Kind.HISTORICAL` (`legacy-v1:unverified:<hex>`) tombstone to a verified anchor.

    Returns `(historical_anchor, verified_anchor, basis)`.
    """
    local_path = Path(path).expanduser().resolve()
    if not local_path.exists():
        raise FileNotFoundError(
            f"Promotion candidate path does not exist: {local_path}"
        )

    mount_map = _normalize_mounts(mounts)
    parsed_expected: Digest | None = (
        (
            Digest.parse(expected_digest)
            if isinstance(expected_digest, str)
            else expected_digest
        )
        if expected_digest is not None
        else None
    )

    def _run_with_ledger(ledger: StoreLedger) -> tuple[Digest, Digest, str]:
        if mount_map:
            ledger.mounts.update(mount_map)
        effective_mounts = ledger.mounts

        _art_id, hist_anchor, driver, v1_hash, table_path, array_path = (
            _resolve_promotion_target(ledger.connection, target)
        )
        member = table_path or array_path

        if parsed_expected is not None:
            scheme = parsed_expected.scheme
        elif local_path.is_dir():
            scheme = "sha256:manifest-v1"
        elif table_path:
            scheme = "ctd1:sha256"
        elif array_path:
            scheme = "sha256:logical_matrix:v1"
        else:
            scheme = "sha256:file"

        verified_anchor, witness = compute_scheme_digest(
            local_path,
            scheme,
            driver=driver,
            member=member,
            ledger=ledger,
        )

        if parsed_expected is not None:
            if verified_anchor != parsed_expected:
                raise ConsistError(
                    f"Promotion digest mismatch for '{local_path}': expected {parsed_expected}, "
                    f"computed {verified_anchor}."
                )
            basis: Literal[
                "v1_fast_recomputed", "caller_supplied_match", "asserted"
            ] = "caller_supplied_match"
            evidence: dict[str, Any] = {
                "expected_digest": str(parsed_expected),
                "path": str(local_path),
            }
        elif (
            not local_path.is_dir()
            and _is_64_hex(v1_hash)
            and stat_v1_fast_file_hash(local_path)[0] == str(v1_hash).lower()
        ):
            basis = "v1_fast_recomputed"
            evidence = {
                "v1_fast_hash": str(v1_hash).lower(),
                "path": str(local_path),
            }
        elif asserted:
            basis = "asserted"
            evidence = {"path": str(local_path)}
        else:
            raise ConsistError(
                f"Candidate file '{local_path}' does not match the original v1 fast-stat "
                f"hash for {hist_anchor} and no --expected-digest was supplied. "
                "Pass --asserted (asserted=True) to record an asserted location promotion."
            )

        repr_kind = (
            "tree"
            if local_path.is_dir()
            else ("member" if member is not None else "file")
        )
        uri, matched_mount = virtualize_path(local_path, effective_mounts)
        mount_inst_id = (
            matched_mount.resolve_instance_id() if matched_mount is not None else None
        )
        obs_basis: Literal["computed", "declared"] = (
            "declared" if basis == "asserted" else "computed"
        )
        obs = Observation(
            location=Location(uri=uri, member=member),
            identity=Identity(anchor=verified_anchor),
            access_driver=driver,
            witness=witness,
            basis=obs_basis,
            site=site,
            mount_instance_id=mount_inst_id,
            observed_at_ns=time.time_ns(),
        )
        promoted_ok = ledger.record_anchor_promotion(
            hist_anchor,
            verified_anchor,
            basis=basis,
            asserted_by=asserted_by,
            evidence=evidence,
        )
        if not promoted_ok:
            raise ConsistError(
                f"Historical anchor {hist_anchor} is already promoted to a different "
                f"verified anchor and cannot be re-promoted to {verified_anchor}."
            )
        ledger.record_observation(
            obs,
            repr_kind=repr_kind,
            writer="consist.promote",
        )
        return (hist_anchor, verified_anchor, basis)

    if isinstance(db_path_or_ledger, StoreLedger):
        return _run_with_ledger(db_path_or_ledger)

    with StoreLedger(
        db_path_or_ledger, mounts=mount_map, writer="consist.promote"
    ) as led:
        return _run_with_ledger(led)


def open_v2_store(
    db_path: Path | str,
    *,
    read_only: bool = False,
    auto_migrate: bool | str | None = None,
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
    site: str = "local",
) -> StoreLedger:
    """Open a v2 `StoreLedger`, running or refusing v1 migration according to `auto_migrate`."""
    resolved = Path(db_path).expanduser().resolve()
    mode = _resolve_auto_migrate(auto_migrate)
    state = detect_schema(resolved)

    if state in (SchemaState.ABSENT, SchemaState.V2):
        return StoreLedger(resolved, read_only=read_only, mounts=mounts)

    if state is SchemaState.V1:
        if mode == "off" or read_only:
            raise inspect_v1_summary(resolved)
        migrate_v1_to_v2(
            resolved,
            quick=(mode == "quick"),
            mounts=mounts,
            site=site,
        )
        return StoreLedger(resolved, read_only=read_only, mounts=mounts)

    if state is SchemaState.FUTURE:
        raise IncompatibleSchemaError(
            f"Database '{resolved}' uses a newer schema version than this Consist release supports."
        )

    raise ConsistError(
        f"Cannot open database '{resolved}' (schema state: {state.value!r})."
    )


__all__ = [
    "ArtifactClassification",
    "ClassificationBasis",
    "ClassificationOutcome",
    "MigrationPlan",
    "MigrationResult",
    "MigrationVerificationReport",
    "_migration_lock_context",
    "activate_migration",
    "apply_migration",
    "classify_v1_artifact",
    "default_migration_workers",
    "migrate_v1_to_v2",
    "open_v2_store",
    "plan_migration",
    "preflight_check_migration",
    "promote_historical_artifact",
    "undo_migration",
    "verify_migration",
]
