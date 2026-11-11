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
from dataclasses import dataclass, field
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
import warnings

import duckdb

from consist.core.identity import compute_sig_v2
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
    _compute_manifest_v1_and_witness,
    _hash_file_descriptor_pinned,
    _normalize_mounts,
    compute_scheme_digest,
    resolve_uri_to_path,
    virtualize_path,
)
from consist.location import (
    DbTableChangeToken,
    DirTreeStat,
    FileStat,
    Location,
    Mount,
    Observation,
    Witness,
)
from consist.meta import strip_system_meta
from consist.migrations.v1.fasthash import (
    stat_v1_fast_directory_hash,
    stat_v1_fast_file_hash,
)
from consist.migrations.v1.reader import (
    V1ArtifactRecord,
    V1ArtifactSchemaBundle,
    V1BindingInvocationRecord,
    V1ConfigFacetRecord,
    V1CouplerSnapshotRecord,
    V1DatabaseReader,
    V1DirEntryRecord,
    V1DltTableInfo,
    V1LinkRecord,
    V1RunConfigKvRecord,
    V1RunRecord,
    V1UserSchemasDDL,
    unflatten_facet_key_path,
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
    dlt_schema: str | None = None
    dlt_table: str | None = None
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
    config_facets: Mapping[str, V1ConfigFacetRecord] = field(default_factory=dict)
    run_config_kv: Mapping[str, tuple[V1RunConfigKvRecord, ...]] = field(
        default_factory=dict
    )
    coupler_snapshots: tuple[V1CouplerSnapshotRecord, ...] = ()
    binding_invocations: tuple[V1BindingInvocationRecord, ...] = ()
    artifact_schemas: V1ArtifactSchemaBundle | None = None
    user_schemas_ddl: V1UserSchemasDDL | None = None


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

    for root in search_roots:
        if rel_candidate is not None and not rel_candidate.is_absolute():
            joined = root / rel_candidate
            if joined.exists():
                return joined.resolve()

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


def _is_ingested_v1_artifact(art: V1ArtifactRecord) -> bool:
    return (
        art.driver.strip().lower() in ("dlt", "duckdb", "table")
        or art.container_uri.strip().lower().startswith("db://")
        or bool(art.meta.get("is_ingested"))
    )


def _match_ingested_dlt_table(
    art: V1ArtifactRecord,
    dlt_tables: Sequence[V1DltTableInfo],
) -> V1DltTableInfo | None:
    if not dlt_tables:
        return None
    cand_names: list[str] = []
    for meta_key in ("dlt_table_name", "_dlt_table_name"):
        dlt_meta_name = art.meta.get(meta_key)
        if isinstance(dlt_meta_name, str) and dlt_meta_name.strip():
            cand_names.append(dlt_meta_name.strip())
    if art.container_uri.strip().lower().startswith("db://"):
        tail = art.container_uri.strip()[5:].strip("/")
        if "?" in tail:
            tail = tail.split("?", 1)[0]
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
    for cand in cand_names:
        cand_lower = cand.lower()
        for info in non_internal:
            if info.table_name.lower() == cand_lower:
                return info
    return None


def _is_nested_duckdb_type(dtype_str: str) -> bool:
    upper_t = dtype_str.strip().upper()
    if "[" in upper_t or "]" in upper_t:
        return True
    return any(tok in upper_t for tok in ("LIST", "STRUCT", "MAP", "UNION"))


def _is_ieee_float_duckdb_type(dtype_str: str) -> bool:
    upper_t = dtype_str.strip().upper()
    return upper_t in ("FLOAT", "REAL", "DOUBLE", "FLOAT4", "FLOAT8")


def _stream_ctd1_hex_from_duckdb(
    con: duckdb.DuckDBPyConnection,
    *,
    q_schema: str,
    q_table: str,
    select_cols: Sequence[str],
    column_types: Mapping[str, str],
    where_sql: str,
    params: Sequence[Any],
    rows_per_batch: int = 65_536,
) -> str | None:
    import pyarrow as pa

    for col in select_cols:
        if _is_nested_duckdb_type(column_types.get(col, "")):
            return None

    cols_sql = ", ".join(_quote_ident(c) for c in select_cols)
    order_parts: list[str] = []
    for c in select_cols:
        c_ident = _quote_ident(c)
        if _is_ieee_float_duckdb_type(column_types.get(c, "")):
            order_parts.append(
                f"CASE WHEN {c_ident} IS NULL THEN 0 WHEN isnan({c_ident}) THEN 1 ELSE 2 END ASC, {c_ident} ASC"
            )
        else:
            order_parts.append(f"{c_ident} ASC NULLS FIRST")
    order_sql = ", ".join(order_parts)

    cur = con.cursor()
    try:
        cur.execute(
            f"SELECT {cols_sql} FROM {q_schema}.{q_table}{where_sql} ORDER BY {order_sql}",
            list(params),
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            batch_reader = cur.fetch_record_batch(rows_per_batch=rows_per_batch)
        return compute_ctd1_digest(
            batch_reader,
            columns=select_cols,
            presorted=True,
        ).hex
    except (duckdb.Error, pa.ArrowException, ValueError, TypeError):
        return None
    finally:
        cur.close()


def _try_classify_dlt_hot_table(
    art: V1ArtifactRecord,
    *,
    con: duckdb.DuckDBPyConnection,
    dlt_tables: Sequence[V1DltTableInfo],
    candidate_path: Path | None = None,
    ingested_table_counts_per_run: Mapping[tuple[str, str], int] | None = None,
    v1_row_json: str,
) -> ArtifactClassification | None:
    import pyarrow as pa

    if not dlt_tables:
        return None

    try:
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

        matched_info = _match_ingested_dlt_table(art, dlt_tables)
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

        where_sql = ""
        params: list[Any] = []
        dlt_member = matched_info.table_name
        matched_rows = 0

        if matched_info.has_consist_artifact_id:
            cand_where = " WHERE CAST(consist_artifact_id AS VARCHAR) = ?"
            cand_params = [str(art.id)]
            cnt_row = con.execute(
                f"SELECT COUNT(*) FROM {q_schema}.{q_table}{cand_where}",
                cand_params,
            ).fetchone()
            cnt = (
                int(cnt_row[0]) if cnt_row is not None and cnt_row[0] is not None else 0
            )
            if cnt > 0:
                matched_rows = cnt
                where_sql = cand_where
                params = cand_params
                dlt_member = f"{matched_info.table_name}?consist_artifact_id={art.id}"

        if (
            matched_rows == 0
            and (is_explicit_dlt or is_ingested)
            and matched_info.has_consist_run_id
            and art.run_id is not None
        ):
            run_tbl_count = (
                ingested_table_counts_per_run.get(
                    (art.run_id, matched_info.table_name.lower()),
                    1,
                )
                if ingested_table_counts_per_run is not None
                else 1
            )
            if run_tbl_count == 1:
                cand_where = (
                    " WHERE CAST(consist_run_id AS VARCHAR) = ? AND consist_artifact_id IS NULL"
                    if matched_info.has_consist_artifact_id
                    else " WHERE CAST(consist_run_id AS VARCHAR) = ?"
                )
                cand_params = [art.run_id]
                cnt_row = con.execute(
                    f"SELECT COUNT(*) FROM {q_schema}.{q_table}{cand_where}",
                    cand_params,
                ).fetchone()
                cnt = (
                    int(cnt_row[0])
                    if cnt_row is not None and cnt_row[0] is not None
                    else 0
                )
                if cnt > 0:
                    matched_rows = cnt
                    where_sql = cand_where
                    params = cand_params
                    dlt_member = (
                        f"{matched_info.table_name}?consist_run_id={art.run_id}"
                    )

        if matched_rows == 0:
            return None

        hex_val = _stream_ctd1_hex_from_duckdb(
            con,
            q_schema=q_schema,
            q_table=q_table,
            select_cols=select_cols,
            column_types=matched_info.columns,
            where_sql=where_sql,
            params=params,
            rows_per_batch=65_536,
        )
        if hex_val is None:
            return None

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
            witness=DbTableChangeToken(load_id=load_id, row_count=matched_rows),
            manifest_payload=None,
            dlt_member=dlt_member,
            dlt_schema=matched_info.schema_name,
            dlt_table=matched_info.table_name,
            v1_row_json=v1_row_json,
        )
    except (duckdb.Error, pa.ArrowException, ValueError, TypeError):
        return None


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
        recorded_ns = (
            int(art.created_at.timestamp() * 1_000_000_000)
            if art.created_at is not None
            else None
        )
        if candidate_path.is_dir():
            if effective_hash is None:
                return ArtifactClassification(
                    artifact_id=art.id,
                    outcome="historical",
                    basis="unhashed_v1",
                    anchor=Digest(
                        "legacy-v1:unverified", _fallback_historical_hex(art)
                    ),
                    repr_kind="historical",
                    resolved_path=None,
                    witness=None,
                    v1_row_json=v1_row_json,
                )
            lower_hash = effective_hash.lower()
            raw_v1_hash = (art.hash or "").strip()
            try:
                fast_dir = stat_v1_fast_directory_hash(
                    candidate_path,
                    v1_recorded_ns=recorded_ns,
                )
                matched_fast_dir = (
                    fast_dir.legacy_fast_hex == lower_hash
                    or fast_dir.stat_v2_observation == raw_v1_hash
                )
                if matched_fast_dir and fast_dir.is_racy:
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
                anchor_dir, body_bytes, entry_count, total_bytes, dir_stat = (
                    _compute_manifest_v1_and_witness(
                        candidate_path,
                        max_workers=max(1, max_workers),
                    )
                )
                if matched_fast_dir or lower_hash == anchor_dir.hex:
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
                current_fast, _st, is_racy = stat_v1_fast_file_hash(
                    candidate_path,
                    v1_recorded_ns=recorded_ns,
                )
                if current_fast == lower_hash:
                    if is_racy:
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
    ingested_table_counts_per_run: Mapping[tuple[str, str], int] | None = None,
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
        ingested_table_counts_per_run=ingested_table_counts_per_run,
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
    ingested_table_counts_per_run: Mapping[tuple[str, str], int] | None = None,
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
        ingested_table_counts_per_run=ingested_table_counts_per_run,
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
        config_facets = reader.read_config_facets()
        run_config_kv = {
            rid: tuple(items) for rid, items in reader.read_run_config_kv().items()
        }
        coupler_snapshots = tuple(reader.read_coupler_snapshots())
        binding_invocations = tuple(reader.read_binding_invocations())
        artifact_schemas = reader.read_artifact_schemas()
        user_schemas_ddl = reader.read_user_schema_tables_ddl()

        ingested_counts_per_run: Counter[tuple[str, str]] = Counter()
        for art_item in artifacts:
            if art_item.run_id is not None and _is_ingested_v1_artifact(art_item):
                matched_t = _match_ingested_dlt_table(art_item, dlt_tables)
                if matched_t is not None:
                    ingested_counts_per_run[
                        (art_item.run_id, matched_t.table_name.lower())
                    ] += 1

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
                ingested_table_counts_per_run=ingested_counts_per_run,
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
            config_facets=config_facets,
            run_config_kv=run_config_kv,
            coupler_snapshots=coupler_snapshots,
            binding_invocations=binding_invocations,
            artifact_schemas=artifact_schemas,
            user_schemas_ddl=user_schemas_ddl,
        )


def _lex_sql_ddl(sql: str) -> list[tuple[str, str, str]]:
    """Lex SQL DDL into `(kind, raw_text, normalized_val)` tokens."""
    tokens: list[tuple[str, str, str]] = []
    n = len(sql)
    i = 0
    while i < n:
        ch = sql[i]
        if ch.isspace():
            start = i
            while i < n and sql[i].isspace():
                i += 1
            tokens.append(("ws", sql[start:i], ""))
        elif ch == "-" and i + 1 < n and sql[i + 1] == "-":
            start = i
            i += 2
            while i < n and sql[i] != "\n":
                i += 1
            if i < n:
                i += 1
            tokens.append(("comment", sql[start:i], ""))
        elif ch == "/" and i + 1 < n and sql[i + 1] == "*":
            start = i
            i += 2
            while i < n and not (sql[i] == "*" and i + 1 < n and sql[i + 1] == "/"):
                i += 1
            i = min(n, i + 2)
            tokens.append(("comment", sql[start:i], ""))
        elif ch == "'":
            start = i
            i += 1
            inner: list[str] = []
            while i < n:
                if sql[i] == "'":
                    if i + 1 < n and sql[i + 1] == "'":
                        inner.append("'")
                        i += 2
                    else:
                        i += 1
                        break
                else:
                    inner.append(sql[i])
                    i += 1
            tokens.append(("str", sql[start:i], "".join(inner)))
        elif ch == '"':
            start = i
            i += 1
            inner_q: list[str] = []
            while i < n:
                if sql[i] == '"':
                    if i + 1 < n and sql[i + 1] == '"':
                        inner_q.append('"')
                        i += 2
                    else:
                        i += 1
                        break
                else:
                    inner_q.append(sql[i])
                    i += 1
            tokens.append(("ident", sql[start:i], "".join(inner_q)))
        elif ch.isalpha() or ch == "_":
            start = i
            i += 1
            while i < n and (sql[i].isalnum() or sql[i] in ("_", "$")):
                i += 1
            word = sql[start:i]
            tokens.append(("ident", word, word))
        elif ch == ".":
            tokens.append(("dot", ".", ""))
            i += 1
        elif ch == "(":
            tokens.append(("lparen", "(", ""))
            i += 1
        else:
            tokens.append(("other", ch, ""))
            i += 1
    return tokens


def _strip_catalog_prefix_from_ddl(
    sql: str,
    *,
    catalog_names: Sequence[str],
    known_schemas: Sequence[str],
) -> str:
    catalog_set = {c.lower() for c in catalog_names if c}
    schema_set = {s.lower() for s in known_schemas if s}
    if not catalog_set or not schema_set or not sql:
        return sql

    tokens = _lex_sql_ddl(sql)
    total = len(tokens)

    def _next_sig(idx: int) -> int | None:
        j = idx
        while j < total and tokens[j][0] in ("ws", "comment"):
            j += 1
        return j if j < total else None

    def _prev_sig(idx: int) -> int | None:
        j = idx
        while j >= 0 and tokens[j][0] in ("ws", "comment"):
            j -= 1
        return j if j >= 0 else None

    out: list[str] = []
    i = 0
    while i < total:
        kind, raw, val = tokens[i]
        if kind == "ident" and val.lower() in catalog_set:
            j1 = _next_sig(i + 1)
            j2 = (
                _next_sig(j1 + 1) if j1 is not None and tokens[j1][0] == "dot" else None
            )
            j3 = (
                _next_sig(j2 + 1)
                if j2 is not None
                and tokens[j2][0] == "ident"
                and tokens[j2][2].lower() in schema_set
                else None
            )
            j4 = (
                _next_sig(j3 + 1) if j3 is not None and tokens[j3][0] == "dot" else None
            )
            if j4 is not None and tokens[j4][0] == "ident":
                assert j2 is not None
                i = j2
                continue
        elif kind == "str":
            p1 = _prev_sig(i - 1)
            p2 = (
                _prev_sig(p1 - 1)
                if p1 is not None and tokens[p1][0] == "lparen"
                else None
            )
            if (
                p2 is not None
                and tokens[p2][0] == "ident"
                and tokens[p2][2].lower() == "nextval"
            ):
                stripped_seq = _strip_catalog_prefix_from_ddl(
                    val,
                    catalog_names=catalog_names,
                    known_schemas=known_schemas,
                )
                if stripped_seq != val:
                    escaped_seq = stripped_seq.replace("'", "''")
                    out.append(f"'{escaped_seq}'")
                    i += 1
                    continue
        out.append(raw)
        i += 1
    return "".join(out)


def _resolve_ingested_db_location(
    art: V1ArtifactRecord,
    cls: ArtifactClassification,
    dlt_tables: Sequence[V1DltTableInfo],
) -> tuple[str, str | None] | None:
    if cls.dlt_table is not None:
        schema_name = cls.dlt_schema or "global_tables"
        return (
            f"db://{schema_name}/{cls.dlt_table}",
            cls.dlt_member or cls.dlt_table,
        )
    if not _is_ingested_v1_artifact(art):
        return None
    matched = _match_ingested_dlt_table(art, dlt_tables)
    if matched is not None:
        member = (
            f"{matched.table_name}?consist_artifact_id={art.id}"
            if matched.has_consist_artifact_id
            else f"{matched.table_name}?artifact_id={art.id}"
        )
        return (f"db://{matched.schema_name}/{matched.table_name}", member)

    raw_tbl = art.meta.get("dlt_table_name") or art.meta.get("_dlt_table_name")
    if isinstance(raw_tbl, str) and raw_tbl.strip():
        tbl = raw_tbl.strip()
        return (f"db://global_tables/{tbl}", f"{tbl}?artifact_id={art.id}")

    raw_uri = art.container_uri.strip()
    if raw_uri.lower().startswith("db://"):
        tail = raw_uri[5:].split("?", 1)[0].strip("/")
        if "/" in tail:
            schema_part, tbl_part = tail.split("/", 1)
            return (
                f"db://{schema_part}/{tbl_part}",
                f"{tbl_part}?artifact_id={art.id}",
            )
        if "." in tail:
            schema_part, tbl_part = tail.split(".", 1)
            return (
                f"db://{schema_part}/{tbl_part}",
                f"{tbl_part}?artifact_id={art.id}",
            )
        if tail:
            return (f"db://global_tables/{tail}", f"{tail}?artifact_id={art.id}")

    if art.key.strip():
        k = art.key.strip()
        return (f"db://global_tables/{k}", f"{k}?artifact_id={art.id}")
    return None


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
        config_facets: Mapping[str, V1ConfigFacetRecord] = plan.config_facets
        run_config_kv: Mapping[str, tuple[V1RunConfigKvRecord, ...]] = (
            plan.run_config_kv
        )
        coupler_snapshots: Sequence[V1CouplerSnapshotRecord] = plan.coupler_snapshots
        binding_invocations: Sequence[V1BindingInvocationRecord] = (
            plan.binding_invocations
        )
        artifact_schemas: V1ArtifactSchemaBundle | None = plan.artifact_schemas
        user_schemas_ddl: V1UserSchemasDDL | None = plan.user_schemas_ddl
    else:
        with V1DatabaseReader(resolved_v1) as reader:
            runs = reader.read_runs()
            artifacts = reader.read_artifacts()
            links = reader.read_links()
            config_facets = reader.read_config_facets()
            run_config_kv = reader.read_run_config_kv()
            coupler_snapshots = reader.read_coupler_snapshots()
            binding_invocations = reader.read_binding_invocations()
            artifact_schemas = reader.read_artifact_schemas()
            user_schemas_ddl = reader.read_user_schema_tables_ddl()

    if user_schemas_ddl is None and plan.dlt_tables and resolved_v1.exists():
        with V1DatabaseReader(resolved_v1) as reader:
            user_schemas_ddl = reader.read_user_schema_tables_ddl()

    cls_by_art_id: dict[uuid.UUID, ArtifactClassification] = {
        c.artifact_id: c for c in plan.classifications
    }
    art_by_id: dict[uuid.UUID, V1ArtifactRecord] = {a.id: a for a in artifacts}

    with StoreLedger(resolved_v2, mounts=mount_map, writer="consist.migrate") as ledger:
        con = ledger.connection

        # 1. Replay user schemas (global_tables / _dlt_*): sequences -> FK-ordered tables -> row copy -> indexes
        has_user_ddl = user_schemas_ddl is not None and bool(
            user_schemas_ddl.schemas
            or user_schemas_ddl.sequences
            or user_schemas_ddl.tables
        )
        if has_user_ddl or plan.dlt_tables:
            escaped_v1 = str(resolved_v1).replace("'", "''")
            con.execute(f"ATTACH '{escaped_v1}' AS v1_src (READ_ONLY)")
            try:
                known_schemas: list[str] = []
                if user_schemas_ddl is not None:
                    for s_name in user_schemas_ddl.schemas:
                        if s_name not in known_schemas:
                            known_schemas.append(s_name)
                for t_info in plan.dlt_tables:
                    if t_info.schema_name not in known_schemas:
                        known_schemas.append(t_info.schema_name)

                raw_prefixes = [
                    "v1_src",
                    resolved_v1.stem,
                    resolved_v1.name.split(".", 1)[0],
                    user_schemas_ddl.catalog_name
                    if user_schemas_ddl is not None
                    else "",
                ]
                catalog_prefixes = tuple(dict.fromkeys(p for p in raw_prefixes if p))
                for s_name in known_schemas:
                    con.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(s_name)}")

                if user_schemas_ddl is not None:
                    for seq_ddl in user_schemas_ddl.sequences:
                        if seq_ddl.sql.strip():
                            seq_sql = _strip_catalog_prefix_from_ddl(
                                seq_ddl.sql,
                                catalog_names=catalog_prefixes,
                                known_schemas=known_schemas,
                            )
                            con.execute(seq_sql)

                copied_tables: set[tuple[str, str]] = set()
                if user_schemas_ddl is not None:
                    for tbl_ddl in user_schemas_ddl.tables:
                        q_schema = _quote_ident(tbl_ddl.schema_name)
                        q_table = _quote_ident(tbl_ddl.table_name)
                        if tbl_ddl.sql.strip():
                            tbl_sql = _strip_catalog_prefix_from_ddl(
                                tbl_ddl.sql,
                                catalog_names=catalog_prefixes,
                                known_schemas=known_schemas,
                            )
                            con.execute(tbl_sql)
                        else:
                            con.execute(
                                f"CREATE TABLE {q_schema}.{q_table} AS "
                                f"SELECT * FROM v1_src.{q_schema}.{q_table} WHERE 1=0"
                            )
                        con.execute(
                            f"INSERT INTO {q_schema}.{q_table} "
                            f"SELECT * FROM v1_src.{q_schema}.{q_table}"
                        )
                        copied_tables.add((tbl_ddl.schema_name, tbl_ddl.table_name))

                for t_info in plan.dlt_tables:
                    if (t_info.schema_name, t_info.table_name) in copied_tables:
                        continue
                    q_schema = _quote_ident(t_info.schema_name)
                    q_table = _quote_ident(t_info.table_name)
                    con.execute(
                        f"CREATE TABLE {q_schema}.{q_table} AS "
                        f"SELECT * FROM v1_src.{q_schema}.{q_table}"
                    )
                    copied_tables.add((t_info.schema_name, t_info.table_name))

                if user_schemas_ddl is not None:
                    for idx_ddl in user_schemas_ddl.indexes:
                        if idx_ddl.sql.strip():
                            idx_sql = _strip_catalog_prefix_from_ddl(
                                idx_ddl.sql,
                                catalog_names=catalog_prefixes,
                                known_schemas=known_schemas,
                            )
                            con.execute(idx_sql)
            finally:
                con.execute("DETACH v1_src")

        now_utc = datetime.now(timezone.utc)
        now_ns = time.time_ns()

        with ledger.transaction() as tx_cur:
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

                cleaned_art_meta = dict(strip_system_meta(art.meta))
                if art.facet:
                    cleaned_art_meta["facet"] = art.facet
                if art.facet_id and "artifact_facet_id" not in cleaned_art_meta:
                    cleaned_art_meta["artifact_facet_id"] = art.facet_id
                if art.facet_namespace and "facet_namespace" not in cleaned_art_meta:
                    cleaned_art_meta["facet_namespace"] = art.facet_namespace
                if (
                    art.facet_schema_version
                    and "facet_schema_version" not in cleaned_art_meta
                ):
                    cleaned_art_meta["facet_schema_version"] = art.facet_schema_version
                if art.parent_artifact_id is not None:
                    cleaned_art_meta["parent_artifact_id"] = str(art.parent_artifact_id)

                v2_meta = json.loads(json.dumps(cleaned_art_meta, default=str))
                ledger.record_artifact(
                    artifact_id=art.id,
                    key=art.key,
                    driver=art.driver,
                    anchor=cls.anchor,
                    run_id=art.run_id,
                    parent_artifact_id=art.parent_artifact_id,
                    schema_id=art.schema_id,
                    meta=v2_meta,
                    created_at=art.created_at or now_utc,
                )

                tx_cur.execute(
                    "INSERT INTO main.artifact_migration_audit "
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
                ingested_db_loc = _resolve_ingested_db_location(
                    art,
                    cls,
                    plan.dlt_tables,
                )

                if cls.outcome == "verified" and cls.dlt_member is not None:
                    db_uri = (
                        ingested_db_loc[0]
                        if ingested_db_loc is not None
                        else f"db://global_tables/{cls.dlt_member.split('?', 1)[0]}"
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
                    if (
                        art.container_uri.strip()
                        and "://" in art.container_uri
                        and not art.container_uri.strip().lower().startswith("db://")
                    ):
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
                    if ingested_db_loc is not None:
                        ledger.record_observation(
                            Observation(
                                location=Location(
                                    uri=ingested_db_loc[0],
                                    member=ingested_db_loc[1],
                                ),
                                identity=identity,
                                access_driver=art.driver or "dlt",
                                witness=None,
                                basis="declared",
                                site=site,
                                observed_at_ns=obs_ts_ns + 2,
                            ),
                            repr_kind=None,
                            writer="consist.migrate",
                        )
                else:
                    raw_uri = art.container_uri.strip()
                    if raw_uri and not raw_uri.lower().startswith("db://"):
                        obs_id = ledger.record_observation(
                            Observation(
                                location=Location(
                                    uri=raw_uri,
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
                    if ingested_db_loc is not None:
                        ledger.record_observation(
                            Observation(
                                location=Location(
                                    uri=ingested_db_loc[0],
                                    member=ingested_db_loc[1],
                                ),
                                identity=identity,
                                access_driver=art.driver or "dlt",
                                witness=None,
                                basis="declared",
                                site=site,
                                observed_at_ns=obs_ts_ns + 2,
                            ),
                            repr_kind=None,
                            writer="consist.migrate",
                        )
                    elif raw_uri and raw_uri.lower().startswith("db://"):
                        ledger.record_observation(
                            Observation(
                                location=Location(
                                    uri=raw_uri,
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

                cleaned_run_meta = {
                    str(k): v
                    for k, v in run.meta.items()
                    if not str(k).startswith("_dlt_")
                }
                if run.stage is not None:
                    cleaned_run_meta.setdefault("stage", run.stage)
                if run.phase is not None:
                    cleaned_run_meta.setdefault("phase", run.phase)
                if run.signature is not None and sig_val is None:
                    cleaned_run_meta.setdefault("v1_signature", run.signature)

                kv_rows_for_run = run_config_kv.get(run.id, ())
                raw_cf_id = (
                    cleaned_run_meta.get("config_facet_id")
                    or cleaned_run_meta.get("facet_id")
                    or next(
                        (
                            kv.facet_id
                            for kv in kv_rows_for_run
                            if kv.facet_id is not None
                        ),
                        None,
                    )
                )
                cf_id = (
                    str(raw_cf_id).strip()
                    if raw_cf_id is not None and str(raw_cf_id).strip()
                    else None
                )
                cf_rec = config_facets.get(cf_id) if cf_id is not None else None

                merged_run_facet: dict[str, Any] = {}
                if cf_rec is not None and cf_rec.facet_json:
                    merged_run_facet.update(cf_rec.facet_json)
                if isinstance(cleaned_run_meta.get("facet"), Mapping):
                    merged_run_facet.update(cleaned_run_meta["facet"])
                if run.facet is not None:
                    merged_run_facet.update(run.facet)
                for kv_rec in kv_rows_for_run:
                    unflatten_facet_key_path(
                        merged_run_facet,
                        kv_rec.key,
                        kv_rec.value,
                    )

                if merged_run_facet:
                    cleaned_run_meta["facet"] = merged_run_facet
                if cf_id is not None:
                    cleaned_run_meta.setdefault("config_facet_id", cf_id)
                if cf_rec is not None:
                    if cf_rec.namespace:
                        cleaned_run_meta.setdefault(
                            "facet_namespace",
                            cf_rec.namespace,
                        )
                    if cf_rec.schema_version:
                        cleaned_run_meta.setdefault(
                            "facet_schema_version",
                            cf_rec.schema_version,
                        )
                    if cf_rec.schema_name:
                        cleaned_run_meta.setdefault(
                            "config_facet_schema_name",
                            cf_rec.schema_name,
                        )
                elif kv_rows_for_run:
                    first_ns = next(
                        (
                            kv.namespace
                            for kv in kv_rows_for_run
                            if kv.namespace is not None and kv.namespace
                        ),
                        None,
                    )
                    if first_ns is not None:
                        cleaned_run_meta.setdefault("facet_namespace", first_ns)

                v2_run_meta = json.loads(json.dumps(cleaned_run_meta, default=str))
                ledger.record_run(
                    run_id=run.id,
                    model_name=run.model_name,
                    status=run.status,
                    cache_status=cache_status,
                    description=run.description,
                    year=run.year,
                    iteration=run.iteration,
                    stage=run.stage,
                    phase=run.phase,
                    parent_run_id=run.parent_run_id,
                    code_hash=run.git_hash,
                    git_hash=run.git_hash,
                    config_hash=run.config_hash,
                    input_hash=run.input_hash,
                    signature=sig_val,
                    signature_scheme=sig_scheme,
                    tags=run.tags,
                    meta=v2_run_meta,
                    started_at=run.started_at,
                    ended_at=run.ended_at,
                    created_at=run.created_at or now_utc,
                    updated_at=run.updated_at,
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
                        "input"
                        if raw_dir == "input"
                        else ("depends_on" if raw_dir == "depends_on" else "output")
                    )
                    _insert_binding(norm_dir, art_id)
                    art_rec = art_by_id[art_id]
                    if norm_dir in ("input", "depends_on") and art_rec.run_id == run_id:
                        _insert_binding("output", art_id)

            # 5. Copy operational tables (run_coupler_snapshot, run_binding_invocation, artifact_schema*)
            if coupler_snapshots:
                tx_cur.executemany(
                    "INSERT INTO main.run_coupler_snapshot ("
                    "run_id, phase, role_key, scenario_run_id, step_ordinal, "
                    "artifact_id, producer_run_id, updated_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT (run_id, phase, role_key) DO UPDATE SET "
                    "scenario_run_id = EXCLUDED.scenario_run_id, "
                    "step_ordinal = EXCLUDED.step_ordinal, "
                    "artifact_id = EXCLUDED.artifact_id, "
                    "producer_run_id = EXCLUDED.producer_run_id, "
                    "updated_at = EXCLUDED.updated_at",
                    [
                        [
                            snap.run_id,
                            snap.phase,
                            snap.role_key,
                            snap.scenario_run_id,
                            snap.step_ordinal,
                            snap.artifact_id,
                            snap.producer_run_id,
                            snap.updated_at or now_utc,
                        ]
                        for snap in coupler_snapshots
                    ],
                )

            if binding_invocations:
                tx_cur.executemany(
                    "INSERT INTO main.run_binding_invocation ("
                    "id, requested_run_id, execution_run_id, cache_source_run_id, "
                    "cache_outcome, binding_json, created_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT (id) DO UPDATE SET "
                    "requested_run_id = EXCLUDED.requested_run_id, "
                    "execution_run_id = EXCLUDED.execution_run_id, "
                    "cache_source_run_id = EXCLUDED.cache_source_run_id, "
                    "cache_outcome = EXCLUDED.cache_outcome, "
                    "binding_json = EXCLUDED.binding_json, "
                    "created_at = EXCLUDED.created_at",
                    [
                        [
                            inv.id,
                            inv.requested_run_id,
                            inv.execution_run_id,
                            inv.cache_source_run_id,
                            inv.cache_outcome,
                            inv.binding_json,
                            inv.created_at or now_utc,
                        ]
                        for inv in binding_invocations
                    ],
                )

            if artifact_schemas is not None:
                if artifact_schemas.schemas:
                    tx_cur.executemany(
                        "INSERT INTO main.artifact_schema ("
                        "id, profile_version, summary_json, profile_json, created_at"
                        ") VALUES (?, ?, ?, ?, ?) "
                        "ON CONFLICT (id) DO UPDATE SET "
                        "profile_version = EXCLUDED.profile_version, "
                        "summary_json = EXCLUDED.summary_json, "
                        "profile_json = EXCLUDED.profile_json, "
                        "created_at = EXCLUDED.created_at",
                        [
                            [
                                sch_rec.id,
                                sch_rec.profile_version,
                                json.dumps(
                                    sch_rec.summary_json,
                                    sort_keys=True,
                                    separators=(",", ":"),
                                ),
                                (
                                    json.dumps(
                                        sch_rec.profile_json,
                                        sort_keys=True,
                                        separators=(",", ":"),
                                    )
                                    if sch_rec.profile_json is not None
                                    else None
                                ),
                                sch_rec.created_at or now_utc,
                            ]
                            for sch_rec in artifact_schemas.schemas
                        ],
                    )
                if artifact_schemas.fields:
                    tx_cur.executemany(
                        "INSERT INTO main.artifact_schema_field ("
                        "schema_id, ordinal_position, name, logical_type, nullable, "
                        "stats_json, is_enum, enum_values_json"
                        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                        "ON CONFLICT (schema_id, name) DO UPDATE SET "
                        "ordinal_position = EXCLUDED.ordinal_position, "
                        "logical_type = EXCLUDED.logical_type, "
                        "nullable = EXCLUDED.nullable, "
                        "stats_json = EXCLUDED.stats_json, "
                        "is_enum = EXCLUDED.is_enum, "
                        "enum_values_json = EXCLUDED.enum_values_json",
                        [
                            [
                                fld_rec.schema_id,
                                fld_rec.ordinal_position,
                                fld_rec.name,
                                fld_rec.logical_type,
                                fld_rec.nullable,
                                (
                                    json.dumps(
                                        fld_rec.stats_json,
                                        sort_keys=True,
                                        separators=(",", ":"),
                                    )
                                    if fld_rec.stats_json is not None
                                    else None
                                ),
                                fld_rec.is_enum,
                                (
                                    json.dumps(
                                        fld_rec.enum_values_json,
                                        separators=(",", ":"),
                                    )
                                    if fld_rec.enum_values_json is not None
                                    else None
                                ),
                            ]
                            for fld_rec in artifact_schemas.fields
                        ],
                    )
                if artifact_schemas.observations:
                    tx_cur.executemany(
                        "INSERT INTO main.artifact_schema_observation ("
                        "id, artifact_id, schema_id, run_id, source, sample_rows, observed_at"
                        ") VALUES (?, ?, ?, ?, ?, ?, ?) "
                        "ON CONFLICT (id) DO UPDATE SET "
                        "artifact_id = EXCLUDED.artifact_id, "
                        "schema_id = EXCLUDED.schema_id, "
                        "run_id = EXCLUDED.run_id, "
                        "source = EXCLUDED.source, "
                        "sample_rows = EXCLUDED.sample_rows, "
                        "observed_at = EXCLUDED.observed_at",
                        [
                            [
                                obs_rec.id,
                                obs_rec.artifact_id,
                                obs_rec.schema_id,
                                obs_rec.run_id,
                                obs_rec.source,
                                obs_rec.sample_rows,
                                obs_rec.observed_at or now_utc,
                            ]
                            for obs_rec in artifact_schemas.observations
                        ],
                    )
                if artifact_schemas.relations:
                    tx_cur.executemany(
                        "INSERT INTO main.artifact_schema_relation ("
                        "schema_id, from_field, to_table, to_field, relationship_type, cardinality"
                        ") VALUES (?, ?, ?, ?, ?, ?) "
                        "ON CONFLICT (schema_id, from_field) DO UPDATE SET "
                        "to_table = EXCLUDED.to_table, "
                        "to_field = EXCLUDED.to_field, "
                        "relationship_type = EXCLUDED.relationship_type, "
                        "cardinality = EXCLUDED.cardinality",
                        [
                            [
                                rel_rec.schema_id,
                                rel_rec.from_field,
                                rel_rec.to_table,
                                rel_rec.to_field,
                                rel_rec.relationship_type,
                                rel_rec.cardinality,
                            ]
                            for rel_rec in artifact_schemas.relations
                        ],
                    )


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
                    "LEFT JOIN main.anchor_promotion ap "
                    "ON ap.historical_anchor = rb.anchor "
                    "AND ap.basis IN ('v1_fast_recomputed', 'caller_supplied_match') "
                    "WHERE r.cache_status = 'live' "
                    "AND COALESCE(ap.verified_anchor, rb.anchor) LIKE 'legacy-v1:unverified:%'"
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
    """Checkpoint `v1_path`, copy `.v1-backup-YYYY-MM-DD.duckdb`, unlink residual WAL, and swap `v2_tmp_path`."""
    resolved_v1 = Path(v1_path).expanduser().resolve()
    resolved_tmp = Path(v2_tmp_path).expanduser().resolve()
    if resolved_v1.exists():
        try:
            with duckdb.connect(str(resolved_v1)) as v1_con:
                v1_con.execute("CHECKPOINT")
        except duckdb.Error:
            pass
    backup_path = _choose_backup_path(resolved_v1)
    shutil.copy2(resolved_v1, backup_path)
    for wal_candidate in (Path(f"{resolved_v1}.wal"), Path(f"{resolved_tmp}.wal")):
        try:
            wal_candidate.unlink(missing_ok=True)
        except OSError:
            pass
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


class PromotionError(ConsistError, ValueError):
    """Raised when historical artifact or run promotion fails validation."""


@dataclass(frozen=True, slots=True)
class _PromotionTargetInfo:
    artifact_id: uuid.UUID
    key: str
    historical_anchor: Digest
    driver: str
    v1_hash: str | None
    table_path: str | None
    array_path: str | None
    audit_basis: str | None
    v1_container_uri: str | None


def _inspect_artifact_promotion_info(
    cur: duckdb.DuckDBPyConnection,
    art_row: tuple[Any, ...],
) -> _PromotionTargetInfo:
    art_id = (
        art_row[0] if isinstance(art_row[0], uuid.UUID) else uuid.UUID(str(art_row[0]))
    )
    key_str = str(art_row[1] or "")
    anchor_digest = Digest.parse(str(art_row[2]))
    if anchor_digest.kind is not Kind.HISTORICAL:
        raise PromotionError(
            f"Artifact {art_id} already has a verified anchor ({anchor_digest}); "
            "only historical ('legacy-v1:unverified:*') anchors can be promoted."
        )
    driver = str(art_row[3] or "file")

    audit_row = cur.execute(
        "SELECT v1_row_json, basis FROM main.artifact_migration_audit WHERE artifact_id = ?",
        [art_id],
    ).fetchone()
    v1_hash: str | None = None
    table_path: str | None = None
    array_path: str | None = None
    audit_basis: str | None = None
    v1_container_uri: str | None = None
    if audit_row is not None:
        if audit_row[1] is not None:
            audit_basis = str(audit_row[1])
        if audit_row[0]:
            try:
                parsed_v1 = json.loads(str(audit_row[0]))
                if isinstance(parsed_v1, Mapping):
                    if isinstance(parsed_v1.get("hash"), str):
                        v1_hash = parsed_v1["hash"]
                    if isinstance(parsed_v1.get("table_path"), str):
                        table_path = parsed_v1["table_path"]
                    if isinstance(parsed_v1.get("array_path"), str):
                        array_path = parsed_v1["array_path"]
                    if isinstance(parsed_v1.get("container_uri"), str):
                        v1_container_uri = parsed_v1["container_uri"]
            except ValueError:
                pass
    if v1_hash is None and _is_64_hex(anchor_digest.hex):
        v1_hash = anchor_digest.hex

    return _PromotionTargetInfo(
        artifact_id=art_id,
        key=key_str,
        historical_anchor=anchor_digest,
        driver=driver,
        v1_hash=v1_hash,
        table_path=table_path,
        array_path=array_path,
        audit_basis=audit_basis,
        v1_container_uri=v1_container_uri,
    )


def _resolve_promotion_target(
    cur: duckdb.DuckDBPyConnection,
    target: uuid.UUID | str,
) -> _PromotionTargetInfo | None:
    """Resolve `target` as `<historical-anchor>`, `<artifact-id>`, or `<artifact-key>`, or return `None`."""
    raw_target = str(target).strip()
    if not raw_target:
        return None
    queries: list[tuple[str, list[Any]]] = []
    if raw_target.startswith("legacy-v1:unverified:"):
        Digest.parse(raw_target)
        queries.append(("anchor = ? ORDER BY created_at DESC", [raw_target]))
    try:
        target_uuid = target if isinstance(target, uuid.UUID) else uuid.UUID(raw_target)
        queries.append(("id = ?", [target_uuid]))
    except ValueError:
        pass
    if not raw_target.startswith("legacy-v1:unverified:"):
        queries.extend(
            [
                (
                    "key = ? AND anchor LIKE 'legacy-v1:unverified:%' "
                    "AND anchor NOT IN (SELECT historical_anchor FROM main.anchor_promotion) "
                    "ORDER BY created_at DESC",
                    [raw_target],
                ),
                (
                    "key = ? AND anchor LIKE 'legacy-v1:unverified:%' ORDER BY created_at DESC",
                    [raw_target],
                ),
                ("key = ? ORDER BY created_at DESC", [raw_target]),
            ]
        )

    for clause, params in queries:
        art_row = cur.execute(
            f"SELECT id, key, anchor, driver FROM main.artifact WHERE {clause} LIMIT 1",
            params,
        ).fetchone()
        if art_row is not None:
            return _inspect_artifact_promotion_info(cur, art_row)
    return None


def _resolve_stored_candidate_path(
    cur: duckdb.DuckDBPyConnection,
    info: _PromotionTargetInfo,
    effective_mounts: Mapping[str, Mount],
    *,
    search_root: Path | None = None,
) -> Path | None:
    loc_rows = cur.execute(
        "SELECT uri FROM main.artifact_location_current "
        "WHERE anchor = ? AND status = 'active' "
        "ORDER BY observed_at_ns DESC",
        [str(info.historical_anchor)],
    ).fetchall()
    raw_uris = [str(r[0]).strip() for r in loc_rows if r[0]]
    if info.v1_container_uri and info.v1_container_uri.strip() not in raw_uris:
        raw_uris.append(info.v1_container_uri.strip())

    for uri in raw_uris:
        if not uri or uri.lower().startswith("db://"):
            continue
        resolved, _ = resolve_uri_to_path(uri, effective_mounts)
        if resolved is not None and resolved.exists():
            return resolved
        if "://" not in uri:
            raw_p = Path(uri).expanduser()
            if raw_p.is_absolute() and raw_p.exists():
                return raw_p.resolve()
            if not raw_p.is_absolute() and search_root is not None:
                cand = (search_root / raw_p).resolve()
                if cand.exists():
                    return cand
    return None


def _upgrade_eligible_historical_runs(
    ledger: StoreLedger,
    *,
    candidate_run_ids: Sequence[str],
    require_non_trivial: bool = False,
) -> list[str]:
    """Compute `sig-v2` and mark `cache_status='live'` for eligible completed historical runs."""
    unique_run_ids = sorted(
        {str(r).strip() for r in candidate_run_ids if str(r).strip()}
    )
    if not unique_run_ids:
        return []

    upgraded: list[str] = []
    with ledger.exec_cursor() as cur:
        hist_runs = cur.execute(
            "SELECT id, code_hash, git_hash, config_hash FROM main.run "
            "WHERE id IN (SELECT UNNEST(?)) "
            "AND cache_status = 'historical' AND status = 'completed' "
            "ORDER BY id ASC",
            [unique_run_ids],
        ).fetchall()
        for raw_run_id, raw_code_hash, raw_git_hash, raw_config_hash in hist_runs:
            run_id = str(raw_run_id)
            bind_rows = cur.execute(
                "SELECT rb.direction, rb.slot, rb.ordinal, "
                "COALESCE(ap.verified_anchor, rb.anchor) AS resolved_anchor "
                "FROM main.run_binding rb "
                "LEFT JOIN main.anchor_promotion ap "
                "ON ap.historical_anchor = rb.anchor "
                "AND ap.basis IN ('v1_fast_recomputed', 'caller_supplied_match') "
                "WHERE rb.run_id = ? "
                "ORDER BY rb.occurrence_index ASC, rb.ordinal ASC",
                [run_id],
            ).fetchall()
            prod_rows = cur.execute(
                "SELECT COALESCE(ap.verified_anchor, a.anchor) AS resolved_anchor "
                "FROM main.artifact a "
                "LEFT JOIN main.anchor_promotion ap "
                "ON ap.historical_anchor = a.anchor "
                "AND ap.basis IN ('v1_fast_recomputed', 'caller_supplied_match') "
                "WHERE a.run_id = ?",
                [run_id],
            ).fetchall()

            if (
                require_non_trivial
                and not bind_rows
                and not prod_rows
                and raw_code_hash is None
                and raw_git_hash is None
                and raw_config_hash is None
            ):
                continue

            all_resolved_strs = [str(r[3]) for r in bind_rows] + [
                str(r[0]) for r in prod_rows
            ]
            all_verified = True
            for a_str in all_resolved_strs:
                try:
                    parsed_d = Digest.parse(a_str)
                except ValueError:
                    all_verified = False
                    break
                if parsed_d.kind is Kind.HISTORICAL:
                    all_verified = False
                    break
            if not all_verified:
                continue

            input_slot_anchors: list[tuple[str, int, str]] = [
                (
                    f"depends_on:{slot}"
                    if str(direction) == "depends_on"
                    else str(slot),
                    int(ordinal),
                    str(res_anchor),
                )
                for direction, slot, ordinal, res_anchor in bind_rows
                if str(direction) in ("input", "depends_on")
            ]
            code_identity = (
                str(raw_code_hash)
                if raw_code_hash is not None
                else (str(raw_git_hash) if raw_git_hash is not None else None)
            )
            config_hash = str(raw_config_hash) if raw_config_hash is not None else None
            new_input_hash, new_sig = compute_sig_v2(
                code_identity,
                config_hash,
                input_slot_anchors,
            )
            cur.execute(
                "UPDATE main.run "
                "SET input_hash = ?, signature = ?, signature_scheme = 'sig-v2', "
                "cache_status = 'live' "
                "WHERE id = ?",
                [new_input_hash, new_sig, run_id],
            )
            upgraded.append(run_id)
    return upgraded


def _promote_single_info(
    ledger: StoreLedger,
    info: _PromotionTargetInfo,
    *,
    candidate_path: Path | str | None,
    asserted: bool,
    asserted_by: str | None,
    expected_digest: Digest | str | None,
    site: str,
    effective_mounts: Mapping[str, Mount],
) -> tuple[Digest, Digest, str]:
    parsed_expected: Digest | None = (
        (
            Digest.parse(expected_digest)
            if isinstance(expected_digest, str)
            else expected_digest
        )
        if expected_digest is not None
        else None
    )
    if parsed_expected is not None and parsed_expected.kind is Kind.HISTORICAL:
        raise PromotionError(
            f"expected_digest must be a verifiable digest, got {parsed_expected}"
        )

    local_path: Path | None
    if candidate_path is not None:
        local_path = Path(candidate_path).expanduser()
        if not local_path.exists():
            if info.audit_basis == "racy_fast_stat":
                raise PromotionError(
                    f"Cannot promote racy_fast_stat artifact {info.artifact_id} "
                    f"({info.historical_anchor}): content bytes do not exist at '{local_path}'."
                )
            raise FileNotFoundError(
                f"Promotion candidate path does not exist: {local_path}"
            )
        local_path = (
            local_path.parent.resolve() / local_path.name
            if local_path.is_dir()
            else local_path.resolve()
        )
    else:
        with ledger.exec_cursor() as cur:
            local_path = _resolve_stored_candidate_path(
                cur,
                info,
                effective_mounts,
            )
        if local_path is None or not local_path.exists():
            raise PromotionError(
                f"Cannot promote historical artifact {info.artifact_id} "
                f"({info.historical_anchor}): full content bytes do not exist on disk."
            )

    member = info.table_path or info.array_path
    if parsed_expected is not None:
        scheme = parsed_expected.scheme
    elif local_path.is_dir():
        scheme = "sha256:manifest-v1"
    elif info.table_path:
        scheme = "ctd1:sha256"
    elif info.array_path:
        scheme = "sha256:logical_matrix:v1"
    else:
        scheme = "sha256:file"

    verified_anchor, witness = compute_scheme_digest(
        local_path,
        scheme,
        driver=info.driver,
        member=member,
        ledger=ledger,
    )
    witness_is_racy = isinstance(witness, (FileStat, DirTreeStat)) and witness.racy
    if (
        info.audit_basis == "racy_fast_stat"
        and witness_is_racy
        and parsed_expected is None
    ):
        raise PromotionError(
            f"Cannot promote racy_fast_stat artifact {info.artifact_id}: "
            f"candidate '{local_path}' is still within the active racy stat window."
        )

    hist_anchor = info.historical_anchor
    v1_hash = info.v1_hash

    if parsed_expected is not None:
        if verified_anchor != parsed_expected:
            raise PromotionError(
                f"Promotion digest mismatch for '{local_path}': expected {parsed_expected}, "
                f"computed {verified_anchor}."
            )
        basis: Literal["v1_fast_recomputed", "caller_supplied_match", "asserted"] = (
            "caller_supplied_match"
        )
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
    elif local_path.is_dir() and v1_hash is not None:
        raw_v1 = str(v1_hash).strip()
        dir_stat = stat_v1_fast_directory_hash(local_path)
        if (_is_64_hex(raw_v1) and dir_stat.legacy_fast_hex == raw_v1.lower()) or (
            dir_stat.stat_v2_observation == raw_v1
        ):
            basis = "v1_fast_recomputed"
            evidence = {
                "v1_fast_hash": raw_v1,
                "path": str(local_path),
            }
        elif asserted and info.audit_basis != "racy_fast_stat":
            basis = "asserted"
            evidence = {"path": str(local_path)}
        else:
            raise PromotionError(
                f"Candidate directory '{local_path}' does not match the original v1 fast-stat "
                f"hash for {hist_anchor} and no --expected-digest was supplied."
            )
    elif asserted and info.audit_basis != "racy_fast_stat":
        basis = "asserted"
        evidence = {"path": str(local_path)}
    else:
        if info.audit_basis == "racy_fast_stat":
            raise PromotionError(
                f"Cannot promote racy_fast_stat artifact {info.artifact_id} ({hist_anchor}) "
                f"from '{local_path}' without matching on-disk content or --expected-digest."
            )
        raise PromotionError(
            f"Candidate file '{local_path}' does not match the original v1 fast-stat "
            f"hash for {hist_anchor} and no --expected-digest was supplied. "
            "Pass --asserted (asserted=True) to record an asserted location promotion."
        )

    repr_kind = (
        "tree" if local_path.is_dir() else ("member" if member is not None else "file")
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
        access_driver=info.driver,
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
        raise PromotionError(
            f"Historical anchor {hist_anchor} is already promoted to a different "
            f"verified anchor and cannot be re-promoted to {verified_anchor}."
        )
    ledger.record_observation(
        obs,
        repr_kind=repr_kind,
        writer="consist.promote",
    )
    return (hist_anchor, verified_anchor, basis)


def promote_historical_run(
    db_path_or_ledger: Path | str | StoreLedger,
    run_id: str,
    *,
    path: Path | str | None = None,
    paths: Mapping[uuid.UUID | str, Path | str] | None = None,
    asserted: bool = False,
    asserted_by: str | None = None,
    expected_digests: Mapping[uuid.UUID | str, Digest | str] | None = None,
    site: str = "local",
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
) -> list[tuple[Digest, Digest, str]]:
    """Promote all historical input/output artifacts bound to `run_id` and upgrade `run_id` to `sig-v2` if eligible."""
    clean_run_id = str(run_id).strip()
    if not clean_run_id:
        raise PromotionError("run_id must be a non-empty string")
    mount_map = _normalize_mounts(mounts)

    def _run_with_ledger(ledger: StoreLedger) -> list[tuple[Digest, Digest, str]]:
        if mount_map:
            ledger.mounts.update(mount_map)
        effective_mounts = ledger.mounts

        with ledger.transaction() as cur:
            run_row = cur.execute(
                "SELECT id FROM main.run WHERE id = ?",
                [clean_run_id],
            ).fetchone()
            if run_row is None:
                raise PromotionError(
                    f"Could not find run {clean_run_id!r} for historical promotion."
                )

            art_rows = cur.execute(
                "SELECT DISTINCT a.id, a.key, a.anchor, a.driver, a.created_at "
                "FROM main.artifact a "
                "LEFT JOIN main.run_binding rb ON rb.artifact_id = a.id AND rb.run_id = ? "
                "WHERE (rb.run_id = ? OR a.run_id = ?) "
                "AND a.anchor LIKE 'legacy-v1:unverified:%' "
                "ORDER BY a.created_at ASC, a.id ASC",
                [clean_run_id, clean_run_id, clean_run_id],
            ).fetchall()

            search_dir: Path | None = None
            single_override_path: Path | str | None = None
            if path is not None:
                p_obj = Path(path).expanduser()
                if p_obj.exists() and p_obj.is_dir() and len(art_rows) != 1:
                    search_dir = p_obj.resolve()
                elif len(art_rows) == 1:
                    single_override_path = path
                elif p_obj.exists() and p_obj.is_dir():
                    search_dir = p_obj.resolve()
                else:
                    single_override_path = path

            results: list[tuple[Digest, Digest, str]] = []
            for raw_row in art_rows:
                info = _inspect_artifact_promotion_info(cur, raw_row[:4])
                existing_promo = cur.execute(
                    "SELECT verified_anchor, basis FROM main.anchor_promotion "
                    "WHERE historical_anchor = ? "
                    "AND basis IN ('v1_fast_recomputed', 'caller_supplied_match')",
                    [str(info.historical_anchor)],
                ).fetchone()

                cand_path: Path | str | None = single_override_path
                if paths is not None:
                    for lookup_key in (
                        info.artifact_id,
                        str(info.artifact_id),
                        info.key,
                        str(info.historical_anchor),
                    ):
                        if lookup_key in paths:
                            cand_path = paths[lookup_key]
                            break
                if cand_path is None and search_dir is not None:
                    cand_path = _resolve_stored_candidate_path(
                        cur,
                        info,
                        effective_mounts,
                        search_root=search_dir,
                    )

                exp_dig: Digest | str | None = None
                if expected_digests is not None:
                    for lookup_key in (
                        info.artifact_id,
                        str(info.artifact_id),
                        info.key,
                        str(info.historical_anchor),
                    ):
                        if lookup_key in expected_digests:
                            exp_dig = expected_digests[lookup_key]
                            break

                if existing_promo is not None and cand_path is None and exp_dig is None:
                    stored_cand = _resolve_stored_candidate_path(
                        cur,
                        info,
                        effective_mounts,
                    )
                    if stored_cand is None or not stored_cand.exists():
                        results.append(
                            (
                                info.historical_anchor,
                                Digest.parse(str(existing_promo[0])),
                                str(existing_promo[1]),
                            )
                        )
                        continue

                promo_res = _promote_single_info(
                    ledger,
                    info,
                    candidate_path=cand_path,
                    asserted=asserted,
                    asserted_by=asserted_by,
                    expected_digest=exp_dig,
                    site=site,
                    effective_mounts=effective_mounts,
                )
                results.append(promo_res)

            _upgrade_eligible_historical_runs(
                ledger,
                candidate_run_ids=[clean_run_id],
            )
            return results

    if isinstance(db_path_or_ledger, StoreLedger):
        return _run_with_ledger(db_path_or_ledger)

    with StoreLedger(
        db_path_or_ledger, mounts=mount_map, writer="consist.promote"
    ) as led:
        return _run_with_ledger(led)


def promote_historical_artifact(
    db_path_or_ledger: Path | str | StoreLedger,
    target: uuid.UUID | str,
    *,
    path: Path | str | None = None,
    asserted: bool = False,
    asserted_by: str | None = None,
    expected_digest: Digest | str | None = None,
    site: str = "local",
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
) -> tuple[Digest, Digest, str]:
    """Promote a historical `<artifact-id>`, `<historical-anchor>`, `<key>`, or `<run-id>`.

    Returns `(historical_anchor, verified_anchor, basis)` and upgrades any completed
    historical runs whose bound inputs and outputs are now all verified to `sig-v2`.
    """
    mount_map = _normalize_mounts(mounts)

    def _run_with_ledger(ledger: StoreLedger) -> tuple[Digest, Digest, str]:
        if mount_map:
            ledger.mounts.update(mount_map)
        effective_mounts = ledger.mounts

        with ledger.transaction() as cur:
            info = _resolve_promotion_target(cur, target)
            if info is not None:
                res = _promote_single_info(
                    ledger,
                    info,
                    candidate_path=path,
                    asserted=asserted,
                    asserted_by=asserted_by,
                    expected_digest=expected_digest,
                    site=site,
                    effective_mounts=effective_mounts,
                )
                if res[2] in ("v1_fast_recomputed", "caller_supplied_match"):
                    hist_str = str(info.historical_anchor)
                    linked_run_rows = cur.execute(
                        "SELECT DISTINCT run_id FROM ("
                        "SELECT rb.run_id AS run_id FROM main.run_binding rb "
                        "WHERE rb.anchor = ? OR rb.artifact_id = ? "
                        "UNION "
                        "SELECT a.run_id AS run_id FROM main.artifact a "
                        "WHERE (a.anchor = ? OR a.id = ?) AND a.run_id IS NOT NULL"
                        ") ORDER BY run_id ASC",
                        [hist_str, info.artifact_id, hist_str, info.artifact_id],
                    ).fetchall()
                    _upgrade_eligible_historical_runs(
                        ledger,
                        candidate_run_ids=[str(r[0]) for r in linked_run_rows],
                        require_non_trivial=True,
                    )
                return res

            raw_target = str(target).strip()
            if not isinstance(target, uuid.UUID) and not raw_target.startswith(
                "legacy-v1:unverified:"
            ):
                run_row = cur.execute(
                    "SELECT id FROM main.run WHERE id = ?",
                    [raw_target],
                ).fetchone()
                if run_row is not None:
                    exp_map: dict[uuid.UUID | str, Digest | str] | None = (
                        {raw_target: expected_digest}
                        if expected_digest is not None
                        else None
                    )
                    run_results = promote_historical_run(
                        ledger,
                        raw_target,
                        path=path,
                        asserted=asserted,
                        asserted_by=asserted_by,
                        expected_digests=exp_map,
                        site=site,
                        mounts=mounts,
                    )
                    if not run_results:
                        raise PromotionError(
                            f"Run {raw_target!r} has no historical artifacts to promote."
                        )
                    return run_results[-1]

            raise PromotionError(
                f"Could not find historical artifact or run matching target {raw_target!r}."
            )

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
    "PromotionError",
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
    "promote_historical_run",
    "undo_migration",
    "verify_migration",
]
