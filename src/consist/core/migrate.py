"""Cold-safe database migration from Consist v0.4 (schema v1) to v0.5 (schema v2)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import re
import tempfile
import time
from typing import Any, Optional
import uuid

import duckdb

from consist.core.fs import FileSystemManager
from consist.core.persistence import (
    DatabaseManager,
    IncompatibleSchemaError,
    _coerce_optional_uuid,
    _coerce_run_tags,
    _coerce_uuid,
    _dump_json,
    _ensure_non_null_utc_dt,
    _ensure_utc_dt,
    _execute_multirow_values,
    _parse_json_dict,
    _quote_ident,
)
from consist.digest import (
    Digest,
    build_manifest_v1,
    build_shapefile_manifest_v1,
    canonical_json_sha256,
    compute_digest,
    normalize_logical_matrices_identity,
)
from consist.location import ArtifactLocationRow, LocationSite, Witness, capture_witness

_LOWER_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_WEAK_SCHEMES = frozenset(
    {"stat_file_v1:sha256", "stat_dir_v1:sha256", "caller_supplied_v1:sha256"}
)
_PASSTHROUGH_CORE_TABLES = (
    "config_facet",
    "run_config_kv",
    "artifact_facet",
    "artifact_kv",
    "run_binding_invocation",
    "run_coupler_snapshot",
    "artifact_schema",
    "artifact_schema_field",
    "artifact_schema_observation",
    "artifact_schema_relation",
)
_CORE_MAIN_TABLES = frozenset(
    {
        "consist_schema",
        "run",
        "artifact",
        "artifact_content",
        "artifact_location",
        "run_artifact_link",
        *_PASSTHROUGH_CORE_TABLES,
    }
)


@dataclass(frozen=True, slots=True)
class MigrationReport:
    """Summary of a Consist v0.4 -> v0.5 database migration."""

    db_path: Path
    backup_path: Optional[Path] = None
    already_current: bool = False
    dry_run: bool = False
    runs_migrated: int = 0
    cacheable_runs: int = 0
    uncacheable_runs: int = 0
    artifacts_migrated: int = 0
    artifacts_hashed: int = 0
    artifacts_trusted: int = 0
    artifacts_cold_unverified: int = 0
    locations_created: int = 0
    global_tables_copied: int = 0
    views_copied: int = 0


@dataclass
class _MigratingRun:
    id: str
    parent_run_id: Optional[str]
    status: str
    model_name: str
    description: Optional[str]
    year: Optional[int]
    iteration: Optional[int]
    stage: Optional[str]
    phase: Optional[str]
    tags: list[str]
    config_hash: Optional[str]
    git_hash: Optional[str]
    input_hash: Optional[str]
    signature: Optional[str]
    cacheable: bool
    facet_id: Optional[str]
    meta: dict[str, Any]
    started_at: datetime
    ended_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime

    def to_insert_row(self) -> list[Any]:
        return [
            self.id,
            self.parent_run_id,
            self.status,
            self.model_name,
            self.description,
            self.year,
            self.iteration,
            self.stage,
            self.phase,
            _dump_json(self.tags),
            self.config_hash,
            self.git_hash,
            self.input_hash,
            self.signature,
            self.cacheable,
            self.facet_id,
            _dump_json(self.meta),
            self.started_at,
            self.ended_at,
            self.created_at,
            self.updated_at,
        ]


@dataclass(frozen=True, slots=True)
class _MigratedArtifactInfo:
    id: uuid.UUID
    key: str
    driver: str
    digest: Optional[Digest]
    member: Optional[str]


def _table_columns(
    cur: duckdb.DuckDBPyConnection, *, db_alias: str, schema: str, table: str
) -> list[str]:
    rows = cur.execute(
        "SELECT column_name FROM duckdb_columns() "
        "WHERE database_name = ? AND schema_name = ? AND table_name = ? "
        "ORDER BY column_index",
        [db_alias, schema, table],
    ).fetchall()
    return [str(r[0]) for r in rows]


def _copy_passthrough_tables(
    cur: duckdb.DuckDBPyConnection, *, dst_catalog: str, src_tables: set[str]
) -> None:
    fallbacks = {"json": "facet_json"}
    for tbl in _PASSTHROUGH_CORE_TABLES:
        if tbl not in src_tables:
            continue
        dst_cols = _table_columns(cur, db_alias=dst_catalog, schema="main", table=tbl)
        src_cols = set(
            _table_columns(cur, db_alias="v04_src", schema="main", table=tbl)
        )
        sel_dst: list[str] = []
        sel_src: list[str] = []
        for col in dst_cols:
            if col in src_cols:
                sel_dst.append(_quote_ident(col))
                sel_src.append(_quote_ident(col))
            elif col in fallbacks and fallbacks[col] in src_cols:
                sel_dst.append(_quote_ident(col))
                sel_src.append(_quote_ident(fallbacks[col]))
        if sel_dst:
            qt = _quote_ident(tbl)
            cur.execute(
                f"INSERT INTO main.{qt} ({', '.join(sel_dst)}) "
                f"SELECT {', '.join(sel_src)} FROM v04_src.main.{qt} ON CONFLICT DO NOTHING"
            )


def _extract_recovery_roots(raw: Any) -> list[str]:
    if raw is None:
        return []
    items = (
        [raw]
        if isinstance(raw, str)
        else (
            raw
            if isinstance(raw, Sequence) and not isinstance(raw, (bytes, bytearray))
            else ()
        )
    )
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        if isinstance(item, (str, os.PathLike)):
            text = os.fspath(item).strip()
            if text and text not in seen:
                seen.add(text)
                out.append(text)
    return out


def _try_trust_v04_digest(
    *,
    old_hash: Optional[str],
    old_semantics: Any,
    driver: str,
    member: Optional[str],
    candidate_path: Path,
    meta: Mapping[str, Any],
) -> Optional[Digest]:
    if not old_hash:
        return None
    text = old_hash.strip()
    if not text or text.startswith(("fast:", "stat-v1:", "stat-v2:", "stat_")):
        return None
    if isinstance(old_semantics, Mapping):
        if old_semantics.get("source") in {
            "caller_supplied",
            "computed_fast",
            "computed_fast_directory",
        }:
            return None
        if (
            old_semantics.get("kind") == "directory"
            and old_semantics.get("digest_contract") == "legacy_directory_content"
        ):
            return None
    if text.startswith("sha256:manifest-v1:"):
        hex_part = text[len("sha256:manifest-v1:") :].lower()
        return (
            Digest("dir_manifest_v1:sha256", hex_part)
            if _LOWER_HEX64_RE.fullmatch(hex_part)
            else None
        )
    if text.startswith("sha256:logical_matrices:"):
        try:
            return Digest.parse(normalize_logical_matrices_identity(text))
        except ValueError:
            return None
    if ":" in text:
        try:
            parsed = Digest.parse(text)
            return None if parsed.scheme in _WEAK_SCHEMES else parsed
        except ValueError:
            return None
    lowered = text.lower()
    if not _LOWER_HEX64_RE.fullmatch(lowered):
        return None
    if driver == "artifact_set" or meta.get("artifact_set") is True:
        return Digest("output_set_v1:sha256", lowered)
    if (
        member is not None
        or driver in {"h5", "hdf5", "h5_table", "zarr", "shapefile", "directory"}
        or candidate_path.is_dir()
        or candidate_path.suffix.lower() == ".shp"
    ):
        return None
    return Digest("sha256:file", lowered)


def _compute_migrated_digest(
    candidate_path: Path, *, driver: str, member: Optional[str], meta: dict[str, Any]
) -> Digest:
    options: dict[str, Any] = {}
    if driver == "artifact_set" or meta.get("artifact_set") is True:
        member_paths = meta.get("output_set_member_paths")
        if isinstance(member_paths, Mapping):
            options["member_paths"] = member_paths
    elif candidate_path.is_dir():
        meta.setdefault("directory_artifact", True)
        if driver != "zarr" and "directory_manifest" not in meta:
            meta["directory_manifest"] = build_manifest_v1(candidate_path)
    elif driver == "shapefile" or candidate_path.suffix.lower() == ".shp":
        options["shapefile_bundle"] = True
        meta.setdefault("file_bundle_artifact", True)
        meta.setdefault("file_bundle_entry", candidate_path.name)
        if "file_bundle_manifest" not in meta:
            meta["file_bundle_manifest"] = build_shapefile_manifest_v1(candidate_path)
    return compute_digest(
        candidate_path, driver=driver, member=member, options=options or None
    )


def _capture_location_witness(
    path: Path, *, driver: str, meta: Mapping[str, Any], observed_at_ns: int
) -> Optional[Witness]:
    is_dir = bool(
        path.is_dir()
        or meta.get("directory_artifact") is True
        or meta.get("artifact_set") is True
        or driver in {"zarr", "directory", "artifact_set"}
    )
    is_shp = bool(
        meta.get("file_bundle_artifact") is True
        or driver == "shapefile"
        or path.suffix.lower() == ".shp"
    )
    entry_raw = meta.get("file_bundle_entry")
    bundle_entry = (
        entry_raw
        if isinstance(entry_raw, str) and entry_raw
        else (path.name if is_shp and path.is_file() else None)
    )
    return capture_witness(
        path,
        observed_at_ns=observed_at_ns,
        driver=driver,
        is_directory=is_dir,
        is_shapefile_bundle=is_shp,
        bundle_entry=bundle_entry,
    )


def _probe_uri_on_fs(
    fs: FileSystemManager,
    uri: str,
    art_id: uuid.UUID,
    member_val: Optional[str],
    obs_ns: int,
) -> tuple[str, str, str, LocationSite, Optional[Path]]:
    m_name, m_inst, m_uri = fs.location_coordinate_for_uri(uri)
    site: LocationSite = "workspace" if m_name in ("workspace", "runs") else "external"
    proto = ArtifactLocationRow.create(
        digest=None,
        artifact_id=art_id,
        site=site,
        mount_name=m_name,
        mount_instance_id=m_inst,
        uri=m_uri,
        member=member_val or "",
        observed_at_ns=obs_ns,
    )
    return m_name, m_inst, m_uri, site, fs.resolve_location_row_path(proto)


def _load_v04_runs(
    cur: duckdb.DuckDBPyConnection, src_tables: set[str]
) -> list[_MigratingRun]:
    if "run" not in src_tables:
        return []
    cols = set(_table_columns(cur, db_alias="v04_src", schema="main", table="run"))
    now = datetime.now(timezone.utc)
    spec = (
        ("id", "NULL"),
        ("parent_run_id", "NULL"),
        ("status", "'completed'"),
        ("model_name", "'unknown'"),
        ("description", "NULL"),
        ("year", "NULL"),
        ("iteration", "NULL"),
        ("stage", "NULL"),
        ("phase", "NULL"),
        ("tags", "'[]'"),
        ("config_hash", "NULL"),
        ("git_hash", "NULL"),
        ("input_hash", "NULL"),
        ("signature", "NULL"),
        ("facet_id", "NULL"),
        ("meta", "'{}'"),
        ("started_at", "NULL"),
        ("ended_at", "NULL"),
        ("created_at", "NULL"),
        ("updated_at", "NULL"),
    )
    exprs = [_quote_ident(c) if c in cols else d for c, d in spec]
    rows = cur.execute(f"SELECT {', '.join(exprs)} FROM v04_src.main.run").fetchall()
    out: list[_MigratingRun] = []
    for r in rows:
        meta = _parse_json_dict(r[15])
        stage = (
            str(r[7])
            if r[7] is not None
            else (str(meta["stage"]) if isinstance(meta.get("stage"), str) else None)
        )
        phase = (
            str(r[8])
            if r[8] is not None
            else (str(meta["phase"]) if isinstance(meta.get("phase"), str) else None)
        )
        started = _ensure_utc_dt(r[16]) or _ensure_utc_dt(r[18]) or now
        created = _ensure_utc_dt(r[18]) or started
        status = str(r[2]) if r[2] is not None else "completed"
        out.append(
            _MigratingRun(
                id=str(r[0]),
                parent_run_id=str(r[1]) if r[1] is not None else None,
                status=status,
                model_name=str(r[3]) if r[3] is not None else "unknown",
                description=str(r[4]) if r[4] is not None else None,
                year=int(r[5]) if r[5] is not None else None,
                iteration=int(r[6]) if r[6] is not None else None,
                stage=stage,
                phase=phase,
                tags=_coerce_run_tags(r[9]),
                config_hash=str(r[10]) if r[10] is not None else None,
                git_hash=str(r[11]) if r[11] is not None else None,
                input_hash=str(r[12]) if r[12] is not None else None,
                signature=str(r[13]) if r[13] is not None else None,
                cacheable=(status == "completed"),
                facet_id=str(r[14]) if r[14] is not None else None,
                meta=meta,
                started_at=started,
                ended_at=_ensure_utc_dt(r[17]),
                created_at=created,
                updated_at=_ensure_utc_dt(r[19]) or created,
            )
        )
    return out


def _promote_run_identities_and_links(
    *,
    runs: list[_MigratingRun],
    artifacts_by_id: Mapping[uuid.UUID, _MigratedArtifactInfo],
    raw_links: Sequence[tuple[str, uuid.UUID, str, str, bool]],
) -> list[list[Any]]:
    inputs_by_run: dict[str, list[tuple[uuid.UUID, str, bool]]] = {}
    outputs_by_run: dict[str, list[tuple[uuid.UUID, str, bool]]] = {}
    for run_id, aid, direction, raw_role, is_implicit in raw_links:
        bucket = inputs_by_run if direction == "input" else outputs_by_run
        bucket.setdefault(run_id, []).append((aid, raw_role, is_implicit))

    link_dedup: dict[tuple[str, uuid.UUID, str, str], bool] = {}
    for run in runs:
        run_inputs = inputs_by_run.get(run.id, [])
        role_by_aid: dict[uuid.UUID, str] = {}
        for meta_key in ("input_binding", "input_identity"):
            block = run.meta.get(meta_key)
            bindings = block.get("bindings") if isinstance(block, Mapping) else None
            if not isinstance(bindings, Sequence) or isinstance(bindings, (str, bytes)):
                continue
            for idx, b in enumerate(bindings):
                if not isinstance(b, Mapping):
                    continue
                kind, role_name = b.get("kind"), b.get("role")
                if not isinstance(kind, str) or role_name is None:
                    continue
                raw_aid = b.get("artifact_id")
                target_aid: Optional[uuid.UUID] = None
                if raw_aid is not None:
                    try:
                        target_aid = _coerce_uuid(raw_aid)
                    except (ValueError, TypeError):
                        target_aid = None
                elif idx < len(run_inputs):
                    target_aid = run_inputs[idx][0]
                if target_aid is not None:
                    role_by_aid[target_aid] = f"{kind}:{role_name}"

        for aid, raw_role, is_imp in run_inputs:
            info = artifacts_by_id.get(aid)
            role_str = role_by_aid.get(aid) or (
                raw_role
                if (raw_role and raw_role != "input")
                else (info.key if info else "")
            )
            k = (run.id, aid, "input", role_str)
            link_dedup[k] = link_dedup.get(k, False) or bool(is_imp)

        for aid, raw_role, is_imp in outputs_by_run.get(run.id, []):
            info = artifacts_by_id.get(aid)
            role_str = (
                raw_role
                if (raw_role and raw_role != "output")
                else (info.key if info else "")
            )
            k = (run.id, aid, "output", role_str)
            link_dedup[k] = link_dedup.get(k, False) or bool(is_imp)

        if run.status != "completed" or not run.git_hash or not run.config_hash:
            run.cacheable = False
            continue

        id_blk = run.meta.get("input_identity")
        bind_blk = run.meta.get("input_binding")
        src_blk = (
            id_blk
            if isinstance(id_blk, Mapping) and isinstance(id_blk.get("bindings"), list)
            else (
                bind_blk
                if isinstance(bind_blk, Mapping)
                and isinstance(bind_blk.get("bindings"), list)
                else None
            )
        )
        pairs: list[tuple[str, str]] = []
        upgraded_bindings: list[dict[str, Any]] = []
        all_strong = True

        if src_blk is not None:
            for idx, b in enumerate(src_blk["bindings"]):
                if not isinstance(b, Mapping):
                    all_strong = False
                    break
                kind = str(b.get("kind", "named"))
                role_val = b.get("role", idx)
                raw_aid = b.get("artifact_id")
                aid_lookup: Optional[uuid.UUID] = None
                if raw_aid is not None:
                    try:
                        aid_lookup = _coerce_uuid(raw_aid)
                    except (ValueError, TypeError):
                        aid_lookup = None
                elif idx < len(run_inputs):
                    aid_lookup = run_inputs[idx][0]
                info = (
                    artifacts_by_id.get(aid_lookup) if aid_lookup is not None else None
                )
                if (
                    info is None
                    or info.digest is None
                    or info.digest.scheme in _WEAK_SCHEMES
                ):
                    all_strong = False
                    break
                tok = (
                    f"{info.digest}#member={info.member}"
                    if info.member is not None
                    else str(info.digest)
                )
                pairs.append((f"{kind}:{role_val}", tok))
                upgraded_bindings.append(
                    {
                        "kind": kind,
                        "role": role_val,
                        "mode": "content",
                        "value": tok,
                        "artifact_id": str(info.id),
                        "selector": {"driver": info.driver, "member": info.member},
                        "evidence": {"identity_strength": "content"},
                    }
                )
            strict_id = (
                id_blk.get("strict_binding_identity")
                if isinstance(id_blk, Mapping)
                else None
            )
            if isinstance(strict_id, str) and strict_id.strip():
                pairs.append(("__strict_binding__", f"strict_binding_v1:{strict_id}"))
        else:
            for aid, raw_role, _ in run_inputs:
                info = artifacts_by_id.get(aid)
                if (
                    info is None
                    or info.digest is None
                    or info.digest.scheme in _WEAK_SCHEMES
                ):
                    all_strong = False
                    break
                tok = (
                    f"{info.digest}#member={info.member}"
                    if info.member is not None
                    else str(info.digest)
                )
                pairs.append(
                    (raw_role if (raw_role and raw_role != "input") else info.key, tok)
                )

        if not all_strong:
            run.cacheable = False
            continue

        pairs.sort()
        new_in_hash = canonical_json_sha256(pairs)
        run.input_hash = new_in_hash
        run.signature = hashlib.sha256(
            f"{run.git_hash}:{run.config_hash}:{new_in_hash}".encode("utf-8")
        ).hexdigest()
        run.cacheable = True
        if src_blk is not None:
            new_id_meta: dict[str, Any] = {"version": 1, "bindings": upgraded_bindings}
            if isinstance(id_blk, Mapping):
                for pk in (
                    "strict_input_count",
                    "ordinary_input_count",
                    "strict_binding_identity",
                ):
                    if pk in id_blk:
                        new_id_meta[pk] = id_blk[pk]
            run.meta["input_identity"] = new_id_meta

    return [
        [rid, aid, direction, role_str, is_imp]
        for (rid, aid, direction, role_str), is_imp in link_dedup.items()
    ]


def migrate_database(
    db_path: str | os.PathLike[str],
    *,
    run_dir: Optional[str | os.PathLike[str]] = None,
    mounts: Optional[Mapping[str, str]] = None,
    trust_v04_hashes: bool = False,
    dry_run: bool = False,
) -> MigrationReport:
    """Migrate a Consist v0.4 DuckDB database cleanly to Consist v0.5 (schema v2)."""
    src_path = Path(db_path).expanduser().resolve()
    if not src_path.is_file():
        raise FileNotFoundError(f"Database file not found: {src_path}")

    existing_db_uid: Optional[str] = None
    probe = duckdb.connect(str(src_path), read_only=True)
    try:
        main_tables = {
            str(r[0])
            for r in probe.execute(
                "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main'"
            ).fetchall()
        }
        if "consist_schema" in main_tables:
            ver_row = probe.execute(
                "SELECT MAX(version) FROM consist_schema"
            ).fetchone()
            version = int(ver_row[0]) if ver_row and ver_row[0] is not None else None
            if version is not None and version > 2:
                raise IncompatibleSchemaError(
                    f"Database {src_path!s} uses unsupported schema version {version} (> 2)."
                )
            s_cols = {
                str(r[0])
                for r in probe.execute(
                    "SELECT column_name FROM duckdb_columns() "
                    "WHERE schema_name = 'main' AND table_name = 'consist_schema'"
                ).fetchall()
            }
            if "db_uid" in s_cols:
                uid_row = probe.execute(
                    "SELECT db_uid FROM consist_schema ORDER BY version DESC LIMIT 1"
                ).fetchone()
                if uid_row and uid_row[0]:
                    existing_db_uid = str(uid_row[0])
            if version == 2:
                if "artifact" in main_tables:
                    a_cols = {
                        str(r[0])
                        for r in probe.execute(
                            "SELECT column_name FROM duckdb_columns() "
                            "WHERE schema_name = 'main' AND table_name = 'artifact'"
                        ).fetchall()
                    }
                    if not {"uri", "digest"}.issubset(a_cols):
                        raise IncompatibleSchemaError(
                            f"Database {src_path!s} declares schema v2 with invalid artifact table."
                        )
                return MigrationReport(
                    db_path=src_path, already_current=True, dry_run=dry_run
                )
    finally:
        probe.close()

    cli_run_dir = Path(run_dir).expanduser().resolve() if run_dir is not None else None
    cli_mounts = (
        {str(k): str(Path(v).expanduser().resolve()) for k, v in mounts.items()}
        if mounts
        else {}
    )

    temp_dir_ctx: Optional[tempfile.TemporaryDirectory[str]] = None
    if dry_run:
        temp_dir_ctx = tempfile.TemporaryDirectory(prefix="consist_migrate_dryrun_")
        tmp_path = Path(temp_dir_ctx.name) / f"{src_path.name}.v05.tmp"
    else:
        tmp_path = Path(f"{src_path}.v05.tmp")
        if tmp_path.resolve() == src_path or (
            tmp_path.exists() and os.path.samefile(tmp_path, src_path)
        ):
            raise ValueError(f"Temporary migration path {tmp_path} aliases {src_path}.")
        tmp_path.unlink(missing_ok=True)
        Path(f"{tmp_path}.wal").unlink(missing_ok=True)

    try:
        dst_db = DatabaseManager(str(tmp_path), db_uid=existing_db_uid)
        global_tables_copied = views_copied = 0
        artifacts_hashed = artifacts_trusted = artifacts_cold = 0

        try:
            escaped_src = str(src_path).replace("'", "''")
            with dst_db.transaction() as cur:
                dst_cat_row = cur.execute("SELECT current_database()").fetchone()
                dst_catalog = str(dst_cat_row[0]) if dst_cat_row else "main"
                cur.execute(f"ATTACH '{escaped_src}' AS v04_src (READ_ONLY)")
                try:
                    src_tables = {
                        str(r[0])
                        for r in cur.execute(
                            "SELECT table_name FROM duckdb_tables() "
                            "WHERE database_name = 'v04_src' AND schema_name = 'main' AND NOT internal"
                        ).fetchall()
                    }
                    for raw_s, raw_t in cur.execute(
                        "SELECT schema_name, table_name FROM duckdb_tables() "
                        "WHERE database_name = 'v04_src' AND schema_name != 'main' AND NOT internal"
                    ).fetchall():
                        qs, qt = _quote_ident(str(raw_s)), _quote_ident(str(raw_t))
                        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {qs}")
                        cur.execute(
                            f"CREATE TABLE {qs}.{qt} AS SELECT * FROM v04_src.{qs}.{qt}"
                        )
                        if str(raw_s) == "global_tables":
                            global_tables_copied += 1

                    for custom_tbl in sorted(src_tables - _CORE_MAIN_TABLES):
                        qc = _quote_ident(custom_tbl)
                        cur.execute(
                            f"CREATE TABLE main.{qc} AS SELECT * FROM v04_src.main.{qc}"
                        )

                    _copy_passthrough_tables(
                        cur, dst_catalog=dst_catalog, src_tables=src_tables
                    )
                    runs = _load_v04_runs(cur, src_tables)
                    runs_by_id = {r.id: r for r in runs}

                    raw_links: list[tuple[str, uuid.UUID, str, str, bool]] = []
                    linked_runs_by_art: dict[uuid.UUID, list[str]] = {}
                    if "run_artifact_link" in src_tables:
                        l_cols = set(
                            _table_columns(
                                cur,
                                db_alias="v04_src",
                                schema="main",
                                table="run_artifact_link",
                            )
                        )
                        r_expr = '"role"' if "role" in l_cols else "''"
                        i_expr = '"is_implicit"' if "is_implicit" in l_cols else "FALSE"
                        for lr in cur.execute(
                            f"SELECT run_id, artifact_id, direction, {r_expr}, {i_expr} "
                            "FROM v04_src.main.run_artifact_link"
                        ).fetchall():
                            l_rid, l_aid = str(lr[0]), _coerce_uuid(lr[1])
                            raw_links.append(
                                (
                                    l_rid,
                                    l_aid,
                                    str(lr[2]),
                                    str(lr[3] or ""),
                                    bool(lr[4]),
                                )
                            )
                            lst = linked_runs_by_art.setdefault(l_aid, [])
                            if l_rid not in lst:
                                lst.append(l_rid)

                    content_hash_by_id: dict[str, str] = {}
                    if "artifact_content" in src_tables:
                        ac_cols = set(
                            _table_columns(
                                cur,
                                db_alias="v04_src",
                                schema="main",
                                table="artifact_content",
                            )
                        )
                        h_col = (
                            "content_hash"
                            if "content_hash" in ac_cols
                            else ("hash" if "hash" in ac_cols else None)
                        )
                        if "id" in ac_cols and h_col is not None:
                            for cid, chash in cur.execute(
                                f"SELECT id, {_quote_ident(h_col)} FROM v04_src.main.artifact_content"
                            ).fetchall():
                                if cid is not None and chash:
                                    content_hash_by_id[str(cid)] = str(chash)

                    artifact_insert_rows: list[list[Any]] = []
                    location_rows: list[ArtifactLocationRow] = []
                    artifacts_by_id: dict[uuid.UUID, _MigratedArtifactInfo] = {}

                    if "artifact" in src_tables:
                        a_cols = set(
                            _table_columns(
                                cur, db_alias="v04_src", schema="main", table="artifact"
                            )
                        )
                        u_expr = (
                            'COALESCE("container_uri", "uri")'
                            if {"container_uri", "uri"}.issubset(a_cols)
                            else (
                                '"container_uri"'
                                if "container_uri" in a_cols
                                else '"uri"'
                            )
                        )
                        a_spec = (
                            ("id", "NULL"),
                            ("key", "'artifact'"),
                            (None, u_expr),
                            ("driver", "'unknown'"),
                            ("table_path", "NULL"),
                            ("array_path", "NULL"),
                            ("member", "NULL"),
                            ("hash", "NULL"),
                            ("digest", "NULL"),
                            ("content_id", "NULL"),
                            ("run_id", "NULL"),
                            ("parent_artifact_id", "NULL"),
                            ("facet_id", "NULL"),
                            ("meta", "'{}'"),
                            ("created_at", "NULL"),
                        )
                        a_exprs = [
                            d if c is None else (_quote_ident(c) if c in a_cols else d)
                            for c, d in a_spec
                        ]
                        raw_artifacts = cur.execute(
                            f"SELECT {', '.join(a_exprs)} FROM v04_src.main.artifact"
                        ).fetchall()
                        obs_base_ns = time.time_ns()

                        def _fs_for_run(
                            m_run: Optional[_MigratingRun],
                        ) -> FileSystemManager:
                            hm: dict[str, str] = {}
                            hrd: Optional[Path] = None
                            if m_run is not None:
                                rm = m_run.meta.get("mounts")
                                if isinstance(rm, Mapping):
                                    hm = {
                                        mk: mv
                                        for mk, mv in rm.items()
                                        if isinstance(mk, str) and isinstance(mv, str)
                                    }
                                rrd = m_run.meta.get("_physical_run_dir")
                                if isinstance(rrd, str) and rrd.strip():
                                    hrd = Path(rrd).expanduser().resolve()
                            eff_rd = hrd or cli_run_dir or src_path.parent
                            return FileSystemManager(
                                run_dir=eff_rd, mounts={**hm, **cli_mounts}
                            )

                        for a_idx, a_row in enumerate(raw_artifacts):
                            art_id = _coerce_uuid(a_row[0])
                            key, uri, driver = (
                                str(a_row[1]),
                                str(a_row[2]),
                                str(a_row[3]),
                            )
                            member_val = next(
                                (
                                    str(mc)
                                    for mc in (a_row[4], a_row[5], a_row[6])
                                    if mc is not None and str(mc).strip()
                                ),
                                None,
                            )
                            old_hash = next(
                                (
                                    str(hc).strip()
                                    for hc in (a_row[8], a_row[7])
                                    if hc is not None and str(hc).strip()
                                ),
                                None,
                            )
                            if old_hash is None and a_row[9] is not None:
                                old_hash = content_hash_by_id.get(str(a_row[9]))

                            prod_run_id = (
                                str(a_row[10]) if a_row[10] is not None else None
                            )
                            parent_aid = _coerce_optional_uuid(a_row[11])
                            facet_id = str(a_row[12]) if a_row[12] is not None else None
                            meta = _parse_json_dict(a_row[13])
                            created_at = _ensure_non_null_utc_dt(
                                a_row[14] or datetime.now(timezone.utc)
                            )
                            recovery_roots = _extract_recovery_roots(
                                meta.pop("recovery_roots", None)
                            )
                            old_semantics = meta.pop("hash_semantics", None)
                            meta.pop("content_identity", None)

                            cand_runs: list[_MigratingRun] = []
                            if prod_run_id and prod_run_id in runs_by_id:
                                cand_runs.append(runs_by_id[prod_run_id])
                            for l_rid in linked_runs_by_art.get(art_id, ()):
                                lr_obj = runs_by_id.get(l_rid)
                                if lr_obj is not None and lr_obj not in cand_runs:
                                    cand_runs.append(lr_obj)

                            obs_ns = obs_base_ns + a_idx * 8
                            fs = _fs_for_run(cand_runs[0] if cand_runs else None)
                            p_mount, p_inst, p_uri, p_site, primary_cand = (
                                _probe_uri_on_fs(fs, uri, art_id, member_val, obs_ns)
                            )
                            if (
                                primary_cand is None or not primary_cand.exists()
                            ) and len(cand_runs) > 1:
                                for alt_run in cand_runs[1:]:
                                    alt_fs = _fs_for_run(alt_run)
                                    am, ai, au, asite, acand = _probe_uri_on_fs(
                                        alt_fs, uri, art_id, member_val, obs_ns
                                    )
                                    if acand is not None and acand.exists():
                                        fs = alt_fs
                                        p_mount, p_inst, p_uri, p_site, primary_cand = (
                                            am,
                                            ai,
                                            au,
                                            asite,
                                            acand,
                                        )
                                        break
                            if (
                                (primary_cand is None or not primary_cand.exists())
                                and cli_run_dir is not None
                                and cli_run_dir != fs.run_dir
                                and p_mount in ("workspace", "runs")
                            ):
                                cli_fs = FileSystemManager(
                                    run_dir=cli_run_dir,
                                    mounts={**fs.mounts, **cli_mounts},
                                )
                                am, ai, au, _asite, acand = _probe_uri_on_fs(
                                    cli_fs, uri, art_id, member_val, obs_ns
                                )
                                if acand is not None and acand.exists():
                                    p_mount, p_inst, p_uri, primary_cand = (
                                        am,
                                        ai,
                                        au,
                                        acand,
                                    )

                            recovery_coords: list[
                                tuple[str, str, str, Optional[Path]]
                            ] = []
                            for r_root in recovery_roots:
                                rm_name, rm_inst, rm_uri, rm_target = (
                                    fs.archive_coordinate_for_root(uri, r_root)
                                )
                                rm_cand = (
                                    None
                                    if str(rm_target).startswith("/__unmounted__/")
                                    else rm_target
                                )
                                recovery_coords.append(
                                    (rm_name, rm_inst, rm_uri, rm_cand)
                                )

                            existing_cands = [
                                c
                                for c in (
                                    primary_cand,
                                    *(rc for _, _, _, rc in recovery_coords),
                                )
                                if c is not None and c.exists()
                            ]
                            resolved_digest: Optional[Digest] = None
                            if existing_cands:
                                first_cand = existing_cands[0]
                                if trust_v04_hashes:
                                    resolved_digest = _try_trust_v04_digest(
                                        old_hash=old_hash,
                                        old_semantics=old_semantics,
                                        driver=driver,
                                        member=member_val,
                                        candidate_path=first_cand,
                                        meta=meta,
                                    )
                                    if resolved_digest is not None:
                                        artifacts_trusted += 1
                                if resolved_digest is None:
                                    resolved_digest = _compute_migrated_digest(
                                        first_cand,
                                        driver=driver,
                                        member=member_val,
                                        meta=meta,
                                    )
                                    artifacts_hashed += 1
                            else:
                                artifacts_cold += 1
                                if old_hash:
                                    meta["legacy_v04_hash"] = old_hash

                            digest_str = (
                                str(resolved_digest)
                                if resolved_digest is not None
                                else None
                            )
                            primary_witness = (
                                _capture_location_witness(
                                    primary_cand,
                                    driver=driver,
                                    meta=meta,
                                    observed_at_ns=obs_ns,
                                )
                                if (primary_cand is not None and primary_cand.exists())
                                else None
                            )
                            location_rows.append(
                                ArtifactLocationRow.create(
                                    digest=digest_str,
                                    artifact_id=art_id,
                                    site=p_site,
                                    mount_name=p_mount,
                                    mount_instance_id=p_inst,
                                    uri=p_uri,
                                    member=member_val or "",
                                    witness=primary_witness,
                                    observed_at_ns=obs_ns,
                                )
                            )
                            for r_idx, (rm_name, rm_inst, rm_uri, rm_cand) in enumerate(
                                recovery_coords, start=1
                            ):
                                rec_exists = rm_cand is not None and rm_cand.exists()
                                rec_witness = (
                                    _capture_location_witness(
                                        rm_cand,
                                        driver=driver,
                                        meta=meta,
                                        observed_at_ns=obs_ns + r_idx,
                                    )
                                    if (rec_exists and rm_cand is not None)
                                    else None
                                )
                                location_rows.append(
                                    ArtifactLocationRow.create(
                                        digest=digest_str,
                                        artifact_id=art_id,
                                        site="archive" if rec_exists else "advisory",
                                        mount_name=rm_name,
                                        mount_instance_id=rm_inst,
                                        uri=rm_uri,
                                        member=member_val or "",
                                        witness=rec_witness,
                                        observed_at_ns=obs_ns + r_idx,
                                    )
                                )

                            artifact_insert_rows.append(
                                [
                                    art_id,
                                    key,
                                    uri,
                                    driver,
                                    digest_str,
                                    member_val,
                                    prod_run_id,
                                    parent_aid,
                                    facet_id,
                                    _dump_json(meta),
                                    created_at,
                                ]
                            )
                            artifacts_by_id[art_id] = _MigratedArtifactInfo(
                                id=art_id,
                                key=key,
                                driver=driver,
                                digest=resolved_digest,
                                member=member_val,
                            )

                    if artifact_insert_rows:
                        _execute_multirow_values(
                            cur,
                            prefix_sql=dst_db._ARTIFACT_INSERT_PREFIX,
                            row_placeholder="(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            suffix_sql=dst_db._ARTIFACT_UPSERT_SUFFIX,
                            rows=artifact_insert_rows,
                        )
                    if location_rows:
                        dst_db._upsert_artifact_locations_locked(cur, location_rows)

                    link_insert_rows = _promote_run_identities_and_links(
                        runs=runs, artifacts_by_id=artifacts_by_id, raw_links=raw_links
                    )
                    if runs:
                        _execute_multirow_values(
                            cur,
                            prefix_sql=(
                                "INSERT INTO run (id, parent_run_id, status, model_name, "
                                "description, year, iteration, stage, phase, tags, "
                                "config_hash, git_hash, input_hash, signature, cacheable, "
                                "facet_id, meta, started_at, ended_at, created_at, updated_at) VALUES"
                            ),
                            row_placeholder="(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            suffix_sql="ON CONFLICT (id) DO NOTHING",
                            rows=[r.to_insert_row() for r in runs],
                        )
                    if link_insert_rows:
                        _execute_multirow_values(
                            cur,
                            prefix_sql=(
                                "INSERT INTO run_artifact_link "
                                "(run_id, artifact_id, direction, role, is_implicit) VALUES"
                            ),
                            row_placeholder="(?, ?, ?, ?, ?)",
                            suffix_sql="ON CONFLICT (run_id, artifact_id, direction, role) DO NOTHING",
                            rows=link_insert_rows,
                        )

                    src_views = cur.execute(
                        "SELECT schema_name, view_name, sql FROM duckdb_views() "
                        "WHERE database_name = 'v04_src' AND NOT internal "
                        "AND view_name NOT IN ('consist_schema_links')"
                    ).fetchall()
                finally:
                    cur.execute("DETACH v04_src")

                for v_schema, _v_name, v_sql in src_views:
                    if not v_sql:
                        continue
                    s_str = str(v_schema)
                    if s_str != "main":
                        cur.execute(
                            f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(s_str)}"
                        )
                    cleaned_sql = str(v_sql).strip()
                    if cleaned_sql.upper().startswith("CREATE VIEW "):
                        cleaned_sql = (
                            "CREATE OR REPLACE VIEW "
                            + cleaned_sql[len("CREATE VIEW ") :]
                        )
                    cur.execute(cleaned_sql)
                    views_copied += 1

            dst_db.execute("CHECKPOINT")
        finally:
            dst_db.close()

        cacheable_count = sum(1 for r in runs if r.cacheable)
        bak_path: Optional[Path] = None
        if not dry_run:
            bak_path = Path(f"{src_path}.v04.bak")
            src_wal = Path(f"{src_path}.wal")
            bak_wal = Path(f"{bak_path}.wal")
            bak_wal.unlink(missing_ok=True)
            os.replace(src_path, bak_path)
            if src_wal.exists():
                os.replace(src_wal, bak_wal)
            os.replace(tmp_path, src_path)
            tmp_wal = Path(f"{tmp_path}.wal")
            if tmp_wal.exists():
                os.replace(tmp_wal, src_wal)

        return MigrationReport(
            db_path=src_path,
            backup_path=bak_path,
            dry_run=dry_run,
            runs_migrated=len(runs),
            cacheable_runs=cacheable_count,
            uncacheable_runs=len(runs) - cacheable_count,
            artifacts_migrated=len(artifact_insert_rows),
            artifacts_hashed=artifacts_hashed,
            artifacts_trusted=artifacts_trusted,
            artifacts_cold_unverified=artifacts_cold,
            locations_created=len(location_rows),
            global_tables_copied=global_tables_copied,
            views_copied=views_copied,
        )
    finally:
        if temp_dir_ctx is not None:
            temp_dir_ctx.cleanup()


def undo_migrate_database(db_path: str | os.PathLike[str]) -> Path:
    """Atomically restore ``<db_path>.v04.bak`` back to ``<db_path>``."""
    target = Path(db_path).expanduser().resolve()
    bak_path = Path(f"{target}.v04.bak")
    if not bak_path.is_file():
        raise FileNotFoundError(
            f"Backup database not found: {bak_path}. Cannot undo migration."
        )
    target_wal = Path(f"{target}.wal")
    bak_wal = Path(f"{bak_path}.wal")
    target_wal.unlink(missing_ok=True)
    os.replace(bak_path, target)
    if bak_wal.exists():
        os.replace(bak_wal, target_wal)
    return target
