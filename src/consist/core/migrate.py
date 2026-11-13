"""Cold-safe database migration from Consist v0.4 (schema v1) to v0.5 (schema v2)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import tempfile
import time
from typing import Any, Optional
import uuid

import duckdb

from consist.core.fs import FileSystemManager, build_verified_location_row
from consist.core.identity import IdentityManager
from consist.core.persistence import (
    DatabaseManager,
    IncompatibleSchemaError,
    _coerce_optional_uuid,
    _coerce_uuid,
    _dump_json,
    _ensure_non_null_utc_dt,
    _execute_multirow_values,
    _parse_json_dict,
    _quote_ident,
)
from consist.core.resolved_binding import _identity_payload_from_binding_evidence
from consist.core.run_resolution import format_input_link_role
from consist.digest import (
    Digest,
    build_manifest_v1,
    build_shapefile_manifest_v1,
    canonical_json_sha256,
    compute_digest,
    normalize_logical_matrices_identity,
)
from consist.location import ArtifactLocationRow, LocationSite

logger = logging.getLogger(__name__)

_LOWER_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_WEAK_SCHEMES = frozenset(
    {"stat_file_v1:sha256", "stat_dir_v1:sha256", "caller_supplied_v1:sha256"}
)
_STRICT_SCHEMES = frozenset({"sha256:file", "dir_manifest_v1:sha256"})
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


@dataclass(frozen=True, slots=True)
class _MigratedArtifactInfo:
    id: uuid.UUID
    key: str
    uri: str
    driver: str
    digest: Optional[Digest]
    member: Optional[str]
    meta: Mapping[str, Any]


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


def _copy_non_core_tables_and_views(
    cur: duckdb.DuckDBPyConnection, *, dst_catalog: str, src_tables: set[str]
) -> tuple[int, list[tuple[Any, ...]]]:
    global_tables_copied = 0
    for raw_s, raw_t in cur.execute(
        "SELECT schema_name, table_name FROM duckdb_tables() "
        "WHERE database_name = 'v04_src' AND schema_name != 'main' AND NOT internal"
    ).fetchall():
        qs, qt = _quote_ident(str(raw_s)), _quote_ident(str(raw_t))
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {qs}")
        cur.execute(f"CREATE TABLE {qs}.{qt} AS SELECT * FROM v04_src.{qs}.{qt}")
        if str(raw_s) == "global_tables":
            global_tables_copied += 1

    for custom_tbl in sorted(src_tables - _CORE_MAIN_TABLES):
        qc = _quote_ident(custom_tbl)
        cur.execute(f"CREATE TABLE main.{qc} AS SELECT * FROM v04_src.main.{qc}")

    for tbl in _PASSTHROUGH_CORE_TABLES:
        if tbl not in src_tables:
            continue
        dst_cols = _table_columns(cur, db_alias=dst_catalog, schema="main", table=tbl)
        src_cols = set(
            _table_columns(cur, db_alias="v04_src", schema="main", table=tbl)
        )
        pairs: list[tuple[str, str]] = []
        for c in dst_cols:
            if c in src_cols:
                src_expr = (
                    "COALESCE(\"json\", CAST('{}' AS JSON))"
                    if tbl in ("config_facet", "artifact_facet") and c == "json"
                    else _quote_ident(c)
                )
                pairs.append((_quote_ident(c), src_expr))
            elif c == "json" and "facet_json" in src_cols:
                src_expr = (
                    "COALESCE(\"facet_json\", CAST('{}' AS JSON))"
                    if tbl in ("config_facet", "artifact_facet")
                    else '"facet_json"'
                )
                pairs.append((_quote_ident(c), src_expr))
        if pairs:
            qt = _quote_ident(tbl)
            d_sql = ", ".join(d for d, _ in pairs)
            s_sql = ", ".join(s for _, s in pairs)
            cur.execute(
                f"INSERT INTO main.{qt} ({d_sql}) "
                f"SELECT {s_sql} FROM v04_src.main.{qt} ON CONFLICT DO NOTHING"
            )

    src_views = cur.execute(
        "SELECT schema_name, view_name, sql FROM duckdb_views() "
        "WHERE database_name = 'v04_src' AND NOT internal "
        "AND view_name NOT IN ('consist_schema_links')"
    ).fetchall()
    return global_tables_copied, src_views


def _copy_v04_runs_sql(cur: duckdb.DuckDBPyConnection, src_tables: set[str]) -> None:
    if "run" not in src_tables:
        return
    cols = set(_table_columns(cur, db_alias="v04_src", schema="main", table="run"))
    col = lambda n, d="NULL": _quote_ident(n) if n in cols else d  # noqa: E731
    meta_r, tags_r = col("meta", "'{}'"), col("tags", "'[]'")
    meta_e = (
        f"CASE WHEN {meta_r} IS NULL THEN CAST('{{}}' AS JSON) "
        f"WHEN json_valid(CAST({meta_r} AS VARCHAR)) THEN CAST({meta_r} AS JSON) "
        "ELSE CAST('{}' AS JSON) END"
    )
    tags_e = (
        f"CASE WHEN {tags_r} IS NULL THEN CAST('[]' AS JSON) "
        f"WHEN ends_with(typeof({tags_r}), '[]') THEN to_json({tags_r}) "
        f"WHEN json_valid(CAST({tags_r} AS VARCHAR)) THEN CAST({tags_r} AS JSON) "
        "ELSE CAST('[]' AS JSON) END"
    )
    started_e = f"COALESCE({col('started_at')}, {col('created_at')}, current_timestamp)"
    created_e = f"COALESCE({col('created_at')}, {col('started_at')}, current_timestamp)"
    updated_e = f"COALESCE({col('updated_at')}, {col('created_at')}, {col('started_at')}, current_timestamp)"
    cur.execute(
        "INSERT INTO main.run ("
        "id, parent_run_id, status, model_name, description, year, iteration, "
        "stage, phase, tags, config_hash, git_hash, input_hash, signature, "
        "cacheable, facet_id, meta, started_at, ended_at, created_at, updated_at"
        ") SELECT "
        f"{col('id')}, {col('parent_run_id')}, COALESCE({col('status')}, 'completed'), "
        f"COALESCE({col('model_name')}, 'unknown'), {col('description')}, "
        f"{col('year')}, {col('iteration')}, "
        f"COALESCE({col('stage')}, json_extract_string({meta_e}, '$.stage')), "
        f"COALESCE({col('phase')}, json_extract_string({meta_e}, '$.phase')), "
        f"{tags_e}, {col('config_hash')}, {col('git_hash')}, {col('input_hash')}, "
        f"{col('signature')}, FALSE, {col('facet_id')}, {meta_e}, {started_e}, "
        f"{col('ended_at')}, {created_e}, {updated_e} "
        "FROM v04_src.main.run ON CONFLICT (id) DO NOTHING"
    )


def _extract_recovery_roots(raw: Any) -> list[str]:
    if raw is None:
        return []
    seq = (
        [raw]
        if isinstance(raw, str)
        else (
            raw
            if isinstance(raw, Sequence) and not isinstance(raw, (bytes, bytearray))
            else ()
        )
    )
    out: list[str] = []
    for item in seq:
        if isinstance(item, (str, os.PathLike)):
            text = os.fspath(item).strip()
            if text and text not in out:
                out.append(text)
    return out


def _try_trust_v04_digest(
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
        hx = text[len("sha256:manifest-v1:") :].lower()
        return (
            Digest("dir_manifest_v1:sha256", hx)
            if _LOWER_HEX64_RE.fullmatch(hx)
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
    candidate_path: Path, driver: str, member: Optional[str], meta: dict[str, Any]
) -> Digest:
    options: dict[str, Any] = {}
    if driver == "artifact_set" or meta.get("artifact_set") is True:
        if isinstance(meta.get("output_set_member_paths"), Mapping):
            options["member_paths"] = meta["output_set_member_paths"]
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


def _migrate_artifacts_and_locations(
    cur: duckdb.DuckDBPyConnection,
    *,
    src_tables: set[str],
    run_meta_by_id: Mapping[str, dict[str, Any]],
    linked_runs_by_art: Mapping[uuid.UUID, Sequence[str]],
    src_path: Path,
    cli_run_dir: Optional[Path],
    cli_mounts: Mapping[str, str],
    trust_v04_hashes: bool,
) -> tuple[
    list[list[Any]],
    list[ArtifactLocationRow],
    dict[uuid.UUID, _MigratedArtifactInfo],
    tuple[int, int, int],
]:
    if "artifact" not in src_tables:
        return [], [], {}, (0, 0, 0)

    content_hash_by_id: dict[str, str] = {}
    if "artifact_content" in src_tables:
        ac_cols = set(
            _table_columns(
                cur, db_alias="v04_src", schema="main", table="artifact_content"
            )
        )
        h_col = (
            "content_hash"
            if "content_hash" in ac_cols
            else ("hash" if "hash" in ac_cols else None)
        )
        if "id" in ac_cols and h_col is not None:
            content_hash_by_id = {
                str(cid): str(chash)
                for cid, chash in cur.execute(
                    f"SELECT id, {_quote_ident(h_col)} FROM v04_src.main.artifact_content"
                ).fetchall()
                if cid is not None and chash
            }

    a_cols = set(
        _table_columns(cur, db_alias="v04_src", schema="main", table="artifact")
    )
    u_expr = (
        'COALESCE("container_uri", "uri")'
        if {"container_uri", "uri"}.issubset(a_cols)
        else ('"container_uri"' if "container_uri" in a_cols else '"uri"')
    )
    defaults = {"key": "'artifact'", "driver": "'unknown'", "meta": "'{}'"}
    tail_cols = (
        "driver",
        "table_path",
        "array_path",
        "member",
        "hash",
        "digest",
        "content_id",
        "run_id",
        "parent_artifact_id",
        "facet_id",
        "meta",
        "created_at",
    )
    a_exprs = [
        _quote_ident("id") if "id" in a_cols else "NULL",
        _quote_ident("key") if "key" in a_cols else "'artifact'",
        u_expr,
        *(
            _quote_ident(c) if c in a_cols else defaults.get(c, "NULL")
            for c in tail_cols
        ),
    ]
    raw_artifacts = cur.execute(
        f"SELECT {', '.join(a_exprs)} FROM v04_src.main.artifact"
    ).fetchall()

    fs_cache: dict[tuple[Path, tuple[tuple[str, str], ...]], FileSystemManager] = {}
    fs_by_run_id: dict[Optional[str], FileSystemManager] = {}

    def _get_fs(eff_rd: Path, eff_mounts: Mapping[str, str]) -> FileSystemManager:
        key = (eff_rd, tuple(sorted(eff_mounts.items())))
        if key not in fs_cache:
            fs_cache[key] = FileSystemManager(
                run_dir=eff_rd, mounts=dict(eff_mounts), create_run_dir=False
            )
        return fs_cache[key]

    def _fs_for_run(run_id: Optional[str]) -> FileSystemManager:
        cached_fs = fs_by_run_id.get(run_id)
        if cached_fs is not None:
            return cached_fs
        rm_meta = run_meta_by_id.get(run_id) if run_id else None
        rm = rm_meta.get("mounts") if rm_meta else None
        hm = (
            {k: v for k, v in rm.items() if isinstance(k, str) and isinstance(v, str)}
            if isinstance(rm, Mapping)
            else {}
        )
        rrd = rm_meta.get("_physical_run_dir") if rm_meta else None
        hrd = (
            Path(rrd).expanduser().resolve()
            if isinstance(rrd, str) and rrd.strip()
            else None
        )
        resolved_fs = _get_fs(
            hrd or cli_run_dir or src_path.parent, {**hm, **cli_mounts}
        )
        fs_by_run_id[run_id] = resolved_fs
        return resolved_fs

    def _probe(fs: FileSystemManager, uri: str) -> tuple[LocationSite, Optional[Path]]:
        m_name, m_inst, m_uri = fs.location_coordinate_for_uri(uri)
        site: LocationSite = (
            "workspace" if m_name in ("workspace", "runs") else "external"
        )
        return site, fs.resolve_location_coordinate(m_name, m_inst, m_uri)

    artifact_rows: list[list[Any]] = []
    location_rows: list[ArtifactLocationRow] = []
    artifacts_by_id: dict[uuid.UUID, _MigratedArtifactInfo] = {}
    hashed = trusted = cold = 0
    obs_base_ns = time.time_ns()

    for a_idx, a_row in enumerate(raw_artifacts):
        art_id = _coerce_uuid(a_row[0])
        key, uri, driver = str(a_row[1]), str(a_row[2]), str(a_row[3])
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
        ) or (content_hash_by_id.get(str(a_row[9])) if a_row[9] is not None else None)

        prod_run_id = str(a_row[10]) if a_row[10] is not None else None
        meta = _parse_json_dict(a_row[13])
        recovery_roots = _extract_recovery_roots(meta.pop("recovery_roots", None))
        old_semantics = meta.pop("hash_semantics", None)
        meta.pop("content_identity", None)

        cand_rids = [
            r
            for r in [prod_run_id, *linked_runs_by_art.get(art_id, ())]
            if r and r in run_meta_by_id
        ]
        obs_ns = obs_base_ns + a_idx * 8
        fs = primary_fs = _fs_for_run(cand_rids[0] if cand_rids else None)
        p_site, primary_cand = _probe(fs, uri)
        if (primary_cand is None or not primary_cand.exists()) and len(cand_rids) > 1:
            for alt_rid in cand_rids[1:]:
                alt_fs = _fs_for_run(alt_rid)
                asite, acand = _probe(alt_fs, uri)
                if acand is not None and acand.exists():
                    fs = primary_fs = alt_fs
                    p_site, primary_cand = asite, acand
                    break
        if (
            (primary_cand is None or not primary_cand.exists())
            and cli_run_dir is not None
            and cli_run_dir != fs.run_dir
            and p_site == "workspace"
        ):
            cli_fs = _get_fs(cli_run_dir, {**fs.mounts, **cli_mounts})
            _, acand = _probe(cli_fs, uri)
            if acand is not None and acand.exists():
                primary_fs, primary_cand = cli_fs, acand

        rec_targets = [
            (
                r_root,
                None if str(t).startswith("/__unmounted__/") else t,
            )
            for r_root in recovery_roots
            for _, _, _, t in (fs.archive_coordinate_for_root(uri, r_root),)
        ]
        existing_cands = [
            c
            for c in (primary_cand, *(rc for _, rc in rec_targets))
            if c is not None and c.exists()
        ]
        resolved_digest: Optional[Digest] = None
        if existing_cands:
            if trust_v04_hashes:
                resolved_digest = _try_trust_v04_digest(
                    old_hash, old_semantics, driver, member_val, existing_cands[0], meta
                )
                if resolved_digest is not None:
                    trusted += 1
            if resolved_digest is None:
                resolved_digest = _compute_migrated_digest(
                    existing_cands[0], driver, member_val, meta
                )
                hashed += 1
        else:
            cold += 1
            if old_hash:
                meta["legacy_v04_hash"] = old_hash

        art_info = _MigratedArtifactInfo(
            art_id, key, uri, driver, resolved_digest, member_val, meta
        )
        artifacts_by_id[art_id] = art_info
        p_ok = primary_cand is not None and primary_cand.exists()
        location_rows.append(
            build_verified_location_row(
                primary_fs,
                art_info,
                local_path=primary_cand if p_ok else None,
                uri=uri,
                site=p_site,
                status="active" if p_ok else "stale",
                capture_local_witness=p_ok,
                observed_at_ns=obs_ns,
            )
        )
        for r_idx, (r_root, rm_cand) in enumerate(rec_targets, start=1):
            r_ok = rm_cand is not None and rm_cand.exists()
            location_rows.append(
                build_verified_location_row(
                    fs,
                    art_info,
                    local_path=rm_cand if r_ok else None,
                    uri=uri,
                    archive_root=r_root,
                    site="archive" if r_ok else "advisory",
                    capture_local_witness=r_ok,
                    observed_at_ns=obs_ns + r_idx,
                )
            )
        artifact_rows.append(
            [
                art_id,
                key,
                uri,
                driver,
                str(resolved_digest) if resolved_digest is not None else None,
                member_val,
                prod_run_id,
                _coerce_optional_uuid(a_row[11]),
                str(a_row[12]) if a_row[12] is not None else None,
                _dump_json(meta),
                _ensure_non_null_utc_dt(a_row[14] or datetime.now(timezone.utc)),
            ]
        )

    return artifact_rows, location_rows, artifacts_by_id, (hashed, trusted, cold)


def _upgrade_strict_binding_evidence(
    evidence_json: str, artifacts_by_id: Mapping[uuid.UUID, _MigratedArtifactInfo]
) -> Optional[tuple[str, str]]:
    try:
        evidence = json.loads(evidence_json)
        inputs = evidence.get("inputs")
        if not isinstance(inputs, dict) or not inputs:
            return None
        adm = evidence.get("admission_evidence")
        for key, item in inputs.items():
            art_blk = item.get("artifact") if isinstance(item, dict) else None
            if not isinstance(art_blk, dict) or not art_blk.get("artifact_id"):
                return None
            info = artifacts_by_id.get(_coerce_uuid(art_blk["artifact_id"]))
            if (
                info is None
                or info.digest is None
                or info.digest.scheme not in _STRICT_SCHEMES
            ):
                return None
            art_blk["identity"] = str(info.digest)
            if isinstance(adm, dict) and isinstance(adm.get(key), dict):
                adm[key]["observed_identity"] = str(info.digest)
        upgraded_json = json.dumps(
            evidence, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        )
        payload = _identity_payload_from_binding_evidence(upgraded_json)
        digest = hashlib.sha256(
            json.dumps(
                payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True
            ).encode("utf-8")
        ).hexdigest()
        return digest, upgraded_json
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


def _promote_run_identities_and_links(
    *,
    run_rows: Sequence[tuple[str, str, Optional[str], Optional[str], dict[str, Any]]],
    artifacts_by_id: Mapping[uuid.UUID, _MigratedArtifactInfo],
    raw_links: Sequence[tuple[str, uuid.UUID, str, str, bool]],
    invocations_by_exec_run: Mapping[str, list[tuple[uuid.UUID, str]]],
) -> tuple[list[list[Any]], list[list[Any]], list[list[Any]]]:
    identity_mgr = IdentityManager()
    inputs_by_run: dict[str, list[tuple[uuid.UUID, str, bool]]] = {}
    outputs_by_run: dict[str, list[tuple[uuid.UUID, str, bool]]] = {}
    for run_id, aid, direction, raw_role, is_implicit in raw_links:
        (inputs_by_run if direction == "input" else outputs_by_run).setdefault(
            run_id, []
        ).append((aid, raw_role, is_implicit))

    link_dedup: dict[tuple[str, uuid.UUID, str, str], bool] = {}
    run_updates: list[list[Any]] = []
    inv_updates: list[list[Any]] = []

    def _resolve_aid(
        b: Mapping[str, Any],
        run_inputs: Sequence[tuple[uuid.UUID, str, bool]],
        idx: int,
    ) -> Optional[uuid.UUID]:
        if b.get("artifact_id") is not None:
            try:
                return _coerce_uuid(b["artifact_id"])
            except (ValueError, TypeError):
                return None
        return run_inputs[idx][0] if 0 <= idx < len(run_inputs) else None

    for run_id, status, git_hash, config_hash, meta in run_rows:
        run_inputs = inputs_by_run.get(run_id, [])
        id_map = (
            meta["input_identity"]
            if isinstance(meta.get("input_identity"), Mapping)
            else {}
        )
        bind_map = (
            meta["input_binding"]
            if isinstance(meta.get("input_binding"), Mapping)
            else {}
        )
        raw_sc = id_map.get("strict_input_count", 0)
        strict_cnt = (
            int(raw_sc)
            if isinstance(raw_sc, int) and not isinstance(raw_sc, bool) and raw_sc > 0
            else 0
        )
        strict_id_raw = id_map.get("strict_binding_identity")
        has_strict = strict_cnt > 0 or (
            isinstance(strict_id_raw, str) and bool(strict_id_raw.strip())
        )

        roles_by_aid: dict[uuid.UUID, list[str]] = {}
        for blk_bindings, offset in (
            (bind_map.get("bindings"), 0),
            (id_map.get("bindings"), strict_cnt),
        ):
            if not isinstance(blk_bindings, Sequence) or isinstance(
                blk_bindings, (str, bytes)
            ):
                continue
            for idx, b in enumerate(blk_bindings):
                if not isinstance(b, Mapping):
                    continue
                kind_raw, role_name = b.get("kind"), b.get("role")
                if role_name is not None:
                    kind_str = kind_raw if isinstance(kind_raw, str) else None
                    aid = _resolve_aid(b, run_inputs, idx + offset)
                    if aid is not None:
                        r_val = format_input_link_role(kind_str, role_name)
                        lst = roles_by_aid.setdefault(aid, [])
                        if r_val not in lst:
                            lst.append(r_val)

        for aid, raw_role, is_imp in run_inputs:
            info = artifacts_by_id.get(aid)
            roles = roles_by_aid.get(aid) or [
                raw_role
                if (raw_role and raw_role != "input")
                else (info.key if info else "")
            ]
            for role_str in roles:
                k = (run_id, aid, "input", role_str)
                link_dedup[k] = link_dedup.get(k, False) or bool(is_imp)

        for aid, raw_role, is_imp in outputs_by_run.get(run_id, []):
            info = artifacts_by_id.get(aid)
            role_str = (
                raw_role
                if (raw_role and raw_role != "output")
                else (info.key if info else "")
            )
            k = (run_id, aid, "output", role_str)
            link_dedup[k] = link_dedup.get(k, False) or bool(is_imp)

        if status != "completed" or not git_hash or not config_hash:
            continue

        all_strong = True
        upgraded_strict_id: Optional[str] = None
        if has_strict:
            strict_ok = len(run_inputs) >= strict_cnt and all(
                (si := artifacts_by_id.get(s_aid)) is not None
                and si.digest is not None
                and si.digest.scheme in _STRICT_SCHEMES
                for s_aid, _, _ in run_inputs[:strict_cnt]
            )
            inv_list = invocations_by_exec_run.get(run_id, [])
            upgraded = (
                _upgrade_strict_binding_evidence(inv_list[0][1], artifacts_by_id)
                if strict_ok and inv_list
                else None
            )
            if upgraded is None:
                continue
            upgraded_strict_id, upgraded_ev_json = upgraded
            for inv_id, _ in inv_list:
                inv_updates.append([upgraded_ev_json, inv_id])

        pairs: list[tuple[str, str]] = []
        upgraded_bindings: list[dict[str, Any]] = []
        ord_src = (
            (id_map["bindings"], strict_cnt)
            if isinstance(id_map.get("bindings"), list)
            else (
                (bind_map["bindings"], 0)
                if not has_strict and isinstance(bind_map.get("bindings"), list)
                else None
            )
        )
        if ord_src is not None:
            src_bindings, offset = ord_src
            for idx, b in enumerate(src_bindings):
                if not isinstance(b, Mapping):
                    all_strong = False
                    break
                kind, role_val = str(b.get("kind", "named")), b.get("role", idx)
                aid_lookup = _resolve_aid(b, run_inputs, idx + offset)
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
        elif not has_strict:
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
            continue
        if has_strict and upgraded_strict_id is not None:
            pairs.append(
                ("__strict_binding__", f"strict_binding_v1:{upgraded_strict_id}")
            )

        pairs.sort()
        new_in_hash = canonical_json_sha256(pairs)
        new_sig = identity_mgr.calculate_run_signature(
            git_hash, config_hash, new_in_hash
        )
        updated_meta = dict(meta)
        if ord_src is not None or has_strict:
            new_id_meta: dict[str, Any] = {"version": 1, "bindings": upgraded_bindings}
            for pk in ("strict_input_count", "ordinary_input_count"):
                if pk in id_map:
                    new_id_meta[pk] = id_map[pk]
            if upgraded_strict_id is not None:
                new_id_meta["strict_binding_identity"] = upgraded_strict_id
            updated_meta["input_identity"] = new_id_meta
        run_updates.append(
            [new_in_hash, new_sig, True, _dump_json(updated_meta), run_id]
        )

    link_rows = [
        [rid, aid, d, r_str, imp] for (rid, aid, d, r_str), imp in link_dedup.items()
    ]
    return link_rows, run_updates, inv_updates


def _atomic_swap_duckdb_files(src_path: Path, tmp_path: Path) -> Path:
    bak_path = Path(f"{src_path}.v04.bak")
    src_wal, bak_wal, tmp_wal = (
        Path(f"{src_path}.wal"),
        Path(f"{bak_path}.wal"),
        Path(f"{tmp_path}.wal"),
    )
    bak_wal.unlink(missing_ok=True)
    moved_src = moved_wal = False
    try:
        os.replace(src_path, bak_path)
        moved_src = True
        if src_wal.exists():
            os.replace(src_wal, bak_wal)
            moved_wal = True
        os.replace(tmp_path, src_path)
        if tmp_wal.exists():
            os.replace(tmp_wal, src_wal)
    except BaseException:
        if moved_src and bak_path.exists() and not src_path.exists():
            try:
                os.replace(bak_path, src_path)
            except OSError:
                pass
        if moved_wal and bak_wal.exists() and not src_wal.exists():
            try:
                os.replace(bak_wal, src_wal)
            except OSError:
                pass
        raise
    return bak_path


def _inspect_source_schema(src_path: Path) -> tuple[bool, Optional[str]]:
    existing_db_uid: Optional[str] = None
    probe = duckdb.connect(str(src_path), read_only=True)
    try:
        main_tables = {
            str(r[0])
            for r in probe.execute(
                "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main'"
            ).fetchall()
        }
        if "consist_schema" not in main_tables:
            return False, None
        ver_row = probe.execute("SELECT MAX(version) FROM consist_schema").fetchone()
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
            return True, existing_db_uid
        return False, existing_db_uid
    finally:
        probe.close()


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

    already_current, existing_db_uid = _inspect_source_schema(src_path)
    if already_current:
        return MigrationReport(db_path=src_path, already_current=True, dry_run=dry_run)

    cli_run_dir = Path(run_dir).expanduser().resolve() if run_dir is not None else None
    cli_mounts = {
        str(k): str(Path(v).expanduser().resolve()) for k, v in (mounts or {}).items()
    }
    temp_dir_ctx = (
        tempfile.TemporaryDirectory(prefix="consist_migrate_dryrun_")
        if dry_run
        else tempfile.TemporaryDirectory(
            prefix=f".{src_path.name}.migrate-", dir=str(src_path.parent)
        )
    )
    try:
        tmp_path = Path(temp_dir_ctx.name) / f"{src_path.name}.v05.tmp"
        dst_db = DatabaseManager(str(tmp_path), db_uid=existing_db_uid)
        views_copied = 0
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
                    global_tables_copied, src_views = _copy_non_core_tables_and_views(
                        cur, dst_catalog=dst_catalog, src_tables=src_tables
                    )
                    _copy_v04_runs_sql(cur, src_tables)

                    run_rows: list[
                        tuple[str, str, Optional[str], Optional[str], dict[str, Any]]
                    ] = []
                    run_meta_by_id: dict[str, dict[str, Any]] = {}
                    for r_id, r_status, r_git, r_cfg, r_meta in cur.execute(
                        "SELECT id, status, git_hash, config_hash, meta FROM main.run"
                    ).fetchall():
                        rid, parsed_meta = str(r_id), _parse_json_dict(r_meta)
                        run_meta_by_id[rid] = parsed_meta
                        run_rows.append(
                            (
                                rid,
                                str(r_status or "completed"),
                                str(r_git) if r_git is not None else None,
                                str(r_cfg) if r_cfg is not None else None,
                                parsed_meta,
                            )
                        )

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

                    (
                        artifact_insert_rows,
                        location_rows,
                        artifacts_by_id,
                        (artifacts_hashed, artifacts_trusted, artifacts_cold),
                    ) = _migrate_artifacts_and_locations(
                        cur,
                        src_tables=src_tables,
                        run_meta_by_id=run_meta_by_id,
                        linked_runs_by_art=linked_runs_by_art,
                        src_path=src_path,
                        cli_run_dir=cli_run_dir,
                        cli_mounts=cli_mounts,
                        trust_v04_hashes=trust_v04_hashes,
                    )
                finally:
                    try:
                        cur.execute("DETACH v04_src")
                    except duckdb.Error:
                        pass

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

                invocations_by_exec_run: dict[str, list[tuple[uuid.UUID, str]]] = {}
                for inv_id, exec_rid, b_json in cur.execute(
                    "SELECT id, execution_run_id, binding_json FROM main.run_binding_invocation"
                ).fetchall():
                    if exec_rid and b_json:
                        invocations_by_exec_run.setdefault(str(exec_rid), []).append(
                            (_coerce_uuid(inv_id), str(b_json))
                        )

                link_rows, run_updates, inv_updates = _promote_run_identities_and_links(
                    run_rows=run_rows,
                    artifacts_by_id=artifacts_by_id,
                    raw_links=raw_links,
                    invocations_by_exec_run=invocations_by_exec_run,
                )
                if run_updates:
                    _execute_multirow_values(
                        cur,
                        prefix_sql=(
                            "UPDATE main.run AS r SET input_hash = v.in_h, signature = v.sig, "
                            "cacheable = v.cacheable, meta = CAST(v.meta AS JSON) FROM (VALUES"
                        ),
                        row_placeholder="(CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS BOOLEAN), CAST(? AS VARCHAR), CAST(? AS VARCHAR))",
                        suffix_sql=") AS v(in_h, sig, cacheable, meta, rid) WHERE r.id = v.rid",
                        rows=run_updates,
                    )
                if inv_updates:
                    _execute_multirow_values(
                        cur,
                        prefix_sql=(
                            "UPDATE main.run_binding_invocation AS rbi SET "
                            "binding_json = v.bjson FROM (VALUES"
                        ),
                        row_placeholder="(CAST(? AS VARCHAR), CAST(? AS UUID))",
                        suffix_sql=") AS v(bjson, iid) WHERE rbi.id = v.iid",
                        rows=inv_updates,
                    )
                if link_rows:
                    _execute_multirow_values(
                        cur,
                        prefix_sql=(
                            "INSERT INTO run_artifact_link "
                            "(run_id, artifact_id, direction, role, is_implicit) VALUES"
                        ),
                        row_placeholder="(?, ?, ?, ?, ?)",
                        suffix_sql="ON CONFLICT (run_id, artifact_id, direction, role) DO NOTHING",
                        rows=link_rows,
                    )

            for v_schema, _v_name, v_sql in src_views:
                if not v_sql:
                    continue
                s_str = str(v_schema)
                cleaned_sql = str(v_sql).strip()
                if cleaned_sql.upper().startswith("CREATE VIEW "):
                    cleaned_sql = (
                        "CREATE OR REPLACE VIEW " + cleaned_sql[len("CREATE VIEW ") :]
                    )
                try:
                    if s_str != "main":
                        dst_db.execute(
                            f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(s_str)}"
                        )
                    dst_db.execute(cleaned_sql)
                except duckdb.Error as exc:
                    logger.warning(
                        "Skipping stale or incompatible v0.4 view %s.%s during migration: %s",
                        s_str,
                        _v_name,
                        exc,
                    )
                else:
                    views_copied += 1

            dst_db.execute("CHECKPOINT")
        finally:
            dst_db.close()

        bak_path = None if dry_run else _atomic_swap_duckdb_files(src_path, tmp_path)
        cacheable_count = len(run_updates)
        return MigrationReport(
            db_path=src_path,
            backup_path=bak_path,
            dry_run=dry_run,
            runs_migrated=len(run_rows),
            cacheable_runs=cacheable_count,
            uncacheable_runs=len(run_rows) - cacheable_count,
            artifacts_migrated=len(artifact_insert_rows),
            artifacts_hashed=artifacts_hashed,
            artifacts_trusted=artifacts_trusted,
            artifacts_cold_unverified=artifacts_cold,
            locations_created=len(location_rows),
            global_tables_copied=global_tables_copied,
            views_copied=views_copied,
        )
    finally:
        temp_dir_ctx.cleanup()


def undo_migrate_database(db_path: str | os.PathLike[str]) -> Path:
    """Atomically restore ``<db_path>.v04.bak`` back to ``<db_path>``."""
    target = Path(db_path).expanduser().resolve()
    bak_path = Path(f"{target}.v04.bak")
    if not bak_path.is_file():
        raise FileNotFoundError(
            f"Backup database not found: {bak_path}. Cannot undo migration."
        )
    target_wal, bak_wal = Path(f"{target}.wal"), Path(f"{bak_path}.wal")
    target_wal.unlink(missing_ok=True)
    os.replace(bak_path, target)
    if bak_wal.exists():
        os.replace(bak_wal, target_wal)
    return target
