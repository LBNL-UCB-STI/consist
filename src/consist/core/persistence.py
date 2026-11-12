from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import itertools
import json
import logging
import math
import numbers
import os
from pathlib import Path
import re
import threading
import time
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
)
import uuid
import weakref

import duckdb
import pandas as pd
from pydantic import BaseModel, TypeAdapter

from consist.core._performance_attribution import _track_begin_run_phase
from consist.core._schema_model_introspection import (
    field_duckdb_type,
    field_is_primary_key,
    field_sql_column_name,
    model_table_name,
    model_table_schema,
    optional_model_table_name,
    schema_model_user_columns,
)
from consist.core.db_runtime import DB_CALL_PROFILER as _DB_CALL_PROFILER
from consist.core.db_snapshot import DatabaseSnapshotOps, snapshot_sidecar_path
from consist.core.facet_common import flatten_facet_values
from consist.core.provenance_writer import ProvenanceWriter
from consist.core.run_ordering import recent_run_order_by_sql
from consist.digest import Digest
from consist.location import (
    ArtifactLocationRow,
    DirTreeStat,
    FileStat,
    LocationSite,
    LocationStatus,
    Witness,
    witness_from_json,
)
from consist.models.artifact import Artifact
from consist.models.artifact_facet import ArtifactFacet
from consist.models.artifact_kv import ArtifactKV
from consist.models.artifact_schema import (
    ArtifactSchema,
    ArtifactSchemaField,
    ArtifactSchemaObservation,
    ArtifactSchemaRelation,
)
from consist.models.config_facet import ConfigFacet
from consist.models.run import (
    CouplerSnapshotPhase,
    Run,
    RunBindingInvocation,
    resolve_canonical_run_meta_field,
)
from consist.models.run_config_kv import RunConfigKV
from consist.types import ParameterizedSQL


MAX_JSON_DEPTH = 50
LIKE_ESCAPE_CHAR = "~"
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SAFE_JSON_KEY_RE = re.compile(r"^[A-Za-z0-9_.:/\-]+$")
_RELATION_TOKEN_RE = re.compile(r"__consist_rel_([0-9a-f]{16})")

SchemaProfileSource = Literal["file", "duckdb", "user_provided"]
_RowModelT = TypeVar("_RowModelT")


def _escape_like_literal(value: str) -> str:
    return (
        value.replace(LIKE_ESCAPE_CHAR, LIKE_ESCAPE_CHAR * 2)
        .replace("%", f"{LIKE_ESCAPE_CHAR}%")
        .replace("_", f"{LIKE_ESCAPE_CHAR}_")
    )


def _validate_identifier(identifier: str, *, label: str) -> str:
    if not _SAFE_IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(
            f"Invalid {label}. Only letters, numbers, and underscores are allowed, "
            "and the identifier must not start with a number."
        )
    return identifier


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _quote_qualified_identifier(name: str, *, label: str) -> str:
    parts = name.split(".")
    if not parts or any(part == "" for part in parts):
        raise ValueError(f"Invalid {label}: empty identifier component.")
    safe_parts = [_validate_identifier(part, label=label) for part in parts]
    return ".".join(_quote_ident(part) for part in safe_parts)


def _assert_json_depth(obj: Any, *, max_depth: int) -> None:
    stack: list[tuple[Any, int]] = [(obj, 1)]
    while stack:
        value, depth = stack.pop()
        if depth > max_depth:
            raise ValueError(f"JSON nesting depth exceeds limit of {max_depth}.")
        if isinstance(value, dict):
            stack.extend((v, depth + 1) for v in value.values())
        elif isinstance(value, list):
            stack.extend((v, depth + 1) for v in value)


def load_json_safe(json_string: str, *, max_depth: int = MAX_JSON_DEPTH) -> Any:
    try:
        parsed = json.loads(json_string)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON: {exc}") from exc
    _assert_json_depth(parsed, max_depth=max_depth)
    return parsed


def _coerce_run_tags(run_tags: Any) -> list[str]:
    if run_tags is None:
        return []
    if isinstance(run_tags, str):
        if not run_tags:
            return []
        try:
            parsed = load_json_safe(run_tags, max_depth=10)
        except ValueError:
            return []
        if isinstance(parsed, Sequence) and not isinstance(parsed, (str, bytes)):
            return [str(item) for item in parsed]
        return []
    if isinstance(run_tags, Sequence) and not isinstance(run_tags, (str, bytes)):
        return [str(item) for item in run_tags]
    return []


def _parse_json_dict(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, str):
        if not raw:
            return {}
        parsed = json.loads(raw)
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _parse_optional_json_dict(raw: Any) -> Optional[dict[str, Any]]:
    if raw is None:
        return None
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, str):
        if not raw:
            return None
        parsed = json.loads(raw)
        return dict(parsed) if isinstance(parsed, Mapping) else None
    return None


def _parse_optional_json_list_str(raw: Any) -> Optional[list[str]]:
    if raw is None:
        return None
    parsed = json.loads(raw) if isinstance(raw, str) else raw
    if isinstance(parsed, Sequence) and not isinstance(parsed, (str, bytes)):
        return [str(item) for item in parsed]
    return None


def _merge_persisted_artifact_meta(
    base_meta: Mapping[str, Any], current_meta: Mapping[str, Any]
) -> dict[str, Any]:
    merged_meta = dict(base_meta)
    merged_meta.update(current_meta)
    merged_meta.pop("recovery_roots", None)
    return merged_meta


def _json_default(value: Any) -> Any:
    if isinstance(value, (Path, os.PathLike)):
        return os.fspath(value)
    if isinstance(value, (uuid.UUID, Digest)):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, (set, frozenset)):
        return sorted(str(item) for item in value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _dump_json(obj: Any) -> str:
    return json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=_json_default,
    )


def _dump_optional_json(obj: Any) -> Optional[str]:
    if obj is None:
        return None
    return _dump_json(obj)


def _coerce_uuid(value: uuid.UUID | str) -> uuid.UUID:
    return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))


def _coerce_optional_uuid(value: Any) -> Optional[uuid.UUID]:
    if value is None:
        return None
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))


def _ensure_utc_dt(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _ensure_non_null_utc_dt(value: Optional[datetime]) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _profile_call_name(name: str, profile_label: Optional[str] = None) -> str:
    if not profile_label:
        return name
    return f"{name}[{profile_label}]"


def _strip_leading_sql_comments(sql: str) -> str:
    idx = 0
    n = len(sql)
    while idx < n:
        while idx < n and sql[idx].isspace():
            idx += 1
        if sql.startswith("--", idx):
            nl = sql.find("\n", idx + 2)
            idx = n if nl < 0 else nl + 1
            continue
        if sql.startswith("/*", idx):
            end = sql.find("*/", idx + 2)
            idx = n if end < 0 else end + 2
            continue
        break
    return sql[idx:]


_DISALLOWED_SELECT_LEADING_KEYWORDS: frozenset[str] = frozenset(
    {"PRAGMA", "PIVOT", "UNPIVOT", "DESCRIBE", "SUMMARIZE"}
)


def _validate_select_only_sql(sql: str) -> None:
    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("SQL query must be a non-empty SELECT statement")
    stripped = _strip_leading_sql_comments(sql)
    if not stripped:
        raise ValueError("SQL query must be a non-empty SELECT statement")
    first_word = stripped.split(None, 1)[0].rstrip("(").upper()
    if first_word in _DISALLOWED_SELECT_LEADING_KEYWORDS:
        raise ValueError(
            "query_df, query_rows, and run_query only permit a single read-only SELECT statement"
        )
    try:
        stmts = duckdb.extract_statements(sql)
    except duckdb.Error as exc:
        raise ValueError(f"Invalid SQL query: {exc}") from exc
    if not (len(stmts) == 1 and stmts[0].type == duckdb.StatementType.SELECT):  # ty: ignore[unresolved-attribute]
        raise ValueError(
            "query_df, query_rows, and run_query only permit a single read-only SELECT statement"
        )


class IncompatibleSchemaError(RuntimeError):
    """Raised when a Consist database uses an incompatible schema version."""


class DatabaseMigrationRequired(IncompatibleSchemaError):
    """Raised when opening a Consist v0.4 (version=1) database."""


class TransactionAbortedError(RuntimeError):
    """Raised when an outer re-entrant transaction exits after an inner transaction failed."""


@dataclass(frozen=True)
class ArtifactSchemaSelection:
    schema_id: str
    source: str
    candidate_count: int
    selection_rule: str


__all__ = [
    "ArtifactSchemaSelection",
    "DatabaseManager",
    "DatabaseMigrationRequired",
    "IncompatibleSchemaError",
    "ProvenanceWriter",
    "SchemaProfileSource",
    "TransactionAbortedError",
    "_validate_select_only_sql",
    "load_json_safe",
]


_LIVE_DB_LOCK = threading.Lock()
_LIVE_DB_MANAGERS: dict[str, weakref.WeakSet[DatabaseManager]] = {}
_GLOBAL_LOCATION_EPOCH = itertools.count(1)
_DB_PATH_LOCATION_EPOCH: dict[str, int] = {}
_TRACKED_RELATION_LOCK = threading.Lock()
_TRACKED_RELATIONS: dict[
    str,
    tuple[duckdb.DuckDBPyConnection, Optional[weakref.ReferenceType[DatabaseManager]]],
] = {}


def register_tracked_relation_cursor(
    token: str,
    cursor: duckdb.DuckDBPyConnection,
    db: Optional[DatabaseManager] = None,
) -> None:
    db_ref = weakref.ref(db) if db is not None else None
    with _TRACKED_RELATION_LOCK:
        _TRACKED_RELATIONS[token] = (cursor, db_ref)


def extract_tracked_relation_tokens(relation: duckdb.DuckDBPyRelation) -> list[str]:
    try:
        query_text = relation.sql_query()
    except Exception:
        return []
    if not isinstance(query_text, str):
        return []
    return list(dict.fromkeys(_RELATION_TOKEN_RE.findall(query_text)))


def close_tracked_relation_tokens(tokens: Sequence[str]) -> bool:
    closed_any = False
    for token in tokens:
        with _TRACKED_RELATION_LOCK:
            entry = _TRACKED_RELATIONS.pop(token, None)
        if entry is None:
            continue
        cursor, db_ref = entry
        db = db_ref() if db_ref is not None else None
        if db is not None:
            db._discard_open_cursor(token)
        try:
            cursor.close()
        except Exception:
            pass
        closed_any = True
    return closed_any


def tracked_relation_count() -> int:
    with _TRACKED_RELATION_LOCK:
        return len(_TRACKED_RELATIONS)


def close_all_tracked_relations() -> None:
    with _TRACKED_RELATION_LOCK:
        tokens = list(_TRACKED_RELATIONS.keys())
    close_tracked_relation_tokens(tokens)


def _live_managers_for_path(
    db_path: str | os.PathLike[str],
) -> list[DatabaseManager]:
    raw = str(db_path).strip()
    if not raw or raw == ":memory:":
        return []
    norm = str(Path(raw).expanduser().resolve())
    with _LIVE_DB_LOCK:
        return list(_LIVE_DB_MANAGERS.get(norm, ()))


def lookup_live_db_by_path(
    db_path: str | os.PathLike[str],
) -> Optional[DatabaseManager]:
    managers = _live_managers_for_path(db_path)
    for mgr in managers:
        if not mgr._closed and not mgr._suspended and not mgr.read_only:
            return mgr
    for mgr in managers:
        if not mgr._closed and not mgr._suspended:
            return mgr
    return None


def has_live_writable_db_for_path(db_path: str | os.PathLike[str]) -> bool:
    return any(
        not mgr._closed and not mgr._suspended and not mgr._con_read_only
        for mgr in _live_managers_for_path(db_path)
    )


_CORE_SCHEMA_DDL: tuple[str, ...] = (
    "CREATE TABLE IF NOT EXISTS consist_schema ("
    "version INTEGER PRIMARY KEY, db_uid VARCHAR NOT NULL, updated_at TIMESTAMPTZ NOT NULL)",
    "CREATE TABLE IF NOT EXISTS run ("
    "id VARCHAR PRIMARY KEY, parent_run_id VARCHAR, status VARCHAR NOT NULL DEFAULT 'running', "
    "model_name VARCHAR NOT NULL, description VARCHAR, year INTEGER, iteration INTEGER, "
    "stage VARCHAR, phase VARCHAR, tags JSON NOT NULL DEFAULT '[]', config_hash VARCHAR, "
    "git_hash VARCHAR, input_hash VARCHAR, signature VARCHAR, "
    "cacheable BOOLEAN NOT NULL DEFAULT TRUE, facet_id VARCHAR, meta JSON NOT NULL DEFAULT '{}', "
    "started_at TIMESTAMPTZ NOT NULL, ended_at TIMESTAMPTZ, "
    "created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL)",
    "CREATE INDEX IF NOT EXISTS ix_run_parent_run_id ON run (parent_run_id)",
    "CREATE INDEX IF NOT EXISTS ix_run_status ON run (status)",
    "CREATE INDEX IF NOT EXISTS ix_run_model_name ON run (model_name)",
    "CREATE INDEX IF NOT EXISTS ix_run_config_hash ON run (config_hash)",
    "CREATE INDEX IF NOT EXISTS ix_run_input_hash ON run (input_hash)",
    "CREATE INDEX IF NOT EXISTS ix_run_signature ON run (signature)",
    "CREATE TABLE IF NOT EXISTS artifact ("
    "id UUID PRIMARY KEY, key VARCHAR NOT NULL, uri VARCHAR NOT NULL, driver VARCHAR NOT NULL, "
    "digest VARCHAR, member VARCHAR, run_id VARCHAR, parent_artifact_id UUID, "
    "facet_id VARCHAR, meta JSON NOT NULL DEFAULT '{}', created_at TIMESTAMPTZ NOT NULL)",
    "CREATE INDEX IF NOT EXISTS ix_artifact_key ON artifact (key)",
    "CREATE INDEX IF NOT EXISTS ix_artifact_uri ON artifact (uri)",
    "CREATE INDEX IF NOT EXISTS ix_artifact_digest ON artifact (digest)",
    "CREATE INDEX IF NOT EXISTS ix_artifact_run_id ON artifact (run_id)",
    "CREATE INDEX IF NOT EXISTS ix_artifact_parent_artifact_id ON artifact (parent_artifact_id)",
    "CREATE TABLE IF NOT EXISTS artifact_location ("
    "location_id UUID PRIMARY KEY, digest VARCHAR, artifact_id UUID, "
    "site VARCHAR NOT NULL, mount_name VARCHAR NOT NULL, mount_instance_id VARCHAR NOT NULL, "
    "uri VARCHAR NOT NULL, member VARCHAR NOT NULL DEFAULT '', "
    "status VARCHAR NOT NULL DEFAULT 'active', witness_json JSON, observed_at_ns BIGINT NOT NULL)",
    "CREATE INDEX IF NOT EXISTS ix_artifact_location_digest_status ON artifact_location (digest, status)",
    "CREATE INDEX IF NOT EXISTS ix_artifact_location_artifact_status ON artifact_location (artifact_id, status)",
    "CREATE INDEX IF NOT EXISTS ix_artifact_location_coord_status ON artifact_location (site, mount_name, mount_instance_id, uri, member, status)",
    "CREATE TABLE IF NOT EXISTS run_artifact_link ("
    "run_id VARCHAR NOT NULL, artifact_id UUID NOT NULL, direction VARCHAR NOT NULL, "
    "role VARCHAR NOT NULL DEFAULT '', is_implicit BOOLEAN NOT NULL DEFAULT FALSE, "
    "PRIMARY KEY (run_id, artifact_id, direction, role))",
    "CREATE INDEX IF NOT EXISTS ix_run_artifact_link_run_id ON run_artifact_link (run_id)",
    "CREATE INDEX IF NOT EXISTS ix_run_artifact_link_artifact_id ON run_artifact_link (artifact_id)",
    "CREATE TABLE IF NOT EXISTS run_binding_invocation ("
    "id UUID PRIMARY KEY, requested_run_id VARCHAR NOT NULL, execution_run_id VARCHAR NOT NULL, "
    "cache_source_run_id VARCHAR, cache_outcome VARCHAR NOT NULL, "
    "binding_json VARCHAR NOT NULL, created_at TIMESTAMPTZ NOT NULL)",
    "CREATE INDEX IF NOT EXISTS ix_run_binding_invocation_requested ON run_binding_invocation (requested_run_id)",
    "CREATE TABLE IF NOT EXISTS config_facet ("
    "id VARCHAR PRIMARY KEY, namespace VARCHAR NOT NULL, schema_name VARCHAR NOT NULL, "
    "schema_version VARCHAR, \"json\" JSON NOT NULL DEFAULT '{}', created_at TIMESTAMPTZ NOT NULL)",
    "CREATE INDEX IF NOT EXISTS ix_config_facet_namespace ON config_facet (namespace)",
    "CREATE TABLE IF NOT EXISTS run_config_kv ("
    "run_id VARCHAR NOT NULL, facet_id VARCHAR NOT NULL, namespace VARCHAR NOT NULL, "
    "key VARCHAR NOT NULL, value_type VARCHAR NOT NULL, value_str VARCHAR, "
    "value_num DOUBLE, value_bool BOOLEAN, value_json JSON, created_at TIMESTAMPTZ NOT NULL, "
    "PRIMARY KEY (run_id, facet_id, namespace, key))",
    "CREATE INDEX IF NOT EXISTS ix_run_config_kv_run_id ON run_config_kv (run_id)",
    "CREATE TABLE IF NOT EXISTS artifact_facet ("
    "id VARCHAR PRIMARY KEY, namespace VARCHAR, schema_name VARCHAR, schema_version VARCHAR, "
    "\"json\" JSON NOT NULL DEFAULT '{}', created_at TIMESTAMPTZ NOT NULL)",
    "CREATE TABLE IF NOT EXISTS artifact_kv ("
    "artifact_id UUID NOT NULL, facet_id VARCHAR NOT NULL, key_path VARCHAR NOT NULL, "
    "namespace VARCHAR, value_type VARCHAR NOT NULL, value_str VARCHAR, "
    "value_num DOUBLE, value_bool BOOLEAN, value_json JSON, created_at TIMESTAMPTZ NOT NULL, "
    "PRIMARY KEY (artifact_id, facet_id, key_path))",
    "CREATE INDEX IF NOT EXISTS ix_artifact_kv_artifact_id ON artifact_kv (artifact_id)",
    "CREATE INDEX IF NOT EXISTS ix_artifact_kv_key_path ON artifact_kv (key_path)",
    "CREATE TABLE IF NOT EXISTS run_coupler_snapshot ("
    "run_id VARCHAR NOT NULL, phase VARCHAR NOT NULL, role_key VARCHAR NOT NULL, "
    "scenario_run_id VARCHAR NOT NULL, step_ordinal INTEGER NOT NULL DEFAULT 0, "
    "artifact_id UUID NOT NULL, producer_run_id VARCHAR, updated_at TIMESTAMPTZ NOT NULL, "
    "PRIMARY KEY (run_id, phase, role_key))",
    "CREATE INDEX IF NOT EXISTS ix_run_coupler_snapshot_scenario_ordinal ON run_coupler_snapshot (scenario_run_id, step_ordinal, phase)",
    "CREATE INDEX IF NOT EXISTS ix_run_coupler_snapshot_scenario_role ON run_coupler_snapshot (scenario_run_id, role_key)",
    "CREATE TABLE IF NOT EXISTS artifact_schema ("
    "id VARCHAR PRIMARY KEY, profile_version INTEGER NOT NULL DEFAULT 1, "
    "summary_json JSON NOT NULL DEFAULT '{}', profile_json JSON, created_at TIMESTAMPTZ NOT NULL)",
    "CREATE TABLE IF NOT EXISTS artifact_schema_field ("
    "schema_id VARCHAR NOT NULL, ordinal_position INTEGER, name VARCHAR NOT NULL, "
    "logical_type VARCHAR NOT NULL, nullable BOOLEAN NOT NULL DEFAULT TRUE, "
    "stats_json JSON, is_enum BOOLEAN NOT NULL DEFAULT FALSE, enum_values_json JSON, "
    "PRIMARY KEY (schema_id, name))",
    "CREATE TABLE IF NOT EXISTS artifact_schema_observation ("
    "id UUID PRIMARY KEY, artifact_id UUID NOT NULL, schema_id VARCHAR NOT NULL, "
    "run_id VARCHAR, source VARCHAR NOT NULL, sample_rows INTEGER, observed_at TIMESTAMPTZ NOT NULL)",
    "CREATE INDEX IF NOT EXISTS ix_artifact_schema_obs_artifact_id ON artifact_schema_observation (artifact_id)",
    "CREATE INDEX IF NOT EXISTS ix_artifact_schema_obs_schema_id ON artifact_schema_observation (schema_id)",
    "CREATE TABLE IF NOT EXISTS artifact_schema_relation ("
    "schema_id VARCHAR NOT NULL, from_field VARCHAR NOT NULL, to_table VARCHAR NOT NULL, "
    "to_field VARCHAR NOT NULL, relationship_type VARCHAR NOT NULL DEFAULT 'foreign_key', "
    "cardinality VARCHAR, PRIMARY KEY (schema_id, from_field))",
    "CREATE OR REPLACE VIEW consist_schema_links AS "
    "SELECT rel.schema_id, src.summary_json->>'$.table_name' AS from_table, "
    "rel.from_field, rel.to_table, rel.to_field, rel.relationship_type, rel.cardinality "
    "FROM artifact_schema_relation rel JOIN artifact_schema src ON rel.schema_id = src.id",
)

_RUN_SELECT_COLS = (
    "r.id, r.parent_run_id, r.status, r.model_name, r.description, "
    "r.year, r.iteration, r.stage, r.phase, r.tags, r.config_hash, "
    "r.git_hash, r.input_hash, r.signature, r.cacheable, r.meta, "
    "r.started_at, r.ended_at, r.created_at, r.updated_at"
)

_ARTIFACT_SELECT_COLS = (
    "a.id, a.key, a.uri, a.driver, a.digest, a.member, "
    "a.run_id, a.parent_artifact_id, a.facet_id, a.meta, a.created_at"
)


def _hydrate_run_row(row: Sequence[Any]) -> Run:
    return Run(
        id=str(row[0]),
        parent_run_id=str(row[1]) if row[1] is not None else None,
        status=str(row[2]),
        model_name=str(row[3]),
        description=str(row[4]) if row[4] is not None else None,
        year=int(row[5]) if row[5] is not None else None,
        iteration=int(row[6]) if row[6] is not None else None,
        stage=str(row[7]) if row[7] is not None else None,
        phase=str(row[8]) if row[8] is not None else None,
        tags=_coerce_run_tags(row[9]),
        config_hash=str(row[10]) if row[10] is not None else None,
        git_hash=str(row[11]) if row[11] is not None else None,
        input_hash=str(row[12]) if row[12] is not None else None,
        signature=str(row[13]) if row[13] is not None else None,
        cacheable=bool(row[14]) if row[14] is not None else True,
        meta=_parse_json_dict(row[15]),
        started_at=_ensure_non_null_utc_dt(row[16]),
        ended_at=_ensure_utc_dt(row[17]),
        created_at=_ensure_non_null_utc_dt(row[18]),
        updated_at=_ensure_non_null_utc_dt(row[19]),
    )


def _hydrate_artifact_row(row: Sequence[Any]) -> Artifact:
    raw_digest = row[4]
    parsed_digest = Digest.parse(str(raw_digest)) if raw_digest is not None else None
    return Artifact._from_trusted_db_row(
        id=_coerce_uuid(row[0]),
        key=str(row[1]),
        uri=str(row[2]),
        driver=str(row[3]),
        digest=parsed_digest,
        member=str(row[5]) if row[5] is not None else None,
        run_id=str(row[6]) if row[6] is not None else None,
        parent_artifact_id=_coerce_optional_uuid(row[7]),
        facet_id=str(row[8]) if row[8] is not None else None,
        owned_meta=_parse_json_dict(row[9]),
        created_at=_ensure_non_null_utc_dt(row[10]),
    )


def _hydrate_schema_row(row: Sequence[Any]) -> ArtifactSchema:
    return ArtifactSchema(
        id=str(row[0]),
        profile_version=int(row[1]) if row[1] is not None else 1,
        summary_json=_parse_json_dict(row[2]),
        profile_json=_parse_optional_json_dict(row[3]),
        created_at=_ensure_non_null_utc_dt(row[4]),
    )


def _hydrate_schema_field_row(row: Sequence[Any]) -> ArtifactSchemaField:
    return ArtifactSchemaField(
        schema_id=str(row[0]),
        ordinal_position=int(row[1]) if row[1] is not None else None,
        name=str(row[2]),
        logical_type=str(row[3]),
        nullable=bool(row[4]) if row[4] is not None else True,
        stats_json=_parse_optional_json_dict(row[5]),
        is_enum=bool(row[6]) if row[6] is not None else False,
        enum_values_json=_parse_optional_json_list_str(row[7]),
    )


def _hydrate_schema_observation_row(row: Sequence[Any]) -> ArtifactSchemaObservation:
    return ArtifactSchemaObservation(
        id=_coerce_uuid(row[0]),
        artifact_id=_coerce_uuid(row[1]),
        schema_id=str(row[2]),
        run_id=str(row[3]) if row[3] is not None else None,
        source=str(row[4]),
        sample_rows=int(row[5]) if row[5] is not None else None,
        observed_at=_ensure_non_null_utc_dt(row[6]),
    )


def _hydrate_schema_relation_row(row: Sequence[Any]) -> ArtifactSchemaRelation:
    return ArtifactSchemaRelation(
        schema_id=str(row[0]),
        from_field=str(row[1]),
        to_table=str(row[2]),
        to_field=str(row[3]),
        relationship_type=str(row[4]) if row[4] is not None else "foreign_key",
        cardinality=str(row[5]) if row[5] is not None else None,
    )


def _hydrate_config_facet_row(row: Sequence[Any]) -> ConfigFacet:
    return ConfigFacet(
        id=str(row[0]),
        namespace=str(row[1]),
        schema_name=str(row[2]),
        schema_version=str(row[3]) if row[3] is not None else None,
        facet_json=_parse_json_dict(row[4]),
        created_at=_ensure_non_null_utc_dt(row[5]),
    )


def _parse_optional_json_any(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, str):
        if not raw:
            return None
        return json.loads(raw)
    return raw


def _load_json_value_for_model(model: Any, col_name: str, value: Any) -> Any:
    if not isinstance(value, str):
        return value
    if model is dict:
        return value
    if isinstance(model, type) and issubclass(model, BaseModel):
        field_info = model.model_fields.get(col_name)
        if field_info is None:
            for fname, finfo in model.model_fields.items():
                if field_sql_column_name(fname, finfo) == col_name:
                    field_info = finfo
                    break
        if field_info is not None and field_duckdb_type(field_info) == "JSON":
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
    return value


def _hydrate_run_config_kv_row(row: Sequence[Any]) -> RunConfigKV:
    return RunConfigKV(
        run_id=str(row[0]),
        facet_id=str(row[1]),
        namespace=str(row[2]),
        key=str(row[3]),
        value_type=str(row[4]),
        value_str=str(row[5]) if row[5] is not None else None,
        value_num=float(row[6]) if row[6] is not None else None,
        value_bool=bool(row[7]) if row[7] is not None else None,
        value_json=_parse_optional_json_any(row[8]),
        created_at=_ensure_non_null_utc_dt(row[9]),
    )


def _hydrate_artifact_kv_row(row: Sequence[Any]) -> ArtifactKV:
    return ArtifactKV(
        artifact_id=_coerce_uuid(row[0]),
        facet_id=str(row[1]),
        key_path=str(row[2]),
        namespace=str(row[3]) if row[3] is not None else None,
        value_type=str(row[4]),
        value_str=str(row[5]) if row[5] is not None else None,
        value_num=float(row[6]) if row[6] is not None else None,
        value_bool=bool(row[7]) if row[7] is not None else None,
        value_json=_parse_optional_json_any(row[8]),
        created_at=_ensure_non_null_utc_dt(row[9]),
    )


def _decode_kv_value(
    value_type: str,
    value_str: Optional[str],
    value_num: Optional[float],
    value_bool: Optional[bool],
    value_json: Any,
) -> Any:
    if value_type == "str":
        return value_str
    if value_type == "int":
        return int(value_num) if value_num is not None else None
    if value_type == "float":
        return value_num
    if value_type == "bool":
        return value_bool
    if value_type == "null":
        return None
    return value_json


def _execute_multirow_values(
    cur: duckdb.DuckDBPyConnection,
    *,
    prefix_sql: str,
    row_placeholder: str,
    suffix_sql: str = "",
    rows: Sequence[Sequence[Any]],
) -> None:
    if not rows:
        return
    cols_per_row = max(1, len(rows[0]))
    chunk_size = max(1, min(500, 30_000 // cols_per_row))
    trailing = f" {suffix_sql}" if suffix_sql else ""
    for start in range(0, len(rows), chunk_size):
        chunk = rows[start : start + chunk_size]
        values_sql = ", ".join(row_placeholder for _ in chunk)
        flat_params = [val for row in chunk for val in row]
        cur.execute(f"{prefix_sql} {values_sql}{trailing}", flat_params)


class DatabaseManager:
    """Single-connection native DuckDB persistence manager for Consist v0.5."""

    def __init__(
        self,
        db_path: str | os.PathLike[str] = ":memory:",
        *,
        skip_schema_init: bool = False,
        read_only: bool = False,
        db_uid: Optional[str] = None,
        lock_retries: int = 20,
        lock_base_sleep_seconds: float = 0.05,
        lock_max_sleep_seconds: float = 0.5,
    ) -> None:
        raw_path = os.fspath(db_path).strip()
        self.db_path: str = raw_path if raw_path else ":memory:"
        self._resolved_db_path: Optional[Path] = (
            None
            if self.db_path == ":memory:"
            else Path(self.db_path).expanduser().resolve()
        )
        self.read_only: bool = bool(read_only)
        self._read_only: bool = self.read_only
        self._skip_schema_init: bool = bool(skip_schema_init) or self.read_only
        self._lock: threading.RLock = threading.RLock()
        self._cond: threading.Condition = threading.Condition(self._lock)
        self._suspended: bool = False
        self._suspended_owner_thread: Optional[int] = None
        self._suspended_group_has_writable: bool = False
        self._closed: bool = False
        self._tx_depth: int = 0
        self._tx_poisoned: bool = False
        self._open_cursors: dict[str, duckdb.DuckDBPyConnection] = {}
        self._suspend_hooks: list[Callable[[], None]] = []
        self._temp_views: dict[str, str] = {}
        self._temp_view_epoch: int = 0
        self.location_epoch: int = next(_GLOBAL_LOCATION_EPOCH)
        self._has_artifact_location_table: Optional[bool] = None
        self._last_seen_location_epoch: Optional[tuple[Any, ...]] = None
        self._epoch_invalidation_hooks: list[Callable[[], None]] = []
        self._active_coord_witness: dict[
            tuple[str, str, str, str, str],
            tuple[Optional[str], Optional[Witness]],
        ] = {}
        self._location_resolution_cache: dict[
            tuple[Any, ...],
            tuple[
                tuple[
                    ArtifactLocationRow,
                    Optional[Path],
                    Optional[Path],
                    Optional[Path],
                    str,
                    Optional[Path],
                    str,
                ],
                ...,
            ],
        ] = {}
        self._settled_witnesses: dict[uuid.UUID, Witness] = {}
        self._ephemeral_stale_locations: dict[uuid.UUID, LocationStatus] = {}
        self._snapshot_ops = DatabaseSnapshotOps(self)

        if self._resolved_db_path is not None:
            if self.read_only and not self._resolved_db_path.exists():
                raise FileNotFoundError(
                    f"Database file does not exist for read-only access: {self.db_path}"
                )
            if (
                self.read_only
                and self._resolved_db_path.exists()
                and self._resolved_db_path.stat().st_size == 0
            ):
                raise IncompatibleSchemaError(
                    f"Cannot open empty 0-byte database in read-only mode: {self.db_path}"
                )
            if (
                not self.read_only
                and self._resolved_db_path.exists()
                and self._resolved_db_path.stat().st_size == 0
            ):
                self._resolved_db_path.unlink()

        target = (
            ":memory:"
            if self._resolved_db_path is None
            else str(self._resolved_db_path)
        )
        in_proc_writable = (
            self._resolved_db_path is not None
            and has_live_writable_db_for_path(self._resolved_db_path)
        )

        # Inspect preexisting on-disk schema read-only before opening a RW handle
        # whenever no writable handle is already live in this process.
        if (
            self._resolved_db_path is not None
            and self._resolved_db_path.exists()
            and self._resolved_db_path.stat().st_size > 0
            and not in_proc_writable
        ):
            try:
                self._inspect_existing_file_schema_readonly(self._resolved_db_path)
            except duckdb.IOException as exc:
                msg = str(exc).lower()
                if self.read_only or (
                    "could not set lock" not in msg and "conflicting lock" not in msg
                ):
                    raise

        # DuckDB forbids opening read_only=True in the same OS process as an active
        # read_only=False connection on the same file; keep self.read_only=True for
        # Consist write guards/TEMP VIEW routing while matching DuckDB's config.
        self._con_read_only: bool = False if in_proc_writable else self.read_only
        if self._resolved_db_path is not None and not self._con_read_only:
            self._resolved_db_path.parent.mkdir(parents=True, exist_ok=True)

        ro_siblings_to_upgrade: list[DatabaseManager] = []
        if self._resolved_db_path is not None and not self._con_read_only:
            ro_siblings_to_upgrade = sorted(
                [
                    mgr
                    for mgr in _live_managers_for_path(self._resolved_db_path)
                    if not mgr._closed and not mgr._suspended and mgr._con_read_only
                ],
                key=id,
            )

        with ExitStack() as sibling_stack:
            suspended_siblings: list[DatabaseManager] = []
            for sib in ro_siblings_to_upgrade:
                sibling_stack.enter_context(sib._cond)
                if not sib._closed and not sib._suspended and sib._con_read_only:
                    sib._run_suspend_hooks_locked()
                    sib._close_tracked_cursors_locked()
                    sib._con.close()
                    suspended_siblings.append(sib)

            try:
                self._con: duckdb.DuckDBPyConnection = self._connect_with_lock_backoff(
                    target,
                    read_only=self._con_read_only,
                    lock_retries=0 if self.read_only else max(0, int(lock_retries)),
                    base_sleep_seconds=max(0.005, float(lock_base_sleep_seconds)),
                    max_sleep_seconds=max(0.01, float(lock_max_sleep_seconds)),
                )
            except BaseException:
                for sib in suspended_siblings:
                    try:
                        restored = duckdb.connect(target, read_only=True)
                        self._configure_connection(restored)
                        sib._replay_temp_views_locked(restored)
                        sib._con = restored
                        sib._con_read_only = True
                    except duckdb.Error:
                        sib._closed = True
                        sib._unregister_live_manager()
                raise

            for sib in suspended_siblings:
                upgraded = duckdb.connect(target, read_only=False)
                try:
                    self._configure_connection(upgraded)
                    sib._replay_temp_views_locked(upgraded)
                except BaseException:
                    upgraded.close()
                    self._con.close()
                    raise
                sib._con = upgraded
                sib._con_read_only = False

        try:
            self._configure_connection(self._con)
            self._validate_open_schema_locked()
            if not self._skip_schema_init:
                self.db_uid: str = self._init_schema_locked(db_uid=db_uid)
            else:
                self.db_uid = self._read_existing_db_uid_locked(fallback=db_uid)
        except BaseException:
            try:
                self._con.close()
            except duckdb.Error:
                pass
            raise

        self._register_live_manager()

    @staticmethod
    def _connect_with_lock_backoff(
        target: str,
        *,
        read_only: bool,
        lock_retries: int,
        base_sleep_seconds: float,
        max_sleep_seconds: float,
    ) -> duckdb.DuckDBPyConnection:
        attempt = 0
        while True:
            try:
                return duckdb.connect(target, read_only=read_only)
            except duckdb.IOException as exc:
                msg = str(exc).lower()
                is_lock_conflict = (
                    "could not set lock" in msg or "conflicting lock" in msg
                )
                if not is_lock_conflict or attempt >= lock_retries:
                    raise
                sleep_s = min(base_sleep_seconds * (1.35**attempt), max_sleep_seconds)
                time.sleep(sleep_s)
                attempt += 1

    @staticmethod
    def _configure_connection(conn: duckdb.DuckDBPyConnection) -> None:
        conn.execute("SET GLOBAL TimeZone = 'UTC'")
        conn.execute("SET search_path = 'main'")

    def _replay_temp_views_locked(self, conn: duckdb.DuckDBPyConnection) -> None:
        for view_name, create_sql in list(self._temp_views.items()):
            try:
                conn.execute(create_sql)
            except duckdb.Error as exc:
                logging.warning(
                    "Skipping stale temporary view %r replay: %s", view_name, exc
                )

    def _prepare_cursor_locked(
        self,
        cur: duckdb.DuckDBPyConnection,
        *,
        include_temp_views: bool = False,
    ) -> duckdb.DuckDBPyConnection:
        if include_temp_views and self._temp_views:
            self._replay_temp_views_locked(cur)
        return cur

    def _register_live_manager(self) -> None:
        if self._resolved_db_path is None:
            return
        key = str(self._resolved_db_path)
        with _LIVE_DB_LOCK:
            bucket = _LIVE_DB_MANAGERS.get(key)
            if bucket is None:
                bucket = weakref.WeakSet()
                _LIVE_DB_MANAGERS[key] = bucket
            bucket.add(self)

    def _unregister_live_manager(self) -> None:
        if self._resolved_db_path is None:
            return
        key = str(self._resolved_db_path)
        with _LIVE_DB_LOCK:
            bucket = _LIVE_DB_MANAGERS.get(key)
            if bucket is not None:
                bucket.discard(self)
                if not bucket:
                    _LIVE_DB_MANAGERS.pop(key, None)

    def _inspect_existing_file_schema_readonly(self, path: Path) -> None:
        probe = duckdb.connect(str(path), read_only=True)
        try:
            self._validate_connection_schema(probe)
        finally:
            probe.close()

    def _validate_connection_schema(self, conn: duckdb.DuckDBPyConnection) -> None:
        tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main'"
            ).fetchall()
        }
        if "consist_schema" in tables:
            row = conn.execute("SELECT MAX(version) FROM consist_schema").fetchone()
            version = int(row[0]) if row is not None and row[0] is not None else None
            if version == 1:
                raise DatabaseMigrationRequired(
                    f"Database uses Consist v0.4 schema (version=1). "
                    f"Run 'consist migrate {self.db_path}'."
                )
            if version is not None and version > 2:
                raise IncompatibleSchemaError(
                    f"Database {self.db_path!r} uses Consist schema version {version}, "
                    "which is newer than supported version 2."
                )
            if version == 2:
                if "artifact" in tables:
                    art_cols = {
                        str(r[0])
                        for r in conn.execute(
                            "SELECT column_name FROM duckdb_columns() "
                            "WHERE schema_name = 'main' AND table_name = 'artifact'"
                        ).fetchall()
                    }
                    if not {"uri", "digest"}.issubset(art_cols):
                        raise IncompatibleSchemaError(
                            f"Database {self.db_path!r} declares schema version 2 "
                            "but has an incompatible artifact table layout."
                        )
                return
            raise DatabaseMigrationRequired(
                f"Database uses Consist v0.4 schema (version=1). "
                f"Run 'consist migrate {self.db_path}'."
            )

        if tables & {"run", "artifact", "artifact_content"}:
            raise DatabaseMigrationRequired(
                f"Database uses Consist v0.4 schema (version=1). "
                f"Run 'consist migrate {self.db_path}'."
            )

    def _validate_open_schema_locked(self) -> None:
        self._validate_connection_schema(self._con)

    def _read_existing_db_uid_locked(self, *, fallback: Optional[str]) -> str:
        tables = {
            str(row[0])
            for row in self._con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main'"
            ).fetchall()
        }
        if "consist_schema" in tables:
            cols = {
                str(r[0])
                for r in self._con.execute(
                    "SELECT column_name FROM duckdb_columns() "
                    "WHERE schema_name = 'main' AND table_name = 'consist_schema'"
                ).fetchall()
            }
            if "db_uid" in cols:
                row = self._con.execute(
                    "SELECT db_uid FROM consist_schema WHERE version = 2 LIMIT 1"
                ).fetchone()
                if row is not None and row[0]:
                    return str(row[0])
        return fallback or str(uuid.uuid4())

    def _init_schema_locked(self, *, db_uid: Optional[str]) -> str:
        for stmt in _CORE_SCHEMA_DDL:
            self._con.execute(stmt)
        existing = self._con.execute(
            "SELECT db_uid FROM consist_schema WHERE version = 2 LIMIT 1"
        ).fetchone()
        if existing is not None and existing[0]:
            return str(existing[0])
        resolved_uid = db_uid or str(uuid.uuid4())
        self._con.execute(
            "INSERT INTO consist_schema (version, db_uid, updated_at) "
            "VALUES (?, ?, ?) ON CONFLICT (version) DO NOTHING",
            [2, resolved_uid, datetime.now(timezone.utc)],
        )
        return resolved_uid

    def init_schema(self, *, db_uid: Optional[str] = None) -> str:
        """Ensure core Consist v0.5 tables and views exist on this database."""
        self._require_writable("init_schema")
        with self._lock:
            self._wait_until_active_locked()
            return self._init_schema_locked(db_uid=db_uid)

    def _require_writable(self, op_name: str) -> None:
        if self.read_only:
            raise PermissionError(
                f"Cannot execute {op_name}() on a read-only DatabaseManager."
            )

    def _reopen_single_manager_locked(self, *, deadline: float) -> None:
        if not self._suspended or self._resolved_db_path is None:
            return
        attempt = 0
        last_err: Optional[duckdb.Error] = None
        reopened: Optional[duckdb.DuckDBPyConnection] = None
        reopen_ro = (
            False
            if (
                self._suspended_group_has_writable
                or has_live_writable_db_for_path(self._resolved_db_path)
            )
            else self.read_only
        )
        while True:
            try:
                reopened = duckdb.connect(
                    str(self._resolved_db_path),
                    read_only=reopen_ro,
                )
                last_err = None
                break
            except duckdb.Error as exc:
                last_err = exc
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                time.sleep(min(0.02 * (1.3**attempt), 0.25, remaining))
                attempt += 1

        if reopened is None:
            self._suspended = False
            self._suspended_owner_thread = None
            self._suspended_group_has_writable = False
            self._closed = True
            self._cond.notify_all()
            if last_err is not None:
                raise last_err
            raise RuntimeError("Failed to reopen suspended DuckDB connection.")

        try:
            self._configure_connection(reopened)
            self._replay_temp_views_locked(reopened)
        except BaseException:
            try:
                reopened.close()
            except duckdb.Error:
                pass
            self._suspended = False
            self._suspended_owner_thread = None
            self._suspended_group_has_writable = False
            self._closed = True
            self._cond.notify_all()
            raise

        self._con = reopened
        self._con_read_only = reopen_ro
        self._suspended = False
        self._suspended_owner_thread = None
        self._suspended_group_has_writable = False
        self._register_live_manager()
        self._cond.notify_all()

    def _wait_until_active_locked(self) -> None:
        if (
            self._suspended
            and self._suspended_owner_thread == threading.get_ident()
            and self._resolved_db_path is not None
        ):
            self._reopen_single_manager_locked(deadline=time.monotonic() + 20.0)
        while self._suspended:
            self._cond.wait()
        if self._closed:
            raise RuntimeError("DatabaseManager is closed.")

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        with self._lock:
            self._wait_until_active_locked()
            return self._con

    @property
    def instance_key(self) -> str:
        with self._lock:
            return f"{id(self)}:{id(self._con)}:{self._temp_view_epoch}"

    def register_suspend_hook(self, hook: Callable[[], None]) -> None:
        with self._lock:
            if hook not in self._suspend_hooks:
                self._suspend_hooks.append(hook)

    def has_temp_view(self, view_name: str) -> bool:
        with self._lock:
            return view_name in self._temp_views

    def register_temp_view(self, view_name: str, create_sql: str) -> None:
        with self._lock:
            self._wait_until_active_locked()
            self._con.execute(create_sql)
            self._temp_views[view_name] = create_sql
            self._temp_view_epoch += 1

    def _discard_open_cursor(self, token: str) -> None:
        with self._lock:
            self._open_cursors.pop(token, None)

    def close_cursor(self, cur: duckdb.DuckDBPyConnection) -> None:
        """Close a cursor previously returned by ``cursor()`` and untrack it."""
        with self._lock:
            matching_tokens = [
                tok for tok, existing in self._open_cursors.items() if existing is cur
            ]
            for tok in matching_tokens:
                self._open_cursors.pop(tok, None)
                with _TRACKED_RELATION_LOCK:
                    _TRACKED_RELATIONS.pop(tok, None)
        try:
            cur.close()
        except duckdb.Error:
            pass

    def _close_tracked_cursors_locked(self) -> None:
        tokens = list(self._open_cursors.keys())
        for token in tokens:
            cur = self._open_cursors.pop(token, None)
            with _TRACKED_RELATION_LOCK:
                _TRACKED_RELATIONS.pop(token, None)
            if cur is not None:
                try:
                    cur.close()
                except duckdb.Error:
                    pass

    def _run_suspend_hooks_locked(self) -> None:
        for hook in list(self._suspend_hooks):
            try:
                hook()
            except Exception as exc:
                logging.warning("Database suspend hook failed: %s", exc)

    def close(self) -> None:
        """Close all tracked cursors and the underlying DuckDB connection."""
        with self._cond:
            if (
                self._suspended
                and self._suspended_owner_thread == threading.get_ident()
            ):
                self._suspended = False
                self._suspended_owner_thread = None
                self._suspended_group_has_writable = False
                self._closed = True
                self._unregister_live_manager()
                self._cond.notify_all()
                return
            while self._suspended:
                self._cond.wait()
            if self._closed:
                return
            self._run_suspend_hooks_locked()
            self._close_tracked_cursors_locked()
            if self._tx_depth > 0:
                try:
                    self._con.execute("ROLLBACK")
                except duckdb.Error:
                    pass
                self._tx_depth = 0
                self._tx_poisoned = False
            self._closed = True
            self._unregister_live_manager()
            try:
                self._con.close()
            except duckdb.Error:
                pass
            self._cond.notify_all()

    def __enter__(self) -> DatabaseManager:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def checkpoint(self) -> None:
        with self._lock:
            self._wait_until_active_locked()
            if not self._con_read_only:
                self._con.execute("CHECKPOINT")

    @contextmanager
    def suspend_for_subprocesses(self) -> Iterator[None]:
        """Release all in-process file-backed DuckDB handles while child processes run."""
        if self._resolved_db_path is None:
            with self._lock:
                self._wait_until_active_locked()
            yield
            return

        owner_tid = threading.get_ident()
        candidates = sorted(
            {self, *_live_managers_for_path(self._resolved_db_path)},
            key=id,
        )
        suspended_targets: list[DatabaseManager] = []
        with ExitStack() as lock_stack:
            for mgr in candidates:
                lock_stack.enter_context(mgr._cond)
                while mgr._suspended and mgr._suspended_owner_thread != owner_tid:
                    mgr._cond.wait()

            if self._closed:
                raise RuntimeError("DatabaseManager is closed.")

            active_targets = [
                mgr
                for mgr in candidates
                if not mgr._closed
                and not mgr._suspended
                and mgr._resolved_db_path == self._resolved_db_path
            ]
            for mgr in active_targets:
                if mgr._tx_depth > 0:
                    raise RuntimeError(
                        "Cannot suspend DatabaseManager for subprocesses inside an active transaction."
                    )

            group_has_writable = any(
                not mgr.read_only or not mgr._con_read_only for mgr in active_targets
            )
            for mgr in active_targets:
                mgr._suspended = True
                mgr._suspended_owner_thread = owner_tid
                mgr._suspended_group_has_writable = group_has_writable
                mgr._run_suspend_hooks_locked()
                mgr._close_tracked_cursors_locked()
                if not mgr._con_read_only:
                    try:
                        mgr._con.execute("CHECKPOINT")
                    except duckdb.Error as exc:
                        logging.warning(
                            "DuckDB CHECKPOINT before subprocess suspend failed: %s",
                            exc,
                        )
                mgr._unregister_live_manager()
                mgr._con.close()
                suspended_targets.append(mgr)

        try:
            yield
        finally:
            if suspended_targets:
                deadline = time.monotonic() + 20.0
                first_reopen_err: Optional[BaseException] = None
                reopen_order = sorted(
                    suspended_targets, key=lambda mgr: (mgr.read_only, id(mgr))
                )
                for mgr in reopen_order:
                    with mgr._cond:
                        if mgr._suspended:
                            try:
                                mgr._reopen_single_manager_locked(deadline=deadline)
                                mgr._has_artifact_location_table = None
                                mgr.bump_location_epoch()
                            except BaseException as exc:
                                if first_reopen_err is None:
                                    first_reopen_err = exc
                if first_reopen_err is not None:
                    raise first_reopen_err

    @contextmanager
    def transaction(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Poison-safe re-entrant transaction without SAVEPOINT."""
        self._require_writable("transaction")
        with self._lock:
            self._wait_until_active_locked()
            if self._tx_depth > 0:
                if self._tx_poisoned:
                    raise TransactionAbortedError(
                        "Current transaction is already aborted due to an earlier failure."
                    )
                self._tx_depth += 1
                try:
                    yield self._con
                except BaseException:
                    self._tx_poisoned = True
                    raise
                finally:
                    self._tx_depth -= 1
                return

            self._con.execute("BEGIN TRANSACTION")
            self._tx_depth = 1
            self._tx_poisoned = False
            try:
                yield self._con
            except BaseException:
                self._tx_poisoned = True
                try:
                    self._con.execute("ROLLBACK")
                except duckdb.Error:
                    pass
                raise
            else:
                if self._tx_poisoned:
                    try:
                        self._con.execute("ROLLBACK")
                    except duckdb.Error:
                        pass
                    raise TransactionAbortedError(
                        "Outer transaction rolled back because a nested transaction failed."
                    )
                try:
                    self._con.execute("COMMIT")
                except BaseException:
                    try:
                        self._con.execute("ROLLBACK")
                    except duckdb.Error:
                        pass
                    raise
            finally:
                self._tx_depth = 0
                self._tx_poisoned = False

    @contextmanager
    def borrow_for_dlt(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Yield the locked DuckDB connection to dlt and restore clean session state."""
        self._require_writable("borrow_for_dlt")
        with self._lock:
            self._wait_until_active_locked()
            if self._tx_depth > 0:
                raise RuntimeError(
                    "Cannot borrow DatabaseManager connection for dlt inside an active transaction."
                )
            try:
                yield self._con
            finally:
                try:
                    self._con.execute("ROLLBACK")
                except duckdb.Error:
                    pass
                try:
                    self._con.execute("SET search_path = 'main'")
                except duckdb.Error:
                    pass
                try:
                    self._con.execute("SET GLOBAL TimeZone = 'UTC'")
                except duckdb.Error:
                    pass

    @contextmanager
    def exec_cursor(
        self, *, include_temp_views: bool = False
    ) -> Iterator[duckdb.DuckDBPyConnection]:
        """Yield ``self._con`` in an active transaction, or a temporary cloned cursor."""
        with self._lock:
            self._wait_until_active_locked()
            if self._tx_depth > 0:
                if self._tx_poisoned:
                    raise TransactionAbortedError(
                        "Current transaction is aborted due to an earlier failure."
                    )
                yield self._con
                return
            cur = self._prepare_cursor_locked(
                self._con.cursor(), include_temp_views=include_temp_views
            )
            try:
                yield cur
            finally:
                cur.close()

    def cursor(self) -> duckdb.DuckDBPyConnection:
        """Clone a tracked cursor outside an active transaction."""
        with self._lock:
            self._wait_until_active_locked()
            if self._tx_depth > 0:
                raise RuntimeError(
                    "Cannot allocate an independent cursor() inside an active transaction."
                )
            token = uuid.uuid4().hex[:16]
            cur = self._prepare_cursor_locked(
                self._con.cursor(), include_temp_views=True
            )
            self._open_cursors[token] = cur
            return cur

    @staticmethod
    def _unwrap_sql_and_params(
        sql: str | ParameterizedSQL,
        params: Optional[Sequence[Any] | Mapping[str, Any]] = None,
    ) -> tuple[str, Optional[list[Any] | dict[str, Any]]]:
        if isinstance(sql, str):
            if params is None:
                return sql, None
            if isinstance(params, Mapping):
                return sql, dict(params)
            return sql, list(params)
        if isinstance(sql, ParameterizedSQL):
            if params is not None:
                raise ValueError(
                    "Cannot supply separate 'params' when passing a ParameterizedSQL query."
                )
            return sql.sql, list(sql.params)
        raise TypeError("SQL query must be a str or ParameterizedSQL instance.")

    def execute(
        self,
        sql: str | ParameterizedSQL,
        params: Optional[Sequence[Any] | Mapping[str, Any]] = None,
    ) -> None:
        raw_sql, raw_params = self._unwrap_sql_and_params(sql, params)
        with self.exec_cursor() as cur:
            if raw_params is None:
                cur.execute(raw_sql)
            else:
                cur.execute(raw_sql, raw_params)

    def executemany(self, sql: str, params_seq: Sequence[Sequence[Any]]) -> None:
        if not params_seq:
            return
        with self.exec_cursor() as cur:
            cur.executemany(sql, [list(row) for row in params_seq])

    def fetchone(
        self,
        sql: str | ParameterizedSQL,
        params: Optional[Sequence[Any] | Mapping[str, Any]] = None,
    ) -> Optional[Tuple[Any, ...]]:
        raw_sql, raw_params = self._unwrap_sql_and_params(sql, params)
        with self.exec_cursor() as cur:
            res = (
                cur.execute(raw_sql)
                if raw_params is None
                else cur.execute(raw_sql, raw_params)
            )
            return res.fetchone()

    def fetchall(
        self,
        sql: str | ParameterizedSQL,
        params: Optional[Sequence[Any] | Mapping[str, Any]] = None,
    ) -> List[Tuple[Any, ...]]:
        raw_sql, raw_params = self._unwrap_sql_and_params(sql, params)
        with self.exec_cursor() as cur:
            res = (
                cur.execute(raw_sql)
                if raw_params is None
                else cur.execute(raw_sql, raw_params)
            )
            return res.fetchall()

    def fetchdf(
        self,
        sql: str | ParameterizedSQL,
        params: Optional[Sequence[Any] | Mapping[str, Any]] = None,
    ) -> pd.DataFrame:
        raw_sql, raw_params = self._unwrap_sql_and_params(sql, params)
        with self.exec_cursor() as cur:
            res = (
                cur.execute(raw_sql)
                if raw_params is None
                else cur.execute(raw_sql, raw_params)
            )
            return res.fetchdf()

    def query_df(
        self,
        sql: str | ParameterizedSQL,
        params: Optional[Sequence[Any] | Mapping[str, Any]] = None,
    ) -> pd.DataFrame:
        """Execute a validated read-only SELECT query and return a DataFrame."""
        raw_sql, raw_params = self._unwrap_sql_and_params(sql, params)
        _validate_select_only_sql(raw_sql)
        with self.exec_cursor(include_temp_views=True) as cur:
            res = (
                cur.execute(raw_sql)
                if raw_params is None
                else cur.execute(raw_sql, raw_params)
            )
            return res.fetchdf()

    def query_rows(
        self,
        sql: str | ParameterizedSQL,
        params: Optional[Sequence[Any] | Mapping[str, Any]] = None,
        *,
        model: Optional[Type[_RowModelT]] = None,
    ) -> List[Any]:
        """Execute a validated read-only SELECT query and return rows or validated models."""
        raw_sql, raw_params = self._unwrap_sql_and_params(sql, params)
        _validate_select_only_sql(raw_sql)
        with self.exec_cursor(include_temp_views=True) as cur:
            res = (
                cur.execute(raw_sql)
                if raw_params is None
                else cur.execute(raw_sql, raw_params)
            )
            description = res.description or []
            col_names = [str(col[0]) for col in description]
            raw_rows = res.fetchall()

        if not raw_rows:
            return []

        if model is None:
            if len(col_names) == 1:
                return [row[0] for row in raw_rows]
            return [tuple(row) for row in raw_rows]

        adapter = TypeAdapter(model)
        is_mapping_model = isinstance(model, type) and issubclass(model, BaseModel)
        results: list[Any] = []
        for row in raw_rows:
            if is_mapping_model or model is dict:
                payload = {
                    col_names[idx]: _load_json_value_for_model(
                        model, col_names[idx], row[idx]
                    )
                    for idx in range(len(col_names))
                }
                results.append(adapter.validate_python(payload))
            elif len(col_names) == 1:
                results.append(adapter.validate_python(row[0]))
            else:
                results.append(adapter.validate_python(tuple(row)))
        return results

    def relation_query(
        self,
        sql: str | ParameterizedSQL,
        params: Optional[Sequence[Any] | Mapping[str, Any]] = None,
    ) -> duckdb.DuckDBPyRelation:
        """Return a lazy DuckDBPyRelation backed by a tracked root cursor."""
        raw_sql, raw_params = self._unwrap_sql_and_params(sql, params)
        _validate_select_only_sql(raw_sql)
        stripped = raw_sql.strip().rstrip(";").strip()
        with self._lock:
            self._wait_until_active_locked()
            token = uuid.uuid4().hex[:16]
            alias = f"__consist_rel_{token}"
            if self._tx_depth > 0:
                if self._tx_poisoned:
                    raise TransactionAbortedError(
                        "Current transaction is aborted due to an earlier failure."
                    )
                res = (
                    self._con.execute(stripped)
                    if raw_params is None
                    else self._con.execute(stripped, raw_params)
                )
                arrow_tbl = res.to_arrow_table()
                cur = duckdb.connect(":memory:")
                try:
                    self._configure_connection(cur)
                    cur.register(alias, arrow_tbl)
                    rel = cur.sql(f"SELECT * FROM {alias}")
                except BaseException:
                    cur.close()
                    raise
            else:
                cur = self._prepare_cursor_locked(
                    self._con.cursor(), include_temp_views=True
                )
                try:
                    if raw_params is None:
                        rel = cur.sql(f"SELECT * FROM ({stripped}) AS {alias}")
                    else:
                        arrow_tbl = cur.execute(stripped, raw_params).to_arrow_table()
                        cur.register(alias, arrow_tbl)
                        rel = cur.sql(f"SELECT * FROM {alias}")
                except BaseException:
                    cur.close()
                    raise
            self._open_cursors[token] = cur
            register_tracked_relation_cursor(token, cur, self)
            return rel

    def has_table(self, table_name: str, *, table_schema: str = "main") -> bool:
        if not _SAFE_IDENTIFIER_RE.fullmatch(
            table_schema or ""
        ) or not _SAFE_IDENTIFIER_RE.fullmatch(table_name or ""):
            return False
        row = self.fetchone(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ? LIMIT 1",
            [table_schema, table_name],
        )
        return row is not None

    def has_table_column(
        self,
        *,
        table_name: str,
        column_name: str,
        table_schema: str = "main",
    ) -> bool:
        if (
            not _SAFE_IDENTIFIER_RE.fullmatch(table_schema or "")
            or not _SAFE_IDENTIFIER_RE.fullmatch(table_name or "")
            or not _SAFE_IDENTIFIER_RE.fullmatch(column_name or "")
        ):
            return False
        row = self.fetchone(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? AND column_name = ? LIMIT 1",
            [table_schema, table_name, column_name],
        )
        return row is not None

    def ensure_pydantic_table(
        self,
        model_cls: Type[BaseModel],
        *,
        schema: Optional[str] = None,
    ) -> tuple[str, str]:
        """Create a DuckDB table from a Pydantic/SQLModel class's ``model_fields``."""
        self._require_writable("ensure_pydantic_table")
        table_name = model_table_name(model_cls)
        target_schema = schema or model_table_schema(model_cls) or "main"
        _validate_identifier(target_schema, label="table_schema")
        _validate_identifier(table_name, label="table_name")

        col_defs: list[str] = []
        pk_cols: list[str] = []
        for attr_name, field_info in model_cls.model_fields.items():
            sql_col = field_sql_column_name(attr_name, field_info)
            _validate_identifier(sql_col, label="column_name")
            duck_type = field_duckdb_type(field_info)
            is_pk = field_is_primary_key(field_info)
            if is_pk:
                pk_cols.append(_quote_ident(sql_col))
                col_defs.append(f"{_quote_ident(sql_col)} {duck_type} NOT NULL")
            else:
                col_defs.append(f"{_quote_ident(sql_col)} {duck_type}")

        if pk_cols:
            col_defs.append(f"PRIMARY KEY ({', '.join(pk_cols)})")

        if target_schema != "main":
            self.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(target_schema)}")
        qualified = f"{_quote_ident(target_schema)}.{_quote_ident(table_name)}"
        self.execute(f"CREATE TABLE IF NOT EXISTS {qualified} ({', '.join(col_defs)})")
        return target_schema, table_name

    # --- Snapshot Helpers ---

    def _atomic_copy_file(self, src: Path, dest: Path) -> None:
        self._snapshot_ops._atomic_copy_file(src, dest)

    def _atomic_write_json_file(self, payload: Dict[str, Any], dest: Path) -> None:
        self._snapshot_ops._atomic_write_json_file(payload, dest)

    def _snapshot_sidecar_path(self, destination: Path) -> Path:
        return snapshot_sidecar_path(destination)

    def snapshot_to(
        self,
        dest_path: str | os.PathLike[str],
        checkpoint: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Path:
        return self._snapshot_ops.snapshot_to(
            dest_path,
            checkpoint=checkpoint,
            metadata=metadata,
        )

    @staticmethod
    def _sync_run_stage_phase(run: Run) -> None:
        """Keep ``Run.stage`` / ``Run.phase`` aligned with mirrored JSON metadata."""
        if run.meta is None:
            run.meta = {}
        if not isinstance(run.meta, dict):
            return

        stage_val = resolve_canonical_run_meta_field(run, "stage")
        if stage_val is not None:
            run.stage = stage_val
            run.meta["stage"] = stage_val
        elif "stage" in run.meta:
            raw_stage = run.meta.get("stage")
            if isinstance(raw_stage, str) or raw_stage is None:
                run.stage = raw_stage

        phase_val = resolve_canonical_run_meta_field(run, "phase")
        if phase_val is not None:
            run.phase = phase_val
            run.meta["phase"] = phase_val
        elif "phase" in run.meta:
            raw_phase = run.meta.get("phase")
            if isinstance(raw_phase, str) or raw_phase is None:
                run.phase = raw_phase

    # --- Internal Upsert Helpers ---

    def _upsert_run_locked(self, cur: duckdb.DuckDBPyConnection, run: Run) -> None:
        self._sync_run_stage_phase(run)
        meta_dict = dict(run.meta) if isinstance(run.meta, Mapping) else {}
        facet_id_raw = meta_dict.get("config_facet_id")
        facet_id = (
            str(facet_id_raw)
            if isinstance(facet_id_raw, str) and facet_id_raw
            else None
        )
        cur.execute(
            """
            INSERT INTO run (
                id, parent_run_id, status, model_name, description,
                year, iteration, stage, phase, tags,
                config_hash, git_hash, input_hash, signature, cacheable,
                facet_id, meta, started_at, ended_at, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                parent_run_id = EXCLUDED.parent_run_id,
                status = EXCLUDED.status,
                model_name = EXCLUDED.model_name,
                description = EXCLUDED.description,
                year = EXCLUDED.year,
                iteration = EXCLUDED.iteration,
                stage = EXCLUDED.stage,
                phase = EXCLUDED.phase,
                tags = EXCLUDED.tags,
                config_hash = EXCLUDED.config_hash,
                git_hash = EXCLUDED.git_hash,
                input_hash = EXCLUDED.input_hash,
                signature = EXCLUDED.signature,
                cacheable = EXCLUDED.cacheable,
                facet_id = COALESCE(EXCLUDED.facet_id, run.facet_id),
                meta = EXCLUDED.meta,
                started_at = EXCLUDED.started_at,
                ended_at = EXCLUDED.ended_at,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at
            """,
            [
                run.id,
                run.parent_run_id,
                run.status,
                run.model_name,
                run.description,
                run.year,
                run.iteration,
                run.stage,
                run.phase,
                _dump_json(list(run.tags or [])),
                run.config_hash,
                run.git_hash,
                run.input_hash,
                run.signature,
                bool(run.cacheable),
                facet_id,
                _dump_json(meta_dict),
                _ensure_non_null_utc_dt(run.started_at),
                _ensure_utc_dt(run.ended_at),
                _ensure_non_null_utc_dt(run.created_at),
                _ensure_non_null_utc_dt(run.updated_at),
            ],
        )

    def _insert_binding_invocation_locked(
        self,
        cur: duckdb.DuckDBPyConnection,
        inv: RunBindingInvocation,
    ) -> None:
        cur.execute(
            """
            INSERT INTO run_binding_invocation (
                id, requested_run_id, execution_run_id, cache_source_run_id,
                cache_outcome, binding_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                requested_run_id = EXCLUDED.requested_run_id,
                execution_run_id = EXCLUDED.execution_run_id,
                cache_source_run_id = EXCLUDED.cache_source_run_id,
                cache_outcome = EXCLUDED.cache_outcome,
                binding_json = EXCLUDED.binding_json,
                created_at = EXCLUDED.created_at
            """,
            [
                _coerce_uuid(inv.id),
                inv.requested_run_id,
                inv.execution_run_id,
                inv.cache_source_run_id,
                inv.cache_outcome,
                inv.binding_json,
                _ensure_non_null_utc_dt(inv.created_at),
            ],
        )

    @staticmethod
    def _artifact_upsert_params(
        artifact: Artifact, *, explicit_facet_id: Optional[str] = None
    ) -> list[Any]:
        meta_for_db = dict(artifact.meta) if isinstance(artifact.meta, Mapping) else {}
        meta_for_db.pop("recovery_roots", None)
        facet_id = explicit_facet_id or artifact.facet_id
        if facet_id is None:
            raw_fid = meta_for_db.get("artifact_facet_id")
            if isinstance(raw_fid, str) and raw_fid:
                facet_id = raw_fid
        return [
            _coerce_uuid(artifact.id),
            artifact.key,
            artifact.uri,
            artifact.driver,
            str(artifact.digest) if artifact.digest is not None else None,
            artifact.member,
            artifact.run_id,
            _coerce_optional_uuid(artifact.parent_artifact_id),
            facet_id,
            _dump_json(meta_for_db),
            _ensure_non_null_utc_dt(artifact.created_at),
        ]

    _ARTIFACT_INSERT_PREFIX = (
        "INSERT INTO artifact ("
        "id, key, uri, driver, digest, member, "
        "run_id, parent_artifact_id, facet_id, meta, created_at"
        ") VALUES"
    )
    _ARTIFACT_UPSERT_SUFFIX = (
        "ON CONFLICT (id) DO UPDATE SET "
        "key = EXCLUDED.key, uri = EXCLUDED.uri, driver = EXCLUDED.driver, "
        "digest = EXCLUDED.digest, member = EXCLUDED.member, run_id = EXCLUDED.run_id, "
        "parent_artifact_id = EXCLUDED.parent_artifact_id, "
        "facet_id = COALESCE(EXCLUDED.facet_id, artifact.facet_id), "
        "meta = CAST(json_merge_patch(COALESCE(artifact.meta, '{}'), COALESCE(EXCLUDED.meta, '{}')) AS VARCHAR), "
        "created_at = EXCLUDED.created_at"
    )
    _ARTIFACT_UPSERT_SQL = (
        f"{_ARTIFACT_INSERT_PREFIX} (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        f"{_ARTIFACT_UPSERT_SUFFIX}"
    )

    def _upsert_artifact_locked(
        self,
        cur: duckdb.DuckDBPyConnection,
        artifact: Artifact,
        *,
        explicit_facet_id: Optional[str] = None,
    ) -> None:
        cur.execute(
            self._ARTIFACT_UPSERT_SQL,
            self._artifact_upsert_params(artifact, explicit_facet_id=explicit_facet_id),
        )

    def _link_artifacts_locked(
        self,
        cur: duckdb.DuckDBPyConnection,
        *,
        run_id: str,
        artifact_ids: Sequence[uuid.UUID],
        direction: str,
        roles_by_id: Optional[Mapping[uuid.UUID, str]] = None,
        is_implicit: bool = False,
    ) -> None:
        unique_ids = [_coerce_uuid(aid) for aid in dict.fromkeys(artifact_ids)]
        if not unique_ids:
            return
        placeholders = ", ".join("?" for _ in unique_ids)
        existing_rows = cur.execute(
            f"SELECT artifact_id, direction, role FROM run_artifact_link "
            f"WHERE run_id = ? AND artifact_id IN ({placeholders})",
            [run_id, *unique_ids],
        ).fetchall()
        existing_dirs: dict[uuid.UUID, set[str]] = {}
        existing_pairs: set[tuple[uuid.UUID, str, str]] = set()
        for raw_aid, raw_dir, raw_role in existing_rows:
            aid = _coerce_uuid(raw_aid)
            d_str = str(raw_dir)
            r_str = str(raw_role or "")
            existing_dirs.setdefault(aid, set()).add(d_str)
            existing_pairs.add((aid, d_str, r_str))

        insert_rows: list[list[Any]] = []
        for aid in unique_ids:
            prior_dirs = existing_dirs.get(aid)
            if prior_dirs:
                if direction not in prior_dirs:
                    prior_dir = sorted(prior_dirs)[0]
                    logging.warning(
                        "[Consist] Ignoring attempt to link artifact_id=%s to run_id=%s as '%s' "
                        "because it is already linked as '%s'. "
                        "If this step truly produces a new output, write to a new path (preferred), "
                        "or log a distinct Artifact instance rather than reusing the same Artifact reference.",
                        aid,
                        run_id,
                        direction,
                        prior_dir,
                    )
                    continue
                role_val = (roles_by_id.get(aid, "") if roles_by_id else "") or ""
                if (aid, direction, role_val) in existing_pairs or role_val == "":
                    continue
            role_val = (roles_by_id.get(aid, "") if roles_by_id else "") or ""
            insert_rows.append([run_id, aid, direction, role_val, bool(is_implicit)])

        if insert_rows:
            _execute_multirow_values(
                cur,
                prefix_sql=(
                    "INSERT INTO run_artifact_link "
                    "(run_id, artifact_id, direction, role, is_implicit) VALUES"
                ),
                row_placeholder="(?, ?, ?, ?, ?)",
                suffix_sql="ON CONFLICT (run_id, artifact_id, direction, role) DO NOTHING",
                rows=insert_rows,
            )

    def _upsert_artifact_facet_locked(
        self, cur: duckdb.DuckDBPyConnection, facet: ArtifactFacet
    ) -> None:
        schema_ver = (
            str(facet.schema_version) if facet.schema_version is not None else None
        )
        cur.execute(
            """
            INSERT INTO artifact_facet (
                id, namespace, schema_name, schema_version, "json", created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                namespace = COALESCE(artifact_facet.namespace, EXCLUDED.namespace),
                schema_name = COALESCE(artifact_facet.schema_name, EXCLUDED.schema_name),
                schema_version = COALESCE(artifact_facet.schema_version, EXCLUDED.schema_version)
            """,
            [
                facet.id,
                facet.namespace,
                facet.schema_name,
                schema_ver,
                _dump_json(facet.facet_json or {}),
                _ensure_non_null_utc_dt(facet.created_at),
            ],
        )

    def _replace_run_config_kv_locked(
        self, cur: duckdb.DuckDBPyConnection, rows: Sequence[RunConfigKV]
    ) -> None:
        if not rows:
            return
        combos = {
            (row.run_id, row.facet_id, row.namespace)
            for row in rows
            if row.run_id and row.facet_id and row.namespace
        }
        for run_id, facet_id, namespace in combos:
            cur.execute(
                "DELETE FROM run_config_kv WHERE run_id = ? AND facet_id = ? AND namespace = ?",
                [run_id, facet_id, namespace],
            )
        deduped: dict[tuple[str, str, str, str], RunConfigKV] = {}
        for row in rows:
            deduped[(row.run_id, row.facet_id, row.namespace, row.key)] = row
        _execute_multirow_values(
            cur,
            prefix_sql=(
                "INSERT INTO run_config_kv ("
                "run_id, facet_id, namespace, key, value_type, "
                "value_str, value_num, value_bool, value_json, created_at"
                ") VALUES"
            ),
            row_placeholder="(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            suffix_sql=(
                "ON CONFLICT (run_id, facet_id, namespace, key) DO UPDATE SET "
                "value_type = EXCLUDED.value_type, "
                "value_str = EXCLUDED.value_str, "
                "value_num = EXCLUDED.value_num, "
                "value_bool = EXCLUDED.value_bool, "
                "value_json = EXCLUDED.value_json, "
                "created_at = EXCLUDED.created_at"
            ),
            rows=[
                [
                    r.run_id,
                    r.facet_id,
                    r.namespace,
                    r.key,
                    r.value_type,
                    r.value_str,
                    float(r.value_num) if r.value_num is not None else None,
                    r.value_bool,
                    _dump_optional_json(r.value_json),
                    _ensure_non_null_utc_dt(r.created_at),
                ]
                for r in deduped.values()
            ],
        )

    def _replace_artifact_kv_locked(
        self, cur: duckdb.DuckDBPyConnection, rows: Sequence[ArtifactKV]
    ) -> None:
        if not rows:
            return
        combos = {
            (_coerce_uuid(row.artifact_id), row.facet_id, row.namespace)
            for row in rows
            if row.artifact_id and row.facet_id
        }
        for artifact_id, facet_id, namespace in combos:
            if namespace is None:
                cur.execute(
                    "DELETE FROM artifact_kv WHERE artifact_id = ? AND facet_id = ? AND namespace IS NULL",
                    [artifact_id, facet_id],
                )
            else:
                cur.execute(
                    "DELETE FROM artifact_kv WHERE artifact_id = ? AND facet_id = ? AND namespace = ?",
                    [artifact_id, facet_id, namespace],
                )
        deduped: dict[tuple[uuid.UUID, str, str], ArtifactKV] = {}
        for row in rows:
            deduped[(_coerce_uuid(row.artifact_id), row.facet_id, row.key_path)] = row
        _execute_multirow_values(
            cur,
            prefix_sql=(
                "INSERT INTO artifact_kv ("
                "artifact_id, facet_id, key_path, namespace, value_type, "
                "value_str, value_num, value_bool, value_json, created_at"
                ") VALUES"
            ),
            row_placeholder="(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            suffix_sql=(
                "ON CONFLICT (artifact_id, facet_id, key_path) DO UPDATE SET "
                "namespace = EXCLUDED.namespace, "
                "value_type = EXCLUDED.value_type, "
                "value_str = EXCLUDED.value_str, "
                "value_num = EXCLUDED.value_num, "
                "value_bool = EXCLUDED.value_bool, "
                "value_json = EXCLUDED.value_json, "
                "created_at = EXCLUDED.created_at"
            ),
            rows=[
                [
                    _coerce_uuid(r.artifact_id),
                    r.facet_id,
                    r.key_path,
                    r.namespace,
                    r.value_type,
                    r.value_str,
                    float(r.value_num) if r.value_num is not None else None,
                    r.value_bool,
                    _dump_optional_json(r.value_json),
                    _ensure_non_null_utc_dt(r.created_at),
                ]
                for r in deduped.values()
            ],
        )

    # --- Write Operations ---

    def update_artifact_meta(
        self,
        artifact: Artifact,
        updates: Mapping[str, Any],
        *,
        replace: bool = False,
        raise_on_error: bool = False,
    ) -> bool:
        """Update artifact metadata atomically."""
        try:
            with self.transaction() as cur:
                existing_row = cur.execute(
                    "SELECT meta FROM artifact WHERE id = ?",
                    [_coerce_uuid(artifact.id)],
                ).fetchone()
                base_meta = (
                    _parse_json_dict(existing_row[0])
                    if existing_row is not None
                    else dict(artifact.meta or {})
                )
                current_meta = dict(artifact.meta or {})
                if replace or updates is artifact.meta:
                    merged_meta = {} if replace else dict(current_meta)
                else:
                    merged_meta = _merge_persisted_artifact_meta(
                        base_meta, current_meta
                    )
                for key, value in updates.items():
                    if value is None:
                        merged_meta.pop(key, None)
                    else:
                        merged_meta[key] = value
                updated_artifact = artifact.with_meta(merged_meta, merge=False)
                if existing_row is not None:
                    raw_fid = merged_meta.get("artifact_facet_id")
                    resolved_fid = updated_artifact.facet_id or (
                        raw_fid if isinstance(raw_fid, str) and raw_fid else None
                    )
                    cur.execute(
                        "UPDATE artifact SET meta = ?, facet_id = COALESCE(?, facet_id) WHERE id = ?",
                        [
                            _dump_json(merged_meta),
                            resolved_fid,
                            _coerce_uuid(updated_artifact.id),
                        ],
                    )
                else:
                    self._upsert_artifact_locked(cur, updated_artifact)
            return True
        except Exception as exc:
            if raise_on_error or self._tx_depth > 0:
                raise
            logging.warning("Failed to update artifact metadata: %s", exc)
            return False

    def get_binding_invocations(
        self, *, requested_run_id: Optional[str] = None
    ) -> List[RunBindingInvocation]:
        """Return resolved-binding invocation evidence in creation order."""
        sql = (
            "SELECT id, requested_run_id, execution_run_id, cache_source_run_id, "
            "cache_outcome, binding_json, created_at FROM run_binding_invocation"
        )
        params: list[Any] = []
        if requested_run_id is not None:
            sql += " WHERE requested_run_id = ?"
            params.append(requested_run_id)
        sql += " ORDER BY created_at ASC"
        rows = self.fetchall(sql, params or None)
        return [
            RunBindingInvocation(
                id=_coerce_uuid(r[0]),
                requested_run_id=str(r[1]),
                execution_run_id=str(r[2]),
                cache_source_run_id=str(r[3]) if r[3] is not None else None,
                cache_outcome=str(r[4]),
                binding_json=str(r[5]),
                created_at=_ensure_non_null_utc_dt(r[6]),
            )
            for r in rows
        ]

    def sync_run(
        self,
        run: Run,
        *,
        binding_invocation: Optional[RunBindingInvocation] = None,
        require_success: bool = False,
    ) -> None:
        """Upsert a run and optional binding invocation atomically."""
        try:
            with _DB_CALL_PROFILER.track("sync_run"):
                with _track_begin_run_phase("db.sync_run.transaction"):
                    with self.transaction() as cur:
                        self._upsert_run_locked(cur, run)
                        if binding_invocation is not None:
                            self._insert_binding_invocation_locked(
                                cur, binding_invocation
                            )
        except Exception as exc:
            logging.warning("Database sync failed: %s", exc)
            if require_success or self._tx_depth > 0:
                raise

    def upsert_run(self, run: Run) -> None:
        """Upsert a ``Run`` row atomically, raising on database errors."""
        with self.transaction() as cur:
            self._upsert_run_locked(cur, run)

    def update_run_meta(self, run_id: str, meta_updates: Dict[str, Any]) -> None:
        """Update a run's ``meta`` JSON field with a partial dictionary."""
        with self.transaction() as cur:
            row = cur.execute(
                f"SELECT {_RUN_SELECT_COLS} FROM run r WHERE r.id = ?",
                [run_id],
            ).fetchone()
            if row is None:
                return
            db_run = _hydrate_run_row(row)
            current = dict(db_run.meta or {})
            current.update(meta_updates)
            db_run.meta = current
            db_run.updated_at = datetime.now(timezone.utc)
            self._upsert_run_locked(cur, db_run)

    def update_run_signature(self, run_id: str, signature: str) -> None:
        """Set the ``signature`` column for an existing run."""
        with self.transaction() as cur:
            cur.execute(
                "UPDATE run SET signature = ?, updated_at = ? WHERE id = ?",
                [signature, datetime.now(timezone.utc), run_id],
            )

    def _upsert_config_facet_locked(
        self, cur: duckdb.DuckDBPyConnection, facet: ConfigFacet
    ) -> None:
        schema_ver = (
            str(facet.schema_version) if facet.schema_version is not None else None
        )
        cur.execute(
            'INSERT INTO config_facet (id, namespace, schema_name, schema_version, "json", created_at) '
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (id) DO UPDATE SET namespace = EXCLUDED.namespace, "
            "schema_name = EXCLUDED.schema_name, schema_version = EXCLUDED.schema_version, "
            '"json" = EXCLUDED."json"',
            [
                facet.id,
                facet.namespace,
                facet.schema_name,
                schema_ver,
                _dump_json(facet.facet_json or {}),
                _ensure_non_null_utc_dt(facet.created_at),
            ],
        )

    def _merge_artifact_meta_locked(
        self,
        cur: duckdb.DuckDBPyConnection,
        artifact: Artifact,
        meta_updates: Mapping[str, Any],
    ) -> Artifact:
        existing_row = cur.execute(
            "SELECT meta FROM artifact WHERE id = ?",
            [_coerce_uuid(artifact.id)],
        ).fetchone()
        current_meta = dict(artifact.meta or {})
        if existing_row is not None:
            merged_meta = _merge_persisted_artifact_meta(
                _parse_json_dict(existing_row[0]), current_meta
            )
        else:
            merged_meta = current_meta
        merged_meta.update(meta_updates)
        return artifact.with_meta(merged_meta, merge=False)

    def upsert_config_facet(self, facet: ConfigFacet) -> None:
        """Upsert a ``ConfigFacet`` deduplicated by ``facet.id``."""
        with self.transaction() as cur:
            self._upsert_config_facet_locked(cur, facet)

    def persist_config_facet_bundle(
        self,
        *,
        facet: ConfigFacet,
        kv_rows: Optional[List[RunConfigKV]] = None,
    ) -> None:
        """Persist a ``ConfigFacet`` and optional ``RunConfigKV`` rows in one transaction."""
        with _DB_CALL_PROFILER.track("persist_config_facet_bundle"):
            with self.transaction() as cur:
                self._upsert_config_facet_locked(cur, facet)
                if kv_rows:
                    self._replace_run_config_kv_locked(cur, kv_rows)
                    run_ids = {r.run_id for r in kv_rows if r.run_id}
                    for rid in run_ids:
                        cur.execute(
                            "UPDATE run SET facet_id = ? WHERE id = ?",
                            [facet.id, rid],
                        )

    def persist_artifact_facet_bundle(
        self,
        *,
        artifact: Artifact,
        facet: ArtifactFacet,
        meta_updates: Mapping[str, Any],
        kv_rows: Optional[List[ArtifactKV]] = None,
    ) -> Artifact:
        """Persist artifact facet, metadata updates, and optional KV index atomically."""
        with _DB_CALL_PROFILER.track("persist_artifact_facet_bundle"):
            with self.transaction() as cur:
                updated_artifact = self._merge_artifact_meta_locked(
                    cur, artifact, meta_updates
                ).with_facet_id(facet.id)
                self._upsert_artifact_facet_locked(cur, facet)
                self._upsert_artifact_locked(
                    cur, updated_artifact, explicit_facet_id=facet.id
                )
                if kv_rows:
                    self._replace_artifact_kv_locked(cur, kv_rows)
                return updated_artifact

    def sync_artifact_with_facet_bundle(
        self,
        artifact: Artifact,
        run_id: str,
        direction: str,
        *,
        facet: ArtifactFacet,
        meta_updates: Mapping[str, Any],
        kv_rows: Optional[List[ArtifactKV]] = None,
        locations: Optional[Sequence[ArtifactLocationRow]] = None,
    ) -> Artifact:
        """Upsert an Artifact, its run link, and its facet bundle in one transaction."""
        with _DB_CALL_PROFILER.track("sync_artifact_with_facet_bundle"):
            with self.transaction() as cur:
                updated_artifact = artifact.with_meta(
                    meta_updates, merge=True
                ).with_facet_id(facet.id)

                self._upsert_artifact_facet_locked(cur, facet)
                self._upsert_artifact_locked(
                    cur, updated_artifact, explicit_facet_id=facet.id
                )
                if locations:
                    self._upsert_artifact_locations_locked(cur, locations)
                self._link_artifacts_locked(
                    cur,
                    run_id=run_id,
                    artifact_ids=[updated_artifact.id],
                    direction=direction,
                    roles_by_id={updated_artifact.id: updated_artifact.key},
                )
                if kv_rows:
                    self._replace_artifact_kv_locked(cur, kv_rows)
                return updated_artifact

    def insert_run_config_kv_bulk(self, rows: List[RunConfigKV]) -> None:
        """Bulk insert ``RunConfigKV`` rows via ``executemany``."""
        if not rows:
            return
        with self.transaction() as cur:
            self._replace_run_config_kv_locked(cur, rows)

    def insert_artifact_kv_bulk(self, rows: List[ArtifactKV]) -> None:
        """Bulk insert ``ArtifactKV`` rows via ``executemany``."""
        if not rows:
            return
        with self.transaction() as cur:
            self._replace_artifact_kv_locked(cur, rows)

    def _upsert_schema_rows_locked(
        self,
        cur: duckdb.DuckDBPyConnection,
        schema: ArtifactSchema,
        fields: Sequence[ArtifactSchemaField],
        relations: Optional[Sequence[ArtifactSchemaRelation]] = None,
    ) -> None:
        cur.execute(
            """
            INSERT INTO artifact_schema (
                id, profile_version, summary_json, profile_json, created_at
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                profile_version = EXCLUDED.profile_version,
                summary_json = EXCLUDED.summary_json,
                profile_json = EXCLUDED.profile_json
            """,
            [
                schema.id,
                int(schema.profile_version),
                _dump_json(schema.summary_json or {}),
                _dump_optional_json(schema.profile_json),
                _ensure_non_null_utc_dt(schema.created_at),
            ],
        )
        cur.execute(
            "DELETE FROM artifact_schema_field WHERE schema_id = ?",
            [schema.id],
        )
        if fields:
            deduped_fields: dict[tuple[str, str], ArtifactSchemaField] = {}
            for f in fields:
                deduped_fields[(f.schema_id, f.name)] = f
            _execute_multirow_values(
                cur,
                prefix_sql=(
                    "INSERT INTO artifact_schema_field ("
                    "schema_id, ordinal_position, name, logical_type, "
                    "nullable, stats_json, is_enum, enum_values_json"
                    ") VALUES"
                ),
                row_placeholder="(?, ?, ?, ?, ?, ?, ?, ?)",
                rows=[
                    [
                        f.schema_id,
                        f.ordinal_position,
                        f.name,
                        f.logical_type,
                        bool(f.nullable),
                        _dump_optional_json(f.stats_json),
                        bool(f.is_enum),
                        _dump_optional_json(f.enum_values_json),
                    ]
                    for f in deduped_fields.values()
                ],
            )
        cur.execute(
            "DELETE FROM artifact_schema_relation WHERE schema_id = ?",
            [schema.id],
        )
        if relations:
            deduped_relations: dict[tuple[str, str], ArtifactSchemaRelation] = {}
            for rel in relations:
                deduped_relations[(rel.schema_id, rel.from_field)] = rel
            _execute_multirow_values(
                cur,
                prefix_sql=(
                    "INSERT INTO artifact_schema_relation ("
                    "schema_id, from_field, to_table, to_field, "
                    "relationship_type, cardinality"
                    ") VALUES"
                ),
                row_placeholder="(?, ?, ?, ?, ?, ?)",
                rows=[
                    [
                        rel.schema_id,
                        rel.from_field,
                        rel.to_table,
                        rel.to_field,
                        rel.relationship_type,
                        rel.cardinality,
                    ]
                    for rel in deduped_relations.values()
                ],
            )

    def _insert_schema_observation_locked(
        self,
        cur: duckdb.DuckDBPyConnection,
        observation: ArtifactSchemaObservation,
    ) -> None:
        cur.execute(
            """
            INSERT INTO artifact_schema_observation (
                id, artifact_id, schema_id, run_id, source, sample_rows, observed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                artifact_id = EXCLUDED.artifact_id,
                schema_id = EXCLUDED.schema_id,
                run_id = EXCLUDED.run_id,
                source = EXCLUDED.source,
                sample_rows = EXCLUDED.sample_rows,
                observed_at = EXCLUDED.observed_at
            """,
            [
                _coerce_uuid(observation.id),
                _coerce_uuid(observation.artifact_id),
                observation.schema_id,
                observation.run_id,
                observation.source,
                observation.sample_rows,
                _ensure_non_null_utc_dt(observation.observed_at),
            ],
        )

    def upsert_artifact_schema(
        self,
        schema: ArtifactSchema,
        fields: List[ArtifactSchemaField],
        relations: Optional[List[ArtifactSchemaRelation]] = None,
    ) -> None:
        """Upsert a deduplicated ``ArtifactSchema`` and its field/relation rows."""
        with self.transaction() as cur:
            self._upsert_schema_rows_locked(cur, schema, fields, relations)

    def persist_artifact_schema(
        self,
        schema: ArtifactSchema,
        fields: Optional[List[ArtifactSchemaField]] = None,
        relations: Optional[List[ArtifactSchemaRelation]] = None,
        observation: Optional[ArtifactSchemaObservation] = None,
    ) -> None:
        """Persist an ``ArtifactSchema`` with optional fields, relations, and observation."""
        with self.transaction() as cur:
            self._upsert_schema_rows_locked(cur, schema, fields or [], relations)
            if observation is not None:
                self._insert_schema_observation_locked(cur, observation)

    def persist_artifact_schema_profile(
        self,
        *,
        artifact: Artifact,
        schema: ArtifactSchema,
        fields: List[ArtifactSchemaField],
        observation: ArtifactSchemaObservation,
        meta_updates: Mapping[str, Any],
        relations: Optional[List[ArtifactSchemaRelation]] = None,
    ) -> Artifact:
        """Persist schema rows, observation, and artifact metadata in one transaction."""
        with _DB_CALL_PROFILER.track("persist_artifact_schema_profile"):
            with self.transaction() as cur:
                updated_artifact = self._merge_artifact_meta_locked(
                    cur, artifact, meta_updates
                )
                self._upsert_schema_rows_locked(cur, schema, fields, relations)
                self._insert_schema_observation_locked(cur, observation)
                self._upsert_artifact_locked(cur, updated_artifact)
                return updated_artifact

    def persist_artifact_schema_observation_bundle(
        self,
        *,
        artifact: Artifact,
        observation: ArtifactSchemaObservation,
        meta_updates: Mapping[str, Any],
    ) -> Artifact:
        """Persist a schema observation plus artifact metadata in one transaction."""
        with _DB_CALL_PROFILER.track("persist_artifact_schema_observation_bundle"):
            with self.transaction() as cur:
                updated_artifact = self._merge_artifact_meta_locked(
                    cur, artifact, meta_updates
                )
                self._insert_schema_observation_locked(cur, observation)
                self._upsert_artifact_locked(cur, updated_artifact)
                return updated_artifact

    def insert_artifact_schema_observation(
        self, observation: ArtifactSchemaObservation
    ) -> None:
        """Insert a new ``ArtifactSchemaObservation`` row."""
        with self.transaction() as cur:
            self._insert_schema_observation_locked(cur, observation)

    def get_artifact_schema_relations(
        self, *, schema_id: str
    ) -> List[ArtifactSchemaRelation]:
        rows = self.fetchall(
            "SELECT schema_id, from_field, to_table, to_field, "
            "relationship_type, cardinality "
            "FROM artifact_schema_relation WHERE schema_id = ?",
            [schema_id],
        )
        return [_hydrate_schema_relation_row(row) for row in rows]

    def apply_physical_fks(self) -> int:
        """Best-effort creation of physical FOREIGN KEY constraints in DuckDB."""
        self._require_writable("apply_physical_fks")
        rows = self.fetchall(
            """
            SELECT
                rel.from_field,
                rel.to_table,
                rel.to_field,
                src.summary_json
            FROM artifact_schema_relation rel
            JOIN artifact_schema src ON rel.schema_id = src.id
            """
        )
        applied = 0
        for raw_from_field, raw_to_table, raw_to_field, raw_summary in rows:
            summary = _parse_json_dict(raw_summary)
            table_name = summary.get("table_name")
            table_schema = summary.get("table_schema")
            if not isinstance(table_name, str) or not table_name:
                continue
            if not isinstance(raw_from_field, str) or not raw_from_field:
                continue
            if not isinstance(raw_to_table, str) or not raw_to_table:
                continue
            if not isinstance(raw_to_field, str) or not raw_to_field:
                continue

            try:
                from_parts: list[str] = []
                if isinstance(table_schema, str) and table_schema:
                    from_parts.append(
                        _validate_identifier(table_schema, label="table_schema")
                    )
                from_parts.append(_validate_identifier(table_name, label="table_name"))
                quoted_from_table = ".".join(_quote_ident(part) for part in from_parts)
                from_table = ".".join(from_parts)

                safe_from_field = _validate_identifier(
                    raw_from_field, label="from_field"
                )
                safe_to_field = _validate_identifier(raw_to_field, label="to_field")
                to_parts = raw_to_table.split(".")
                if (
                    len(to_parts) == 1
                    and isinstance(table_schema, str)
                    and table_schema
                ):
                    quoted_to_table = (
                        f"{_quote_ident(_validate_identifier(table_schema, label='to_schema'))}."
                        f"{_quote_ident(_validate_identifier(to_parts[0], label='to_table'))}"
                    )
                else:
                    quoted_to_table = _quote_qualified_identifier(
                        raw_to_table, label="to_table"
                    )

                constraint = _validate_identifier(
                    f"fk_{'_'.join(from_parts)}_{safe_from_field}",
                    label="constraint_name",
                )
            except ValueError as exc:
                logging.warning(
                    "Skipping FK creation due to invalid identifier(s) "
                    "(table=%s, from_field=%s, to_table=%s, to_field=%s): %s",
                    table_name,
                    raw_from_field,
                    raw_to_table,
                    raw_to_field,
                    exc,
                )
                continue

            stmt = (
                f"ALTER TABLE {quoted_from_table} "
                f"ADD CONSTRAINT {_quote_ident(constraint)} "
                f"FOREIGN KEY ({_quote_ident(safe_from_field)}) "
                f"REFERENCES {quoted_to_table}({_quote_ident(safe_to_field)})"
            )
            try:
                self.execute(stmt)
                applied += 1
            except duckdb.Error as exc:
                logging.warning(
                    "Failed to apply FK %s (%s.%s -> %s.%s): %s",
                    constraint,
                    from_table,
                    raw_from_field,
                    raw_to_table,
                    raw_to_field,
                    exc,
                )
        return applied

    def link_artifact_to_run(
        self,
        artifact_id: uuid.UUID,
        run_id: str,
        direction: str,
        *,
        role: str = "",
        is_implicit: bool = False,
    ) -> None:
        """Link an artifact to a run without modifying the artifact row."""
        aid = _coerce_uuid(artifact_id)
        with self.transaction() as cur:
            self._link_artifacts_locked(
                cur,
                run_id=run_id,
                artifact_ids=[aid],
                direction=direction,
                roles_by_id={aid: role} if role else None,
                is_implicit=is_implicit,
            )

    def link_run_artifact(
        self,
        *,
        run_id: str,
        artifact_id: uuid.UUID,
        direction: str,
        role: str = "",
        is_implicit: bool = False,
    ) -> None:
        """Keyword-oriented alias for ``link_artifact_to_run``."""
        self.link_artifact_to_run(
            artifact_id,
            run_id,
            direction,
            role=role,
            is_implicit=is_implicit,
        )

    def link_artifacts_to_run_bulk(
        self,
        *,
        artifact_ids: List[uuid.UUID],
        run_id: str,
        direction: str,
        roles_by_id: Optional[Mapping[uuid.UUID, str]] = None,
        is_implicit: bool = False,
    ) -> None:
        """Bulk link multiple artifacts to a run in one transaction."""
        if not artifact_ids:
            return
        with _DB_CALL_PROFILER.track("link_artifacts_to_run_bulk"):
            with self.transaction() as cur:
                self._link_artifacts_locked(
                    cur,
                    run_id=run_id,
                    artifact_ids=artifact_ids,
                    direction=direction,
                    roles_by_id=roles_by_id,
                    is_implicit=is_implicit,
                )

    def sync_run_with_links(
        self,
        *,
        run: Run,
        artifact_ids: List[uuid.UUID],
        direction: str = "output",
        binding_invocation: Optional[RunBindingInvocation] = None,
        require_success: bool = False,
    ) -> None:
        """Upsert a Run and link artifacts in a single transaction."""
        if not artifact_ids:
            self.sync_run(
                run,
                binding_invocation=binding_invocation,
                require_success=require_success,
            )
            return
        try:
            with _DB_CALL_PROFILER.track("sync_run_with_links"):
                with _track_begin_run_phase("db.sync_run_with_links.transaction"):
                    with self.transaction() as cur:
                        self._upsert_run_locked(cur, run)
                        if binding_invocation is not None:
                            self._insert_binding_invocation_locked(
                                cur, binding_invocation
                            )
                        self._link_artifacts_locked(
                            cur,
                            run_id=run.id,
                            artifact_ids=artifact_ids,
                            direction=direction,
                        )
        except Exception as exc:
            logging.warning("Database sync failed: %s", exc)
            if require_success or self._tx_depth > 0:
                raise

    def sync_artifact(
        self,
        artifact: Artifact,
        run_id: Optional[str] = None,
        direction: Optional[str] = None,
        *,
        profile_label: Optional[str] = None,
        role: Optional[str] = None,
        locations: Optional[Sequence[ArtifactLocationRow]] = None,
    ) -> None:
        """Upsert an Artifact and optionally link it to a Run atomically."""
        with _DB_CALL_PROFILER.track(
            _profile_call_name("sync_artifact", profile_label)
        ):
            with self.transaction() as cur:
                self._upsert_artifact_locked(cur, artifact)
                if locations:
                    self._upsert_artifact_locations_locked(cur, locations)
                if run_id is not None and direction is not None:
                    aid = _coerce_uuid(artifact.id)
                    resolved_role = artifact.key if role is None else role
                    self._link_artifacts_locked(
                        cur,
                        run_id=run_id,
                        artifact_ids=[aid],
                        direction=direction,
                        roles_by_id={aid: resolved_role},
                    )

    def upsert_artifact(
        self,
        artifact: Artifact,
        *,
        locations: Optional[Sequence[ArtifactLocationRow]] = None,
    ) -> None:
        """Persist an artifact that is not yet linked to a run."""
        with self.transaction() as cur:
            self._upsert_artifact_locked(cur, artifact)
            if locations:
                self._upsert_artifact_locations_locked(cur, locations)

    def sync_artifacts(
        self,
        *,
        artifacts: Sequence[Artifact],
        run_id: str,
        direction: str,
        locations: Optional[Sequence[ArtifactLocationRow]] = None,
    ) -> None:
        """Upsert multiple Artifacts and link them to a Run in one transaction."""
        if not artifacts:
            return
        with _DB_CALL_PROFILER.track("sync_artifacts"):
            with _track_begin_run_phase("db.sync_artifacts.transaction"):
                deduped: dict[uuid.UUID, Artifact] = {}
                for artifact in artifacts:
                    deduped[_coerce_uuid(artifact.id)] = artifact
                artifact_list = list(deduped.values())
                roles_by_id = {_coerce_uuid(art.id): art.key for art in artifact_list}
                with self.transaction() as cur:
                    _execute_multirow_values(
                        cur,
                        prefix_sql=self._ARTIFACT_INSERT_PREFIX,
                        row_placeholder="(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        suffix_sql=self._ARTIFACT_UPSERT_SUFFIX,
                        rows=[
                            self._artifact_upsert_params(art) for art in artifact_list
                        ],
                    )
                    if locations:
                        self._upsert_artifact_locations_locked(cur, locations)
                    self._link_artifacts_locked(
                        cur,
                        run_id=run_id,
                        artifact_ids=list(deduped.keys()),
                        direction=direction,
                        roles_by_id=roles_by_id,
                    )

    def backfill_artifact_schema_field_ordinals(self, *, schema_id: str) -> None:
        """Backfill missing ``ordinal_position`` values on ``artifact_schema_field``."""
        if self.read_only:
            return
        s_row = self.fetchone(
            "SELECT profile_json FROM artifact_schema WHERE id = ?",
            [schema_id],
        )
        if s_row is None:
            return
        f_rows = self.fetchall(
            "SELECT name, ordinal_position FROM artifact_schema_field "
            "WHERE schema_id = ? ORDER BY name ASC",
            [schema_id],
        )
        if not f_rows or all(r[1] is not None for r in f_rows):
            return

        ordered_names: list[str] = []
        profile = _parse_optional_json_dict(s_row[0])
        if isinstance(profile, dict):
            fields = profile.get("fields")
            if isinstance(fields, list) and fields:
                for entry in fields:
                    if isinstance(entry, dict) and isinstance(entry.get("name"), str):
                        ordered_names.append(entry["name"])

        updates: list[tuple[int, str, str]] = []
        if ordered_names:
            ordinal_by_name = {name: idx + 1 for idx, name in enumerate(ordered_names)}
            for fname, cur_ord in f_rows:
                if cur_ord is None and str(fname) in ordinal_by_name:
                    updates.append((ordinal_by_name[str(fname)], schema_id, str(fname)))
        else:
            for idx, (fname, cur_ord) in enumerate(f_rows, start=1):
                if cur_ord is None:
                    updates.append((idx, schema_id, str(fname)))

        if updates:
            with self.transaction() as cur:
                _execute_multirow_values(
                    cur,
                    prefix_sql=(
                        "UPDATE artifact_schema_field AS f "
                        "SET ordinal_position = v.ord FROM (VALUES"
                    ),
                    row_placeholder="(CAST(? AS INTEGER), CAST(? AS VARCHAR), CAST(? AS VARCHAR))",
                    suffix_sql=" ) AS v(ord, sid, fname) WHERE f.schema_id = v.sid AND f.name = v.fname",
                    rows=updates,
                )

    def find_latest_artifact_at_uri(
        self,
        uri: str,
        run_id: Optional[str] = None,
        driver: Optional[str] = None,
        member: Optional[str] = None,
        include_inputs: bool = False,
    ) -> Optional[Artifact]:
        """Find the most recent artifact recorded at ``uri``."""
        clauses: list[str] = ["a.uri = ?"]
        params: list[Any] = [uri]
        if not include_inputs:
            clauses.append("a.run_id IS NOT NULL")
        if run_id is not None:
            clauses.append("a.run_id = ?")
            params.append(run_id)
        if driver is not None:
            clauses.append("a.driver = ?")
            params.append(driver)
        if member is not None:
            clauses.append("a.member = ?")
            params.append(member)
        elif driver in {"h5", "hdf5"}:
            clauses.append("a.member IS NULL")

        with _DB_CALL_PROFILER.track("find_latest_artifact_at_uri"):
            row = self.fetchone(
                f"SELECT {_ARTIFACT_SELECT_COLS} FROM artifact a "
                f"WHERE {' AND '.join(clauses)} "
                "ORDER BY a.created_at DESC, a.id DESC LIMIT 1",
                params,
            )
        return _hydrate_artifact_row(row) if row is not None else None

    def find_latest_artifacts_at_uris(
        self,
        lookups: Sequence[Tuple[str, Optional[str], Optional[str]]],
    ) -> Dict[Tuple[str, Optional[str], Optional[str]], Optional[Artifact]]:
        """Bulk variant of produced-artifact URI lookup keyed by ``(uri, driver, member)``."""
        if not lookups:
            return {}
        unique_lookups = list(dict.fromkeys(lookups))
        results: Dict[Tuple[str, Optional[str], Optional[str]], Optional[Artifact]] = {
            lookup: None for lookup in unique_lookups
        }
        lookups_by_uri: Dict[str, List[Tuple[str, Optional[str], Optional[str]]]] = {}
        for lookup in unique_lookups:
            lookups_by_uri.setdefault(lookup[0], []).append(lookup)

        uris = list(lookups_by_uri.keys())
        placeholders = ", ".join("?" for _ in uris)
        with _DB_CALL_PROFILER.track("find_latest_artifacts_at_uris"):
            rows = self.fetchall(
                f"SELECT {_ARTIFACT_SELECT_COLS} FROM artifact a "
                f"WHERE a.run_id IS NOT NULL AND a.uri IN ({placeholders}) "
                "ORDER BY a.created_at DESC, a.id DESC",
                uris,
            )
        for row in rows:
            art = _hydrate_artifact_row(row)
            candidates = lookups_by_uri.get(art.uri, [])
            for lookup in candidates:
                if results[lookup] is not None:
                    continue
                _, l_driver, l_member = lookup
                if l_driver is not None and art.driver != l_driver:
                    continue
                if l_member is not None:
                    if art.member != l_member:
                        continue
                elif l_driver in {"h5", "hdf5"} and art.member is not None:
                    continue
                results[lookup] = art
        return results

    def find_latest_artifact_by_hash(
        self,
        content_hash: str,
        *,
        driver: Optional[str] = None,
        include_inputs: bool = False,
    ) -> Optional[Artifact]:
        """Find the most recent artifact with ``digest == content_hash``."""
        clauses: list[str] = ["a.digest = ?"]
        params: list[Any] = [content_hash]
        if not include_inputs:
            clauses.append("a.run_id IS NOT NULL")
        if driver is not None:
            clauses.append("a.driver = ?")
            params.append(driver)
        row = self.fetchone(
            f"SELECT {_ARTIFACT_SELECT_COLS} FROM artifact a "
            f"WHERE {' AND '.join(clauses)} "
            "ORDER BY a.created_at DESC, a.id DESC LIMIT 1",
            params,
        )
        return _hydrate_artifact_row(row) if row is not None else None

    @staticmethod
    def _content_identity_sql_predicate(
        *,
        content_identity: Optional[str],
        digest: Optional[str],
        driver: Optional[str],
        alias: str = "a",
    ) -> tuple[Optional[str], list[Any]]:
        subclauses: list[str] = []
        params: list[Any] = []
        if content_identity:
            subclauses.append(
                f"({alias}.digest = ? "
                f"OR json_extract_string({alias}.meta, '/content_identity') = ?)"
            )
            params.extend([content_identity, content_identity])
        if digest and driver:
            subclauses.append(f"({alias}.digest = ? AND {alias}.driver = ?)")
            params.extend([digest, driver])
        elif digest and not content_identity:
            subclauses.append(f"{alias}.digest = ?")
            params.append(digest)
        if not subclauses:
            return None, []
        return "(" + " OR ".join(subclauses) + ")", params

    def find_artifacts_by_content_identity(
        self,
        *,
        content_identity: Optional[str] = None,
        digest: Optional[str] = None,
        driver: Optional[str] = None,
    ) -> list[Artifact]:
        """Return all artifact occurrences sharing a content identity or digest+driver."""
        pred, params = self._content_identity_sql_predicate(
            content_identity=content_identity,
            digest=digest,
            driver=driver,
            alias="a",
        )
        if pred is None:
            return []
        rows = self.fetchall(
            f"SELECT {_ARTIFACT_SELECT_COLS} FROM artifact a "
            f"WHERE {pred} ORDER BY a.created_at DESC, a.id DESC",
            params,
        )
        return [_hydrate_artifact_row(r) for r in rows]

    def find_runs_producing_content_identity(
        self,
        *,
        content_identity: Optional[str] = None,
        digest: Optional[str] = None,
        driver: Optional[str] = None,
    ) -> list[str]:
        """Return distinct run IDs that produced artifacts sharing a content identity."""
        pred, params = self._content_identity_sql_predicate(
            content_identity=content_identity,
            digest=digest,
            driver=driver,
            alias="a",
        )
        if pred is None:
            return []
        rows = self.fetchall(
            "SELECT DISTINCT l.run_id FROM run_artifact_link l "
            "JOIN artifact a ON a.id = l.artifact_id "
            f"WHERE l.direction = 'output' AND {pred}",
            params,
        )
        return [str(r[0]) for r in rows if r[0] is not None]

    @staticmethod
    def _rank_schema_observations(
        observations: Sequence[ArtifactSchemaObservation],
        *,
        prefer_source: Optional[str] = None,
    ) -> Optional[ArtifactSchemaObservation]:
        if not observations:
            return None
        user_obs = next(
            (obs for obs in observations if obs.source == "user_provided"), None
        )
        if user_obs is not None:
            return user_obs
        if prefer_source:
            preferred = next(
                (obs for obs in observations if obs.source == prefer_source), None
            )
            if preferred is not None:
                return preferred
        for src in ("file", "duckdb"):
            matched = next((obs for obs in observations if obs.source == src), None)
            if matched is not None:
                return matched
        return observations[0]

    def find_schema_observation_for_hash(
        self,
        content_hash: str,
        *,
        prefer_source: Optional[str] = None,
    ) -> Optional[ArtifactSchemaObservation]:
        """Find the highest-priority schema observation for artifacts matching ``content_hash``."""
        with _DB_CALL_PROFILER.track("find_schema_observation_for_hash"):
            rows = self.fetchall(
                "SELECT o.id, o.artifact_id, o.schema_id, o.run_id, "
                "o.source, o.sample_rows, o.observed_at "
                "FROM artifact_schema_observation o "
                "JOIN artifact a ON o.artifact_id = a.id "
                "WHERE a.digest = ? "
                "ORDER BY o.observed_at DESC",
                [content_hash],
            )
        observations = [_hydrate_schema_observation_row(r) for r in rows]
        return self._rank_schema_observations(observations, prefer_source=prefer_source)

    def find_schema_observation_for_content_identity(
        self,
        *,
        content_identity: Optional[str] = None,
        digest: Optional[str] = None,
        driver: Optional[str] = None,
        prefer_source: Optional[str] = None,
    ) -> Optional[ArtifactSchemaObservation]:
        """Find the highest-priority schema observation for artifacts sharing a content identity."""
        pred, params = self._content_identity_sql_predicate(
            content_identity=content_identity,
            digest=digest,
            driver=driver,
            alias="a",
        )
        if pred is None:
            return None
        with _DB_CALL_PROFILER.track("find_schema_observation_for_content_identity"):
            rows = self.fetchall(
                "SELECT o.id, o.artifact_id, o.schema_id, o.run_id, "
                "o.source, o.sample_rows, o.observed_at "
                "FROM artifact_schema_observation o "
                "JOIN artifact a ON o.artifact_id = a.id "
                f"WHERE {pred} "
                "ORDER BY o.observed_at DESC",
                params,
            )
        observations = [_hydrate_schema_observation_row(r) for r in rows]
        return self._rank_schema_observations(observations, prefer_source=prefer_source)

    def get_run(self, run_id: str) -> Optional[Run]:
        """Fetch a single ``Run`` by primary key."""
        row = self.fetchone(
            f"SELECT {_RUN_SELECT_COLS} FROM run r WHERE r.id = ?",
            [run_id],
        )
        return _hydrate_run_row(row) if row is not None else None

    def get_run_signatures(self, run_ids: List[str]) -> Dict[str, str]:
        """Bulk lookup of ``run_id -> signature`` for runs that have a signature."""
        unique_ids = list(dict.fromkeys(run_ids))
        if not unique_ids:
            return {}
        placeholders = ", ".join("?" for _ in unique_ids)
        rows = self.fetchall(
            f"SELECT id, signature FROM run WHERE id IN ({placeholders}) AND signature IS NOT NULL",
            unique_ids,
        )
        return {str(r[0]): str(r[1]) for r in rows if r[1]}

    def find_matching_run(
        self, config_hash: str, input_hash: str, git_hash: str
    ) -> Optional[Run]:
        """Return the newest cacheable completed run matching ``(config_hash, input_hash, git_hash)``."""
        if (
            not config_hash
            or not input_hash
            or not git_hash
            or config_hash == "error"
            or input_hash == "error"
            or git_hash == "error"
        ):
            return None
        with _DB_CALL_PROFILER.track("cache_lookup"):
            row = self.fetchone(
                "WITH cand AS MATERIALIZED ("
                f"SELECT {_RUN_SELECT_COLS} FROM run r WHERE r.config_hash = ?"
                ") "
                "SELECT * FROM cand r "
                "WHERE r.input_hash = ? AND r.git_hash = ? "
                "AND r.input_hash != 'error' "
                "AND r.status = 'completed' AND r.cacheable = TRUE "
                f"ORDER BY {recent_run_order_by_sql('r')} LIMIT 1",
                [config_hash, input_hash, git_hash],
            )
        return _hydrate_run_row(row) if row is not None else None

    def find_matching_runs(
        self, config_hash: str, input_hash: str, git_hash: str
    ) -> list[Run]:
        """Return all cacheable completed runs matching ``(config_hash, input_hash, git_hash)`` newest first."""
        if (
            not config_hash
            or not input_hash
            or not git_hash
            or config_hash == "error"
            or input_hash == "error"
            or git_hash == "error"
        ):
            return []
        with _DB_CALL_PROFILER.track("cache_lookup_candidates"):
            rows = self.fetchall(
                "WITH cand AS MATERIALIZED ("
                f"SELECT {_RUN_SELECT_COLS} FROM run r WHERE r.config_hash = ?"
                ") "
                "SELECT * FROM cand r "
                "WHERE r.input_hash = ? AND r.git_hash = ? "
                "AND r.input_hash != 'error' "
                "AND r.status = 'completed' AND r.cacheable = TRUE "
                f"ORDER BY {recent_run_order_by_sql('r')}",
                [config_hash, input_hash, git_hash],
            )
        return [_hydrate_run_row(r) for r in rows]

    def find_recent_completed_runs_for_model(
        self, model_name: str, *, limit: int = 20
    ) -> list[Run]:
        """Return up to ``limit`` recent completed runs for ``model_name``."""
        rows = self.fetchall(
            f"SELECT {_RUN_SELECT_COLS} FROM run r "
            "WHERE r.status = 'completed' AND r.model_name = ? "
            f"ORDER BY {recent_run_order_by_sql('r')} LIMIT ?",
            [model_name, max(0, int(limit))],
        )
        return [_hydrate_run_row(r) for r in rows]

    def find_run_by_signature(self, signature: str) -> Optional[Run]:
        """Find the newest cacheable completed run by composite ``signature``."""
        if not signature or signature == "error":
            return None
        row = self.fetchone(
            "WITH cand AS MATERIALIZED ("
            f"SELECT {_RUN_SELECT_COLS} FROM run r WHERE r.signature = ?"
            ") "
            "SELECT * FROM cand r "
            "WHERE r.signature != 'error' AND r.input_hash != 'error' "
            "AND r.status = 'completed' AND r.cacheable = TRUE "
            f"ORDER BY {recent_run_order_by_sql('r')} LIMIT 1",
            [signature],
        )
        return _hydrate_run_row(row) if row is not None else None

    def find_runs_by_signature(self, signature: str) -> list[Run]:
        """Return all cacheable completed runs matching ``signature`` newest first."""
        if not signature or signature == "error":
            return []
        rows = self.fetchall(
            "WITH cand AS MATERIALIZED ("
            f"SELECT {_RUN_SELECT_COLS} FROM run r WHERE r.signature = ?"
            ") "
            "SELECT * FROM cand r "
            "WHERE r.signature != 'error' AND r.input_hash != 'error' "
            "AND r.status = 'completed' AND r.cacheable = TRUE "
            f"ORDER BY {recent_run_order_by_sql('r')}",
            [signature],
        )
        return [_hydrate_run_row(r) for r in rows]

    def get_artifacts_by_ids(
        self, artifact_ids: Sequence[uuid.UUID | str]
    ) -> List[Artifact]:
        """Fetch multiple artifacts by UUID, preserving input order where found."""
        coerced = [_coerce_uuid(aid) for aid in artifact_ids]
        unique_ids = list(dict.fromkeys(coerced))
        if not unique_ids:
            return []
        placeholders = ", ".join("?" for _ in unique_ids)
        rows = self.fetchall(
            f"SELECT {_ARTIFACT_SELECT_COLS} FROM artifact a "
            f"WHERE a.id IN ({placeholders})",
            unique_ids,
        )
        by_id = {_coerce_uuid(row[0]): _hydrate_artifact_row(row) for row in rows}
        return [by_id[aid] for aid in unique_ids if aid in by_id]

    def get_artifact(
        self,
        key_or_id: str | uuid.UUID,
        *,
        run_id: Optional[str] = None,
    ) -> Optional[Artifact]:
        """Fetch an artifact by UUID or semantic key, optionally scoped to ``run_id``."""
        parsed_uuid: Optional[uuid.UUID] = None
        if isinstance(key_or_id, uuid.UUID):
            parsed_uuid = key_or_id
        else:
            try:
                parsed_uuid = uuid.UUID(str(key_or_id))
            except ValueError:
                parsed_uuid = None

        if parsed_uuid is not None:
            if run_id is not None:
                row = self.fetchone(
                    f"SELECT {_ARTIFACT_SELECT_COLS} FROM artifact a "
                    "JOIN run_artifact_link l ON l.artifact_id = a.id "
                    "WHERE a.id = ? AND l.run_id = ? LIMIT 1",
                    [parsed_uuid, run_id],
                )
            else:
                row = self.fetchone(
                    f"SELECT {_ARTIFACT_SELECT_COLS} FROM artifact a WHERE a.id = ?",
                    [parsed_uuid],
                )
            return _hydrate_artifact_row(row) if row is not None else None

        if run_id is not None:
            row = self.fetchone(
                f"SELECT {_ARTIFACT_SELECT_COLS} FROM artifact a "
                "JOIN run_artifact_link l ON l.artifact_id = a.id "
                "WHERE a.key = ? AND l.run_id = ? "
                "ORDER BY a.created_at DESC, a.id DESC LIMIT 1",
                [str(key_or_id), run_id],
            )
        else:
            row = self.fetchone(
                f"SELECT {_ARTIFACT_SELECT_COLS} FROM artifact a "
                "WHERE a.key = ? ORDER BY a.created_at DESC, a.id DESC LIMIT 1",
                [str(key_or_id)],
            )
        return _hydrate_artifact_row(row) if row is not None else None

    def get_child_artifacts(self, parent_id: uuid.UUID) -> List[Artifact]:
        """Return child artifacts referencing ``parent_id`` ordered by creation time."""
        pid = _coerce_uuid(parent_id)
        rows = self.fetchall(
            f"SELECT {_ARTIFACT_SELECT_COLS} FROM artifact a "
            "WHERE a.parent_artifact_id = ? "
            "OR json_extract_string(a.meta, '/parent_id') = ? "
            "ORDER BY a.created_at ASC, a.key ASC, a.id ASC",
            [pid, str(pid)],
        )
        seen: set[uuid.UUID] = set()
        results: list[Artifact] = []
        for row in rows:
            art = _hydrate_artifact_row(row)
            if art.id not in seen:
                seen.add(art.id)
                results.append(art)
        return results

    def get_parent_artifact(self, child_id: uuid.UUID) -> Optional[Artifact]:
        """Return the parent artifact for ``child_id`` if recorded."""
        child = self.get_artifact(_coerce_uuid(child_id))
        if child is None:
            return None
        parent_id = child.parent_artifact_id
        if parent_id is None and isinstance(child.meta, Mapping):
            raw_pid = child.meta.get("parent_id")
            if isinstance(raw_pid, str):
                try:
                    parent_id = uuid.UUID(raw_pid)
                except ValueError:
                    parent_id = None
        if parent_id is None:
            return None
        return self.get_artifact(parent_id)

    def select_artifact_schema_for_artifact(
        self,
        *,
        artifact_id: uuid.UUID,
        prefer_source: Optional[SchemaProfileSource] = None,
        strict_source: bool = False,
    ) -> Optional[ArtifactSchemaSelection]:
        """Select a schema profile for ``artifact_id`` with explainable selection metadata."""
        aid = _coerce_uuid(artifact_id)
        rows = self.fetchall(
            "SELECT id, artifact_id, schema_id, run_id, source, sample_rows, observed_at "
            "FROM artifact_schema_observation "
            "WHERE artifact_id = ? ORDER BY observed_at DESC",
            [aid],
        )
        if not rows:
            return None
        observations = [_hydrate_schema_observation_row(r) for r in rows]
        candidate_count = len(observations)

        if strict_source and prefer_source is not None:
            explicit = next(
                (obs for obs in observations if obs.source == prefer_source), None
            )
            if explicit is None:
                raise ValueError(
                    f"No schema observation with source '{prefer_source}' "
                    f"for artifact '{aid}'. Available candidates: {candidate_count}."
                )
            return ArtifactSchemaSelection(
                schema_id=explicit.schema_id,
                source=explicit.source,
                candidate_count=candidate_count,
                selection_rule=f"explicit source={prefer_source}",
            )

        user_provided_obs = next(
            (obs for obs in observations if obs.source == "user_provided"), None
        )
        if user_provided_obs is not None:
            return ArtifactSchemaSelection(
                schema_id=user_provided_obs.schema_id,
                source=user_provided_obs.source,
                candidate_count=candidate_count,
                selection_rule="user_provided override",
            )

        if prefer_source is not None:
            preferred = next(
                (obs for obs in observations if obs.source == prefer_source), None
            )
            if preferred is not None:
                return ArtifactSchemaSelection(
                    schema_id=preferred.schema_id,
                    source=preferred.source,
                    candidate_count=candidate_count,
                    selection_rule=f"preferred source={prefer_source}",
                )

        for source in ("file", "duckdb"):
            fallback = next((obs for obs in observations if obs.source == source), None)
            if fallback is not None:
                return ArtifactSchemaSelection(
                    schema_id=fallback.schema_id,
                    source=fallback.source,
                    candidate_count=candidate_count,
                    selection_rule="default source order file>duckdb",
                )

        most_recent = observations[0]
        return ArtifactSchemaSelection(
            schema_id=most_recent.schema_id,
            source=most_recent.source,
            candidate_count=candidate_count,
            selection_rule="most recent observation fallback",
        )

    def get_artifact_schema(
        self, *, schema_id: str, backfill_ordinals: bool = True
    ) -> Optional[Tuple[ArtifactSchema, List[ArtifactSchemaField]]]:
        """Fetch ``(ArtifactSchema, List[ArtifactSchemaField])`` by ``schema_id``."""
        with _DB_CALL_PROFILER.track("get_artifact_schema"):
            s_row = self.fetchone(
                "SELECT id, profile_version, summary_json, profile_json, created_at "
                "FROM artifact_schema WHERE id = ?",
                [schema_id],
            )
            if s_row is None:
                return None
            f_rows = self.fetchall(
                "SELECT schema_id, ordinal_position, name, logical_type, nullable, "
                "stats_json, is_enum, enum_values_json "
                "FROM artifact_schema_field WHERE schema_id = ?",
                [schema_id],
            )

        schema = _hydrate_schema_row(s_row)
        fields = [_hydrate_schema_field_row(r) for r in f_rows]

        if (
            backfill_ordinals
            and not self.read_only
            and any(f.ordinal_position is None for f in fields)
        ):
            self.backfill_artifact_schema_field_ordinals(schema_id=schema_id)
            f_rows = self.fetchall(
                "SELECT schema_id, ordinal_position, name, logical_type, nullable, "
                "stats_json, is_enum, enum_values_json "
                "FROM artifact_schema_field WHERE schema_id = ?",
                [schema_id],
            )
            fields = [_hydrate_schema_field_row(r) for r in f_rows]

        fields_sorted = sorted(
            fields,
            key=lambda r: (
                r.ordinal_position is None,
                r.ordinal_position if r.ordinal_position is not None else 0,
                r.name,
            ),
        )
        return schema, fields_sorted

    def get_artifact_schema_for_artifact(
        self,
        *,
        artifact_id: uuid.UUID,
        backfill_ordinals: bool = True,
        prefer_source: Optional[SchemaProfileSource] = None,
    ) -> Optional[Tuple[ArtifactSchema, List[ArtifactSchemaField]]]:
        """Fetch the selected schema and fields for ``artifact_id``."""
        selection = self.select_artifact_schema_for_artifact(
            artifact_id=artifact_id,
            prefer_source=prefer_source,
            strict_source=False,
        )
        if selection is None:
            return None
        return self.get_artifact_schema(
            schema_id=selection.schema_id,
            backfill_ordinals=backfill_ordinals,
        )

    def get_artifacts_for_run(self, run_id: str) -> List[Tuple[Artifact, str]]:
        """Return ``(Artifact, direction)`` pairs linked to ``run_id``."""
        rows = self.fetchall(
            f"SELECT {_ARTIFACT_SELECT_COLS}, l.direction "
            "FROM run_artifact_link l "
            "JOIN artifact a ON a.id = l.artifact_id "
            "WHERE l.run_id = ? "
            "ORDER BY a.created_at ASC, a.key ASC, a.id ASC",
            [run_id],
        )
        return [(_hydrate_artifact_row(r[:11]), str(r[11])) for r in rows]

    def get_output_artifacts_for_run(self, run_id: str) -> List[Artifact]:
        """Return output-linked artifacts for ``run_id`` without key collapsing."""
        rows = self.fetchall(
            f"SELECT {_ARTIFACT_SELECT_COLS} "
            "FROM run_artifact_link l "
            "JOIN artifact a ON a.id = l.artifact_id "
            "WHERE l.run_id = ? AND l.direction = 'output' "
            "ORDER BY a.created_at ASC, a.key ASC, a.id ASC",
            [run_id],
        )
        return [_hydrate_artifact_row(r) for r in rows]

    def get_output_artifacts_by_run_ids(
        self,
        run_ids: Sequence[str],
        *,
        keys: Optional[Sequence[str]] = None,
    ) -> Tuple[Dict[str, Run], Dict[str, Dict[str, Artifact]]]:
        """Fetch runs and output artifacts mapped by ``{run_id: {key: Artifact}}``."""
        unique_run_ids = list(dict.fromkeys(run_ids))
        if not unique_run_ids:
            return {}, {}
        key_filter = list(dict.fromkeys(keys)) if keys is not None else None
        if key_filter is not None and not key_filter:
            return {}, {rid: {} for rid in unique_run_ids}

        run_ph = ", ".join("?" for _ in unique_run_ids)
        run_rows = self.fetchall(
            f"SELECT {_RUN_SELECT_COLS} FROM run r WHERE r.id IN ({run_ph})",
            unique_run_ids,
        )
        runs_by_id = {r.id: r for r in (_hydrate_run_row(row) for row in run_rows)}

        art_params: list[Any] = list(unique_run_ids)
        key_clause = ""
        if key_filter is not None:
            key_ph = ", ".join("?" for _ in key_filter)
            key_clause = f" AND a.key IN ({key_ph})"
            art_params.extend(key_filter)

        art_rows = self.fetchall(
            f"SELECT {_ARTIFACT_SELECT_COLS}, l.run_id "
            "FROM run_artifact_link l "
            "JOIN artifact a ON a.id = l.artifact_id "
            f"WHERE l.run_id IN ({run_ph}) AND l.direction = 'output'{key_clause} "
            "ORDER BY a.created_at ASC, a.key ASC, a.id ASC",
            art_params,
        )
        outputs_by_run: Dict[str, Dict[str, Artifact]] = {
            rid: {} for rid in unique_run_ids
        }
        for row in art_rows:
            art = _hydrate_artifact_row(row[:11])
            linked_run_id = str(row[11])
            if linked_run_id in outputs_by_run:
                outputs_by_run[linked_run_id][art.key] = art
        return runs_by_id, outputs_by_run

    def find_artifacts(
        self,
        *,
        creator: Optional[str] = None,
        consumer: Optional[str] = None,
        key: Optional[str] = None,
        limit: int = 100,
    ) -> List[Artifact]:
        """Find artifacts filtered by producing run, consuming run, or key."""
        joins: list[str] = []
        clauses: list[str] = []
        params: list[Any] = []
        if creator:
            joins.append("JOIN run_artifact_link out_l ON out_l.artifact_id = a.id")
            clauses.append("out_l.run_id = ? AND out_l.direction = 'output'")
            params.append(creator)
        if consumer:
            joins.append("JOIN run_artifact_link in_l ON in_l.artifact_id = a.id")
            clauses.append("in_l.run_id = ? AND in_l.direction = 'input'")
            params.append(consumer)
        if key:
            clauses.append("a.key = ?")
            params.append(key)

        join_sql = (" " + " ".join(joins)) if joins else ""
        where_sql = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(max(0, int(limit)))

        rows = self.fetchall(
            f"SELECT DISTINCT {_ARTIFACT_SELECT_COLS} FROM artifact a"
            f"{join_sql}{where_sql} "
            "ORDER BY a.created_at DESC, a.id DESC LIMIT ?",
            params,
        )
        return [_hydrate_artifact_row(r) for r in rows]

    @staticmethod
    def append_run_tag_and_metadata_clauses(
        clauses: list[str],
        params: list[Any],
        *,
        tags: Optional[Sequence[str]] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        table_alias: str = "r",
    ) -> bool:
        """Append parameterized DuckDB SQL predicates for ``Run.tags`` and ``Run.meta``."""
        _validate_identifier(table_alias, label="table_alias")

        if tags:
            req_tags = list(dict.fromkeys(str(t) for t in tags))
            if req_tags:
                clauses.append(
                    f"CASE WHEN {table_alias}.tags IS NOT NULL "
                    f"AND json_type({table_alias}.tags) = 'ARRAY' "
                    f"THEN COALESCE(list_has_all(TRY_CAST({table_alias}.tags AS VARCHAR[]), ?), FALSE) "
                    "ELSE FALSE END"
                )
                params.append(req_tags)

        needs_client_metadata_filter = False
        if metadata:
            if any(
                isinstance(mk, str) and mv is not None for mk, mv in metadata.items()
            ):
                clauses.append(
                    f"({table_alias}.meta IS NOT NULL "
                    f"AND json_type({table_alias}.meta) = 'OBJECT')"
                )

            def _append_json_pointer_predicate(
                ptr: str, value: Any, *, is_top_level: bool
            ) -> None:
                nonlocal needs_client_metadata_filter
                is_bool = isinstance(value, bool) or type(value).__name__ == "bool_"
                if value is None:
                    if is_top_level:
                        clauses.append(
                            f"({table_alias}.meta IS NULL "
                            f"OR json_type({table_alias}.meta) != 'OBJECT' "
                            f"OR COALESCE(json_type({table_alias}.meta, ?), 'NULL') = 'NULL')"
                        )
                    else:
                        clauses.append(f"json_type({table_alias}.meta, ?) = 'NULL'")
                    params.append(ptr)
                elif is_bool:
                    clauses.append(
                        f"(json_type({table_alias}.meta, ?) = 'BOOLEAN' "
                        f"AND TRY_CAST(json_extract({table_alias}.meta, ?) AS BOOLEAN) = ?)"
                    )
                    params.extend([ptr, ptr, bool(value)])
                elif isinstance(value, numbers.Integral) and not is_bool:
                    iv = int(value)
                    if abs(iv) < (1 << 53):
                        clauses.append(
                            f"(json_type({table_alias}.meta, ?) IN ('BIGINT', 'UBIGINT', 'DOUBLE') "
                            f"AND TRY_CAST(json_extract({table_alias}.meta, ?) AS DOUBLE) = ?)"
                        )
                        params.extend([ptr, ptr, float(iv)])
                    else:
                        needs_client_metadata_filter = True
                        clauses.append(
                            f"json_type({table_alias}.meta, ?) IN ('BIGINT', 'UBIGINT', 'DOUBLE')"
                        )
                        params.append(ptr)
                elif isinstance(value, numbers.Real) and not is_bool:
                    fv = float(value)
                    if not math.isfinite(fv):
                        clauses.append("1 = 0")
                    elif abs(fv) < (1 << 53):
                        clauses.append(
                            f"(json_type({table_alias}.meta, ?) IN ('BIGINT', 'UBIGINT', 'DOUBLE') "
                            f"AND TRY_CAST(json_extract({table_alias}.meta, ?) AS DOUBLE) = ?)"
                        )
                        params.extend([ptr, ptr, fv])
                    else:
                        needs_client_metadata_filter = True
                        clauses.append(
                            f"json_type({table_alias}.meta, ?) IN ('BIGINT', 'UBIGINT', 'DOUBLE')"
                        )
                        params.append(ptr)
                elif isinstance(value, str):
                    clauses.append(
                        f"(json_type({table_alias}.meta, ?) = 'VARCHAR' "
                        f"AND json_extract_string({table_alias}.meta, ?) = ?)"
                    )
                    params.extend([ptr, ptr, value])
                elif isinstance(value, Mapping):
                    clauses.append(
                        f"(json_type({table_alias}.meta, ?) = 'OBJECT' "
                        f"AND length(json_keys(json_extract({table_alias}.meta, ?))) = ?)"
                    )
                    params.extend([ptr, ptr, len(value)])
                    for child_key, child_val in value.items():
                        if not isinstance(child_key, str):
                            clauses.append("1 = 0")
                            return
                        escaped = child_key.replace("~", "~0").replace("/", "~1")
                        _append_json_pointer_predicate(
                            f"{ptr}/{escaped}", child_val, is_top_level=False
                        )
                elif isinstance(value, Sequence) and not isinstance(
                    value, (str, bytes, bytearray)
                ):
                    clauses.append(
                        f"(json_type({table_alias}.meta, ?) = 'ARRAY' "
                        f"AND json_array_length(json_extract({table_alias}.meta, ?)) = ?)"
                    )
                    params.extend([ptr, ptr, len(value)])
                    for idx, child_val in enumerate(value):
                        _append_json_pointer_predicate(
                            f"{ptr}/{idx}", child_val, is_top_level=False
                        )
                else:
                    needs_client_metadata_filter = True

            for mk, mv in metadata.items():
                if not isinstance(mk, str):
                    clauses.append("1 = 1" if mv is None else "1 = 0")
                    continue
                top_ptr = "/" + mk.replace("~", "~0").replace("/", "~1")
                _append_json_pointer_predicate(top_ptr, mv, is_top_level=True)

        return needs_client_metadata_filter

    def find_runs(
        self,
        tags: Optional[Sequence[str]] = None,
        year: Optional[int] = None,
        iteration: Optional[int] = None,
        stage: Optional[str] = None,
        phase: Optional[str] = None,
        model: Optional[str] = None,
        status: Optional[str] = None,
        parent_id: Optional[str] = None,
        facet: Optional[Mapping[str, Any]] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        limit: Optional[int] = 100,
        name: Optional[str] = None,
        run_scope: Optional[str] = None,
        raise_on_error: bool = True,
        *,
        offset: int = 0,
        parent_run_id: Optional[str] = None,
        signature: Optional[str] = None,
        cacheable: Optional[bool] = None,
        run_ids: Optional[Iterable[str]] = None,
    ) -> List[Run]:
        """Query ``run`` with parameterized DuckDB SQL pushdown."""
        del raise_on_error
        clauses: list[str] = []
        params: list[Any] = []

        if run_ids is not None:
            id_list = list(dict.fromkeys(str(rid) for rid in run_ids))
            if not id_list:
                return []
            placeholders = ", ".join("?" for _ in id_list)
            clauses.append(f"r.id IN ({placeholders})")
            params.extend(id_list)
        if status:
            clauses.append("r.status = ?")
            params.append(status)
        if model:
            clauses.append("r.model_name = ?")
            params.append(model)
        if year is not None:
            clauses.append("r.year = ?")
            params.append(int(year))
        if iteration is not None:
            clauses.append("r.iteration = ?")
            params.append(int(iteration))
        if stage is not None:
            clauses.append("r.stage = ?")
            params.append(stage)
        if phase is not None:
            clauses.append("r.phase = ?")
            params.append(phase)
        eff_parent = parent_run_id if parent_run_id is not None else parent_id
        if eff_parent:
            clauses.append("r.parent_run_id = ?")
            params.append(eff_parent)
        if signature is not None:
            clauses.append("r.signature = ?")
            params.append(signature)
        if cacheable is not None:
            clauses.append("r.cacheable = ?")
            params.append(bool(cacheable))
        if name:
            clauses.append("r.description = ?")
            params.append(name)
        if run_scope and run_scope.strip():
            scope = run_scope.strip()
            scope_prefix = f"{scope}__"
            scope_pattern = f"{_escape_like_literal(scope_prefix)}%"
            clauses.append(
                "("
                "r.id = ? "
                f"OR r.id LIKE ? ESCAPE '{LIKE_ESCAPE_CHAR}' "
                "OR r.description = ? "
                f"OR r.description LIKE ? ESCAPE '{LIKE_ESCAPE_CHAR}'"
                ")"
            )
            params.extend([scope, scope_pattern, scope, scope_pattern])

        if facet:
            ns_clause = " AND kv.namespace = ?" if model is not None else ""
            ns_params: list[Any] = [model] if model is not None else []
            for pred in flatten_facet_values(
                facet_dict=dict(facet), include_json_leaves=True
            ):
                if pred.value_type == "null":
                    clauses.append(
                        "EXISTS (SELECT 1 FROM run_config_kv kv "
                        f"WHERE kv.run_id = r.id AND kv.key = ?{ns_clause} "
                        "AND kv.value_type = 'null')"
                    )
                    params.extend([pred.key_path, *ns_params])
                elif pred.value_type == "bool":
                    clauses.append(
                        "EXISTS (SELECT 1 FROM run_config_kv kv "
                        f"WHERE kv.run_id = r.id AND kv.key = ?{ns_clause} "
                        "AND kv.value_type = 'bool' AND kv.value_bool = ?)"
                    )
                    params.extend([pred.key_path, *ns_params, pred.value_bool])
                elif pred.value_type in ("int", "float"):
                    clauses.append(
                        "EXISTS (SELECT 1 FROM run_config_kv kv "
                        f"WHERE kv.run_id = r.id AND kv.key = ?{ns_clause} "
                        "AND kv.value_type = ? AND kv.value_num = ?)"
                    )
                    params.extend(
                        [
                            pred.key_path,
                            *ns_params,
                            pred.value_type,
                            float(pred.value_num or 0.0),
                        ]
                    )
                elif pred.value_type == "str":
                    clauses.append(
                        "EXISTS (SELECT 1 FROM run_config_kv kv "
                        f"WHERE kv.run_id = r.id AND kv.key = ?{ns_clause} "
                        "AND kv.value_type = 'str' AND kv.value_str = ?)"
                    )
                    params.extend([pred.key_path, *ns_params, pred.value_str])
                else:
                    clauses.append(
                        "EXISTS (SELECT 1 FROM run_config_kv kv "
                        f"WHERE kv.run_id = r.id AND kv.key = ?{ns_clause} "
                        "AND kv.value_type = 'json' AND kv.value_json = CAST(? AS JSON))"
                    )
                    params.extend(
                        [pred.key_path, *ns_params, _dump_json(pred.value_json)]
                    )

        needs_client_metadata_filter = self.append_run_tag_and_metadata_clauses(
            clauses,
            params,
            tags=tags,
            metadata=metadata,
            table_alias="r",
        )

        where_sql = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = (
            f"SELECT {_RUN_SELECT_COLS} FROM run r"
            f"{where_sql} ORDER BY {recent_run_order_by_sql('r')}"
        )
        start_idx = max(0, int(offset))
        push_pagination = not needs_client_metadata_filter
        if push_pagination:
            if limit is not None:
                sql += " LIMIT ? OFFSET ?"
                params.extend([max(0, int(limit)), start_idx])
            elif start_idx > 0:
                sql += " OFFSET ?"
                params.append(start_idx)

        with _DB_CALL_PROFILER.track("find_runs"):
            rows = self.fetchall(sql, params)

        runs = [_hydrate_run_row(row) for row in rows]
        if push_pagination:
            return runs
        if metadata:
            runs = [
                rn
                for rn in runs
                if all(rn.meta.get(mk) == mv for mk, mv in metadata.items())
            ]
        if limit is None:
            return runs[start_idx:]
        return runs[start_idx : start_idx + max(0, int(limit))]

    def get_artifact_by_uri(
        self,
        uri: str,
        run_id: Optional[str] = None,
        member: Optional[str] = None,
    ) -> Optional[Artifact]:
        """Get the most recent artifact at ``uri`` (including input artifacts)."""
        return self.find_latest_artifact_at_uri(
            uri=uri,
            run_id=run_id,
            member=member,
            include_inputs=True,
        )

    def get_config_facet(self, facet_id: str) -> Optional[ConfigFacet]:
        """Fetch a ``ConfigFacet`` by ``facet_id``."""
        row = self.fetchone(
            'SELECT id, namespace, schema_name, schema_version, "json", created_at '
            "FROM config_facet WHERE id = ?",
            [facet_id],
        )
        return _hydrate_config_facet_row(row) if row is not None else None

    def get_config_facets(
        self,
        *,
        namespace: Optional[str] = None,
        schema_name: Optional[str] = None,
        limit: int = 100,
    ) -> List[ConfigFacet]:
        """List ``ConfigFacet`` rows ordered newest first."""
        clauses: list[str] = []
        params: list[Any] = []
        if namespace:
            clauses.append("namespace = ?")
            params.append(namespace)
        if schema_name:
            clauses.append("schema_name = ?")
            params.append(schema_name)
        where_sql = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(max(0, int(limit)))
        rows = self.fetchall(
            'SELECT id, namespace, schema_name, schema_version, "json", created_at '
            f"FROM config_facet{where_sql} ORDER BY created_at DESC LIMIT ?",
            params,
        )
        return [_hydrate_config_facet_row(r) for r in rows]

    def get_run_config_kv(
        self,
        run_id: str,
        *,
        namespace: Optional[str] = None,
        prefix: Optional[str] = None,
        limit: int = 10_000,
    ) -> List[RunConfigKV]:
        """Return flattened config KV rows for ``run_id``."""
        clauses: list[str] = ["run_id = ?"]
        params: list[Any] = [run_id]
        if namespace:
            clauses.append("namespace = ?")
            params.append(namespace)
        if prefix:
            clauses.append(f"key LIKE ? ESCAPE '{LIKE_ESCAPE_CHAR}'")
            params.append(f"{_escape_like_literal(prefix)}%")
        params.append(max(0, int(limit)))
        rows = self.fetchall(
            "SELECT run_id, facet_id, namespace, key, value_type, "
            "value_str, value_num, value_bool, value_json, created_at "
            f"FROM run_config_kv WHERE {' AND '.join(clauses)} LIMIT ?",
            params,
        )
        return [_hydrate_run_config_kv_row(r) for r in rows]

    def get_run_config_kv_for_runs(
        self,
        run_ids: Sequence[str],
        *,
        namespace: Optional[str] = None,
        keys: Optional[Sequence[str]] = None,
        limit: int = 100_000,
    ) -> List[RunConfigKV]:
        """Return flattened config KV rows for multiple runs."""
        unique_run_ids = list(dict.fromkeys(run_ids))
        if not unique_run_ids:
            return []
        if keys is not None and len(keys) == 0:
            return []
        run_ph = ", ".join("?" for _ in unique_run_ids)
        clauses: list[str] = [f"run_id IN ({run_ph})"]
        params: list[Any] = list(unique_run_ids)
        if namespace:
            clauses.append("namespace = ?")
            params.append(namespace)
        if keys is not None:
            unique_keys = list(dict.fromkeys(keys))
            key_ph = ", ".join("?" for _ in unique_keys)
            clauses.append(f"key IN ({key_ph})")
            params.extend(unique_keys)
        params.append(max(0, int(limit)))
        rows = self.fetchall(
            "SELECT run_id, facet_id, namespace, key, value_type, "
            "value_str, value_num, value_bool, value_json, created_at "
            f"FROM run_config_kv WHERE {' AND '.join(clauses)} LIMIT ?",
            params,
        )
        return [_hydrate_run_config_kv_row(r) for r in rows]

    def get_artifact_kv(
        self,
        artifact_id: uuid.UUID,
        *,
        namespace: Optional[str] = None,
        prefix: Optional[str] = None,
        limit: int = 10_000,
    ) -> List[ArtifactKV]:
        """Return flattened artifact facet KV rows for ``artifact_id``."""
        clauses: list[str] = ["artifact_id = ?"]
        params: list[Any] = [_coerce_uuid(artifact_id)]
        if namespace is not None:
            clauses.append("namespace = ?")
            params.append(namespace)
        if prefix:
            clauses.append(f"key_path LIKE ? ESCAPE '{LIKE_ESCAPE_CHAR}'")
            params.append(f"{_escape_like_literal(prefix)}%")
        params.append(max(0, int(limit)))
        rows = self.fetchall(
            "SELECT artifact_id, facet_id, key_path, namespace, value_type, "
            "value_str, value_num, value_bool, value_json, created_at "
            f"FROM artifact_kv WHERE {' AND '.join(clauses)} LIMIT ?",
            params,
        )
        return [_hydrate_artifact_kv_row(r) for r in rows]

    @staticmethod
    def _append_artifact_kv_predicates(
        *,
        clauses: list[str],
        params: list[Any],
        predicates: Sequence[Mapping[str, Any]],
        namespace: Optional[str],
        artifact_alias: str = "a",
    ) -> None:
        for predicate in predicates:
            eff_ns = (
                predicate["namespace"]
                if predicate.get("namespace") is not None
                else namespace
            )
            sub_clauses = [
                f"kv.artifact_id = {artifact_alias}.id",
                "kv.key_path = ?",
            ]
            sub_params: list[Any] = [predicate["key_path"]]
            if eff_ns is not None:
                sub_clauses.append("kv.namespace = ?")
                sub_params.append(eff_ns)

            operator = predicate["op"]
            value_kind = predicate["kind"]
            value = predicate["value"]
            if operator == "=":
                if value_kind == "str":
                    sub_clauses.append("kv.value_type = 'str' AND kv.value_str = ?")
                    sub_params.append(value)
                elif value_kind == "bool":
                    sub_clauses.append("kv.value_type = 'bool' AND kv.value_bool = ?")
                    sub_params.append(bool(value))
                elif value_kind == "null":
                    sub_clauses.append("kv.value_type = 'null'")
                else:
                    sub_clauses.append(
                        "kv.value_type IN ('int', 'float') AND kv.value_num = ?"
                    )
                    sub_params.append(float(value))
            elif operator == ">=":
                sub_clauses.append(
                    "kv.value_type IN ('int', 'float') AND kv.value_num >= ?"
                )
                sub_params.append(float(value))
            elif operator == "<=":
                sub_clauses.append(
                    "kv.value_type IN ('int', 'float') AND kv.value_num <= ?"
                )
                sub_params.append(float(value))
            else:
                raise ValueError(f"Unsupported artifact_kv operator: {operator!r}")

            clauses.append(
                f"EXISTS (SELECT 1 FROM artifact_kv kv WHERE {' AND '.join(sub_clauses)})"
            )
            params.extend(sub_params)

    def find_artifacts_by_facet_params(
        self,
        *,
        predicates: List[Dict[str, Any]],
        namespace: Optional[str] = None,
        key_prefix: Optional[str] = None,
        artifact_family_prefix: Optional[str] = None,
        limit: int = 100,
    ) -> List[Artifact]:
        """Find artifacts matching one or more flattened ``artifact_kv`` predicates."""
        clauses: list[str] = []
        params: list[Any] = []

        if key_prefix:
            clauses.append(f"a.key LIKE ? ESCAPE '{LIKE_ESCAPE_CHAR}'")
            params.append(f"{_escape_like_literal(key_prefix)}%")

        if artifact_family_prefix:
            fam_clauses = [
                "fkv.artifact_id = a.id",
                "fkv.key_path = 'artifact_family'",
                "fkv.value_type = 'str'",
                f"fkv.value_str LIKE ? ESCAPE '{LIKE_ESCAPE_CHAR}'",
            ]
            fam_params: list[Any] = [f"{_escape_like_literal(artifact_family_prefix)}%"]
            if namespace is not None:
                fam_clauses.append("fkv.namespace = ?")
                fam_params.append(namespace)
            clauses.append(
                f"EXISTS (SELECT 1 FROM artifact_kv fkv WHERE {' AND '.join(fam_clauses)})"
            )
            params.extend(fam_params)

        self._append_artifact_kv_predicates(
            clauses=clauses,
            params=params,
            predicates=predicates,
            namespace=namespace,
            artifact_alias="a",
        )

        where_sql = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(max(0, int(limit)))
        rows = self.fetchall(
            f"SELECT {_ARTIFACT_SELECT_COLS} FROM artifact a"
            f"{where_sql} ORDER BY a.created_at DESC, a.id DESC LIMIT ?",
            params,
        )
        return [_hydrate_artifact_row(r) for r in rows]

    def _load_schema_field_names(
        self,
        *,
        schema_ids: Optional[Set[str]] = None,
    ) -> Dict[str, Set[str]]:
        """Load schema field-name sets keyed by ``schema_id`` in one query."""
        if schema_ids is not None and len(schema_ids) == 0:
            return {}
        if schema_ids:
            ordered = sorted(schema_ids)
            ph = ", ".join("?" for _ in ordered)
            rows = self.fetchall(
                f"SELECT schema_id, name FROM artifact_schema_field WHERE schema_id IN ({ph})",
                ordered,
            )
        else:
            rows = self.fetchall("SELECT schema_id, name FROM artifact_schema_field")
        fields_by_schema: Dict[str, Set[str]] = {}
        for sid, fname in rows:
            if isinstance(sid, str) and isinstance(fname, str):
                fields_by_schema.setdefault(sid, set()).add(fname)
        return fields_by_schema

    def _resolve_schema_compatible_ids(
        self,
        *,
        schema_id: str,
        all_schemas: Optional[Sequence[ArtifactSchema]] = None,
        schema_fields_by_id: Optional[Dict[str, Set[str]]] = None,
    ) -> Set[str]:
        """Resolve schema IDs compatible with ``schema_id`` for union-by-name views."""
        schema_rows = (
            list(all_schemas)
            if all_schemas is not None
            else [
                _hydrate_schema_row(r)
                for r in self.fetchall(
                    "SELECT id, profile_version, summary_json, profile_json, created_at "
                    "FROM artifact_schema"
                )
            ]
        )
        schemas_by_id = {row.id: row for row in schema_rows}
        base_schema = schemas_by_id.get(schema_id)
        if base_schema is None:
            return {schema_id}

        field_map = (
            schema_fields_by_id
            if schema_fields_by_id is not None
            else self._load_schema_field_names()
        )
        base_fields = field_map.get(schema_id, set())
        if not base_fields:
            return {schema_id}

        base_summary = base_schema.summary_json or {}
        base_table_name = (
            base_summary.get("table_name")
            if isinstance(base_summary.get("table_name"), str)
            else None
        )

        compatible: Set[str] = {schema_id}
        for candidate in schema_rows:
            candidate_id = candidate.id
            if candidate_id == schema_id:
                continue
            candidate_summary = candidate.summary_json or {}
            candidate_table_name = (
                candidate_summary.get("table_name")
                if isinstance(candidate_summary.get("table_name"), str)
                else None
            )
            if (
                base_table_name is not None
                and candidate_table_name is not None
                and candidate_table_name != base_table_name
            ):
                continue
            candidate_fields = field_map.get(candidate_id, set())
            if not candidate_fields:
                continue
            if base_fields.issubset(candidate_fields) or candidate_fields.issubset(
                base_fields
            ):
                compatible.add(candidate_id)
        return compatible

    def find_schema_ids_for_model(
        self, *, schema_model: Type[BaseModel], compatible: bool = True
    ) -> List[str]:
        """Resolve stored schema IDs matching a SQLModel/Pydantic class."""
        explicit_table = optional_model_table_name(schema_model)
        accepted_table_names = (
            {explicit_table, explicit_table.lower()}
            if explicit_table is not None
            else {
                schema_model.__name__,
                schema_model.__name__.lower(),
                model_table_name(schema_model),
            }
        )
        model_fields = set(schema_model_user_columns(schema_model))
        if not model_fields:
            return []

        schema_rows = [
            _hydrate_schema_row(r)
            for r in self.fetchall(
                "SELECT id, profile_version, summary_json, profile_json, created_at "
                "FROM artifact_schema"
            )
        ]
        schema_fields_by_id = self._load_schema_field_names()
        matched: List[str] = []
        for schema_row in schema_rows:
            schema_id = schema_row.id
            summary = schema_row.summary_json or {}
            candidate_table_name = (
                summary.get("table_name")
                if isinstance(summary.get("table_name"), str)
                else None
            )
            if (
                candidate_table_name is not None
                and str(candidate_table_name) not in accepted_table_names
                and str(candidate_table_name).lower() not in accepted_table_names
            ):
                continue
            candidate_fields = schema_fields_by_id.get(schema_id, set())
            if not candidate_fields:
                continue
            if compatible:
                if model_fields.issubset(candidate_fields) or candidate_fields.issubset(
                    model_fields
                ):
                    matched.append(schema_id)
            elif candidate_fields == model_fields:
                matched.append(schema_id)
        return sorted(set(matched))

    def find_artifacts_for_grouped_view(
        self,
        *,
        schema_id: Optional[str] = None,
        schema_ids: Optional[List[str]] = None,
        schema_compatible: bool = False,
        predicates: Optional[List[Dict[str, Any]]] = None,
        namespace: Optional[str] = None,
        drivers: Optional[List[str]] = None,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        model: Optional[str] = None,
        status: Optional[str] = None,
        year: Optional[int] = None,
        iteration: Optional[int] = None,
        limit: int = 1_000_000,
    ) -> List[Tuple[Artifact, Run]]:
        """Resolve ``(Artifact, Run)`` pairs for grouped-view materialization."""
        if schema_id is None and not schema_ids:
            raise ValueError("Provide schema_id or schema_ids for grouped view lookup.")

        base_ids = list(schema_ids or [])
        if schema_id is not None:
            base_ids.append(schema_id)
        base_ids = sorted(set(base_ids))

        if schema_compatible:
            all_schemas = [
                _hydrate_schema_row(r)
                for r in self.fetchall(
                    "SELECT id, profile_version, summary_json, profile_json, created_at "
                    "FROM artifact_schema"
                )
            ]
            schema_fields_by_id = self._load_schema_field_names()
            resolved_schema_ids: Set[str] = set()
            for base_sid in base_ids:
                resolved_schema_ids.update(
                    self._resolve_schema_compatible_ids(
                        schema_id=base_sid,
                        all_schemas=all_schemas,
                        schema_fields_by_id=schema_fields_by_id,
                    )
                )
        else:
            resolved_schema_ids = set(base_ids)

        if not resolved_schema_ids:
            return []

        ordered_sids = sorted(resolved_schema_ids)
        sid_ph = ", ".join("?" for _ in ordered_sids)
        clauses: list[str] = [
            f"EXISTS (SELECT 1 FROM artifact_schema_observation obs "
            f"WHERE obs.artifact_id = a.id AND obs.schema_id IN ({sid_ph}))"
        ]
        params: list[Any] = list(ordered_sids)

        if drivers:
            drv_list = list(dict.fromkeys(drivers))
            drv_ph = ", ".join("?" for _ in drv_list)
            clauses.append(f"a.driver IN ({drv_ph})")
            params.extend(drv_list)
        if run_id is not None:
            clauses.append("r.id = ?")
            params.append(run_id)
        if parent_run_id is not None:
            clauses.append("r.parent_run_id = ?")
            params.append(parent_run_id)
        if model is not None:
            clauses.append("r.model_name = ?")
            params.append(model)
        if status is not None:
            clauses.append("r.status = ?")
            params.append(status)
        if year is not None:
            clauses.append("r.year = ?")
            params.append(int(year))
        if iteration is not None:
            clauses.append("r.iteration = ?")
            params.append(int(iteration))

        self._append_artifact_kv_predicates(
            clauses=clauses,
            params=params,
            predicates=predicates or [],
            namespace=namespace,
            artifact_alias="a",
        )

        params.append(max(0, int(limit)))
        rows = self.fetchall(
            f"SELECT {_ARTIFACT_SELECT_COLS}, {_RUN_SELECT_COLS} "
            "FROM artifact a "
            "JOIN run r ON a.run_id = r.id "
            f"WHERE {' AND '.join(clauses)} "
            "ORDER BY a.created_at ASC, a.id ASC LIMIT ?",
            params,
        )
        return [
            (_hydrate_artifact_row(row[:11]), _hydrate_run_row(row[11:]))
            for row in rows
        ]

    def find_artifacts_with_runs_for_key(
        self,
        *,
        key: str,
        drivers: Optional[Sequence[str]] = None,
    ) -> List[Tuple[Artifact, Run]]:
        """Return ``(Artifact, Run)`` pairs matching ``key`` and optional ``drivers``."""
        clauses: list[str] = ["a.key = ?"]
        params: list[Any] = [key]
        if drivers:
            drv_list = list(dict.fromkeys(drivers))
            drv_ph = ", ".join("?" for _ in drv_list)
            clauses.append(f"a.driver IN ({drv_ph})")
            params.extend(drv_list)
        rows = self.fetchall(
            f"SELECT {_ARTIFACT_SELECT_COLS}, {_RUN_SELECT_COLS} "
            "FROM artifact a "
            "JOIN run r ON a.run_id = r.id "
            f"WHERE {' AND '.join(clauses)} "
            "ORDER BY a.created_at ASC, a.id ASC",
            params,
        )
        return [
            (_hydrate_artifact_row(row[:11]), _hydrate_run_row(row[11:]))
            for row in rows
        ]

    def find_linked_artifacts_exact(
        self,
        *,
        run_id: str,
        key: str,
        direction: Optional[str] = None,
    ) -> List[Artifact]:
        """Return distinct linked ``Artifact`` rows for ``(run_id, key[, direction])`` ordered by id."""
        clauses = ["l.run_id = ?", "a.key = ?"]
        params: list[Any] = [run_id, key]
        if direction in ("input", "output"):
            clauses.append("l.direction = ?")
            params.append(direction)
        rows = self.fetchall(
            f"SELECT DISTINCT {_ARTIFACT_SELECT_COLS} FROM artifact a "
            "JOIN run_artifact_link l ON l.artifact_id = a.id "
            f"WHERE {' AND '.join(clauses)} "
            "ORDER BY a.id ASC",
            params,
        )
        return [_hydrate_artifact_row(r) for r in rows]

    def replace_artifacts_meta_bulk(
        self,
        meta_by_id: Mapping[uuid.UUID, Dict[str, Any]],
        *,
        require_all: bool = True,
    ) -> None:
        """Replace ``artifact.meta`` for multiple artifact IDs in one transaction."""
        if not meta_by_id:
            return
        coerced: dict[uuid.UUID, Dict[str, Any]] = {
            _coerce_uuid(aid): dict(meta) for aid, meta in meta_by_id.items()
        }
        ids = list(coerced.keys())
        placeholders = ", ".join("?" for _ in ids)
        with self.transaction() as cur:
            found_rows = cur.execute(
                f"SELECT id FROM artifact WHERE id IN ({placeholders})",
                ids,
            ).fetchall()
            found_ids: set[uuid.UUID] = {_coerce_uuid(r[0]) for r in found_rows}
            if require_all:
                missing_ids = sorted(str(aid) for aid in set(ids) - found_ids)
                if missing_ids:
                    raise KeyError(
                        "Artifacts were not found for recovery root update: "
                        + ", ".join(missing_ids)
                    )
            batch: list[list[Any]] = [
                [_dump_json(dict(coerced[aid])), aid] for aid in ids if aid in found_ids
            ]
            if batch:
                _execute_multirow_values(
                    cur,
                    prefix_sql=(
                        "UPDATE artifact AS a "
                        "SET meta = CAST(v.meta_json AS JSON) FROM (VALUES"
                    ),
                    row_placeholder="(CAST(? AS VARCHAR), CAST(? AS UUID))",
                    suffix_sql=") AS v(meta_json, aid) WHERE a.id = v.aid",
                    rows=batch,
                )

    @staticmethod
    def _file_stat_stamp(path: Optional[Path]) -> tuple[int, int, int, int]:
        if path is None:
            return (0, 0, 0, 0)
        p_str = os.fspath(path)
        try:
            st = os.stat(p_str)
            db_m, db_s = int(st.st_mtime_ns), int(st.st_size)
        except OSError:
            db_m, db_s = 0, 0
        try:
            wst = os.stat(f"{p_str}.wal")
            wal_m, wal_s = int(wst.st_mtime_ns), int(wst.st_size)
        except OSError:
            wal_m, wal_s = 0, 0
        return (db_m, db_s, wal_m, wal_s)

    @property
    def effective_location_epoch(self) -> tuple[int, int, int, int, int, int]:
        path_epoch = (
            _DB_PATH_LOCATION_EPOCH.get(str(self._resolved_db_path), 0)
            if self._resolved_db_path is not None
            else 0
        )
        db_m, db_s, wal_m, wal_s = (
            self._file_stat_stamp(self._resolved_db_path)
            if self._con_read_only
            else (0, 0, 0, 0)
        )
        return (self.location_epoch, path_epoch, db_m, db_s, wal_m, wal_s)

    def sync_location_epoch_caches(self) -> tuple[int, int, int, int, int, int]:
        cur_epoch = self.effective_location_epoch
        if self._last_seen_location_epoch != cur_epoch:
            if self._last_seen_location_epoch is not None:
                self._location_resolution_cache.clear()
                self._ephemeral_stale_locations.clear()
                self._settled_witnesses.clear()
                self._active_coord_witness.clear()
                for hook in self._epoch_invalidation_hooks:
                    hook()
            self._last_seen_location_epoch = cur_epoch
        return cur_epoch

    def bump_location_epoch(self) -> int:
        nxt = next(_GLOBAL_LOCATION_EPOCH)
        self.location_epoch = nxt
        if self._resolved_db_path is not None:
            _DB_PATH_LOCATION_EPOCH[str(self._resolved_db_path)] = nxt
        self._location_resolution_cache.clear()
        self._active_coord_witness.clear()
        for hook in self._epoch_invalidation_hooks:
            hook()
        self._last_seen_location_epoch = self.effective_location_epoch
        return nxt

    def has_artifact_location_table(self) -> bool:
        if self._has_artifact_location_table is None:
            self._has_artifact_location_table = self.has_table("artifact_location")
        return self._has_artifact_location_table

    @staticmethod
    def _witness_matches_or_settled(
        existing: Optional[Witness], incoming: Optional[Witness]
    ) -> bool:
        if incoming is None:
            return True
        if existing is None:
            return False
        if existing == incoming:
            return True
        return not existing.racy and existing.settled() == incoming.settled()

    @staticmethod
    def _same_stat_footprint(w_a: Optional[Witness], w_b: Optional[Witness]) -> bool:
        if isinstance(w_a, FileStat) and isinstance(w_b, FileStat):
            return w_a.size == w_b.size and w_a.mtime_ns == w_b.mtime_ns
        if isinstance(w_a, DirTreeStat) and isinstance(w_b, DirTreeStat):
            return (
                w_a.file_count == w_b.file_count
                and w_a.total_bytes == w_b.total_bytes
                and w_a.max_mtime_ns == w_b.max_mtime_ns
            )
        return False

    _LOCATION_SELECT_COLS = (
        "location_id, digest, artifact_id, site, mount_name, "
        "mount_instance_id, uri, member, status, witness_json, observed_at_ns"
    )

    def _hydrate_location_row(self, row: Sequence[Any]) -> ArtifactLocationRow:
        loc_id = _coerce_uuid(row[0])
        ephemeral_status = self._ephemeral_stale_locations.get(loc_id)
        raw_status = str(row[8]) if ephemeral_status is None else ephemeral_status
        status: LocationStatus = (
            "stale"
            if raw_status == "stale"
            else ("evicted" if raw_status == "evicted" else "active")
        )
        raw_site = str(row[3])
        site: LocationSite = (
            "archive"
            if raw_site == "archive"
            else (
                "advisory"
                if raw_site == "advisory"
                else ("external" if raw_site == "external" else "workspace")
            )
        )
        settled_witness = self._settled_witnesses.get(loc_id)
        hydrated_witness = (
            settled_witness
            if settled_witness is not None
            else witness_from_json(row[9])
        )
        return ArtifactLocationRow(
            location_id=loc_id,
            digest=str(row[1]) if row[1] is not None else None,
            artifact_id=_coerce_optional_uuid(row[2]),
            site=site,
            mount_name=str(row[4]),
            mount_instance_id=str(row[5]),
            uri=str(row[6]),
            member=str(row[7] or ""),
            status=status,
            witness=hydrated_witness,
            observed_at_ns=int(row[10]),
        )

    @staticmethod
    def _is_superseded_by_winner(
        old_loc: ArtifactLocationRow, win_loc: ArtifactLocationRow
    ) -> bool:
        if old_loc.location_id == win_loc.location_id:
            return False
        if old_loc.site in ("workspace", "external"):
            return True
        if (
            old_loc.digest is not None
            and win_loc.digest is not None
            and old_loc.digest.startswith(("stat_file_v1:", "stat_dir_v1:"))
            and win_loc.digest.startswith(("stat_file_v1:", "stat_dir_v1:"))
        ):
            return (
                old_loc.witness is not None
                and win_loc.witness is not None
                and not DatabaseManager._same_stat_footprint(
                    old_loc.witness, win_loc.witness
                )
            )
        if win_loc.digest is not None:
            return old_loc.digest != win_loc.digest
        return old_loc.digest is not None or _coerce_optional_uuid(
            old_loc.artifact_id
        ) != _coerce_optional_uuid(win_loc.artifact_id)

    def _upsert_artifact_locations_locked(
        self,
        cur: duckdb.DuckDBPyConnection,
        locations: Sequence[ArtifactLocationRow],
    ) -> None:
        if not locations:
            return
        self._has_artifact_location_table = True
        deduped_by_id: dict[uuid.UUID, ArtifactLocationRow] = {}
        for loc in locations:
            deduped_by_id[_coerce_uuid(loc.location_id)] = loc
        ordered = list(deduped_by_id.values())

        winning_by_coord: dict[tuple[str, str, str, str, str], ArtifactLocationRow] = {}
        for loc in ordered:
            if loc.status == "active":
                coord = (
                    loc.site,
                    loc.mount_name,
                    loc.mount_instance_id,
                    loc.uri,
                    loc.member,
                )
                winning_by_coord[coord] = loc

        normalized_rows: list[ArtifactLocationRow] = []
        for loc in ordered:
            coord = (
                loc.site,
                loc.mount_name,
                loc.mount_instance_id,
                loc.uri,
                loc.member,
            )
            winner = winning_by_coord.get(coord)
            if (
                loc.status == "active"
                and winner is not None
                and self._is_superseded_by_winner(loc, winner)
            ):
                loc = ArtifactLocationRow(
                    location_id=loc.location_id,
                    digest=loc.digest,
                    artifact_id=loc.artifact_id,
                    site=loc.site,
                    mount_name=loc.mount_name,
                    mount_instance_id=loc.mount_instance_id,
                    uri=loc.uri,
                    member=loc.member,
                    status="stale",
                    witness=loc.witness,
                    observed_at_ns=loc.observed_at_ns,
                )
            normalized_rows.append(loc)

        incoming_lids = [_coerce_uuid(loc.location_id) for loc in normalized_rows]
        lid_ph = ", ".join("?" for _ in incoming_lids)
        existing_by_lid: dict[uuid.UUID, ArtifactLocationRow] = {}
        existing_active_by_coord: dict[
            tuple[str, str, str, str, str], list[ArtifactLocationRow]
        ] = {}
        for raw_row in cur.execute(
            f"SELECT {self._LOCATION_SELECT_COLS} FROM artifact_location "
            f"WHERE location_id IN ({lid_ph})",
            incoming_lids,
        ).fetchall():
            hydrated = self._hydrate_location_row(raw_row)
            existing_by_lid[hydrated.location_id] = hydrated

        if winning_by_coord:
            coord_list = list(winning_by_coord.keys())
            coord_ph = ", ".join("(?, ?, ?, ?, ?)" for _ in coord_list)
            coord_params: list[Any] = [val for c in coord_list for val in c]
            for raw_row in cur.execute(
                "SELECT al.location_id, al.digest, al.artifact_id, al.site, al.mount_name, "
                "al.mount_instance_id, al.uri, al.member, al.status, al.witness_json, "
                "al.observed_at_ns FROM artifact_location AS al "
                f"JOIN (VALUES {coord_ph}) AS v(site, mount_name, mount_instance_id, uri, member) "
                "ON al.site = v.site AND al.mount_name = v.mount_name "
                "AND al.mount_instance_id = v.mount_instance_id "
                "AND al.uri = v.uri AND al.member = v.member "
                "WHERE al.status = 'active'",
                coord_params,
            ).fetchall():
                hydrated = self._hydrate_location_row(raw_row)
                c_key = (
                    hydrated.site,
                    hydrated.mount_name,
                    hydrated.mount_instance_id,
                    hydrated.uri,
                    hydrated.member,
                )
                existing_active_by_coord.setdefault(c_key, []).append(hydrated)

        incoming_lid_set = set(incoming_lids)
        stale_lids: list[uuid.UUID] = []
        for coord, win_loc in winning_by_coord.items():
            for old_loc in existing_active_by_coord.get(coord, ()):
                if (
                    old_loc.location_id not in incoming_lid_set
                    and self._is_superseded_by_winner(old_loc, win_loc)
                ):
                    stale_lids.append(old_loc.location_id)

        rows_to_upsert: list[ArtifactLocationRow] = []
        for loc in normalized_rows:
            lid = _coerce_uuid(loc.location_id)
            prev = existing_by_lid.get(lid)
            if prev is None:
                rows_to_upsert.append(loc)
                continue
            digest_changed = loc.digest is not None and prev.digest != loc.digest
            aid_changed = loc.artifact_id is not None and prev.artifact_id is None
            witness_changed = not self._witness_matches_or_settled(
                prev.witness, loc.witness
            )
            if (
                prev.status != loc.status
                or digest_changed
                or aid_changed
                or witness_changed
            ):
                rows_to_upsert.append(loc)

        if not stale_lids and not rows_to_upsert:
            for coord, win_loc in winning_by_coord.items():
                prev_win = existing_by_lid.get(_coerce_uuid(win_loc.location_id))
                eff_w = (
                    prev_win.witness
                    if (prev_win is not None and prev_win.witness is not None)
                    else win_loc.witness
                )
                self._active_coord_witness[coord] = (win_loc.digest, eff_w)
            return

        now_ns = time.time_ns()
        if stale_lids:
            _execute_multirow_values(
                cur,
                prefix_sql=(
                    "UPDATE artifact_location AS al "
                    "SET status = 'stale', observed_at_ns = v.obs FROM (VALUES"
                ),
                row_placeholder="(CAST(? AS BIGINT), CAST(? AS UUID))",
                suffix_sql=") AS v(obs, lid) WHERE al.location_id = v.lid",
                rows=[[now_ns, lid] for lid in dict.fromkeys(stale_lids)],
            )

        if rows_to_upsert:
            _execute_multirow_values(
                cur,
                prefix_sql=(
                    "INSERT INTO artifact_location ("
                    "location_id, digest, artifact_id, site, mount_name, "
                    "mount_instance_id, uri, member, status, witness_json, observed_at_ns"
                    ") VALUES"
                ),
                row_placeholder=(
                    "(CAST(? AS UUID), CAST(? AS VARCHAR), CAST(? AS UUID), CAST(? AS VARCHAR), "
                    "CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS VARCHAR), "
                    "CAST(? AS VARCHAR), CAST(? AS JSON), CAST(? AS BIGINT))"
                ),
                suffix_sql=(
                    "ON CONFLICT (location_id) DO UPDATE SET "
                    "digest = COALESCE(EXCLUDED.digest, artifact_location.digest), "
                    "artifact_id = COALESCE(EXCLUDED.artifact_id, artifact_location.artifact_id), "
                    "status = EXCLUDED.status, "
                    "witness_json = COALESCE(EXCLUDED.witness_json, artifact_location.witness_json), "
                    "observed_at_ns = EXCLUDED.observed_at_ns"
                ),
                rows=[
                    [
                        _coerce_uuid(loc.location_id),
                        loc.digest,
                        _coerce_optional_uuid(loc.artifact_id),
                        loc.site,
                        loc.mount_name,
                        loc.mount_instance_id,
                        loc.uri,
                        loc.member,
                        loc.status,
                        loc.witness.to_json() if loc.witness is not None else None,
                        int(loc.observed_at_ns),
                    ]
                    for loc in rows_to_upsert
                ],
            )

        for lid in stale_lids:
            self._ephemeral_stale_locations.pop(lid, None)
            self._settled_witnesses.pop(lid, None)
        for loc in rows_to_upsert:
            lid = _coerce_uuid(loc.location_id)
            self._ephemeral_stale_locations.pop(lid, None)
            if loc.witness is not None:
                self._settled_witnesses.pop(lid, None)
        self.bump_location_epoch()
        for coord, win_loc in winning_by_coord.items():
            self._active_coord_witness[coord] = (win_loc.digest, win_loc.witness)

    def upsert_artifact_locations(
        self, locations: Sequence[ArtifactLocationRow]
    ) -> None:
        if not locations:
            return
        with self.transaction() as cur:
            self._upsert_artifact_locations_locked(cur, locations)

    def mark_artifact_location_status(
        self, location_id: uuid.UUID, status: LocationStatus
    ) -> None:
        self.mark_artifact_locations_status_bulk([location_id], status)

    def mark_artifact_locations_status_bulk(
        self, location_ids: Sequence[uuid.UUID], status: LocationStatus
    ) -> None:
        coerced_ids = list(dict.fromkeys(_coerce_uuid(lid) for lid in location_ids))
        if not coerced_ids:
            return
        for lid in coerced_ids:
            self._settled_witnesses.pop(lid, None)
        if self.read_only:
            for lid in coerced_ids:
                if status == "active":
                    self._ephemeral_stale_locations.pop(lid, None)
                else:
                    self._ephemeral_stale_locations[lid] = status
            self.bump_location_epoch()
            return
        now_ns = time.time_ns()
        with self.transaction() as cur:
            _execute_multirow_values(
                cur,
                prefix_sql=(
                    "UPDATE artifact_location AS al "
                    "SET status = v.status, observed_at_ns = v.obs FROM (VALUES"
                ),
                row_placeholder="(CAST(? AS VARCHAR), CAST(? AS BIGINT), CAST(? AS UUID))",
                suffix_sql=") AS v(status, obs, lid) WHERE al.location_id = v.lid",
                rows=[[status, now_ns, lid] for lid in coerced_ids],
            )
        for lid in coerced_ids:
            self._ephemeral_stale_locations.pop(lid, None)
        self.bump_location_epoch()

    def mark_coordinate_locations_status(
        self,
        *,
        mount_name: str,
        mount_instance_id: str,
        uri: str,
        member: str = "",
        site: Optional[str] = None,
        artifact_id: Optional[uuid.UUID] = None,
        status: LocationStatus = "evicted",
    ) -> int:
        if not self.has_artifact_location_table():
            return 0
        clauses = [
            "mount_name = ?",
            "mount_instance_id = ?",
            "uri = ?",
            "member = ?",
            "status = 'active'",
        ]
        params: list[Any] = [mount_name, mount_instance_id, uri, member]
        if site is not None:
            clauses.append("site = ?")
            params.append(site)
        if artifact_id is not None:
            clauses.append("artifact_id = ?")
            params.append(_coerce_uuid(artifact_id))
        if self.read_only:
            matching = self.fetchall(
                f"SELECT location_id FROM artifact_location WHERE {' AND '.join(clauses)}",
                params,
            )
            if not matching:
                return 0
            ids = [_coerce_uuid(r[0]) for r in matching]
            self.mark_artifact_locations_status_bulk(ids, status)
            return len(ids)
        now_ns = time.time_ns()
        with self.transaction() as cur:
            updated = cur.execute(
                f"UPDATE artifact_location SET status = ?, observed_at_ns = ? "
                f"WHERE {' AND '.join(clauses)} RETURNING location_id",
                [status, now_ns, *params],
            ).fetchall()
        if not updated:
            return 0
        for (raw_lid,) in updated:
            lid = _coerce_uuid(raw_lid)
            self._ephemeral_stale_locations.pop(lid, None)
            self._settled_witnesses.pop(lid, None)
        self.bump_location_epoch()
        return len(updated)

    def get_active_coordinate_locations(
        self,
        *,
        mount_name: str,
        mount_instance_id: str,
        uri: str,
        site: Optional[str] = None,
    ) -> List[ArtifactLocationRow]:
        if not self.has_artifact_location_table():
            return []
        self.sync_location_epoch_caches()
        clauses = [
            "mount_name = ?",
            "mount_instance_id = ?",
            "uri = ?",
            "status = 'active'",
        ]
        params: list[Any] = [mount_name, mount_instance_id, uri]
        if site is not None:
            clauses.insert(0, "site = ?")
            params.insert(0, site)
        rows = self.fetchall(
            f"SELECT {self._LOCATION_SELECT_COLS} FROM artifact_location WHERE {' AND '.join(clauses)}",
            params,
        )
        return [
            loc
            for loc in (self._hydrate_location_row(r) for r in rows)
            if loc.status == "active"
        ]

    def settle_artifact_location_witness(
        self,
        location_id: uuid.UUID,
        witness: Witness,
        *,
        observed_at_ns: Optional[int] = None,
    ) -> None:
        lid = _coerce_uuid(location_id)
        self._settled_witnesses[lid] = witness
        if self.read_only:
            return
        serialized = witness.to_json()
        with self.transaction() as cur:
            if observed_at_ns is not None:
                cur.execute(
                    "UPDATE artifact_location "
                    "SET witness_json = CAST(? AS JSON), observed_at_ns = ? "
                    "WHERE location_id = ?",
                    [serialized, int(observed_at_ns), lid],
                )
            else:
                cur.execute(
                    "UPDATE artifact_location SET witness_json = CAST(? AS JSON) WHERE location_id = ?",
                    [serialized, lid],
                )

    @staticmethod
    def _sort_active_locations(
        rows: List[ArtifactLocationRow], coerced_aid: Optional[uuid.UUID]
    ) -> List[ArtifactLocationRow]:
        def _sort_key(loc: ArtifactLocationRow) -> tuple[int, int, int, str]:
            site_rank = (
                0 if loc.site == "workspace" else (1 if loc.site == "external" else 2)
            )
            aid_rank = (
                0 if (coerced_aid is not None and loc.artifact_id == coerced_aid) else 1
            )
            obs_order = (
                -loc.observed_at_ns
                if loc.site in ("workspace", "external")
                else loc.observed_at_ns
            )
            return (site_rank, aid_rank, obs_order, str(loc.location_id))

        rows.sort(key=_sort_key)
        return rows

    def get_active_artifact_locations(
        self,
        *,
        artifact_id: Optional[uuid.UUID] = None,
        digest: Optional[Digest | str] = None,
        member: Optional[str] = None,
    ) -> List[ArtifactLocationRow]:
        coerced_aid = _coerce_optional_uuid(artifact_id)
        digest_str = str(digest) if digest is not None else None
        if coerced_aid is None and digest_str is None:
            return []
        if not self.has_artifact_location_table():
            return []
        self.sync_location_epoch_caches()
        member_key = member or ""
        if digest_str is not None and coerced_aid is not None:
            rows = self.fetchall(
                f"SELECT {self._LOCATION_SELECT_COLS} FROM artifact_location "
                "WHERE digest = ? AND member = ? AND status = 'active' "
                "UNION ALL "
                f"SELECT {self._LOCATION_SELECT_COLS} FROM artifact_location "
                "WHERE artifact_id = ? AND member = ? AND status = 'active'",
                [digest_str, member_key, coerced_aid, member_key],
            )
        elif digest_str is not None:
            rows = self.fetchall(
                f"SELECT {self._LOCATION_SELECT_COLS} FROM artifact_location "
                "WHERE digest = ? AND member = ? AND status = 'active'",
                [digest_str, member_key],
            )
        else:
            rows = self.fetchall(
                f"SELECT {self._LOCATION_SELECT_COLS} FROM artifact_location "
                "WHERE artifact_id = ? AND member = ? AND status = 'active'",
                [coerced_aid, member_key],
            )
        by_lid: dict[uuid.UUID, ArtifactLocationRow] = {}
        for r in rows:
            loc = self._hydrate_location_row(r)
            if loc.status == "active":
                by_lid[loc.location_id] = loc
        return self._sort_active_locations(list(by_lid.values()), coerced_aid)

    def has_evicted_artifact_location(
        self,
        *,
        artifact_id: Optional[uuid.UUID] = None,
        digest: Optional[Digest | str] = None,
        member: Optional[str] = None,
    ) -> bool:
        coerced_aid = _coerce_optional_uuid(artifact_id)
        digest_str = str(digest) if digest is not None else None
        if (
            coerced_aid is None and digest_str is None
        ) or not self.has_artifact_location_table():
            return False
        member_key = member or ""
        if digest_str is not None and coerced_aid is not None:
            row = self.fetchone(
                "SELECT 1 FROM ("
                "SELECT 1 FROM artifact_location "
                "WHERE digest = ? AND member = ? AND status = 'evicted' "
                "UNION ALL "
                "SELECT 1 FROM artifact_location "
                "WHERE artifact_id = ? AND member = ? AND status = 'evicted'"
                ") LIMIT 1",
                [digest_str, member_key, coerced_aid, member_key],
            )
        elif digest_str is not None:
            row = self.fetchone(
                "SELECT 1 FROM artifact_location "
                "WHERE digest = ? AND member = ? AND status = 'evicted' LIMIT 1",
                [digest_str, member_key],
            )
        else:
            row = self.fetchone(
                "SELECT 1 FROM artifact_location "
                "WHERE artifact_id = ? AND member = ? AND status = 'evicted' LIMIT 1",
                [coerced_aid, member_key],
            )
        return row is not None

    def get_active_artifact_locations_bulk(
        self, artifacts: Sequence[Any]
    ) -> Dict[uuid.UUID, List[ArtifactLocationRow]]:
        if not artifacts or not self.has_artifact_location_table():
            return {}
        self.sync_location_epoch_caches()
        aids: list[uuid.UUID] = []
        digests: list[str] = []
        for art in artifacts:
            aid = _coerce_optional_uuid(art.id)
            if aid is not None:
                aids.append(aid)
            if art.digest is not None:
                digests.append(str(art.digest))
        unique_aids = list(dict.fromkeys(aids))
        unique_digests = list(dict.fromkeys(digests))
        if not unique_aids and not unique_digests:
            return {}

        queries: list[str] = []
        params: list[Any] = []
        if unique_digests:
            d_ph = ", ".join("?" for _ in unique_digests)
            queries.append(
                f"SELECT {self._LOCATION_SELECT_COLS} FROM artifact_location "
                f"WHERE digest IN ({d_ph}) AND status = 'active'"
            )
            params.extend(unique_digests)
        if unique_aids:
            a_ph = ", ".join("?" for _ in unique_aids)
            queries.append(
                f"SELECT {self._LOCATION_SELECT_COLS} FROM artifact_location "
                f"WHERE artifact_id IN ({a_ph}) AND status = 'active'"
            )
            params.extend(unique_aids)

        raw_rows = self.fetchall(" UNION ALL ".join(queries), params)
        by_lid: dict[uuid.UUID, ArtifactLocationRow] = {}
        for r in raw_rows:
            loc = self._hydrate_location_row(r)
            if loc.status == "active":
                by_lid[loc.location_id] = loc

        by_digest_member: dict[tuple[str, str], list[ArtifactLocationRow]] = {}
        by_aid_member: dict[tuple[uuid.UUID, str], list[ArtifactLocationRow]] = {}
        for loc in by_lid.values():
            if loc.digest is not None:
                by_digest_member.setdefault((loc.digest, loc.member), []).append(loc)
            if loc.artifact_id is not None:
                by_aid_member.setdefault((loc.artifact_id, loc.member), []).append(loc)

        result: Dict[uuid.UUID, List[ArtifactLocationRow]] = {}
        for art in artifacts:
            aid = _coerce_optional_uuid(art.id)
            if aid is None:
                continue
            member_key = art.member or ""
            matched: dict[uuid.UUID, ArtifactLocationRow] = {}
            if art.digest is not None:
                for loc in by_digest_member.get((str(art.digest), member_key), ()):
                    matched[loc.location_id] = loc
            for loc in by_aid_member.get((aid, member_key), ()):
                matched[loc.location_id] = loc
            result[aid] = self._sort_active_locations(list(matched.values()), aid)
        return result

    def get_artifact_recovery_locations(
        self, artifact_id: uuid.UUID
    ) -> List[ArtifactLocationRow]:
        coerced_aid = _coerce_uuid(artifact_id)
        if not self.has_artifact_location_table():
            return []
        self.sync_location_epoch_caches()
        rows = self.fetchall(
            f"SELECT {self._LOCATION_SELECT_COLS} FROM artifact_location "
            "WHERE artifact_id = ? AND site IN ('archive', 'advisory') AND status = 'active' "
            "ORDER BY observed_at_ns ASC, CAST(location_id AS VARCHAR) ASC",
            [coerced_aid],
        )
        return [
            loc
            for loc in (self._hydrate_location_row(r) for r in rows)
            if loc.status == "active"
        ]

    def get_artifacts_recovery_locations_bulk(
        self, artifact_ids: Sequence[uuid.UUID]
    ) -> Dict[uuid.UUID, List[ArtifactLocationRow]]:
        coerced_aids = list(dict.fromkeys(_coerce_uuid(aid) for aid in artifact_ids))
        if not coerced_aids or not self.has_artifact_location_table():
            return {}
        self.sync_location_epoch_caches()
        placeholders = ", ".join("?" for _ in coerced_aids)
        rows = self.fetchall(
            f"SELECT {self._LOCATION_SELECT_COLS} FROM artifact_location "
            f"WHERE artifact_id IN ({placeholders}) "
            "AND site IN ('archive', 'advisory') AND status = 'active' "
            "ORDER BY observed_at_ns ASC, CAST(location_id AS VARCHAR) ASC",
            coerced_aids,
        )
        by_aid: Dict[uuid.UUID, List[ArtifactLocationRow]] = {
            aid: [] for aid in coerced_aids
        }
        for raw_row in rows:
            loc = self._hydrate_location_row(raw_row)
            if loc.status == "active" and loc.artifact_id is not None:
                by_aid.setdefault(loc.artifact_id, []).append(loc)
        return by_aid

    def replace_artifact_recovery_locations_bulk(
        self,
        locations_by_artifact_id: Mapping[uuid.UUID, Sequence[ArtifactLocationRow]],
        *,
        require_all: bool = True,
    ) -> None:
        if not locations_by_artifact_id:
            return
        coerced_map: dict[uuid.UUID, list[ArtifactLocationRow]] = {
            _coerce_uuid(aid): list(locs)
            for aid, locs in locations_by_artifact_id.items()
        }
        ids = list(coerced_map.keys())
        placeholders = ", ".join("?" for _ in ids)
        now_ns = time.time_ns()
        with self.transaction() as cur:
            found_rows = cur.execute(
                f"SELECT id FROM artifact WHERE id IN ({placeholders})",
                ids,
            ).fetchall()
            found_ids: set[uuid.UUID] = {_coerce_uuid(r[0]) for r in found_rows}
            if require_all:
                missing_ids = sorted(str(aid) for aid in set(ids) - found_ids)
                if missing_ids:
                    raise KeyError(
                        "Artifacts were not found for recovery root update: "
                        + ", ".join(missing_ids)
                    )
            target_aids = [aid for aid in ids if aid in found_ids]
            if not target_aids:
                return
            target_ph = ", ".join("?" for _ in target_aids)
            existing_rows = cur.execute(
                f"SELECT location_id, artifact_id FROM artifact_location "
                f"WHERE artifact_id IN ({target_ph}) "
                "AND site IN ('archive', 'advisory') AND status = 'active'",
                target_aids,
            ).fetchall()
            kept_lids_by_aid: dict[uuid.UUID, set[uuid.UUID]] = {
                aid: {_coerce_uuid(loc.location_id) for loc in coerced_map[aid]}
                for aid in target_aids
            }
            evict_lids: list[uuid.UUID] = []
            for raw_lid, raw_aid in existing_rows:
                lid = _coerce_uuid(raw_lid)
                aid = _coerce_uuid(raw_aid)
                if lid not in kept_lids_by_aid.get(aid, set()):
                    evict_lids.append(lid)
            if evict_lids:
                _execute_multirow_values(
                    cur,
                    prefix_sql=(
                        "UPDATE artifact_location AS al "
                        "SET status = 'evicted', observed_at_ns = v.obs FROM (VALUES"
                    ),
                    row_placeholder="(CAST(? AS BIGINT), CAST(? AS UUID))",
                    suffix_sql=") AS v(obs, lid) WHERE al.location_id = v.lid",
                    rows=[[now_ns, lid] for lid in evict_lids],
                )
                for lid in evict_lids:
                    self._ephemeral_stale_locations.pop(lid, None)
                    self._settled_witnesses.pop(lid, None)
            all_new_locs = [loc for aid in target_aids for loc in coerced_map[aid]]
            if all_new_locs:
                self._upsert_artifact_locations_locked(cur, all_new_locs)
            elif evict_lids:
                self.bump_location_epoch()

    def evict_recovery_root_locations(
        self,
        *,
        mount_name: str,
        mount_instance_id: str,
        root_uri: str,
        retained_ids: Set[uuid.UUID],
    ) -> bool:
        """Mark ``archive``/``advisory`` locations beneath ``root_uri`` evicted for non-retained artifacts."""
        retained_coerced = sorted((_coerce_uuid(aid) for aid in retained_ids), key=str)
        prefix = root_uri if root_uri.endswith("://") else root_uri.rstrip("/") + "/"
        clauses = [
            "site IN ('archive', 'advisory')",
            "status = 'active'",
            "mount_name = ?",
            "mount_instance_id = ?",
            "(uri = ? OR starts_with(uri, ?))",
        ]
        params: list[Any] = [mount_name, mount_instance_id, root_uri, prefix]
        if retained_coerced:
            ph = ", ".join("?" for _ in retained_coerced)
            clauses.append(f"(artifact_id IS NULL OR artifact_id NOT IN ({ph}))")
            params.extend(retained_coerced)
        now_ns = time.time_ns()
        with self.transaction() as cur:
            rows = cur.execute(
                f"UPDATE artifact_location SET status = 'evicted', observed_at_ns = ? "
                f"WHERE {' AND '.join(clauses)} RETURNING location_id",
                [now_ns, *params],
            ).fetchall()
            if not rows:
                return False
            for (raw_lid,) in rows:
                lid = _coerce_uuid(raw_lid)
                self._ephemeral_stale_locations.pop(lid, None)
                self._settled_witnesses.pop(lid, None)
            self.bump_location_epoch()
            return True

    def get_artifact_kv_values_bulk(
        self,
        *,
        artifact_ids: List[uuid.UUID],
        key_paths: List[str],
        namespace: Optional[str] = None,
    ) -> Dict[uuid.UUID, Dict[str, Dict[str, Any]]]:
        """Bulk lookup for typed artifact facet values."""
        if not artifact_ids or not key_paths:
            return {}
        coerced_aids = list(dict.fromkeys(_coerce_uuid(aid) for aid in artifact_ids))
        unique_keys = list(dict.fromkeys(key_paths))
        aid_ph = ", ".join("?" for _ in coerced_aids)
        key_ph = ", ".join("?" for _ in unique_keys)
        clauses = [
            f"artifact_id IN ({aid_ph})",
            f"key_path IN ({key_ph})",
        ]
        params: list[Any] = [*coerced_aids, *unique_keys]
        if namespace is not None:
            clauses.append("namespace = ?")
            params.append(namespace)

        rows = self.fetchall(
            "SELECT artifact_id, facet_id, key_path, namespace, value_type, "
            "value_str, value_num, value_bool, value_json, created_at "
            f"FROM artifact_kv WHERE {' AND '.join(clauses)} "
            "ORDER BY created_at DESC, facet_id DESC",
            params,
        )
        out: Dict[uuid.UUID, Dict[str, Dict[str, Any]]] = {}
        for raw_row in rows:
            row = _hydrate_artifact_kv_row(raw_row)
            by_key = out.setdefault(row.artifact_id, {})
            if row.key_path in by_key:
                continue
            value = _decode_kv_value(
                row.value_type,
                row.value_str,
                row.value_num,
                row.value_bool,
                row.value_json,
            )
            by_key[row.key_path] = {"value": value, "type": row.value_type}
        return out

    def find_runs_by_facet_kv(
        self,
        *,
        namespace: str,
        key: str,
        value_type: Optional[str] = None,
        value_str: Optional[str] = None,
        value_num: Optional[float] = None,
        value_bool: Optional[bool] = None,
        limit: int = 100,
    ) -> List[Run]:
        """Find completed runs matching a single ``run_config_kv`` predicate."""
        kv_clauses = ["kv.run_id = r.id", "kv.namespace = ?", "kv.key = ?"]
        params: list[Any] = [namespace, key]
        if value_type is not None:
            kv_clauses.append("kv.value_type = ?")
            params.append(value_type)
        if value_str is not None:
            kv_clauses.append("kv.value_str = ?")
            params.append(value_str)
        elif value_num is not None:
            kv_clauses.append("kv.value_num = ?")
            params.append(float(value_num))
        elif value_bool is not None:
            kv_clauses.append("kv.value_bool = ?")
            params.append(bool(value_bool))
        params.append(max(0, int(limit)))

        rows = self.fetchall(
            f"SELECT {_RUN_SELECT_COLS} FROM run r "
            "WHERE r.status = 'completed' "
            f"AND EXISTS (SELECT 1 FROM run_config_kv kv WHERE {' AND '.join(kv_clauses)}) "
            f"ORDER BY {recent_run_order_by_sql('r')} LIMIT ?",
            params,
        )
        return [_hydrate_run_row(r) for r in rows]

    def get_facet_values_for_runs(
        self,
        run_ids: List[str],
        *,
        key: str,
        namespace: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Fetch ``run_id -> value`` for a single facet ``key`` across ``run_ids``."""
        unique_ids = list(dict.fromkeys(run_ids))
        if not unique_ids:
            return {}
        run_ph = ", ".join("?" for _ in unique_ids)
        clauses = [f"run_id IN ({run_ph})", "key = ?"]
        params: list[Any] = [*unique_ids, key]
        if namespace:
            clauses.append("namespace = ?")
            params.append(namespace)
        rows = self.fetchall(
            "SELECT run_id, facet_id, namespace, key, value_type, "
            "value_str, value_num, value_bool, value_json, created_at "
            f"FROM run_config_kv WHERE {' AND '.join(clauses)}",
            params,
        )
        values: Dict[str, Any] = {}
        for raw_row in rows:
            row = _hydrate_run_config_kv_row(raw_row)
            values[row.run_id] = _decode_kv_value(
                row.value_type,
                row.value_str,
                row.value_num,
                row.value_bool,
                row.value_json,
            )
        return values

    def get_history(
        self, limit: int = 10, tags: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Return recent run history as a DataFrame with ``duration_seconds``."""
        clauses: list[str] = []
        params: list[Any] = []
        self.append_run_tag_and_metadata_clauses(
            clauses, params, tags=tags, table_alias="r"
        )
        where_sql = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(max(0, int(limit)))
        df = self.fetchdf(
            f"SELECT {_RUN_SELECT_COLS} FROM run r{where_sql} "
            f"ORDER BY {recent_run_order_by_sql('r')} LIMIT ?",
            params,
        )
        if "started_at" in df.columns and "ended_at" in df.columns and not df.empty:
            start = pd.to_datetime(df["started_at"], utc=True)
            end = pd.to_datetime(df["ended_at"], utc=True)
            df["duration_seconds"] = (end - start).dt.total_seconds()
        return df

    def sync_run_coupler_snapshot(
        self,
        *,
        scenario_run_id: str,
        run_id: str,
        phase: CouplerSnapshotPhase,
        step_ordinal: int,
        artifacts: Optional[Mapping[str, Artifact]] = None,
    ) -> None:
        """Record a relational snapshot of the coupler for ``(run_id, phase)``."""
        target_artifacts = artifacts or {}
        now = datetime.now(timezone.utc)
        insert_rows: list[list[Any]] = []
        for role_key, artifact in target_artifacts.items():
            if not isinstance(artifact, Artifact) or artifact.id is None:
                continue
            insert_rows.append(
                [
                    run_id,
                    phase,
                    str(role_key),
                    scenario_run_id,
                    int(step_ordinal),
                    _coerce_uuid(artifact.id),
                    artifact.run_id,
                    now,
                ]
            )
        with self.transaction() as cur:
            cur.execute(
                "DELETE FROM run_coupler_snapshot WHERE run_id = ? AND phase = ?",
                [run_id, phase],
            )
            if insert_rows:
                _execute_multirow_values(
                    cur,
                    prefix_sql=(
                        "INSERT INTO run_coupler_snapshot ("
                        "run_id, phase, role_key, scenario_run_id, step_ordinal, "
                        "artifact_id, producer_run_id, updated_at"
                        ") VALUES"
                    ),
                    row_placeholder="(?, ?, ?, ?, ?, ?, ?, ?)",
                    rows=insert_rows,
                )

    def get_run_coupler_snapshot_details(
        self,
        run_id: str,
        phase: CouplerSnapshotPhase = "post",
        *,
        must_exist: bool = False,
    ) -> Tuple[Dict[str, Artifact], int]:
        """Retrieve ``({role_key: Artifact}, max_step_ordinal)`` for ``(run_id, phase)``."""
        rows = self.fetchall(
            f"SELECT s.role_key, s.step_ordinal, {_ARTIFACT_SELECT_COLS} "
            "FROM run_coupler_snapshot s "
            "JOIN artifact a ON s.artifact_id = a.id "
            "WHERE s.run_id = ? AND s.phase = ? "
            "ORDER BY s.role_key ASC",
            [run_id, phase],
        )
        result_map: dict[str, Artifact] = {}
        max_ordinal = 0
        for row in rows:
            role_key = str(row[0])
            step_ordinal = int(row[1]) if row[1] is not None else 0
            art = _hydrate_artifact_row(row[2:])
            result_map[role_key] = art
            if step_ordinal > max_ordinal:
                max_ordinal = step_ordinal
        if not result_map and must_exist:
            raise KeyError(
                f"No coupler snapshot found for run_id={run_id!r}, phase={phase!r}"
            )
        return result_map, max_ordinal

    def get_run_coupler_snapshot(
        self,
        run_id: str,
        phase: CouplerSnapshotPhase = "post",
        *,
        must_exist: bool = False,
    ) -> Dict[str, Artifact]:
        """Retrieve ``{role_key: Artifact}`` for ``(run_id, phase)``."""
        artifacts, _ = self.get_run_coupler_snapshot_details(
            run_id=run_id, phase=phase, must_exist=must_exist
        )
        return artifacts

    def get_max_coupler_snapshot_ordinal(self, scenario_run_id: str) -> int:
        """Return the maximum ``step_ordinal`` recorded for ``scenario_run_id``."""
        row = self.fetchone(
            "SELECT COALESCE(MAX(step_ordinal), 0) FROM run_coupler_snapshot "
            "WHERE scenario_run_id = ?",
            [scenario_run_id],
        )
        return int(row[0]) if row is not None and row[0] is not None else 0

    def find_latest_scenario_coupler_snapshot(
        self,
        scenario_run_id: str,
        *,
        phase: Optional[CouplerSnapshotPhase] = "post",
        year: Optional[int] = None,
        iteration: Optional[int] = None,
        stage: Optional[str] = None,
        model_name: Optional[str] = None,
        before_step_ordinal: Optional[int] = None,
    ) -> Optional[Tuple[str, int, Dict[str, Artifact]]]:
        """Find the latest coupler snapshot for ``scenario_run_id`` matching optional filters."""
        clauses: list[str] = ["s.scenario_run_id = ?"]
        params: list[Any] = [scenario_run_id]
        if phase is not None:
            clauses.append("s.phase = ?")
            params.append(phase)
        if year is not None:
            clauses.append("r.year = ?")
            params.append(int(year))
        if iteration is not None:
            clauses.append("r.iteration = ?")
            params.append(int(iteration))
        if stage is not None:
            clauses.append("r.stage = ?")
            params.append(stage)
        if model_name is not None:
            clauses.append("r.model_name = ?")
            params.append(model_name)
        if before_step_ordinal is not None:
            clauses.append("s.step_ordinal < ?")
            params.append(int(before_step_ordinal))

        head = self.fetchone(
            "SELECT s.run_id, s.phase, s.step_ordinal "
            "FROM run_coupler_snapshot s "
            "LEFT JOIN run r ON s.run_id = r.id "
            f"WHERE {' AND '.join(clauses)} "
            "ORDER BY s.step_ordinal DESC, s.updated_at DESC LIMIT 1",
            params,
        )
        if head is None:
            return None
        matched_run_id = str(head[0])
        matched_phase = str(head[1])
        matched_ordinal = int(head[2])

        art_rows = self.fetchall(
            f"SELECT s.role_key, {_ARTIFACT_SELECT_COLS} "
            "FROM run_coupler_snapshot s "
            "JOIN artifact a ON s.artifact_id = a.id "
            "WHERE s.run_id = ? AND s.phase = ? "
            "ORDER BY s.role_key ASC",
            [matched_run_id, matched_phase],
        )
        result_map = {str(r[0]): _hydrate_artifact_row(r[1:]) for r in art_rows}
        return (matched_run_id, matched_ordinal, result_map)

    def synthesize_coupler_snapshot_from_history(
        self,
        *,
        scenario_run_id: Optional[str] = None,
        run_id: Optional[str] = None,
        phase: CouplerSnapshotPhase = "post",
        year: Optional[int] = None,
        iteration: Optional[int] = None,
        stage: Optional[str] = None,
        model_name: Optional[str] = None,
        before_step_ordinal: Optional[int] = None,
        backfill: bool = True,
    ) -> Tuple[Dict[str, Artifact], int]:
        """Synthesize coupler state from completed child run outputs in DuckDB history."""
        target_run = self.get_run(run_id) if run_id is not None else None
        if run_id is not None and target_run is None:
            return {}, 0

        if target_run is not None:
            if target_run.parent_run_id is not None:
                parent_id = target_run.parent_run_id
                backfill_scenario_id: Optional[str] = target_run.parent_run_id
                backfill_run_id: Optional[str] = target_run.id
                is_child_target = True
            else:
                parent_id = target_run.id
                backfill_scenario_id = target_run.id
                backfill_run_id = target_run.id
                is_child_target = False
        else:
            if scenario_run_id is None:
                return {}, 0
            parent_id = scenario_run_id
            backfill_scenario_id = scenario_run_id
            backfill_run_id = None
            is_child_target = False

        if not parent_id:
            return {}, 0

        child_rows = self.fetchall(
            f"SELECT {_RUN_SELECT_COLS} FROM run r "
            "WHERE r.parent_run_id = ? "
            "ORDER BY r.created_at ASC, r.started_at ASC, r.id ASC",
            [parent_id],
        )
        all_child_runs = [_hydrate_run_row(r) for r in child_rows]

        target_index: Optional[int] = None
        if is_child_target and run_id is not None:
            for idx, r in enumerate(all_child_runs):
                if r.id == run_id:
                    target_index = idx
                    break

        can_backfill = (
            year is None
            and iteration is None
            and stage is None
            and model_name is None
            and before_step_ordinal is None
        )

        inferred_ordinal = 0
        if is_child_target and target_index is not None:
            inferred_ordinal = target_index + 1
            candidate_runs = (
                all_child_runs[:target_index]
                if phase == "pre"
                else all_child_runs[: target_index + 1]
            )
        else:
            candidate_runs = all_child_runs

        candidate_ids = {r.id for r in candidate_runs}
        selected_runs: list[Run] = []
        for idx, r in enumerate(all_child_runs):
            if r.id not in candidate_ids or r.status != "completed":
                continue
            ord_val = idx + 1
            if before_step_ordinal is not None and ord_val >= before_step_ordinal:
                continue
            if year is not None and r.year != year:
                continue
            if iteration is not None and r.iteration != iteration:
                continue
            if stage is not None and r.stage != stage:
                continue
            if model_name is not None and r.model_name != model_name:
                continue
            selected_runs.append(r)

        if not (is_child_target and target_index is not None):
            if selected_runs:
                last_run_id = selected_runs[-1].id
                inferred_ordinal = next(
                    (
                        idx + 1
                        for idx, r in enumerate(all_child_runs)
                        if r.id == last_run_id
                    ),
                    len(selected_runs),
                )
            else:
                inferred_ordinal = 0

        if not selected_runs:
            return {}, inferred_ordinal

        selected_run_ids = [r.id for r in selected_runs]
        run_order = {rid: i for i, rid in enumerate(selected_run_ids)}
        run_ph = ", ".join("?" for _ in selected_run_ids)
        art_rows = self.fetchall(
            f"SELECT {_ARTIFACT_SELECT_COLS}, l.run_id "
            "FROM run_artifact_link l "
            "JOIN artifact a ON l.artifact_id = a.id "
            f"WHERE l.run_id IN ({run_ph}) AND l.direction = 'output'",
            selected_run_ids,
        )
        hydrated_pairs = [
            (_hydrate_artifact_row(row[:11]), str(row[11])) for row in art_rows
        ]
        hydrated_pairs.sort(
            key=lambda item: (
                run_order.get(item[1], 0),
                item[0].created_at.isoformat() if item[0].created_at else "",
            )
        )
        artifacts: Dict[str, Artifact] = {}
        for art, _ in hydrated_pairs:
            if art.key:
                artifacts[art.key] = art

        if (
            backfill
            and not self.read_only
            and can_backfill
            and backfill_run_id is not None
            and artifacts
            and backfill_scenario_id is not None
        ):
            self.sync_run_coupler_snapshot(
                scenario_run_id=backfill_scenario_id,
                run_id=backfill_run_id,
                phase=phase,
                step_ordinal=inferred_ordinal,
                artifacts=artifacts,
            )

        return artifacts, inferred_ordinal

    def diff_coupler_snapshots(
        self,
        left_run_id: str,
        right_run_id: str,
        *,
        left_phase: CouplerSnapshotPhase = "post",
        right_phase: CouplerSnapshotPhase = "post",
    ) -> pd.DataFrame:
        """Compare two coupler snapshots and return a per-role diff DataFrame."""
        left_artifacts = self.get_run_coupler_snapshot(
            left_run_id, phase=left_phase, must_exist=False
        )
        right_artifacts = self.get_run_coupler_snapshot(
            right_run_id, phase=right_phase, must_exist=False
        )

        columns = [
            "role_key",
            "status",
            "left_artifact_id",
            "right_artifact_id",
            "left_hash",
            "right_hash",
            "left_producer_run_id",
            "right_producer_run_id",
        ]
        all_keys = sorted(set(left_artifacts.keys()) | set(right_artifacts.keys()))
        rows: list[dict[str, Any]] = []
        for key in all_keys:
            la = left_artifacts.get(key)
            ra = right_artifacts.get(key)
            left_id = str(la.id) if (la is not None and la.id is not None) else None
            right_id = str(ra.id) if (ra is not None and ra.id is not None) else None
            left_hash = (
                str(la.digest) if (la is not None and la.digest is not None) else None
            )
            right_hash = (
                str(ra.digest) if (ra is not None and ra.digest is not None) else None
            )
            left_prod = la.run_id if la is not None else None
            right_prod = ra.run_id if ra is not None else None

            if la is None and ra is not None:
                status = "added"
            elif la is not None and ra is None:
                status = "removed"
            elif left_id != right_id or left_hash != right_hash:
                status = "modified"
            else:
                status = "unchanged"

            rows.append(
                {
                    "role_key": key,
                    "status": status,
                    "left_artifact_id": left_id,
                    "right_artifact_id": right_id,
                    "left_hash": left_hash,
                    "right_hash": right_hash,
                    "left_producer_run_id": left_prod,
                    "right_producer_run_id": right_prod,
                }
            )
        return pd.DataFrame(rows, columns=columns)

    def get_runs_by_ids_chronological(self, run_ids: Sequence[str]) -> List[Run]:
        """Fetch ``Run`` rows for ``run_ids`` ordered chronologically."""
        unique_ids = list(dict.fromkeys(str(rid) for rid in run_ids))
        if not unique_ids:
            return []
        placeholders = ", ".join("?" for _ in unique_ids)
        rows = self.fetchall(
            f"SELECT {_RUN_SELECT_COLS} FROM run r "
            f"WHERE r.id IN ({placeholders}) "
            "ORDER BY r.created_at ASC, r.started_at ASC, r.id ASC",
            unique_ids,
        )
        return [_hydrate_run_row(row) for row in rows]

    def iter_artifacts_for_rebuild(
        self,
        *,
        scoped_run_ids: Optional[Sequence[str]] = None,
        outputs_only: bool = False,
        batch_size: int = 1000,
    ) -> Iterator[Artifact]:
        """Stream ``Artifact`` rows in deterministic creation order for CLI rebuild/export."""
        if scoped_run_ids is not None and not scoped_run_ids:
            return

        last_created: Any = None
        last_id: Optional[str] = None
        while True:
            where_clauses: list[str] = []
            params: list[Any] = []

            if scoped_run_ids is not None or outputs_only:
                link_clauses: list[str] = []
                link_params: list[Any] = []
                if scoped_run_ids is not None:
                    run_placeholders = ", ".join("?" for _ in scoped_run_ids)
                    link_clauses.append(f"ral.run_id IN ({run_placeholders})")
                    link_params.extend(scoped_run_ids)
                if outputs_only:
                    link_clauses.append("ral.direction = 'output'")
                link_sql = "SELECT ral.artifact_id FROM run_artifact_link ral"
                if link_clauses:
                    link_sql += " WHERE " + " AND ".join(link_clauses)

                if scoped_run_ids is not None:
                    run_placeholders = ", ".join("?" for _ in scoped_run_ids)
                    where_clauses.append(
                        f"(a.id IN ({link_sql}) OR a.run_id IN ({run_placeholders}))"
                    )
                    params.extend(link_params)
                    params.extend(scoped_run_ids)
                else:
                    where_clauses.append(
                        f"(a.id IN ({link_sql}) OR a.run_id IS NOT NULL)"
                    )
                    params.extend(link_params)

            if last_created is not None and last_id is not None:
                where_clauses.append(
                    "(a.created_at > ? OR (a.created_at = ? AND CAST(a.id AS VARCHAR) > ?))"
                )
                params.extend([last_created, last_created, last_id])

            sql = f"SELECT {_ARTIFACT_SELECT_COLS} FROM artifact a"
            if where_clauses:
                sql += " WHERE " + " AND ".join(where_clauses)
            sql += " ORDER BY a.created_at ASC, CAST(a.id AS VARCHAR) ASC LIMIT ?"
            params.append(max(1, int(batch_size)))

            rows = self.fetchall(sql, params)
            if not rows:
                break
            batch = [_hydrate_artifact_row(row) for row in rows]
            yield from batch
            last_id = str(batch[-1].id)
            last_created = batch[-1].created_at

    def search_runs(self, query: str, *, limit: int = 20) -> List[Run]:
        """Substring search across run IDs, model names, tags, and parent run IDs."""
        pattern = f"%{_escape_like_literal(query)}%"
        order_sql = recent_run_order_by_sql("r")
        rows = self.fetchall(
            f"SELECT {_RUN_SELECT_COLS} FROM run r "
            f"WHERE (r.id LIKE ? ESCAPE '{LIKE_ESCAPE_CHAR}' "
            f"OR r.model_name LIKE ? ESCAPE '{LIKE_ESCAPE_CHAR}' "
            f"OR CAST(r.tags AS VARCHAR) LIKE ? ESCAPE '{LIKE_ESCAPE_CHAR}' "
            f"OR (r.parent_run_id IS NOT NULL AND r.parent_run_id LIKE ? ESCAPE '{LIKE_ESCAPE_CHAR}')) "
            f"ORDER BY {order_sql} LIMIT ?",
            [pattern, pattern, pattern, pattern, max(0, int(limit))],
        )
        return [_hydrate_run_row(row) for row in rows]

    def find_artifacts_by_digest_prefix(
        self,
        prefix: str,
        *,
        run_id: Optional[str] = None,
        limit: int = 6,
    ) -> List[Artifact]:
        """Find artifacts whose ``digest`` starts with ``prefix``."""
        where_clauses = ["a.digest IS NOT NULL", "starts_with(a.digest, ?)"]
        params: list[Any] = [prefix]
        if run_id is not None:
            where_clauses.append("a.run_id = ?")
            params.append(run_id)
        params.append(max(0, int(limit)))
        rows = self.fetchall(
            f"SELECT {_ARTIFACT_SELECT_COLS} FROM artifact a "
            f"WHERE {' AND '.join(where_clauses)} "
            "ORDER BY a.created_at DESC LIMIT ?",
            params,
        )
        return [_hydrate_artifact_row(row) for row in rows]
