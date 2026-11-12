from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
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
    get_args,
    get_origin,
)
import uuid
import weakref

import duckdb
import pandas as pd
from pydantic import BaseModel, TypeAdapter
from typing_extensions import Protocol, runtime_checkable

from consist.core._performance_attribution import _track_begin_run_phase
from consist.core.db_runtime import DB_CALL_PROFILER as _DB_CALL_PROFILER
from consist.core.db_snapshot import DatabaseSnapshotOps, snapshot_sidecar_path
from consist.core.facet_common import flatten_facet_values
from consist.core.provenance_writer import ProvenanceWriter
from consist.core.run_ordering import recent_run_order_by_sql
from consist.protocols import ParameterizedSQLProtocol
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


MAX_JSON_DEPTH = 50
LIKE_ESCAPE_CHAR = "~"
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SAFE_JSON_KEY_RE = re.compile(r"^[A-Za-z0-9_.:/\-]+$")
_RELATION_TOKEN_RE = re.compile(r"__consist_rel_([0-9a-f]{16})")
_ARRAY_DRIVERS: frozenset[str] = frozenset({"zarr", "netcdf", "openmatrix", "omx"})
_MEMBER_KIND_META_KEY = "_consist_member_kind"

SchemaProfileSource = Literal["file", "duckdb", "user_provided"]
_RowModelT = TypeVar("_RowModelT")


@runtime_checkable
class _HasSQLColumnInfo(Protocol):
    sa_column: Any


@runtime_checkable
class _HasPrimaryKeyFlag(Protocol):
    primary_key: Any


@runtime_checkable
class _HasTableNameAttr(Protocol):
    __tablename__: str


@runtime_checkable
class _HasTableArgsAttr(Protocol):
    __table_args__: Any


@runtime_checkable
class _HasColumnName(Protocol):
    name: str


@runtime_checkable
class _HasColumnType(Protocol):
    type: Any


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


def _dump_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


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


def _validate_select_only_sql(sql: str) -> None:
    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("SQL query must be a non-empty SELECT statement")
    stripped = _strip_leading_sql_comments(sql)
    if not stripped:
        raise ValueError("SQL query must be a non-empty SELECT statement")
    first_word = stripped.split(None, 1)[0].rstrip("(").upper()
    if first_word == "PRAGMA":
        raise ValueError(
            "query_df, query_rows, and run_query only permit a single read-only SELECT statement"
        )
    stmts = duckdb.extract_statements(sql)
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


def lookup_live_db_by_path(
    db_path: str | os.PathLike[str],
) -> Optional[DatabaseManager]:
    raw = str(db_path).strip()
    if not raw or raw == ":memory:":
        return None
    norm = str(Path(raw).expanduser().resolve())
    with _LIVE_DB_LOCK:
        managers = list(_LIVE_DB_MANAGERS.get(norm, ()))
    for mgr in managers:
        if not mgr._closed and not mgr._suspended and not mgr._con_read_only:
            return mgr
    return None


def has_live_writable_db_for_path(db_path: str | os.PathLike[str]) -> bool:
    return lookup_live_db_by_path(db_path) is not None


_CORE_SCHEMA_DDL: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS consist_schema (
        version INTEGER PRIMARY KEY,
        db_uid VARCHAR NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS run (
        id VARCHAR PRIMARY KEY,
        parent_run_id VARCHAR,
        status VARCHAR NOT NULL DEFAULT 'running',
        model_name VARCHAR NOT NULL,
        description VARCHAR,
        year INTEGER,
        iteration INTEGER,
        stage VARCHAR,
        phase VARCHAR,
        tags JSON NOT NULL DEFAULT '[]',
        config_hash VARCHAR,
        git_hash VARCHAR,
        input_hash VARCHAR,
        signature VARCHAR,
        cacheable BOOLEAN NOT NULL DEFAULT TRUE,
        facet_id VARCHAR,
        meta JSON NOT NULL DEFAULT '{}',
        started_at TIMESTAMPTZ NOT NULL,
        ended_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_run_parent_run_id ON run (parent_run_id)",
    "CREATE INDEX IF NOT EXISTS ix_run_status ON run (status)",
    "CREATE INDEX IF NOT EXISTS ix_run_model_name ON run (model_name)",
    "CREATE INDEX IF NOT EXISTS ix_run_config_hash ON run (config_hash)",
    "CREATE INDEX IF NOT EXISTS ix_run_input_hash ON run (input_hash)",
    "CREATE INDEX IF NOT EXISTS ix_run_signature ON run (signature)",
    "CREATE INDEX IF NOT EXISTS ix_run_cache_lookup ON run (status, cacheable, config_hash, input_hash, git_hash)",
    """
    CREATE TABLE IF NOT EXISTS artifact (
        id UUID PRIMARY KEY,
        key VARCHAR NOT NULL,
        uri VARCHAR NOT NULL,
        driver VARCHAR NOT NULL,
        digest VARCHAR,
        member VARCHAR,
        run_id VARCHAR,
        parent_artifact_id UUID,
        facet_id VARCHAR,
        meta JSON NOT NULL DEFAULT '{}',
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_artifact_key ON artifact (key)",
    "CREATE INDEX IF NOT EXISTS ix_artifact_uri ON artifact (uri)",
    "CREATE INDEX IF NOT EXISTS ix_artifact_digest ON artifact (digest)",
    "CREATE INDEX IF NOT EXISTS ix_artifact_run_id ON artifact (run_id)",
    "CREATE INDEX IF NOT EXISTS ix_artifact_parent_artifact_id ON artifact (parent_artifact_id)",
    """
    CREATE TABLE IF NOT EXISTS run_artifact_link (
        run_id VARCHAR NOT NULL,
        artifact_id UUID NOT NULL,
        direction VARCHAR NOT NULL,
        role VARCHAR NOT NULL DEFAULT '',
        is_implicit BOOLEAN NOT NULL DEFAULT FALSE,
        PRIMARY KEY (run_id, artifact_id, direction, role)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_run_artifact_link_run_id ON run_artifact_link (run_id)",
    "CREATE INDEX IF NOT EXISTS ix_run_artifact_link_artifact_id ON run_artifact_link (artifact_id)",
    """
    CREATE TABLE IF NOT EXISTS run_binding_invocation (
        id UUID PRIMARY KEY,
        requested_run_id VARCHAR NOT NULL,
        execution_run_id VARCHAR NOT NULL,
        cache_source_run_id VARCHAR,
        cache_outcome VARCHAR NOT NULL,
        binding_json VARCHAR NOT NULL,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_run_binding_invocation_requested ON run_binding_invocation (requested_run_id)",
    """
    CREATE TABLE IF NOT EXISTS config_facet (
        id VARCHAR PRIMARY KEY,
        namespace VARCHAR NOT NULL,
        schema_name VARCHAR NOT NULL,
        schema_version VARCHAR,
        "json" JSON NOT NULL DEFAULT '{}',
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_config_facet_namespace ON config_facet (namespace)",
    """
    CREATE TABLE IF NOT EXISTS run_config_kv (
        run_id VARCHAR NOT NULL,
        facet_id VARCHAR NOT NULL,
        namespace VARCHAR NOT NULL,
        key VARCHAR NOT NULL,
        value_type VARCHAR NOT NULL,
        value_str VARCHAR,
        value_num DOUBLE,
        value_bool BOOLEAN,
        value_json JSON,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (run_id, facet_id, namespace, key)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_run_config_kv_run_id ON run_config_kv (run_id)",
    "CREATE INDEX IF NOT EXISTS ix_run_config_kv_lookup ON run_config_kv (namespace, key)",
    """
    CREATE TABLE IF NOT EXISTS artifact_facet (
        id VARCHAR PRIMARY KEY,
        namespace VARCHAR,
        schema_name VARCHAR,
        schema_version VARCHAR,
        "json" JSON NOT NULL DEFAULT '{}',
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS artifact_kv (
        artifact_id UUID NOT NULL,
        facet_id VARCHAR NOT NULL,
        key_path VARCHAR NOT NULL,
        namespace VARCHAR,
        value_type VARCHAR NOT NULL,
        value_str VARCHAR,
        value_num DOUBLE,
        value_bool BOOLEAN,
        value_json JSON,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (artifact_id, facet_id, key_path)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_artifact_kv_artifact_id ON artifact_kv (artifact_id)",
    "CREATE INDEX IF NOT EXISTS ix_artifact_kv_key_path ON artifact_kv (key_path)",
    """
    CREATE TABLE IF NOT EXISTS run_coupler_snapshot (
        run_id VARCHAR NOT NULL,
        phase VARCHAR NOT NULL,
        role_key VARCHAR NOT NULL,
        scenario_run_id VARCHAR NOT NULL,
        step_ordinal INTEGER NOT NULL DEFAULT 0,
        artifact_id UUID NOT NULL,
        producer_run_id VARCHAR,
        updated_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (run_id, phase, role_key)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_run_coupler_snapshot_scenario_ordinal ON run_coupler_snapshot (scenario_run_id, step_ordinal, phase)",
    "CREATE INDEX IF NOT EXISTS ix_run_coupler_snapshot_scenario_role ON run_coupler_snapshot (scenario_run_id, role_key)",
    """
    CREATE TABLE IF NOT EXISTS artifact_schema (
        id VARCHAR PRIMARY KEY,
        profile_version INTEGER NOT NULL DEFAULT 1,
        summary_json JSON NOT NULL DEFAULT '{}',
        profile_json JSON,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS artifact_schema_field (
        schema_id VARCHAR NOT NULL,
        ordinal_position INTEGER,
        name VARCHAR NOT NULL,
        logical_type VARCHAR NOT NULL,
        nullable BOOLEAN NOT NULL DEFAULT TRUE,
        stats_json JSON,
        is_enum BOOLEAN NOT NULL DEFAULT FALSE,
        enum_values_json JSON,
        PRIMARY KEY (schema_id, name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS artifact_schema_observation (
        id UUID PRIMARY KEY,
        artifact_id UUID NOT NULL,
        schema_id VARCHAR NOT NULL,
        run_id VARCHAR,
        source VARCHAR NOT NULL,
        sample_rows INTEGER,
        observed_at TIMESTAMPTZ NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_artifact_schema_obs_artifact_id ON artifact_schema_observation (artifact_id)",
    "CREATE INDEX IF NOT EXISTS ix_artifact_schema_obs_schema_id ON artifact_schema_observation (schema_id)",
    """
    CREATE TABLE IF NOT EXISTS artifact_schema_relation (
        schema_id VARCHAR NOT NULL,
        from_field VARCHAR NOT NULL,
        to_table VARCHAR NOT NULL,
        to_field VARCHAR NOT NULL,
        relationship_type VARCHAR NOT NULL DEFAULT 'foreign_key',
        cardinality VARCHAR,
        PRIMARY KEY (schema_id, from_field)
    )
    """,
    """
    CREATE OR REPLACE VIEW consist_schema_links AS
    SELECT
        rel.schema_id,
        src.summary_json->>'$.table_name' AS from_table,
        rel.from_field,
        rel.to_table,
        rel.to_field,
        rel.relationship_type,
        rel.cardinality
    FROM artifact_schema_relation rel
    JOIN artifact_schema src ON rel.schema_id = src.id
    """,
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


def _extract_artifact_member(
    artifact: Artifact,
) -> tuple[Optional[str], dict[str, Any]]:
    meta = dict(artifact.meta) if isinstance(artifact.meta, Mapping) else {}
    meta.pop(_MEMBER_KIND_META_KEY, None)
    if artifact.array_path is not None:
        if artifact.driver not in _ARRAY_DRIVERS:
            meta[_MEMBER_KIND_META_KEY] = "array"
        return artifact.array_path, meta
    if artifact.table_path is not None:
        if artifact.driver in _ARRAY_DRIVERS:
            meta[_MEMBER_KIND_META_KEY] = "table"
        return artifact.table_path, meta
    return None, meta


def _hydrate_artifact_row(row: Sequence[Any]) -> Artifact:
    driver = str(row[3])
    member = str(row[5]) if row[5] is not None else None
    meta = _parse_json_dict(row[9])
    member_kind = meta.pop(_MEMBER_KIND_META_KEY, None)
    table_path: Optional[str] = None
    array_path: Optional[str] = None
    if member is not None:
        if member_kind == "array" or (
            member_kind != "table" and driver in _ARRAY_DRIVERS
        ):
            array_path = member
        else:
            table_path = member
    return Artifact(
        id=_coerce_uuid(row[0]),
        key=str(row[1]),
        container_uri=str(row[2]),
        driver=driver,
        hash=str(row[4]) if row[4] is not None else None,
        table_path=table_path,
        array_path=array_path,
        run_id=str(row[6]) if row[6] is not None else None,
        parent_artifact_id=_coerce_optional_uuid(row[7]),
        meta=meta,
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
                if _pydantic_field_sql_name(fname, finfo) == col_name:
                    field_info = finfo
                    break
        if field_info is not None:
            sql_type = _pydantic_field_duckdb_type(field_info.annotation, field_info)
            if sql_type == "JSON":
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


def _pydantic_field_sql_name(attr_name: str, field_info: Any) -> str:
    if isinstance(field_info, _HasSQLColumnInfo) and field_info.sa_column is not None:
        sa_col = field_info.sa_column
        if (
            isinstance(sa_col, _HasColumnName)
            and isinstance(sa_col.name, str)
            and sa_col.name
        ):
            return sa_col.name
    return attr_name


def _pydantic_field_is_pk(field_info: Any) -> bool:
    if isinstance(field_info, _HasPrimaryKeyFlag) and field_info.primary_key is True:
        return True
    if isinstance(field_info, _HasSQLColumnInfo) and field_info.sa_column is not None:
        sa_col = field_info.sa_column
        if isinstance(sa_col, _HasPrimaryKeyFlag) and sa_col.primary_key is True:
            return True
    return False


def _pydantic_field_duckdb_type(annotation: Any, field_info: Any) -> str:
    if isinstance(field_info, _HasSQLColumnInfo) and field_info.sa_column is not None:
        sa_col = field_info.sa_column
        if isinstance(sa_col, _HasColumnType) and sa_col.type is not None:
            col_type_name = type(sa_col.type).__name__.upper()
            if "JSON" in col_type_name:
                return "JSON"
            if "FLOAT" in col_type_name or "DOUBLE" in col_type_name:
                return "DOUBLE"
            if "INT" in col_type_name:
                return "BIGINT"
            if "BOOL" in col_type_name:
                return "BOOLEAN"
            if "DATE" in col_type_name or "TIME" in col_type_name:
                return "TIMESTAMPTZ"
            if (
                "STR" in col_type_name
                or "CHAR" in col_type_name
                or "TEXT" in col_type_name
            ):
                return "VARCHAR"

    target = annotation
    origin = get_origin(target)
    if origin is not None:
        non_none = [arg for arg in get_args(target) if arg is not type(None)]
        if len(non_none) == 1:
            target = non_none[0]
            origin = get_origin(target)

    if target is str:
        return "VARCHAR"
    if target is bool:
        return "BOOLEAN"
    if target is int:
        return "BIGINT"
    if target is float:
        return "DOUBLE"
    if target is uuid.UUID:
        return "UUID"
    if target is datetime:
        return "TIMESTAMPTZ"
    if origin in (dict, list, Mapping, Sequence) or target in (dict, list, Any):
        return "JSON"
    return "VARCHAR"


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
        self._closed: bool = False
        self._tx_depth: int = 0
        self._tx_poisoned: bool = False
        self._open_cursors: dict[str, duckdb.DuckDBPyConnection] = {}
        self._suspend_hooks: list[Callable[[], None]] = []
        self._temp_views: dict[str, str] = {}
        self._temp_view_epoch: int = 0
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
        self._con: duckdb.DuckDBPyConnection = self._connect_with_lock_backoff(
            target,
            read_only=self._con_read_only,
            lock_retries=0 if self.read_only else max(0, int(lock_retries)),
            base_sleep_seconds=max(0.005, float(lock_base_sleep_seconds)),
            max_sleep_seconds=max(0.01, float(lock_max_sleep_seconds)),
        )
        self._configure_connection(self._con)

        try:
            self._validate_open_schema_locked()
            if not self._skip_schema_init:
                self.db_uid: str = self._init_schema_locked(db_uid=db_uid)
            else:
                self.db_uid = self._read_existing_db_uid_locked(fallback=db_uid)
        except BaseException:
            try:
                self._con.close()
            except Exception:
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
        conn.execute("SET TimeZone = 'UTC'")
        conn.execute("SET search_path = 'main'")

    def _prepare_cursor_locked(
        self, cur: duckdb.DuckDBPyConnection
    ) -> duckdb.DuckDBPyConnection:
        cur.execute("SET TimeZone = 'UTC'")
        cur.execute("SET search_path = 'main'")
        for create_sql in self._temp_views.values():
            cur.execute(create_sql)
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

    def _ensure_schema_links_view(self) -> None:
        if self.read_only:
            return
        with self._lock:
            self._wait_until_active_locked()
            self._con.execute(
                """
                CREATE OR REPLACE VIEW main.consist_schema_links AS
                SELECT
                    rel.schema_id,
                    json_extract_string(src.summary_json, '$.table_name') AS from_table,
                    rel.from_field,
                    rel.to_table,
                    rel.to_field,
                    rel.relationship_type,
                    rel.cardinality
                FROM main.artifact_schema_relation rel
                JOIN main.artifact_schema src ON rel.schema_id = src.id
                """
            )

    def _require_writable(self, op_name: str) -> None:
        if self.read_only:
            raise PermissionError(
                f"Cannot execute {op_name}() on a read-only DatabaseManager."
            )

    def _wait_until_active_locked(self) -> None:
        while self._suspended:
            self._cond.wait()
        if self._closed:
            raise RuntimeError("DatabaseManager is closed.")

    def _wait_while_suspended(self) -> None:
        with self._lock:
            self._wait_until_active_locked()

    def _ensure_open(self) -> None:
        with self._lock:
            self._wait_until_active_locked()

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        with self._lock:
            self._wait_until_active_locked()
            return self._con

    @property
    def has_open_connections(self) -> bool:
        with self._lock:
            return not self._closed and not self._suspended

    @property
    def instance_key(self) -> str:
        with self._lock:
            return f"{id(self)}:{id(self._con)}:{self._temp_view_epoch}"

    def register_suspend_hook(self, hook: Callable[[], None]) -> None:
        with self._lock:
            if hook not in self._suspend_hooks:
                self._suspend_hooks.append(hook)

    def register_temp_view(self, view_name: str, create_sql: str) -> None:
        with self._lock:
            self._wait_until_active_locked()
            self._con.execute(create_sql)
            self._temp_views[view_name] = create_sql
            self._temp_view_epoch += 1

    def _discard_open_cursor(self, token: str) -> None:
        with self._lock:
            self._open_cursors.pop(token, None)

    def _close_tracked_cursors_locked(self) -> None:
        tokens = list(self._open_cursors.keys())
        for token in tokens:
            cur = self._open_cursors.pop(token, None)
            with _TRACKED_RELATION_LOCK:
                _TRACKED_RELATIONS.pop(token, None)
            if cur is not None:
                try:
                    cur.close()
                except Exception:
                    pass

    def _run_suspend_hooks_locked(self) -> None:
        for hook in list(self._suspend_hooks):
            try:
                hook()
            except Exception:
                pass

    def close(self) -> None:
        """Close all tracked cursors and the underlying DuckDB connection."""
        with self._cond:
            while self._suspended:
                self._cond.wait()
            if self._closed:
                return
            self._run_suspend_hooks_locked()
            self._close_tracked_cursors_locked()
            if self._tx_depth > 0:
                try:
                    self._con.execute("ROLLBACK")
                except Exception:
                    pass
                self._tx_depth = 0
                self._tx_poisoned = False
            self._closed = True
            self._unregister_live_manager()
            try:
                self._con.close()
            except Exception:
                pass
            self._cond.notify_all()

    def dispose(self) -> None:
        """Alias for ``close()``."""
        self.close()

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
        """Release file-backed DuckDB handles while child worker processes run."""
        should_suspend = False
        with self._cond:
            while self._suspended:
                self._cond.wait()
            if not self._closed and self._resolved_db_path is not None:
                if self._tx_depth > 0:
                    raise RuntimeError(
                        "Cannot suspend DatabaseManager for subprocesses inside an active transaction."
                    )
                self._suspended = True
                should_suspend = True
                self._run_suspend_hooks_locked()
                self._close_tracked_cursors_locked()
                if not self._con_read_only:
                    try:
                        self._con.execute("CHECKPOINT")
                    except Exception:
                        pass
                self._unregister_live_manager()
                self._con.close()

        try:
            yield
        finally:
            if should_suspend and self._resolved_db_path is not None:
                deadline = time.monotonic() + 20.0
                attempt = 0
                last_err: Optional[Exception] = None
                reopened: Optional[duckdb.DuckDBPyConnection] = None
                while True:
                    try:
                        in_proc_rw = has_live_writable_db_for_path(
                            self._resolved_db_path
                        )
                        reopen_ro = False if in_proc_rw else self.read_only
                        reopened = duckdb.connect(
                            str(self._resolved_db_path),
                            read_only=reopen_ro,
                        )
                        self._con_read_only = reopen_ro
                        self._configure_connection(reopened)
                        for create_sql in self._temp_views.values():
                            reopened.execute(create_sql)
                        last_err = None
                        break
                    except duckdb.Error as exc:
                        last_err = exc
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            break
                        time.sleep(min(0.02 * (1.3**attempt), 0.25, remaining))
                        attempt += 1

                with self._cond:
                    if reopened is not None:
                        self._con = reopened
                        self._suspended = False
                        self._register_live_manager()
                    else:
                        self._suspended = False
                        self._closed = True
                    self._cond.notify_all()

                if last_err is not None:
                    raise last_err

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
                except Exception:
                    pass
                raise
            else:
                if self._tx_poisoned:
                    try:
                        self._con.execute("ROLLBACK")
                    except Exception:
                        pass
                    raise TransactionAbortedError(
                        "Outer transaction rolled back because a nested transaction failed."
                    )
                self._con.execute("COMMIT")
            finally:
                self._tx_depth = 0
                self._tx_poisoned = False

    @contextmanager
    def borrow_for_dlt(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Yield the locked DuckDB connection to dlt and restore session settings."""
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
                    self._con.execute("SET search_path = 'main'")
                except Exception:
                    pass
                try:
                    self._con.execute("SET TimeZone = 'UTC'")
                except Exception:
                    pass

    @contextmanager
    def exec_cursor(self) -> Iterator[duckdb.DuckDBPyConnection]:
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
            cur = self._prepare_cursor_locked(self._con.cursor())
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
            cur = self._prepare_cursor_locked(self._con.cursor())
            self._open_cursors[token] = cur
            return cur

    @staticmethod
    def _unwrap_sql_and_params(
        sql: str | ParameterizedSQLProtocol,
        params: Optional[Sequence[Any] | Mapping[str, Any]] = None,
    ) -> tuple[str, Optional[list[Any] | dict[str, Any]]]:
        if isinstance(sql, ParameterizedSQLProtocol):
            if params is not None:
                raise ValueError(
                    "Cannot supply separate 'params' when passing a ParameterizedSQL query."
                )
            return sql.sql, list(sql.params)
        if params is None:
            return sql, None
        if isinstance(params, Mapping):
            return sql, dict(params)
        return sql, list(params)

    def execute(
        self,
        sql: str | ParameterizedSQLProtocol,
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
        sql: str | ParameterizedSQLProtocol,
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
        sql: str | ParameterizedSQLProtocol,
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
        sql: str | ParameterizedSQLProtocol,
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
        sql: str | ParameterizedSQLProtocol,
        params: Optional[Sequence[Any] | Mapping[str, Any]] = None,
    ) -> pd.DataFrame:
        """Execute a validated read-only SELECT query and return a DataFrame."""
        raw_sql, raw_params = self._unwrap_sql_and_params(sql, params)
        _validate_select_only_sql(raw_sql)
        return self.fetchdf(raw_sql, raw_params)

    def query_rows(
        self,
        sql: str | ParameterizedSQLProtocol,
        params: Optional[Sequence[Any] | Mapping[str, Any]] = None,
        *,
        model: Optional[Type[_RowModelT]] = None,
    ) -> List[Any]:
        """Execute a validated read-only SELECT query and return rows or validated models."""
        raw_sql, raw_params = self._unwrap_sql_and_params(sql, params)
        _validate_select_only_sql(raw_sql)
        with self.exec_cursor() as cur:
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
        sql: str | ParameterizedSQLProtocol,
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
                res = (
                    self._con.execute(stripped)
                    if raw_params is None
                    else self._con.execute(stripped, raw_params)
                )
                arrow_tbl = res.fetch_arrow_table()
                cur = self._prepare_cursor_locked(self._con.cursor())
                try:
                    cur.register(alias, arrow_tbl)
                    rel = cur.sql(f"SELECT * FROM {alias}")
                except Exception:
                    cur.close()
                    raise
            else:
                cur = self._prepare_cursor_locked(self._con.cursor())
                try:
                    if raw_params is None:
                        rel = cur.sql(f"SELECT * FROM ({stripped}) AS {alias}")
                    else:
                        arrow_tbl = cur.execute(
                            stripped, raw_params
                        ).fetch_arrow_table()
                        cur.register(alias, arrow_tbl)
                        rel = cur.sql(f"SELECT * FROM {alias}")
                except Exception:
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

    def _table_has_column(self, *, table_name: str, column_name: str) -> bool:
        return self.has_table_column(table_name=table_name, column_name=column_name)

    def ensure_pydantic_table(
        self,
        model_cls: Type[BaseModel],
        *,
        schema: Optional[str] = None,
    ) -> tuple[str, str]:
        """Create a DuckDB table from a Pydantic/SQLModel class's ``model_fields``."""
        self._require_writable("ensure_pydantic_table")
        table_name = (
            model_cls.__tablename__
            if isinstance(model_cls, _HasTableNameAttr)
            and isinstance(model_cls.__tablename__, str)
            else model_cls.__name__.lower()
        )
        resolved_schema = schema
        if resolved_schema is None and isinstance(model_cls, _HasTableArgsAttr):
            table_args = model_cls.__table_args__
            if isinstance(table_args, Mapping) and isinstance(
                table_args.get("schema"), str
            ):
                resolved_schema = str(table_args["schema"])
            elif isinstance(table_args, tuple):
                for item in table_args:
                    if isinstance(item, Mapping) and isinstance(
                        item.get("schema"), str
                    ):
                        resolved_schema = str(item["schema"])
                        break
        target_schema = resolved_schema or "main"
        _validate_identifier(target_schema, label="table_schema")
        _validate_identifier(table_name, label="table_name")

        col_defs: list[str] = []
        pk_cols: list[str] = []
        for attr_name, field_info in model_cls.model_fields.items():
            sql_col = _pydantic_field_sql_name(attr_name, field_info)
            _validate_identifier(sql_col, label="column_name")
            duck_type = _pydantic_field_duckdb_type(field_info.annotation, field_info)
            is_pk = _pydantic_field_is_pk(field_info)
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
        member, meta_for_db = _extract_artifact_member(artifact)
        facet_id = explicit_facet_id
        if facet_id is None:
            raw_fid = meta_for_db.get("artifact_facet_id")
            if isinstance(raw_fid, str) and raw_fid:
                facet_id = raw_fid
        return [
            _coerce_uuid(artifact.id),
            artifact.key,
            artifact.container_uri,
            artifact.driver,
            artifact.hash,
            member,
            artifact.run_id,
            _coerce_optional_uuid(artifact.parent_artifact_id),
            facet_id,
            _dump_json(meta_for_db),
            _ensure_non_null_utc_dt(artifact.created_at),
        ]

    _ARTIFACT_UPSERT_SQL = """
        INSERT INTO artifact (
            id, key, uri, driver, digest, member,
            run_id, parent_artifact_id, facet_id, meta, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (id) DO UPDATE SET
            key = EXCLUDED.key,
            uri = EXCLUDED.uri,
            driver = EXCLUDED.driver,
            digest = EXCLUDED.digest,
            member = EXCLUDED.member,
            run_id = EXCLUDED.run_id,
            parent_artifact_id = EXCLUDED.parent_artifact_id,
            facet_id = COALESCE(EXCLUDED.facet_id, artifact.facet_id),
            meta = EXCLUDED.meta,
            created_at = EXCLUDED.created_at
    """

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
            cur.executemany(
                "INSERT INTO run_artifact_link (run_id, artifact_id, direction, role, is_implicit) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT (run_id, artifact_id, direction, role) DO NOTHING",
                insert_rows,
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
        cur.executemany(
            """
            INSERT INTO run_config_kv (
                run_id, facet_id, namespace, key, value_type,
                value_str, value_num, value_bool, value_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (run_id, facet_id, namespace, key) DO UPDATE SET
                value_type = EXCLUDED.value_type,
                value_str = EXCLUDED.value_str,
                value_num = EXCLUDED.value_num,
                value_bool = EXCLUDED.value_bool,
                value_json = EXCLUDED.value_json,
                created_at = EXCLUDED.created_at
            """,
            [
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
                for r in rows
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
        cur.executemany(
            """
            INSERT INTO artifact_kv (
                artifact_id, facet_id, key_path, namespace, value_type,
                value_str, value_num, value_bool, value_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (artifact_id, facet_id, key_path) DO UPDATE SET
                namespace = EXCLUDED.namespace,
                value_type = EXCLUDED.value_type,
                value_str = EXCLUDED.value_str,
                value_num = EXCLUDED.value_num,
                value_bool = EXCLUDED.value_bool,
                value_json = EXCLUDED.value_json,
                created_at = EXCLUDED.created_at
            """,
            [
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
                for r in rows
            ],
        )

    # --- Write Operations ---

    def update_artifact_meta(
        self,
        artifact: Artifact,
        updates: Dict[str, Any],
        *,
        replace: bool = False,
        raise_on_error: bool = False,
    ) -> bool:
        """Update artifact metadata atomically."""
        try:
            with self.transaction() as cur:
                existing_row = cur.execute(
                    "SELECT driver, meta FROM artifact WHERE id = ?",
                    [_coerce_uuid(artifact.id)],
                ).fetchone()
                base_meta = (
                    _parse_json_dict(existing_row[1])
                    if existing_row is not None
                    else dict(artifact.meta or {})
                )
                base_meta.pop(_MEMBER_KIND_META_KEY, None)
                current_meta = dict(artifact.meta or {})
                if replace or updates is artifact.meta:
                    merged_meta = {} if replace else dict(current_meta)
                else:
                    merged_meta = dict(base_meta)
                    merged_meta.update(current_meta)
                for key, value in updates.items():
                    if value is None:
                        merged_meta.pop(key, None)
                    else:
                        merged_meta[key] = value
                artifact.meta = dict(merged_meta)
                self._upsert_artifact_locked(cur, artifact)
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

    def upsert_config_facet(self, facet: ConfigFacet) -> None:
        """Upsert a ``ConfigFacet`` deduplicated by ``facet.id``."""
        schema_ver = (
            str(facet.schema_version) if facet.schema_version is not None else None
        )
        with self.transaction() as cur:
            cur.execute(
                """
                INSERT INTO config_facet (
                    id, namespace, schema_name, schema_version, "json", created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (id) DO UPDATE SET
                    namespace = EXCLUDED.namespace,
                    schema_name = EXCLUDED.schema_name,
                    schema_version = EXCLUDED.schema_version,
                    "json" = EXCLUDED."json"
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

    def persist_config_facet_bundle(
        self,
        *,
        facet: ConfigFacet,
        kv_rows: Optional[List[RunConfigKV]] = None,
    ) -> None:
        """Persist a ``ConfigFacet`` and optional ``RunConfigKV`` rows in one transaction."""
        schema_ver = (
            str(facet.schema_version) if facet.schema_version is not None else None
        )
        with _DB_CALL_PROFILER.track("persist_config_facet_bundle"):
            with self.transaction() as cur:
                cur.execute(
                    """
                    INSERT INTO config_facet (
                        id, namespace, schema_name, schema_version, "json", created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT (id) DO UPDATE SET
                        namespace = EXCLUDED.namespace,
                        schema_name = EXCLUDED.schema_name,
                        schema_version = EXCLUDED.schema_version,
                        "json" = EXCLUDED."json"
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
                if kv_rows:
                    self._replace_run_config_kv_locked(cur, kv_rows)
                    run_ids = {r.run_id for r in kv_rows if r.run_id}
                    for rid in run_ids:
                        cur.execute(
                            "UPDATE run SET facet_id = ? WHERE id = ?",
                            [facet.id, rid],
                        )

    def upsert_artifact_facet(self, facet: ArtifactFacet) -> None:
        """Upsert an ``ArtifactFacet`` deduplicated by ``facet.id``."""
        with self.transaction() as cur:
            self._upsert_artifact_facet_locked(cur, facet)

    def persist_artifact_facet_bundle(
        self,
        *,
        artifact: Artifact,
        facet: ArtifactFacet,
        meta_updates: Dict[str, Any],
        kv_rows: Optional[List[ArtifactKV]] = None,
    ) -> None:
        """Persist artifact facet, metadata updates, and optional KV index atomically."""
        with _DB_CALL_PROFILER.track("persist_artifact_facet_bundle"):
            with self.transaction() as cur:
                existing_row = cur.execute(
                    "SELECT meta FROM artifact WHERE id = ?",
                    [_coerce_uuid(artifact.id)],
                ).fetchone()
                current_meta = (
                    _parse_json_dict(existing_row[0])
                    if existing_row is not None
                    else dict(artifact.meta or {})
                )
                current_meta.pop(_MEMBER_KIND_META_KEY, None)
                current_meta.update(artifact.meta or {})
                current_meta.update(meta_updates)
                artifact.meta = dict(current_meta)

                self._upsert_artifact_facet_locked(cur, facet)
                self._upsert_artifact_locked(cur, artifact, explicit_facet_id=facet.id)
                if kv_rows:
                    self._replace_artifact_kv_locked(cur, kv_rows)

    def sync_artifact_with_facet_bundle(
        self,
        artifact: Artifact,
        run_id: str,
        direction: str,
        *,
        facet: ArtifactFacet,
        meta_updates: Dict[str, Any],
        kv_rows: Optional[List[ArtifactKV]] = None,
    ) -> None:
        """Upsert an Artifact, its run link, and its facet bundle in one transaction."""
        with _DB_CALL_PROFILER.track("sync_artifact_with_facet_bundle"):
            with self.transaction() as cur:
                current_meta = dict(artifact.meta or {})
                current_meta.update(meta_updates)
                artifact.meta = current_meta

                self._upsert_artifact_facet_locked(cur, facet)
                self._upsert_artifact_locked(cur, artifact, explicit_facet_id=facet.id)
                self._link_artifacts_locked(
                    cur,
                    run_id=run_id,
                    artifact_ids=[artifact.id],
                    direction=direction,
                    roles_by_id={artifact.id: artifact.key},
                )
                if kv_rows:
                    self._replace_artifact_kv_locked(cur, kv_rows)

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
            cur.executemany(
                """
                INSERT INTO artifact_schema_field (
                    schema_id, ordinal_position, name, logical_type,
                    nullable, stats_json, is_enum, enum_values_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
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
                    for f in fields
                ],
            )
        cur.execute(
            "DELETE FROM artifact_schema_relation WHERE schema_id = ?",
            [schema.id],
        )
        if relations:
            cur.executemany(
                """
                INSERT INTO artifact_schema_relation (
                    schema_id, from_field, to_table, to_field,
                    relationship_type, cardinality
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    [
                        rel.schema_id,
                        rel.from_field,
                        rel.to_table,
                        rel.to_field,
                        rel.relationship_type,
                        rel.cardinality,
                    ]
                    for rel in relations
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
        meta_updates: Dict[str, Any],
        relations: Optional[List[ArtifactSchemaRelation]] = None,
    ) -> None:
        """Persist schema rows, observation, and artifact metadata in one transaction."""
        with _DB_CALL_PROFILER.track("persist_artifact_schema_profile"):
            with self.transaction() as cur:
                existing_row = cur.execute(
                    "SELECT meta FROM artifact WHERE id = ?",
                    [_coerce_uuid(artifact.id)],
                ).fetchone()
                current_meta = (
                    _parse_json_dict(existing_row[0])
                    if existing_row is not None
                    else dict(artifact.meta or {})
                )
                current_meta.pop(_MEMBER_KIND_META_KEY, None)
                current_meta.update(artifact.meta or {})
                current_meta.update(meta_updates)
                artifact.meta = dict(current_meta)

                self._upsert_schema_rows_locked(cur, schema, fields, relations)
                self._insert_schema_observation_locked(cur, observation)
                self._upsert_artifact_locked(cur, artifact)

    def persist_artifact_schema_observation_bundle(
        self,
        *,
        artifact: Artifact,
        observation: ArtifactSchemaObservation,
        meta_updates: Dict[str, Any],
    ) -> None:
        """Persist a schema observation plus artifact metadata in one transaction."""
        with _DB_CALL_PROFILER.track("persist_artifact_schema_observation_bundle"):
            with self.transaction() as cur:
                existing_row = cur.execute(
                    "SELECT meta FROM artifact WHERE id = ?",
                    [_coerce_uuid(artifact.id)],
                ).fetchone()
                current_meta = (
                    _parse_json_dict(existing_row[0])
                    if existing_row is not None
                    else dict(artifact.meta or {})
                )
                current_meta.pop(_MEMBER_KIND_META_KEY, None)
                current_meta.update(artifact.meta or {})
                current_meta.update(meta_updates)
                artifact.meta = dict(current_meta)

                self._insert_schema_observation_locked(cur, observation)
                self._upsert_artifact_locked(cur, artifact)

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
    ) -> None:
        """Upsert an Artifact and optionally link it to a Run atomically."""
        with _DB_CALL_PROFILER.track(
            _profile_call_name("sync_artifact", profile_label)
        ):
            with self.transaction() as cur:
                self._upsert_artifact_locked(cur, artifact)
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

    def upsert_artifact(self, artifact: Artifact) -> None:
        """Persist an artifact that is not yet linked to a run."""
        with self.transaction() as cur:
            self._upsert_artifact_locked(cur, artifact)

    def sync_artifacts(
        self,
        *,
        artifacts: Sequence[Artifact],
        run_id: str,
        direction: str,
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
                    cur.executemany(
                        self._ARTIFACT_UPSERT_SQL,
                        [self._artifact_upsert_params(art) for art in artifact_list],
                    )
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
                cur.executemany(
                    "UPDATE artifact_schema_field SET ordinal_position = ? "
                    "WHERE schema_id = ? AND name = ?",
                    updates,
                )

    def find_latest_artifact_at_uri(
        self,
        uri: str,
        run_id: Optional[str] = None,
        driver: Optional[str] = None,
        table_path: Optional[str] = None,
        array_path: Optional[str] = None,
        include_inputs: bool = False,
    ) -> Optional[Artifact]:
        """Find the most recent artifact recorded at ``uri``."""
        if (
            table_path is not None
            and array_path is not None
            and table_path != array_path
        ):
            return None

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
        requested_member = table_path if table_path is not None else array_path
        if requested_member is not None:
            clauses.append("a.member = ?")
            params.append(requested_member)

        with _DB_CALL_PROFILER.track("find_latest_artifact_at_uri"):
            rows = self.fetchall(
                f"SELECT {_ARTIFACT_SELECT_COLS} FROM artifact a "
                f"WHERE {' AND '.join(clauses)} "
                "ORDER BY a.created_at DESC, a.id DESC",
                params,
            )
        for row in rows:
            art = _hydrate_artifact_row(row)
            if table_path is not None and art.table_path != table_path:
                continue
            if array_path is not None and art.array_path != array_path:
                continue
            return art
        return None

    def find_latest_artifacts_at_uris(
        self,
        lookups: Sequence[Tuple[str, Optional[str], Optional[str], Optional[str]]],
    ) -> Dict[
        Tuple[str, Optional[str], Optional[str], Optional[str]], Optional[Artifact]
    ]:
        """Bulk variant of produced-artifact URI lookup."""
        if not lookups:
            return {}
        unique_lookups = list(dict.fromkeys(lookups))
        results: Dict[
            Tuple[str, Optional[str], Optional[str], Optional[str]], Optional[Artifact]
        ] = {lookup: None for lookup in unique_lookups}
        lookups_by_uri: Dict[
            str, List[Tuple[str, Optional[str], Optional[str], Optional[str]]]
        ] = {}
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
            candidates = lookups_by_uri.get(art.container_uri, [])
            for lookup in candidates:
                if results[lookup] is not None:
                    continue
                _, l_driver, l_table, l_array = lookup
                if l_driver is not None and art.driver != l_driver:
                    continue
                if l_table is not None and art.table_path != l_table:
                    continue
                if l_array is not None and art.array_path != l_array:
                    continue
                results[lookup] = art.model_copy(deep=True)
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
                f"json_extract_string({alias}.meta, '/content_identity') = ?"
            )
            params.append(content_identity)
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
        with _DB_CALL_PROFILER.track("cache_lookup"):
            row = self.fetchone(
                f"SELECT {_RUN_SELECT_COLS} FROM run r "
                "WHERE r.status = 'completed' AND r.cacheable = TRUE "
                "AND r.config_hash = ? AND r.input_hash = ? AND r.git_hash = ? "
                f"ORDER BY {recent_run_order_by_sql('r')} LIMIT 1",
                [config_hash, input_hash, git_hash],
            )
        return _hydrate_run_row(row) if row is not None else None

    def find_matching_runs(
        self, config_hash: str, input_hash: str, git_hash: str
    ) -> list[Run]:
        """Return all cacheable completed runs matching ``(config_hash, input_hash, git_hash)`` newest first."""
        with _DB_CALL_PROFILER.track("cache_lookup_candidates"):
            rows = self.fetchall(
                f"SELECT {_RUN_SELECT_COLS} FROM run r "
                "WHERE r.status = 'completed' AND r.cacheable = TRUE "
                "AND r.config_hash = ? AND r.input_hash = ? AND r.git_hash = ? "
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
        row = self.fetchone(
            f"SELECT {_RUN_SELECT_COLS} FROM run r "
            "WHERE r.status = 'completed' AND r.cacheable = TRUE AND r.signature = ? "
            f"ORDER BY {recent_run_order_by_sql('r')} LIMIT 1",
            [signature],
        )
        return _hydrate_run_row(row) if row is not None else None

    def find_runs_by_signature(self, signature: str) -> list[Run]:
        """Return all cacheable completed runs matching ``signature`` newest first."""
        rows = self.fetchall(
            f"SELECT {_RUN_SELECT_COLS} FROM run r "
            "WHERE r.status = 'completed' AND r.cacheable = TRUE AND r.signature = ? "
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
        if parent_id is None and isinstance(child.meta, dict):
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

            for mk, mv in metadata.items():
                if not isinstance(mk, str):
                    clauses.append("1 = 1" if mv is None else "1 = 0")
                    continue

                ptr = "/" + mk.replace("~", "~0").replace("/", "~1")
                is_bool = isinstance(mv, bool) or type(mv).__name__ == "bool_"
                if mv is None:
                    clauses.append(
                        f"({table_alias}.meta IS NULL "
                        f"OR json_type({table_alias}.meta) != 'OBJECT' "
                        f"OR COALESCE(json_type({table_alias}.meta, ?), 'NULL') = 'NULL')"
                    )
                    params.append(ptr)
                elif is_bool:
                    clauses.append(
                        f"(json_type({table_alias}.meta, ?) = 'BOOLEAN' "
                        f"AND TRY_CAST(json_extract({table_alias}.meta, ?) AS BOOLEAN) = ?)"
                    )
                    params.extend([ptr, ptr, bool(mv)])
                elif isinstance(mv, numbers.Integral) and not is_bool:
                    iv = int(mv)
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
                elif isinstance(mv, numbers.Real) and not is_bool:
                    fv = float(mv)
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
                elif isinstance(mv, str):
                    clauses.append(
                        f"(json_type({table_alias}.meta, ?) = 'VARCHAR' "
                        f"AND json_extract_string({table_alias}.meta, ?) = ?)"
                    )
                    params.extend([ptr, ptr, mv])
                elif isinstance(mv, Mapping):
                    needs_client_metadata_filter = True
                    clauses.append(f"json_type({table_alias}.meta, ?) = 'OBJECT'")
                    params.append(ptr)
                elif isinstance(mv, Sequence) and not isinstance(
                    mv, (str, bytes, bytearray)
                ):
                    needs_client_metadata_filter = True
                    clauses.append(f"json_type({table_alias}.meta, ?) = 'ARRAY'")
                    params.append(ptr)
                else:
                    needs_client_metadata_filter = True

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
        table_path: Optional[str] = None,
        array_path: Optional[str] = None,
    ) -> Optional[Artifact]:
        """Get the most recent artifact at ``uri`` (including input artifacts)."""
        return self.find_latest_artifact_at_uri(
            uri=uri,
            run_id=run_id,
            table_path=table_path,
            array_path=array_path,
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

    @staticmethod
    def _schema_model_field_names(schema_model: Type[BaseModel]) -> Set[str]:
        field_names: Set[str] = set()
        for attr_name, field_info in schema_model.model_fields.items():
            sql_name = _pydantic_field_sql_name(attr_name, field_info)
            if sql_name.startswith("consist_"):
                continue
            field_names.add(sql_name)
        return field_names

    def find_schema_ids_for_model(
        self, *, schema_model: Type[BaseModel], compatible: bool = True
    ) -> List[str]:
        """Resolve stored schema IDs matching a SQLModel/Pydantic class."""
        model_table_name = (
            schema_model.__tablename__
            if isinstance(schema_model, _HasTableNameAttr)
            and isinstance(schema_model.__tablename__, str)
            else schema_model.__name__
        )
        model_fields = self._schema_model_field_names(schema_model)
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
            if candidate_table_name is not None and str(candidate_table_name) != str(
                model_table_name
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
            found_ids = {_coerce_uuid(r[0]) for r in found_rows}
            if require_all:
                missing_ids = sorted(str(aid) for aid in set(ids) - found_ids)
                if missing_ids:
                    raise KeyError(
                        "Artifacts were not found for recovery root update: "
                        + ", ".join(missing_ids)
                    )
            batch = [[_dump_json(coerced[aid]), aid] for aid in ids if aid in found_ids]
            if batch:
                cur.executemany(
                    "UPDATE artifact SET meta = CAST(? AS JSON) WHERE id = ?",
                    batch,
                )

    def prune_recovery_root_from_artifacts(
        self,
        *,
        target_root: str,
        retained_ids: Set[uuid.UUID],
        normalize_fn: Callable[[Any], List[str]],
    ) -> bool:
        """Remove ``target_root`` from ``meta['recovery_roots']`` on non-retained artifacts."""
        retained_coerced = {_coerce_uuid(aid) for aid in retained_ids}
        with self.transaction() as cur:
            if retained_coerced:
                ordered_retained = sorted(retained_coerced, key=str)
                ph = ", ".join("?" for _ in ordered_retained)
                rows = cur.execute(
                    "SELECT id, meta FROM artifact "
                    f"WHERE id NOT IN ({ph}) AND meta IS NOT NULL",
                    ordered_retained,
                ).fetchall()
            else:
                rows = cur.execute(
                    "SELECT id, meta FROM artifact WHERE meta IS NOT NULL"
                ).fetchall()
            updates: list[list[Any]] = []
            for raw_id, raw_meta in rows:
                meta = _parse_json_dict(raw_meta)
                existing = normalize_fn(meta.get("recovery_roots"))
                if target_root not in existing:
                    continue
                remaining = [r for r in existing if r != target_root]
                next_meta = dict(meta)
                if remaining:
                    next_meta["recovery_roots"] = remaining
                else:
                    next_meta.pop("recovery_roots", None)
                updates.append([_dump_json(next_meta), _coerce_uuid(raw_id)])
            if updates:
                cur.executemany(
                    "UPDATE artifact SET meta = CAST(? AS JSON) WHERE id = ?",
                    updates,
                )
                return True
            return False

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
            value: Any = None
            if row.value_type == "str":
                value = row.value_str
            elif row.value_type == "int":
                value = int(row.value_num) if row.value_num is not None else None
            elif row.value_type == "float":
                value = row.value_num
            elif row.value_type == "bool":
                value = row.value_bool
            elif row.value_type == "json":
                value = row.value_json
            elif row.value_type == "null":
                value = None
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
            if row.value_type == "null":
                val: Any = None
            elif row.value_type == "bool":
                val = row.value_bool
            elif row.value_type == "int":
                val = int(row.value_num) if row.value_num is not None else None
            elif row.value_type == "float":
                val = row.value_num
            elif row.value_type == "str":
                val = row.value_str
            else:
                val = row.value_json
            values[row.run_id] = val
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
                cur.executemany(
                    "INSERT INTO run_coupler_snapshot ("
                    "run_id, phase, role_key, scenario_run_id, step_ordinal, "
                    "artifact_id, producer_run_id, updated_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    insert_rows,
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
            left_hash = la.hash if la is not None else None
            right_hash = ra.hash if ra is not None else None
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
