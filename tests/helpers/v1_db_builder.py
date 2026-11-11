"""Pure-DuckDB Schema v1 (`v0.4.2`) test fixture builder without SQLAlchemy or `SQLModel`."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import json
from typing import Any
import uuid

import duckdb

V1_SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS run (
    id VARCHAR PRIMARY KEY,
    parent_run_id VARCHAR,
    model_name VARCHAR NOT NULL,
    description VARCHAR,
    year INTEGER,
    iteration INTEGER,
    stage VARCHAR,
    phase VARCHAR,
    tags JSON,
    status VARCHAR NOT NULL,
    config_hash VARCHAR,
    git_hash VARCHAR,
    input_hash VARCHAR,
    signature VARCHAR,
    meta JSON,
    facet JSON,
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS artifact_content (
    id UUID PRIMARY KEY,
    content_hash VARCHAR NOT NULL,
    driver VARCHAR NOT NULL,
    created_at TIMESTAMPTZ,
    meta JSON
);

CREATE TABLE IF NOT EXISTS artifact (
    id UUID PRIMARY KEY,
    key VARCHAR NOT NULL,
    container_uri VARCHAR NOT NULL,
    table_path VARCHAR,
    array_path VARCHAR,
    driver VARCHAR NOT NULL,
    hash VARCHAR,
    hash_algorithm VARCHAR,
    hash_semantics JSON,
    content_id UUID,
    parent_artifact_id UUID,
    run_id VARCHAR,
    schema_id VARCHAR,
    recovery_roots JSON,
    facet JSON,
    meta JSON,
    created_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS run_artifact_link (
    run_id VARCHAR NOT NULL,
    artifact_id UUID NOT NULL,
    direction VARCHAR NOT NULL,
    is_implicit BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (run_id, artifact_id)
);

CREATE TABLE IF NOT EXISTS run_artifact_dir_entry (
    artifact_id UUID NOT NULL,
    run_id VARCHAR,
    rel_path VARCHAR NOT NULL,
    kind VARCHAR DEFAULT 'file',
    size BIGINT,
    sha256 VARCHAR,
    mtime_ns BIGINT
);

CREATE TABLE IF NOT EXISTS config_facet (
    id VARCHAR PRIMARY KEY,
    namespace VARCHAR NOT NULL,
    schema_name VARCHAR NOT NULL,
    schema_version VARCHAR,
    json JSON,
    created_at TIMESTAMPTZ
);

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
    created_at TIMESTAMPTZ,
    PRIMARY KEY (run_id, facet_id, namespace, key)
);

CREATE TABLE IF NOT EXISTS artifact_facet (
    id VARCHAR PRIMARY KEY,
    namespace VARCHAR,
    schema_name VARCHAR,
    schema_version VARCHAR,
    json JSON,
    created_at TIMESTAMPTZ
);

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
    created_at TIMESTAMPTZ,
    PRIMARY KEY (artifact_id, facet_id, key_path)
);

CREATE TABLE IF NOT EXISTS artifact_schema (
    id VARCHAR PRIMARY KEY,
    profile_version INTEGER DEFAULT 1,
    summary_json JSON NOT NULL,
    profile_json JSON,
    created_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS artifact_schema_field (
    schema_id VARCHAR NOT NULL,
    ordinal_position INTEGER,
    name VARCHAR NOT NULL,
    logical_type VARCHAR NOT NULL,
    nullable BOOLEAN DEFAULT TRUE,
    stats_json JSON,
    is_enum BOOLEAN DEFAULT FALSE,
    enum_values_json JSON,
    PRIMARY KEY (schema_id, name)
);

CREATE TABLE IF NOT EXISTS artifact_schema_observation (
    id UUID PRIMARY KEY,
    artifact_id UUID NOT NULL,
    schema_id VARCHAR NOT NULL,
    run_id VARCHAR,
    source VARCHAR NOT NULL,
    sample_rows INTEGER,
    observed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS artifact_schema_relation (
    schema_id VARCHAR NOT NULL,
    from_field VARCHAR NOT NULL,
    to_table VARCHAR NOT NULL,
    to_field VARCHAR NOT NULL,
    relationship_type VARCHAR DEFAULT 'foreign_key',
    cardinality VARCHAR,
    PRIMARY KEY (schema_id, from_field)
);

CREATE TABLE IF NOT EXISTS run_coupler_snapshot (
    run_id VARCHAR NOT NULL,
    phase VARCHAR NOT NULL,
    slot_key VARCHAR NOT NULL,
    artifact_id UUID NOT NULL,
    step_ordinal INTEGER DEFAULT 0,
    updated_at TIMESTAMPTZ,
    PRIMARY KEY (run_id, phase, slot_key)
);

CREATE TABLE IF NOT EXISTS run_binding_invocation (
    id UUID PRIMARY KEY,
    requested_run_id VARCHAR NOT NULL,
    execution_run_id VARCHAR NOT NULL,
    cache_source_run_id VARCHAR,
    cache_outcome VARCHAR NOT NULL,
    binding_json VARCHAR NOT NULL,
    created_at TIMESTAMPTZ
);
"""


def _json_or_none(val: Any) -> str | None:
    if val is None:
        return None
    if isinstance(val, str):
        return val
    return json.dumps(val, sort_keys=True, separators=(",", ":"))


def _coerce_uuid(val: uuid.UUID | str) -> uuid.UUID:
    return val if isinstance(val, uuid.UUID) else uuid.UUID(str(val))


def create_v1_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create all Consist `v0.4.2` Schema v1 tables on `con` using raw DuckDB SQL."""
    con.execute(V1_SCHEMA_DDL)


def insert_v1_run(
    con: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    model_name: str = "test_model",
    status: str = "completed",
    parent_run_id: str | None = None,
    description: str | None = None,
    year: int | None = None,
    iteration: int | None = None,
    stage: str | None = None,
    phase: str | None = None,
    tags: Sequence[str] | None = None,
    config_hash: str | None = None,
    git_hash: str | None = None,
    input_hash: str | None = None,
    signature: str | None = None,
    meta: Mapping[str, Any] | None = None,
    facet: Mapping[str, Any] | None = None,
    started_at: datetime | None = None,
    ended_at: datetime | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
) -> str:
    """Insert a legacy v1 `run` row."""
    now = created_at or datetime.now(timezone.utc)
    con.execute(
        "INSERT INTO run ("
        "id, parent_run_id, model_name, description, year, iteration, stage, phase, "
        "tags, status, config_hash, git_hash, input_hash, signature, meta, facet, "
        "started_at, ended_at, created_at, updated_at"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            run_id,
            parent_run_id,
            model_name,
            description,
            year,
            iteration,
            stage,
            phase,
            _json_or_none(list(tags) if tags is not None else []),
            status,
            config_hash,
            git_hash,
            input_hash,
            signature,
            _json_or_none(dict(meta or {})),
            _json_or_none(dict(facet)) if facet is not None else None,
            started_at or now,
            ended_at,
            now,
            updated_at or ended_at or now,
        ],
    )
    return run_id


def insert_v1_artifact_content(
    con: duckdb.DuckDBPyConnection,
    *,
    content_id: uuid.UUID | str | None = None,
    content_hash: str,
    driver: str = "parquet",
    meta: Mapping[str, Any] | None = None,
    created_at: datetime | None = None,
) -> uuid.UUID:
    """Insert a legacy v1 `artifact_content` row."""
    cid = _coerce_uuid(content_id) if content_id is not None else uuid.uuid4()
    con.execute(
        "INSERT INTO artifact_content (id, content_hash, driver, created_at, meta) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            cid,
            content_hash,
            driver,
            created_at or datetime.now(timezone.utc),
            _json_or_none(dict(meta)) if meta is not None else None,
        ],
    )
    return cid


def insert_v1_artifact(
    con: duckdb.DuckDBPyConnection,
    *,
    artifact_id: uuid.UUID | str | None = None,
    key: str,
    container_uri: str,
    driver: str = "parquet",
    hash_val: str | None = None,
    hash_algorithm: str | None = None,
    hash_semantics: Mapping[str, Any] | str | None = None,
    table_path: str | None = None,
    array_path: str | None = None,
    content_id: uuid.UUID | str | None = None,
    parent_artifact_id: uuid.UUID | str | None = None,
    run_id: str | None = None,
    schema_id: str | None = None,
    recovery_roots: Sequence[str] | None = None,
    facet: Mapping[str, Any] | None = None,
    meta: Mapping[str, Any] | None = None,
    created_at: datetime | None = None,
) -> uuid.UUID:
    """Insert a legacy v1 `artifact` row."""
    aid = _coerce_uuid(artifact_id) if artifact_id is not None else uuid.uuid4()
    sem_json = (
        json.dumps(hash_semantics)
        if isinstance(hash_semantics, str)
        else _json_or_none(dict(hash_semantics) if hash_semantics is not None else None)
    )
    con.execute(
        "INSERT INTO artifact ("
        "id, key, container_uri, table_path, array_path, driver, hash, "
        "hash_algorithm, hash_semantics, content_id, parent_artifact_id, run_id, "
        "schema_id, recovery_roots, facet, meta, created_at"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            aid,
            key,
            container_uri,
            table_path,
            array_path,
            driver,
            hash_val,
            hash_algorithm,
            sem_json,
            _coerce_uuid(content_id) if content_id is not None else None,
            _coerce_uuid(parent_artifact_id)
            if parent_artifact_id is not None
            else None,
            run_id,
            schema_id,
            _json_or_none(list(recovery_roots)) if recovery_roots is not None else None,
            _json_or_none(dict(facet)) if facet is not None else None,
            _json_or_none(dict(meta or {})),
            created_at or datetime.now(timezone.utc),
        ],
    )
    return aid


def insert_v1_link(
    con: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    artifact_id: uuid.UUID | str,
    direction: str = "output",
    is_implicit: bool = False,
) -> None:
    """Insert a legacy v1 `run_artifact_link` row."""
    con.execute(
        "INSERT INTO run_artifact_link (run_id, artifact_id, direction, is_implicit) "
        "VALUES (?, ?, ?, ?)",
        [run_id, _coerce_uuid(artifact_id), direction, bool(is_implicit)],
    )


def insert_v1_dir_entry(
    con: duckdb.DuckDBPyConnection,
    *,
    artifact_id: uuid.UUID | str,
    rel_path: str,
    run_id: str | None = None,
    kind: str = "file",
    size: int | None = None,
    sha256: str | None = None,
    mtime_ns: int | None = None,
) -> None:
    """Insert a legacy v1 `run_artifact_dir_entry` row."""
    con.execute(
        "INSERT INTO run_artifact_dir_entry "
        "(artifact_id, run_id, rel_path, kind, size, sha256, mtime_ns) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        [_coerce_uuid(artifact_id), run_id, rel_path, kind, size, sha256, mtime_ns],
    )


def insert_v1_config_facet(
    con: duckdb.DuckDBPyConnection,
    *,
    facet_id: str,
    namespace: str,
    schema_name: str = "DictFacet",
    schema_version: str | None = None,
    facet_json: Mapping[str, Any] | None = None,
    created_at: datetime | None = None,
) -> None:
    """Insert a legacy v1 `config_facet` row."""
    con.execute(
        "INSERT INTO config_facet (id, namespace, schema_name, schema_version, json, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [
            facet_id,
            namespace,
            schema_name,
            schema_version,
            _json_or_none(dict(facet_json or {})),
            created_at or datetime.now(timezone.utc),
        ],
    )


def insert_v1_run_config_kv(
    con: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    facet_id: str,
    namespace: str,
    key: str,
    value_type: str,
    value_str: str | None = None,
    value_num: float | None = None,
    value_bool: bool | None = None,
    value_json: Any = None,
    created_at: datetime | None = None,
) -> None:
    """Insert a legacy v1 `run_config_kv` row."""
    con.execute(
        "INSERT INTO run_config_kv ("
        "run_id, facet_id, namespace, key, value_type, "
        "value_str, value_num, value_bool, value_json, created_at"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            run_id,
            facet_id,
            namespace,
            key,
            value_type,
            value_str,
            value_num,
            value_bool,
            _json_or_none(value_json),
            created_at or datetime.now(timezone.utc),
        ],
    )


def insert_v1_artifact_facet(
    con: duckdb.DuckDBPyConnection,
    *,
    facet_id: str,
    namespace: str | None = None,
    schema_name: str | None = None,
    schema_version: str | None = None,
    facet_json: Mapping[str, Any] | None = None,
    created_at: datetime | None = None,
) -> None:
    """Insert a legacy v1 `artifact_facet` row."""
    con.execute(
        "INSERT INTO artifact_facet (id, namespace, schema_name, schema_version, json, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [
            facet_id,
            namespace,
            schema_name,
            schema_version,
            _json_or_none(dict(facet_json or {})),
            created_at or datetime.now(timezone.utc),
        ],
    )


def insert_v1_artifact_kv(
    con: duckdb.DuckDBPyConnection,
    *,
    artifact_id: uuid.UUID | str,
    facet_id: str,
    key_path: str,
    value_type: str,
    namespace: str | None = None,
    value_str: str | None = None,
    value_num: float | None = None,
    value_bool: bool | None = None,
    value_json: Any = None,
    created_at: datetime | None = None,
) -> None:
    """Insert a legacy v1 `artifact_kv` row."""
    con.execute(
        "INSERT INTO artifact_kv ("
        "artifact_id, facet_id, key_path, namespace, value_type, "
        "value_str, value_num, value_bool, value_json, created_at"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            _coerce_uuid(artifact_id),
            facet_id,
            key_path,
            namespace,
            value_type,
            value_str,
            value_num,
            value_bool,
            _json_or_none(value_json),
            created_at or datetime.now(timezone.utc),
        ],
    )


def insert_v1_schema_bundle(
    con: duckdb.DuckDBPyConnection,
    *,
    schema_id: str,
    summary_json: Mapping[str, Any],
    profile_json: Mapping[str, Any] | None = None,
    profile_version: int = 1,
    fields: Sequence[Mapping[str, Any]] = (),
    relations: Sequence[Mapping[str, Any]] = (),
    artifact_id: uuid.UUID | str | None = None,
    run_id: str | None = None,
    source: str = "file",
    sample_rows: int | None = None,
    created_at: datetime | None = None,
) -> None:
    """Insert deduplicated v1 `artifact_schema`, fields, relations, and optional observation."""
    now = created_at or datetime.now(timezone.utc)
    con.execute(
        "INSERT INTO artifact_schema (id, profile_version, summary_json, profile_json, created_at) "
        "VALUES (?, ?, ?, ?, ?) ON CONFLICT (id) DO NOTHING",
        [
            schema_id,
            int(profile_version),
            _json_or_none(dict(summary_json)),
            _json_or_none(dict(profile_json)) if profile_json is not None else None,
            now,
        ],
    )
    for idx, f in enumerate(fields, start=1):
        con.execute(
            "INSERT INTO artifact_schema_field ("
            "schema_id, ordinal_position, name, logical_type, nullable, "
            "stats_json, is_enum, enum_values_json"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (schema_id, name) DO NOTHING",
            [
                schema_id,
                int(f.get("ordinal_position", idx)),
                str(f["name"]),
                str(f.get("logical_type", "VARCHAR")),
                bool(f.get("nullable", True)),
                _json_or_none(f.get("stats_json")),
                bool(f.get("is_enum", False)),
                _json_or_none(f.get("enum_values_json")),
            ],
        )
    for rel in relations:
        con.execute(
            "INSERT INTO artifact_schema_relation ("
            "schema_id, from_field, to_table, to_field, relationship_type, cardinality"
            ") VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (schema_id, from_field) DO NOTHING",
            [
                schema_id,
                str(rel["from_field"]),
                str(rel["to_table"]),
                str(rel["to_field"]),
                str(rel.get("relationship_type", "foreign_key")),
                rel.get("cardinality"),
            ],
        )
    if artifact_id is not None:
        con.execute(
            "INSERT INTO artifact_schema_observation ("
            "id, artifact_id, schema_id, run_id, source, sample_rows, observed_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                uuid.uuid4(),
                _coerce_uuid(artifact_id),
                schema_id,
                run_id,
                source,
                sample_rows,
                now,
            ],
        )


def insert_v1_coupler_snapshot(
    con: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    phase: str,
    slot_key: str,
    artifact_id: uuid.UUID | str,
    step_ordinal: int = 0,
    updated_at: datetime | None = None,
) -> None:
    """Insert a legacy v1 `run_coupler_snapshot` row."""
    con.execute(
        "INSERT INTO run_coupler_snapshot "
        "(run_id, phase, slot_key, artifact_id, step_ordinal, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [
            run_id,
            phase,
            slot_key,
            _coerce_uuid(artifact_id),
            int(step_ordinal),
            updated_at or datetime.now(timezone.utc),
        ],
    )


def insert_v1_binding_invocation(
    con: duckdb.DuckDBPyConnection,
    *,
    requested_run_id: str,
    execution_run_id: str,
    cache_outcome: str,
    binding_json: str = "{}",
    cache_source_run_id: str | None = None,
    invocation_id: uuid.UUID | str | None = None,
    created_at: datetime | None = None,
) -> uuid.UUID:
    """Insert a legacy v1 `run_binding_invocation` row."""
    inv_id = _coerce_uuid(invocation_id) if invocation_id is not None else uuid.uuid4()
    con.execute(
        "INSERT INTO run_binding_invocation ("
        "id, requested_run_id, execution_run_id, cache_source_run_id, "
        "cache_outcome, binding_json, created_at"
        ") VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            inv_id,
            requested_run_id,
            execution_run_id,
            cache_source_run_id,
            cache_outcome,
            binding_json,
            created_at or datetime.now(timezone.utc),
        ],
    )
    return inv_id


def create_v1_global_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    schema_name: str = "global_tables",
) -> None:
    """Create a `global_tables.<table_name>` ingested table and populate sample rows."""
    q_schema = '"' + schema_name.replace('"', '""') + '"'
    q_table = '"' + table_name.replace('"', '""') + '"'
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {q_schema}")
    if not rows:
        con.execute(
            f"CREATE TABLE IF NOT EXISTS {q_schema}.{q_table} "
            "(consist_run_id VARCHAR, consist_artifact_id VARCHAR)"
        )
        return
    import pandas as pd

    df = pd.DataFrame(list(rows))
    con.register("_tmp_v1_global_df", df)
    try:
        con.execute(
            f"CREATE OR REPLACE TABLE {q_schema}.{q_table} AS SELECT * FROM _tmp_v1_global_df"
        )
    finally:
        con.unregister("_tmp_v1_global_df")
