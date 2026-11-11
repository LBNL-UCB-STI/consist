"""
Clean v2 DuckDB database schema (`provenance.duckdb`), views, table macros,
and v1 tripwire views.
"""

from __future__ import annotations

from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version as package_version
from importlib.resources import files
from typing import TYPE_CHECKING
import uuid

if TYPE_CHECKING:
    import duckdb

V2_SCHEMA_VERSION: int = 2

V1_ONLY_TABLE_NAMES: tuple[str, ...] = (
    "artifact_content",
    "config_facet",
    "artifact_facet",
    "run_artifact_dir_entry",
)

V2_TABLE_NAMES: tuple[str, ...] = (
    "consist_meta",
    "consist_schema",
    "run",
    "artifact",
    "content",
    "content_digest",
    "content_digest_conflict",
    "content_manifest",
    "digest_policy",
    "content_derivation",
    "anchor_promotion",
    "artifact_migration_audit",
    "run_binding",
    "artifact_location",
    "location_event",
    "run_coupler_snapshot",
    "run_binding_invocation",
    "artifact_schema",
    "artifact_schema_field",
    "artifact_schema_observation",
    "artifact_schema_relation",
)

V2_VIEW_NAMES: tuple[str, ...] = (
    "run_artifact_link",
    "run_artifact_role",
    "artifact_location_current",
    "run_config_kv",
    "artifact_kv",
)

V2_EXPECTED_TABLE_COLUMNS: dict[str, dict[str, str]] = {
    "consist_meta": {
        "key": "VARCHAR",
        "value": "VARCHAR",
    },
    "consist_schema": {
        "version": "INTEGER",
        "applied_at": "TIMESTAMP WITH TIME ZONE",
        "consist_version": "VARCHAR",
    },
    "run": {
        "id": "VARCHAR",
        "model_name": "VARCHAR",
        "description": "VARCHAR",
        "year": "INTEGER",
        "iteration": "INTEGER",
        "stage": "VARCHAR",
        "phase": "VARCHAR",
        "parent_run_id": "VARCHAR",
        "status": "VARCHAR",
        "code_hash": "VARCHAR",
        "git_hash": "VARCHAR",
        "config_hash": "VARCHAR",
        "input_hash": "VARCHAR",
        "signature": "VARCHAR",
        "signature_scheme": "VARCHAR",
        "cache_status": "VARCHAR",
        "tags": "JSON",
        "meta": "JSON",
        "facet_kv": "JSON",
        "started_at": "TIMESTAMP WITH TIME ZONE",
        "ended_at": "TIMESTAMP WITH TIME ZONE",
        "created_at": "TIMESTAMP WITH TIME ZONE",
        "updated_at": "TIMESTAMP WITH TIME ZONE",
    },
    "artifact": {
        "id": "UUID",
        "key": "VARCHAR",
        "driver": "VARCHAR",
        "anchor": "VARCHAR",
        "run_id": "VARCHAR",
        "parent_artifact_id": "UUID",
        "schema_id": "VARCHAR",
        "meta": "JSON",
        "facet_kv": "JSON",
        "created_at": "TIMESTAMP WITH TIME ZONE",
    },
    "content": {
        "anchor": "VARCHAR",
        "repr_kind": "VARCHAR",
        "schema_id": "VARCHAR",
        "created_at": "TIMESTAMP WITH TIME ZONE",
    },
    "content_digest": {
        "anchor": "VARCHAR",
        "scheme": "VARCHAR",
        "value": "VARCHAR",
        "basis": "VARCHAR",
        "computed_at": "TIMESTAMP WITH TIME ZONE",
    },
    "content_digest_conflict": {
        "anchor": "VARCHAR",
        "scheme": "VARCHAR",
        "kept_value": "VARCHAR",
        "rejected_value": "VARCHAR",
        "source": "VARCHAR",
        "seen_at": "TIMESTAMP WITH TIME ZONE",
    },
    "content_manifest": {
        "anchor": "VARCHAR",
        "format": "VARCHAR",
        "policy_sha256": "VARCHAR",
        "entry_count": "INTEGER",
        "total_bytes": "BIGINT",
        "body": "BLOB",
    },
    "digest_policy": {
        "policy_sha256": "VARCHAR",
        "policy_json": "JSON",
        "created_at": "TIMESTAMP WITH TIME ZONE",
    },
    "content_derivation": {
        "src_anchor": "VARCHAR",
        "dst_anchor": "VARCHAR",
        "kind": "VARCHAR",
        "created_at": "TIMESTAMP WITH TIME ZONE",
    },
    "anchor_promotion": {
        "historical_anchor": "VARCHAR",
        "verified_anchor": "VARCHAR",
        "basis": "VARCHAR",
        "asserted_by": "VARCHAR",
        "evidence": "JSON",
        "promoted_at": "TIMESTAMP WITH TIME ZONE",
    },
    "artifact_migration_audit": {
        "artifact_id": "UUID",
        "v1_row_json": "JSON",
        "outcome": "VARCHAR",
        "basis": "VARCHAR",
    },
    "run_binding": {
        "run_id": "VARCHAR",
        "direction": "VARCHAR",
        "occurrence_index": "INTEGER",
        "slot": "VARCHAR",
        "ordinal": "INTEGER",
        "artifact_id": "UUID",
        "role": "VARCHAR",
        "anchor": "VARCHAR",
        "contract_scheme": "VARCHAR",
        "sig_scheme": "VARCHAR",
        "sig_hex": "VARCHAR",
    },
    "artifact_location": {
        "observation_id": "UUID",
        "anchor": "VARCHAR",
        "uri": "VARCHAR",
        "member": "VARCHAR",
        "access_driver": "VARCHAR",
        "witness_json": "JSON",
        "basis": "VARCHAR",
        "site": "VARCHAR",
        "scope_key": "VARCHAR",
        "mount_instance_id": "VARCHAR",
        "observed_at_ns": "BIGINT",
        "writer": "VARCHAR",
    },
    "location_event": {
        "event_id": "UUID",
        "observation_id": "UUID",
        "status": "VARCHAR",
        "status_rank": "SMALLINT",
        "transfer_ref": "VARCHAR",
        "at_ns": "BIGINT",
        "writer": "VARCHAR",
    },
    "run_coupler_snapshot": {
        "run_id": "VARCHAR",
        "phase": "VARCHAR",
        "role_key": "VARCHAR",
        "scenario_run_id": "VARCHAR",
        "step_ordinal": "INTEGER",
        "artifact_id": "UUID",
        "producer_run_id": "VARCHAR",
        "updated_at": "TIMESTAMP WITH TIME ZONE",
    },
    "run_binding_invocation": {
        "id": "UUID",
        "requested_run_id": "VARCHAR",
        "execution_run_id": "VARCHAR",
        "cache_source_run_id": "VARCHAR",
        "cache_outcome": "VARCHAR",
        "binding_json": "VARCHAR",
        "created_at": "TIMESTAMP WITH TIME ZONE",
    },
    "artifact_schema": {
        "id": "VARCHAR",
        "profile_version": "INTEGER",
        "summary_json": "JSON",
        "profile_json": "JSON",
        "created_at": "TIMESTAMP WITH TIME ZONE",
    },
    "artifact_schema_field": {
        "schema_id": "VARCHAR",
        "ordinal_position": "INTEGER",
        "name": "VARCHAR",
        "logical_type": "VARCHAR",
        "nullable": "BOOLEAN",
        "stats_json": "JSON",
        "is_enum": "BOOLEAN",
        "enum_values_json": "JSON",
    },
    "artifact_schema_observation": {
        "id": "UUID",
        "artifact_id": "UUID",
        "schema_id": "VARCHAR",
        "run_id": "VARCHAR",
        "source": "VARCHAR",
        "sample_rows": "INTEGER",
        "observed_at": "TIMESTAMP WITH TIME ZONE",
    },
    "artifact_schema_relation": {
        "schema_id": "VARCHAR",
        "from_field": "VARCHAR",
        "to_table": "VARCHAR",
        "to_field": "VARCHAR",
        "relationship_type": "VARCHAR",
        "cardinality": "VARCHAR",
    },
}

V2_EXPECTED_VIEW_COLUMNS: dict[str, dict[str, str]] = {
    "run_artifact_link": {
        "run_id": "VARCHAR",
        "artifact_id": "UUID",
        "direction": "VARCHAR",
        "is_implicit": "BOOLEAN",
    },
    "run_artifact_role": {
        "run_id": "VARCHAR",
        "artifact_id": "UUID",
        "is_input": "BOOLEAN",
        "is_output": "BOOLEAN",
    },
    "run_config_kv": {
        "run_id": "VARCHAR",
        "facet_id": "VARCHAR",
        "namespace": "VARCHAR",
        "key": "VARCHAR",
        "value_type": "VARCHAR",
        "value_str": "VARCHAR",
        "value_num": "DOUBLE",
        "value_bool": "BOOLEAN",
        "value_json": "JSON",
        "created_at": "TIMESTAMP WITH TIME ZONE",
    },
    "artifact_kv": {
        "artifact_id": "UUID",
        "facet_id": "VARCHAR",
        "namespace": "VARCHAR",
        "key_path": "VARCHAR",
        "value_type": "VARCHAR",
        "value_str": "VARCHAR",
        "value_num": "DOUBLE",
        "value_bool": "BOOLEAN",
        "created_at": "TIMESTAMP WITH TIME ZONE",
    },
}


def load_v2_schema_sql() -> str:
    """Load the v2 SQL DDL resource (`schema_v2.sql`)."""
    return files("consist.store").joinpath("schema_v2.sql").read_text(encoding="utf-8")


def _default_consist_version() -> str:
    try:
        return package_version("consist")
    except PackageNotFoundError:
        return "0.5.0"


def install_v1_tripwires(con: duckdb.DuckDBPyConnection) -> None:
    """Install empty tripwire SQL views for every v1-only table name.

    Because SQLAlchemy's `inspect(engine).has_table()` returns `True` for SQL
    views and DuckDB rejects `INSERT` into views, any unpatched `v0.3`/`v0.4`
    process skips `CREATE TABLE` during `SQLModel.metadata.create_all()` and fails
    at bind time before mutating the database.
    """
    for table_name in V1_ONLY_TABLE_NAMES:
        con.execute(
            f'CREATE OR REPLACE VIEW "{table_name}" AS '
            "SELECT NULL::VARCHAR AS upgrade_to_consist_0_5 WHERE false"
        )


def init_v2_schema(
    con: duckdb.DuckDBPyConnection,
    *,
    db_uid: str | None = None,
    consist_version: str | None = None,
) -> str:
    """Initialize all v2 tables, indexes, views, macros, and tripwires in one transaction.

    Stamps `consist_schema(version=2, ...)` LAST inside the transaction so an
    interrupted initialization leaves no partial schema stamped as v2.
    Returns the database's `db_uid`.
    """
    now = datetime.now(timezone.utc)
    resolved_uid = (db_uid or "").strip() or str(uuid.uuid4())
    resolved_version = (consist_version or "").strip() or _default_consist_version()
    ddl_sql = load_v2_schema_sql()

    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(ddl_sql)
        install_v1_tripwires(con)

        con.execute(
            "INSERT INTO consist_meta (key, value) VALUES ('db_uid', ?) "
            "ON CONFLICT (key) DO NOTHING",
            [resolved_uid],
        )
        con.execute(
            "INSERT INTO consist_meta (key, value) VALUES ('created_utc', ?) "
            "ON CONFLICT (key) DO NOTHING",
            [now.isoformat()],
        )

        # Stamp consist_schema LAST inside the single transaction.
        con.execute(
            "INSERT INTO consist_schema (version, applied_at, consist_version) "
            "VALUES (?, ?, ?) ON CONFLICT (version) DO NOTHING",
            [V2_SCHEMA_VERSION, now, resolved_version],
        )
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise

    row = con.execute("SELECT value FROM consist_meta WHERE key = 'db_uid'").fetchone()
    return str(row[0]) if row is not None else resolved_uid


__all__ = [
    "V1_ONLY_TABLE_NAMES",
    "V2_EXPECTED_TABLE_COLUMNS",
    "V2_EXPECTED_VIEW_COLUMNS",
    "V2_SCHEMA_VERSION",
    "V2_TABLE_NAMES",
    "V2_VIEW_NAMES",
    "init_v2_schema",
    "install_v1_tripwires",
    "load_v2_schema_sql",
]
