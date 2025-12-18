from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.engine import Engine

from consist.core.identity import IdentityManager

PROFILE_VERSION = 1

MAX_SCHEMA_JSON_BYTES = 65_536
MAX_INLINE_PROFILE_BYTES = 16_384
MAX_FIELDS = 2_000


@dataclass(frozen=True)
class SchemaFieldProfile:
    name: str
    logical_type: str
    nullable: bool
    ordinal_position: int
    stats: Optional[Dict[str, Any]] = None
    is_enum: bool = False
    enum_values: Optional[List[str]] = None

    def to_row(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "logical_type": self.logical_type,
            "nullable": self.nullable,
            "stats": self.stats,
            "is_enum": self.is_enum,
            "enum_values": self.enum_values,
        }


@dataclass(frozen=True)
class SchemaProfileResult:
    schema_id: str
    summary: Dict[str, Any]
    schema_json: Optional[Dict[str, Any]]
    inline_profile_json: Optional[Dict[str, Any]]
    fields: Sequence[SchemaFieldProfile]


def _json_size_bytes(obj: Any) -> int:
    return len(
        json.dumps(
            obj, sort_keys=True, ensure_ascii=True, separators=(",", ":")
        ).encode("utf-8")
    )


def profile_duckdb_table(
    *,
    engine: Engine,
    identity: IdentityManager,
    table_schema: str,
    table_name: str,
    source: str = "duckdb",
    sample_rows: Optional[int] = None,
) -> SchemaProfileResult:
    """
    Infer a schema profile for a DuckDB relation using information_schema.

    This is intended to capture the post-ingest "ground truth" schema (e.g. dlt-normalized),
    not just file-side dtypes.
    """
    sql = text(
        """
        SELECT
            column_name,
            data_type,
            is_nullable,
            ordinal_position
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
        ORDER BY ordinal_position
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(
            sql, {"schema": table_schema, "table": table_name}
        ).fetchall()

    fields: List[SchemaFieldProfile] = []
    for col_name, data_type, is_nullable, _ordinal in rows:
        fields.append(
            SchemaFieldProfile(
                name=str(col_name),
                logical_type=str(data_type).lower(),
                nullable=str(is_nullable).upper() == "YES",
                ordinal_position=int(_ordinal),
            )
        )

    truncated_flags: Dict[str, Any] = {
        "fields": False,
        "schema_json": False,
        "inline_profile": False,
    }

    field_rows = [f.to_row() for f in fields]
    hash_obj: Dict[str, Any] = {
        "profile_version": PROFILE_VERSION,
        "source": source,
        "table_schema": table_schema,
        "table_name": table_name,
        "sample_rows": sample_rows,
        "fields": field_rows,
    }
    schema_id = identity.canonical_json_sha256(hash_obj)

    summary = {
        "profile_version": PROFILE_VERSION,
        "schema_id": schema_id,
        "source": source,
        "table_schema": table_schema,
        "table_name": table_name,
        "sample_rows": sample_rows,
        "n_columns": len(fields),
        "truncated": truncated_flags,
    }

    profile_obj: Dict[str, Any] = dict(hash_obj)
    if len(fields) > MAX_FIELDS:
        truncated_flags["fields"] = True
        profile_obj["fields"] = []

    schema_json: Optional[Dict[str, Any]] = profile_obj
    if _json_size_bytes(profile_obj) > MAX_SCHEMA_JSON_BYTES:
        truncated_flags["schema_json"] = True
        schema_json = None

    inline_profile_json: Optional[Dict[str, Any]] = profile_obj
    if schema_json is None or _json_size_bytes(profile_obj) > MAX_INLINE_PROFILE_BYTES:
        truncated_flags["inline_profile"] = True
        inline_profile_json = None

    return SchemaProfileResult(
        schema_id=schema_id,
        summary=summary,
        schema_json=schema_json,
        inline_profile_json=inline_profile_json,
        fields=fields,
    )
