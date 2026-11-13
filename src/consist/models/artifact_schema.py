"""
Value models for artifact schema profiling in Consist.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import uuid

from pydantic import BaseModel, Field

UTC = timezone.utc


class ArtifactSchema(BaseModel):
    """Deduplicated schema/profile record stored in Consist."""

    id: str = Field(description="SHA256 of canonicalized schema profile JSON.")
    profile_version: int = Field(
        default=1, description="Internal schema profile format."
    )
    summary_json: Dict[str, Any] = Field(
        default_factory=dict,
        description="Small, always-present schema summary (safe to query/display).",
    )
    profile_json: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Full schema profile JSON (may be omitted when too large).",
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ArtifactSchemaField(BaseModel):
    """Normalized per-field schema entry for a deduplicated ``ArtifactSchema``."""

    schema_id: str = Field(description="FK to ArtifactSchema.id (schema hash).")
    ordinal_position: Optional[int] = Field(
        default=None,
        description="1-based ordinal position in source table when known.",
    )
    name: str = Field(description="Column/field name.")
    logical_type: str = Field(description="Portable logical type string.")
    nullable: bool = True
    stats_json: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional stats (min/max/unique count, etc.).",
    )
    is_enum: bool = False
    enum_values_json: Optional[List[str]] = Field(
        default=None,
        description="Optional enum values (may be truncated).",
    )


class ArtifactSchemaObservation(BaseModel):
    """Time-stamped observation linking an ``Artifact`` to a schema profile."""

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    artifact_id: uuid.UUID
    schema_id: str
    run_id: Optional[str] = Field(
        default=None, description="Associated run id when available."
    )
    source: str = Field(
        description="Source of schema observation (duckdb|file|dlt|...)."
    )
    sample_rows: Optional[int] = None
    observed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ArtifactSchemaRelation(BaseModel):
    """Relational metadata (foreign keys) for a deduplicated ``ArtifactSchema``."""

    schema_id: str = Field(description="Hash of the source schema.")
    from_field: str = Field(description="Local column name holding the foreign key.")
    to_table: str = Field(description="Target table name.")
    to_field: str = Field(description="Target column name.")
    relationship_type: str = Field(
        default="foreign_key",
        description="Relationship type, currently only foreign_key.",
    )
    cardinality: Optional[str] = Field(
        default=None,
        description="Optional cardinality hint (e.g., many_to_one).",
    )
