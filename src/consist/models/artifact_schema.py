"""
Domain models for artifact schema profiling in Consist.

These Pydantic DTOs represent deduplicated schema profiles, normalized per-field
rows, observations, and foreign-key relations stored in `StoreLedger`.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

UTC = timezone.utc


class ArtifactSchema(BaseModel):
    """
    Deduplicated schema/profile record stored in the Consist database.

    This is intentionally separate from `Artifact.meta` to avoid repeating large schema
    payloads across many artifacts and runs. Artifacts store a `schema_id` pointer and
    a small `schema_summary` inline in `meta`, while this table stores the canonical
    schema profile JSON.
    """

    model_config = ConfigDict(validate_assignment=True)

    id: str = Field(
        description="SHA256 of canonicalized schema profile JSON.",
    )
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
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
    )

    @field_validator("summary_json", mode="before")
    @classmethod
    def _coerce_summary_json(cls, value: Any) -> Dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, str):
            parsed = json.loads(value)
            return dict(parsed) if isinstance(parsed, dict) else {}
        return dict(value)

    @field_validator("profile_json", mode="before")
    @classmethod
    def _coerce_profile_json(cls, value: Any) -> Optional[Dict[str, Any]]:
        if value is None:
            return None
        if isinstance(value, str):
            parsed = json.loads(value)
            return dict(parsed) if isinstance(parsed, dict) else None
        return dict(value)


class ArtifactSchemaField(BaseModel):
    """
    Normalized per-field schema rows for a deduplicated `ArtifactSchema`.

    This table makes it possible to query schema columns/types directly in SQL
    without parsing the JSON profile payload.
    """

    model_config = ConfigDict(validate_assignment=True)

    schema_id: str = Field(
        description="Reference to ArtifactSchema.id (schema hash).",
    )
    ordinal_position: Optional[int] = Field(
        default=None,
        description="1-based ordinal position in source table when known.",
    )
    name: str = Field(description="Column/field name.")
    logical_type: str = Field(description="Portable logical type string.")
    nullable: bool = Field(default=True)
    stats_json: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional stats (min/max/unique count, etc.).",
    )
    is_enum: bool = Field(default=False)
    enum_values_json: Optional[List[str]] = Field(
        default=None,
        description="Optional enum values (may be truncated).",
    )

    @field_validator("stats_json", mode="before")
    @classmethod
    def _coerce_stats_json(cls, value: Any) -> Optional[Dict[str, Any]]:
        if value is None:
            return None
        if isinstance(value, str):
            parsed = json.loads(value)
            return dict(parsed) if isinstance(parsed, dict) else None
        return dict(value)

    @field_validator("enum_values_json", mode="before")
    @classmethod
    def _coerce_enum_values(cls, value: Any) -> Optional[List[str]]:
        if value is None:
            return None
        if isinstance(value, str):
            parsed = json.loads(value)
            return [str(v) for v in parsed] if isinstance(parsed, list) else None
        return [str(v) for v in value]


class ArtifactSchemaObservation(BaseModel):
    """
    Time-stamped observation linking an Artifact to a schema profile.

    Each observation records that an artifact was profiled and linked to a
    particular schema hash, optionally tied to a run for provenance.
    """

    model_config = ConfigDict(validate_assignment=True)

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    artifact_id: uuid.UUID
    schema_id: str
    run_id: Optional[str] = Field(
        default=None,
        description="Associated run id when available.",
    )
    source: str = Field(
        description="Source of schema observation (duckdb|file|dlt|...).",
    )
    sample_rows: Optional[int] = Field(default=None)
    observed_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
    )


class ArtifactSchemaRelation(BaseModel):
    """Relational metadata (foreign keys) for a deduped ArtifactSchema."""

    model_config = ConfigDict(validate_assignment=True)

    schema_id: str = Field(
        description="Hash of the source schema.",
    )
    from_field: str = Field(
        description="Local column name holding the foreign key.",
    )
    to_table: str = Field(
        description="Target table name (resolved from the SQLModel class)."
    )
    to_field: str = Field(description="Target column name.")
    relationship_type: str = Field(
        default="foreign_key",
        description="Relationship type, currently only foreign_key.",
    )
    cardinality: Optional[str] = Field(
        default=None,
        description="Optional cardinality hint (e.g., many_to_one).",
    )
