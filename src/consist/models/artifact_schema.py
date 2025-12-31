from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import Column, Integer, JSON, String
from sqlmodel import Field, SQLModel

from consist.models.artifact import UUIDType

UTC = timezone.utc


class ArtifactSchema(SQLModel, table=True):
    """
    Deduped schema/profile record for artifacts.

    This is intentionally separate from `Artifact.meta` to avoid repeating large schema
    payloads across many artifacts and runs. Artifacts store a `schema_id` pointer and
    a small `schema_summary` inline in `meta`.
    """

    __tablename__ = "artifact_schema"

    id: str = Field(
        sa_column=Column(String, primary_key=True, nullable=False),
        description="SHA256 of canonicalized schema profile JSON.",
    )
    profile_version: int = Field(
        default=1, description="Internal schema profile format."
    )
    summary_json: Dict[str, Any] = Field(
        default_factory=dict,
        sa_column=Column(JSON, nullable=False),
        description="Small, always-present schema summary (safe to query/display).",
    )
    profile_json: Optional[Dict[str, Any]] = Field(
        default=None,
        sa_column=Column(JSON, nullable=True),
        description="Full schema profile JSON (may be omitted when too large).",
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ArtifactSchemaField(SQLModel, table=True):
    """
    Normalized per-field schema rows for a deduped `ArtifactSchema`.
    """

    __tablename__ = "artifact_schema_field"

    schema_id: str = Field(
        primary_key=True,
        foreign_key="artifact_schema.id",
        description="FK to ArtifactSchema.id (schema hash).",
    )
    ordinal_position: Optional[int] = Field(
        default=None,
        sa_column=Column(Integer, nullable=True),
        description="1-based ordinal position in source table when known.",
    )
    name: str = Field(primary_key=True, index=True, description="Column/field name.")
    logical_type: str = Field(index=True, description="Portable logical type string.")
    nullable: bool = Field(default=True)
    stats_json: Optional[Dict[str, Any]] = Field(
        default=None,
        sa_column=Column(JSON, nullable=True),
        description="Optional stats (min/max/unique count, etc.).",
    )
    is_enum: bool = Field(default=False, index=True)
    enum_values_json: Optional[List[str]] = Field(
        default=None,
        sa_column=Column(JSON, nullable=True),
        description="Optional enum values (may be truncated).",
    )


class ArtifactSchemaObservation(SQLModel, table=True):
    """
    Time-stamped observation linking an Artifact to a schema profile.
    """

    __tablename__ = "artifact_schema_observation"

    id: uuid.UUID = Field(
        default_factory=uuid.uuid4,
        primary_key=True,
        sa_type=UUIDType,
        sa_column_kwargs={"autoincrement": False},
    )
    artifact_id: uuid.UUID = Field(
        foreign_key="artifact.id",
        index=True,
        sa_type=UUIDType,
        sa_column_kwargs={"autoincrement": False},
    )
    schema_id: str = Field(foreign_key="artifact_schema.id", index=True)
    run_id: Optional[str] = Field(
        default=None,
        sa_column=Column(String, nullable=True, index=True),
        description="Associated run id when available.",
    )
    source: str = Field(
        index=True,
        description="Source of schema observation (duckdb|file|dlt|...).",
    )
    sample_rows: Optional[int] = Field(default=None)
    observed_at: datetime = Field(default_factory=lambda: datetime.now(UTC), index=True)
