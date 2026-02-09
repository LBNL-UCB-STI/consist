"""
Database model for deduplicated artifact facets in the Consist main database.

Artifact facets are compact, structured metadata payloads attached to artifacts.
They are deduplicated by canonical JSON hash and optionally indexed via
``artifact_kv`` for fast filtering.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional, Union

from sqlalchemy import Column, JSON, String
from sqlmodel import Field, SQLModel

UTC = timezone.utc


class ArtifactFacet(SQLModel, table=True):
    """
    Canonical facet payload attached to one or more artifacts.

    ``id`` is the SHA256 of canonical facet JSON. Multiple artifacts can share
    the same payload and therefore the same facet row.
    """

    __tablename__ = "artifact_facet"

    id: str = Field(
        primary_key=True,
        sa_column_kwargs={"autoincrement": False},
        description="SHA256 of canonical artifact facet JSON.",
    )

    namespace: Optional[str] = Field(
        default=None,
        index=True,
        description="Optional namespace for this facet payload (e.g., model name).",
    )

    schema_name: Optional[str] = Field(
        default=None,
        index=True,
        description="Optional facet schema name.",
    )

    schema_version: Optional[Union[str, int]] = Field(
        default=None,
        sa_column=Column(String, nullable=True),
        description="Optional schema version for evolvability.",
    )

    facet_json: Dict[str, Any] = Field(
        default_factory=dict,
        sa_column=Column("json", JSON),
        description="Canonical artifact facet JSON payload.",
    )

    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
