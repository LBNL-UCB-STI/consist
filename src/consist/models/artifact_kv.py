"""
Database model for flattened artifact facet keys in the Consist main database.

This table stores typed scalar facet leaves for fast artifact filtering.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy import Column, JSON, String
from sqlmodel import Field, SQLModel

from consist.models.artifact import UUIDType

UTC = timezone.utc


class ArtifactKV(SQLModel, table=True):
    """
    Query index table for artifact facets.

    Each row represents a scalar leaf from an artifact facet payload.
    """

    __tablename__ = "artifact_kv"

    artifact_id: uuid.UUID = Field(
        primary_key=True,
        sa_type=UUIDType,
        sa_column_kwargs={"autoincrement": False},
        index=True,
    )

    facet_id: str = Field(
        primary_key=True,
        sa_column_kwargs={"autoincrement": False},
        index=True,
    )

    key_path: str = Field(
        primary_key=True,
        index=True,
        description="Flattened dotted key path.",
    )

    namespace: Optional[str] = Field(
        default=None,
        index=True,
        description="Optional namespace for the indexed facet.",
    )

    value_type: str = Field(
        description="One of: str|int|float|bool|null",
    )

    value_str: Optional[str] = Field(
        default=None, sa_column=Column(String, nullable=True)
    )
    value_num: Optional[float] = Field(default=None, nullable=True)
    value_bool: Optional[bool] = Field(default=None, nullable=True)
    value_json: Optional[Dict[str, Any]] = Field(
        default=None,
        sa_column=Column(JSON, nullable=True),
        description="Reserved for future compatibility.",
    )

    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
