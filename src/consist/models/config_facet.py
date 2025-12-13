from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional, Union

from sqlalchemy import Column, JSON, String
from sqlmodel import Field, SQLModel

UTC = timezone.utc


class ConfigFacet(SQLModel, table=True):
    """
    A small, queryable snapshot of a run's configuration (a "facet").

    Facets are intended to be minimal and stable: only include fields that are useful
    for inspection and filtering. Facets are deduplicated by their canonical JSON hash.
    """

    __tablename__ = "config_facet"

    id: str = Field(
        primary_key=True,
        sa_column_kwargs={"autoincrement": False},
        description="SHA256 of canonical facet JSON.",
    )

    # Namespace is the model_name by default (e.g. 'activitysim', 'beam')
    namespace: str = Field(index=True)

    schema_name: str = Field(
        index=True,
        description="Facet schema name (typically the Pydantic model name).",
    )

    schema_version: Optional[Union[str, int]] = Field(
        default=None,
        sa_column=Column(String, nullable=True),
        description="Optional schema version for evolvability.",
    )

    facet_json: Dict[str, Any] = Field(
        default_factory=dict,
        sa_column=Column("json", JSON),
        description="Compact facet JSON (intentionally small).",
    )

    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
