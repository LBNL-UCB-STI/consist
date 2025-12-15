from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy import Column, JSON, String
from sqlmodel import Field, SQLModel

UTC = timezone.utc


class RunConfigKV(SQLModel, table=True):
    """
    Query index for config facets.

    Each row represents a single flattened key from a config facet, stored in a typed column
    to support fast filtering.
    """

    __tablename__ = "run_config_kv"

    run_id: str = Field(
        primary_key=True,
        sa_column_kwargs={"autoincrement": False},
        index=True,
    )

    facet_id: str = Field(
        primary_key=True,
        sa_column_kwargs={"autoincrement": False},
        index=True,
    )

    namespace: str = Field(
        primary_key=True,
        index=True,
        description="Model namespace (defaults to model_name).",
    )

    key: str = Field(
        primary_key=True,
        index=True,
        description="Flattened dotted key (e.g. 'household_sample_size').",
    )

    value_type: str = Field(
        description="One of: str|int|float|bool|json|null",
    )

    value_str: Optional[str] = Field(
        default=None, sa_column=Column(String, nullable=True)
    )
    value_num: Optional[float] = Field(default=None, nullable=True)
    value_bool: Optional[bool] = Field(default=None, nullable=True)
    value_json: Optional[Dict[str, Any]] = Field(
        default=None, sa_column=Column(JSON, nullable=True)
    )

    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
