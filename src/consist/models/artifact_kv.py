"""
Value model for flattened artifact facet key-value rows in Consist.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional
import uuid

from pydantic import BaseModel, Field

UTC = timezone.utc


class ArtifactKV(BaseModel):
    """Typed key-value index entry extracted from an ``ArtifactFacet``."""

    artifact_id: uuid.UUID
    facet_id: str
    key_path: str = Field(description="Flattened dotted key path.")
    namespace: Optional[str] = Field(
        default=None,
        description="Optional namespace for the indexed facet.",
    )
    value_type: str = Field(description="One of: str|int|float|bool|null")
    value_str: Optional[str] = None
    value_num: Optional[float] = None
    value_bool: Optional[bool] = None
    value_json: Optional[Any] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
