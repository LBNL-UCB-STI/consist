"""
Data transfer model for flattened artifact facet keys projected by `main.artifact_kv`.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

UTC = timezone.utc


class ArtifactKV(BaseModel):
    """
    Row DTO for the `artifact_kv` view in `StoreLedger`.

    Each row represents a scalar leaf from an artifact facet payload.
    """

    model_config = ConfigDict(validate_assignment=True)

    artifact_id: uuid.UUID
    facet_id: str
    key_path: str = Field(
        description="Flattened dotted key path.",
    )
    namespace: Optional[str] = Field(
        default=None,
        description="Optional namespace for the indexed facet.",
    )
    value_type: str = Field(
        description="One of: str|int|float|bool|null",
    )
    value_str: Optional[str] = Field(default=None)
    value_num: Optional[float] = Field(default=None)
    value_bool: Optional[bool] = Field(default=None)
    value_json: Optional[Any] = Field(
        default=None,
        description="Reserved for future compatibility.",
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
    )

    @field_validator("value_json", mode="before")
    @classmethod
    def _coerce_value_json(cls, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            try:
                return json.loads(value)
            except ValueError:
                return value
        return value
