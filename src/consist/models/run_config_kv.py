"""
Data transfer model for flattened config facet keys projected by `main.run_config_kv`.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

UTC = timezone.utc


class RunConfigKV(BaseModel):
    """
    Row DTO for the `run_config_kv` view in `StoreLedger`.

    Each row represents a single flattened key from a run's `facet_kv` JSON column.
    """

    model_config = ConfigDict(validate_assignment=True)

    run_id: str
    facet_id: str
    namespace: str = Field(
        description="Model namespace (defaults to model_name).",
    )
    key: str = Field(
        description="Flattened dotted key (e.g. 'household_sample_size').",
    )
    value_type: str = Field(
        description="One of: str|int|float|bool|json|null",
    )
    value_str: Optional[str] = Field(default=None)
    value_num: Optional[float] = Field(default=None)
    value_bool: Optional[bool] = Field(default=None)
    value_json: Optional[Any] = Field(default=None)
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
