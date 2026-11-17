"""
Value model for flattened configuration facet key-value rows in Consist.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, Field

UTC = timezone.utc


class RunConfigKV(BaseModel):
    """Typed key-value index entry extracted from a run's ``ConfigFacet``."""

    run_id: str
    facet_id: str
    namespace: str = Field(description="Model namespace (defaults to model_name).")
    key: str = Field(description="Flattened dotted key (e.g. 'household_sample_size').")
    value_type: str = Field(description="One of: str|int|float|bool|json|null")
    value_str: Optional[str] = None
    value_num: Optional[float] = None
    value_bool: Optional[bool] = None
    value_json: Optional[Any] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
