"""
Value model for deduplicated configuration facets in Consist.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, Field

UTC = timezone.utc


class ConfigFacet(BaseModel):
    """Compact, deduplicated snapshot of a run's queryable configuration."""

    id: str = Field(description="SHA256 of canonical facet JSON.")
    namespace: str
    schema_name: str = Field(
        description="Facet schema name (typically the Pydantic model name)."
    )
    schema_version: Optional[Union[str, int]] = Field(
        default=None,
        description="Optional schema version for evolvability.",
    )
    facet_json: Dict[str, Any] = Field(
        default_factory=dict,
        description="Compact facet JSON (intentionally small).",
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
