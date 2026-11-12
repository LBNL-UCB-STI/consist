"""
Value model for deduplicated artifact facets in Consist.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, Field

UTC = timezone.utc


class ArtifactFacet(BaseModel):
    """Canonical structured metadata payload attached to one or more artifacts."""

    id: str = Field(description="SHA256 of canonical artifact facet JSON.")
    namespace: Optional[str] = Field(
        default=None,
        description="Optional namespace for this facet payload (e.g., model name).",
    )
    schema_name: Optional[str] = Field(
        default=None,
        description="Optional facet schema name.",
    )
    schema_version: Optional[Union[str, int]] = Field(
        default=None,
        description="Optional schema version for evolvability.",
    )
    facet_json: Dict[str, Any] = Field(
        default_factory=dict,
        description="Canonical artifact facet JSON payload.",
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
