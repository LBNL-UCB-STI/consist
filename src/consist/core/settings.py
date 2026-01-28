from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "")
    if raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: Optional[int]) -> Optional[int]:
    raw = os.getenv(name, "")
    if raw == "":
        return default
    lowered = raw.strip().lower()
    if lowered in {"none", "null", "nil"}:
        return None
    try:
        return int(lowered)
    except ValueError:
        return default


@dataclass(frozen=True)
class ConsistSettings:
    schema_sample_rows: Optional[int] = 1000
    schema_profile_enabled: bool = False

    @classmethod
    def from_env(cls) -> "ConsistSettings":
        return cls(
            schema_sample_rows=_env_int("CONSIST_SCHEMA_SAMPLE_ROWS", 1000),
            schema_profile_enabled=_env_bool("CONSIST_SCHEMA_PROFILE_ENABLED", False),
        )
