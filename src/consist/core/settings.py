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


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "")
    if raw == "":
        return default
    try:
        return float(raw.strip())
    except ValueError:
        return default


@dataclass(frozen=True)
class ConsistSettings:
    schema_sample_rows: Optional[int] = 1000
    schema_profile_enabled: bool = False
    dlt_lock_retries: int = 20
    dlt_lock_base_sleep_seconds: float = 0.1
    dlt_lock_max_sleep_seconds: float = 2.0
    db_lock_retries: int = 20
    db_lock_base_sleep_seconds: float = 0.1
    db_lock_max_sleep_seconds: float = 2.0

    @classmethod
    def from_env(cls) -> "ConsistSettings":
        return cls(
            schema_sample_rows=_env_int("CONSIST_SCHEMA_SAMPLE_ROWS", 1000),
            schema_profile_enabled=_env_bool("CONSIST_SCHEMA_PROFILE_ENABLED", False),
            dlt_lock_retries=_env_int("CONSIST_DLT_LOCK_RETRIES", 20) or 20,
            dlt_lock_base_sleep_seconds=_env_float(
                "CONSIST_DLT_LOCK_BASE_SLEEP_SECONDS", 0.1
            ),
            dlt_lock_max_sleep_seconds=_env_float(
                "CONSIST_DLT_LOCK_MAX_SLEEP_SECONDS", 2.0
            ),
            db_lock_retries=_env_int("CONSIST_DB_LOCK_RETRIES", 20) or 20,
            db_lock_base_sleep_seconds=_env_float(
                "CONSIST_DB_LOCK_BASE_SLEEP_SECONDS", 0.1
            ),
            db_lock_max_sleep_seconds=_env_float(
                "CONSIST_DB_LOCK_MAX_SLEEP_SECONDS", 2.0
            ),
        )
