"""Shared validation helpers for output materialization and hydration options."""

from __future__ import annotations

from consist.types import (
    VALID_MATERIALIZE_DB_FALLBACK,
    VALID_MATERIALIZE_ON_MISSING,
    normalize_materialize_output_keys,
    validate_materialize_option,
)

__all__ = [
    "VALID_MATERIALIZE_DB_FALLBACK",
    "VALID_MATERIALIZE_ON_MISSING",
    "normalize_materialize_output_keys",
    "validate_materialize_option",
]
