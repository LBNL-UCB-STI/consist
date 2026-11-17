"""Public exception types for Consist."""

from __future__ import annotations


class ConsistError(Exception):
    """Base exception for Consist errors."""


class DeprecatedViewsError(ConsistError, NotImplementedError):
    """Raised when removed v0.4 SQLModel ORM view helpers (`view` / `register_views`) are called."""
