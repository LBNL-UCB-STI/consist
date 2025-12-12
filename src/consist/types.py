"""
consist/types.py

Lightweight typing helpers for Consist integrations.

These are intentionally minimal and dependency-tolerant:
- `FacetLike` captures what Consist accepts for `facet=...`.
- `HasConsistFacet` is an optional convention for configs that can produce a facet.
"""

from __future__ import annotations

from typing import Any, Mapping, Optional, Protocol, TypeAlias, Union, runtime_checkable

try:
    from pydantic import BaseModel  # type: ignore
except Exception:  # pragma: no cover
    BaseModel = object  # type: ignore[misc,assignment]


FacetDict: TypeAlias = Mapping[str, Any]
FacetLike: TypeAlias = Union[FacetDict, BaseModel]


@runtime_checkable
class HasConsistFacet(Protocol):
    def to_consist_facet(self) -> FacetLike: ...

@runtime_checkable
class HasFacetSchemaVersion(Protocol):
    facet_schema_version: Optional[Union[str, int]]
