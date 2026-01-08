"""
consist/types.py

Lightweight typing helpers for Consist integrations.

These are intentionally minimal and dependency-tolerant:
- `FacetLike` captures what Consist accepts for `facet=...`.
- `HasConsistFacet` is an optional convention for configs that can produce a facet.
"""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Mapping,
    Optional,
    Protocol,
    Literal,
    TypeAlias,
    Union,
    runtime_checkable,
)

try:
    from pydantic import BaseModel  # type: ignore
except Exception:  # pragma: no cover
    BaseModel = object  # type: ignore[misc,assignment]

if TYPE_CHECKING:  # pragma: no cover
    from consist.models.artifact import Artifact

FacetDict: TypeAlias = Mapping[str, Any]
FacetLike: TypeAlias = Union[FacetDict, BaseModel]

# Common “path-like” / “artifact-like” helper types.
PathLike: TypeAlias = Union[str, Path]
ArtifactRef: TypeAlias = Union["Artifact", PathLike]

HashInput: TypeAlias = Union[PathLike, tuple[str, PathLike]]
HashInputs: TypeAlias = Optional[list[HashInput]]

# Known artifact drivers used by Consist loaders.
DriverLiteral: TypeAlias = Literal[
    "parquet",
    "csv",
    "zarr",
    "json",
    "h5_table",
    "h5",
    "hdf5",
    "other",
]


@runtime_checkable
class HasConsistFacet(Protocol):
    def to_consist_facet(self) -> FacetLike: ...


@runtime_checkable
class HasFacetSchemaVersion(Protocol):
    facet_schema_version: Optional[Union[str, int]]
