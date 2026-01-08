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


class DriverType(str, Enum):
    """
    Known artifact format handlers for Consist loaders.

    This enum provides a single source of truth for driver names, enabling:
    - Type-safe driver comparison (instead of magic strings)
    - IDE autocomplete when checking driver types
    - Easy iteration over known drivers
    - Validation in Artifact models

    Note: The Artifact model itself uses `str` for SQLModel compatibility,
    but this enum is used in type guards and validation logic.

    Examples
    --------
    Check artifact type:
    ```python
    if artifact.driver == DriverType.PARQUET.value:
        df = load(artifact)  # Type checker knows return is DataFrame
    ```

    Or use type guards (see `is_parquet`, `is_zarr`, etc. in api.py):
    ```python
    if is_parquet_artifact(artifact):
        df = load(artifact)  # Type narrowing works!
    ```
    """

    PARQUET = "parquet"
    CSV = "csv"
    ZARR = "zarr"
    JSON = "json"
    H5_TABLE = "h5_table"
    H5 = "h5"
    HDF5 = "hdf5"
    OTHER = "other"


@runtime_checkable
class HasConsistFacet(Protocol):
    def to_consist_facet(self) -> FacetLike: ...


@runtime_checkable
class HasFacetSchemaVersion(Protocol):
    facet_schema_version: Optional[Union[str, int]]
