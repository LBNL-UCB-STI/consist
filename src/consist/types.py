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
    from pydantic import BaseModel
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
    "netcdf",
    "openmatrix",
    "geojson",
    "shapefile",
    "geopackage",
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
    NETCDF = "netcdf"
    OPENMATRIX = "openmatrix"
    GEOJSON = "geojson"
    SHAPEFILE = "shapefile"
    GEOPACKAGE = "geopackage"
    JSON = "json"
    H5_TABLE = "h5_table"
    H5 = "h5"
    HDF5 = "hdf5"
    OTHER = "other"

    @classmethod
    def dataframe_drivers(cls) -> frozenset[str]:
        """Drivers that load as tabular relations."""
        return frozenset(
            {
                cls.PARQUET.value,
                cls.CSV.value,
                cls.H5_TABLE.value,
            }
        )

    @classmethod
    def tabular_drivers(cls) -> frozenset[str]:
        """Drivers that load as tabular data (DataFrame or Series)."""
        return frozenset(
            {
                cls.PARQUET.value,
                cls.CSV.value,
                cls.H5_TABLE.value,
                cls.JSON.value,
            }
        )

    @classmethod
    def zarr_drivers(cls) -> frozenset[str]:
        """Drivers that load as xarray.Dataset."""
        return frozenset({cls.ZARR.value})

    @classmethod
    def array_drivers(cls) -> frozenset[str]:
        """Drivers that load as array/multi-dimensional formats (xarray.Dataset, etc.)."""
        return frozenset({cls.ZARR.value, cls.NETCDF.value, cls.OPENMATRIX.value})

    @classmethod
    def spatial_drivers(cls) -> frozenset[str]:
        """Drivers that load as GeoDataFrame (geospatial formats)."""
        return frozenset({cls.GEOJSON.value, cls.SHAPEFILE.value, cls.GEOPACKAGE.value})

    @classmethod
    def hdf_drivers(cls) -> frozenset[str]:
        """Drivers that load as pandas HDFStore."""
        return frozenset({cls.H5.value, cls.HDF5.value})


@runtime_checkable
class HasConsistFacet(Protocol):
    def to_consist_facet(self) -> FacetLike: ...


@runtime_checkable
class HasFacetSchemaVersion(Protocol):
    facet_schema_version: Optional[Union[str, int]]
