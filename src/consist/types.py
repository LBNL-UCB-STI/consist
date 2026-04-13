"""
consist/types.py

Lightweight typing helpers for Consist integrations.

These are intentionally minimal and dependency-tolerant:
- `FacetLike` captures what Consist accepts for `facet=...`.
- `HasConsistFacet` is an optional convention for configs that can produce a facet.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Mapping,
    Optional,
    Protocol,
    Literal,
    TypeAlias,
    Union,
    runtime_checkable,
)
from pydantic import BaseModel

if TYPE_CHECKING:  # pragma: no cover
    from consist.models.artifact import Artifact
    from consist.models.run import RunResult

FacetDict: TypeAlias = Mapping[str, Any]
FacetLike: TypeAlias = Union[FacetDict, BaseModel]

# Common “path-like” / “artifact-like” helper types.
PathLike: TypeAlias = Union[str, Path]
ArtifactRef: TypeAlias = Union["Artifact", PathLike]
# Run/scenario input references additionally allow linking prior step results.
RunInputRef: TypeAlias = Union["Artifact", "RunResult", PathLike]

HashInput: TypeAlias = Union[PathLike, tuple[str, PathLike]]
HashInputs: TypeAlias = Optional[list[HashInput]]
IdentityInputs: TypeAlias = HashInputs
CodeIdentityMode: TypeAlias = Literal[
    "repo_git",
    "callable_module",
    "callable_source",
]
InputBindingMode: TypeAlias = Literal["loaded", "paths", "none"]


@dataclass(frozen=True, slots=True)
class BindingResult:
    """
    Execution-envelope for resolved scenario bindings.

    This object is intentionally narrow: it carries already-decided binding
    inputs for a scenario step and optional debug metadata, but it does not
    perform planning or affect execution semantics on its own.
    """

    inputs: Optional[Mapping[str, RunInputRef]] = None
    input_keys: Optional[Union[Iterable[str], str]] = None
    optional_input_keys: Optional[Union[Iterable[str], str]] = None
    metadata: Optional[Mapping[str, Any]] = None


@dataclass(frozen=True, slots=True)
class CacheOptions:
    """
    Grouped cache behavior options for ``run(...)`` APIs.

    All fields are optional so callers can set only the knobs they need.
    """

    cache_mode: Optional[str] = None
    cache_hydration: Optional[str] = None
    cache_version: Optional[int] = None
    cache_epoch: Optional[int] = None
    validate_cached_outputs: Optional[str] = None
    materialize_cached_outputs_source_root: Optional[PathLike] = None
    code_identity: Optional[CodeIdentityMode] = None
    code_identity_extra_deps: Optional[list[str]] = None


@dataclass(frozen=True, slots=True)
class OutputPolicyOptions:
    """
    Grouped output policy options for ``run(...)`` APIs.
    """

    output_mismatch: Optional[Literal["warn", "error", "ignore"]] = None
    output_missing: Optional[Literal["warn", "error", "ignore"]] = None


@dataclass(frozen=True, slots=True)
class ExecutionOptions:
    """
    Grouped execution options for ``run(...)`` APIs.

    These are runtime controls and do not affect cache identity unless the
    corresponding primitive kwargs already do.

    Attributes
    ----------
    load_inputs : Optional[bool]
        Legacy compatibility switch for automatic input loading. Prefer
        ``input_binding=...`` for new code.
    input_binding : Optional[InputBindingMode]
        Controls what the callable receives for declared named inputs:
        ``"paths"``, ``"loaded"``, or ``"none"``.
    input_paths : Optional[Mapping[str, PathLike]]
        Explicit per-input local destinations used when requested input
        materialization is enabled. Keys must refer to resolved named inputs.
    input_materialization : Optional[Literal["requested"]]
        Enables run-time input staging for the keys listed in ``input_paths``.
        Use this with path-bound runs when a tool expects inputs at specific
        local paths, including on cache hits.
    input_materialization_mode : Optional[Literal["copy"]]
        Strategy for requested input materialization. In v1, only ``"copy"``
        is supported.
    executor : Optional[Literal["python", "container"]]
        Selects the execution backend.
    container : Optional[Mapping[str, Any]]
        Backend-specific container configuration when ``executor="container"``.
    runtime_kwargs : Optional[Mapping[str, Any]]
        Runtime-only keyword arguments forwarded to the callable without
        affecting cache identity.
    inject_context : Optional[Union[bool, str]]
        Whether to inject a ``RunContext`` into the callable.
    """

    load_inputs: Optional[bool] = None
    input_binding: Optional[InputBindingMode] = None
    input_paths: Optional[Mapping[str, PathLike]] = None
    input_materialization: Optional[Literal["requested"]] = None
    input_materialization_mode: Optional[Literal["copy"]] = None
    executor: Optional[Literal["python", "container"]] = None
    container: Optional[Mapping[str, Any]] = None
    runtime_kwargs: Optional[Mapping[str, Any]] = None
    inject_context: Optional[Union[bool, str]] = None


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
