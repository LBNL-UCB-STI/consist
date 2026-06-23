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
    Callable,
    Sequence,
    TypeAlias,
    Union,
    runtime_checkable,
)
from pydantic import BaseModel
from sqlmodel import SQLModel

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
H5ChildSelectionMode: TypeAlias = Literal["all", "include_only"]
BuiltinSchemaLiteral: TypeAlias = Literal["gtfs"]


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
class OutputSet:
    """
    Declares a logical output artifact represented by multiple member files.

    Use an output set when a step writes one conceptual output as several files,
    such as one CSV per year or one partition per worker. Consist records one
    parent artifact for the logical output and records each discovered file as a
    child artifact. The JSON manifest artifact stores the detailed member list.

    Only ``root`` and ``include`` are required. The ``expected_*`` fields are
    optional guardrails: use them when the run should fail if a known partition
    is missing, but omit them for open-ended bundles where "whatever matched the
    include pattern" is the desired set.

    Parameters
    ----------
    root : str | pathlib.Path
        Directory containing the member files. Relative roots are resolved under
        the run's output directory, so ``root="annual"`` means
        ``<run-output-dir>/annual``.
    include : str | Sequence[str]
        Glob-style filename pattern or patterns for files to include. Matching
        is against each path relative to ``root``. For recursive sets, include
        subdirectories in the pattern, for example ``"**/*.csv"``.
    exclude : str | Sequence[str] | None, optional
        Glob-style pattern or patterns to remove from the included files.
    recursive : bool, default False
        If true, search below ``root`` recursively. If false, only direct child
        files of ``root`` are considered.
    kind : str | None, optional
        Human-facing category for the set, such as ``"annual-partitions"`` or
        ``"diagnostic-bundle"``. This is metadata only.
    schema : type[SQLModel] | None, optional
        Optional schema metadata for the logical parent and members. This does
        not make schema validation happen yet.
    expected_count : int | Callable[[Mapping[str, Any]], int] | None, optional
        Optional completeness check for the number of discovered members.
    expected_members : Sequence[str] | Callable[[Mapping[str, Any]], Sequence[str]] | None, optional
        Optional completeness check for exact relative member names. This is not
        required. It is useful for partitioned outputs where the config already
        tells you the expected files, for example one file per requested year.
    partition_key : str | None, optional
        Optional metadata label for the partition dimension represented by the
        members, such as ``"year"``. Consist does not infer facets from this
        value.
    member_facets : Callable[..., Mapping[str, Any]] | None, optional
        Optional callable returning facets for each member. It receives
        ``(path, relative_path, config)``.
    facet : Mapping[str, Any] | None, optional
        Optional facets for the logical parent artifact.
    validate : {"exists", "manifest", "hashes", "schema"}, default "manifest"
        Validation mode. ``"manifest"`` and ``"exists"`` are implemented in v1.
        ``"manifest"`` records the member manifest and applies any
        ``expected_*`` checks. ``"exists"`` also requires at least one member.
        ``"hashes"`` and ``"schema"`` are reserved for future stricter checks.
    """

    root: PathLike
    include: str | Sequence[str]
    exclude: str | Sequence[str] | None = None
    recursive: bool = False
    kind: str | None = None
    schema: type[SQLModel] | None = None
    expected_count: int | Callable[[Mapping[str, Any]], int] | None = None
    expected_members: (
        Sequence[str] | Callable[[Mapping[str, Any]], Sequence[str]] | None
    ) = None
    partition_key: str | None = None
    member_facets: Callable[..., Mapping[str, Any]] | None = None
    facet: Mapping[str, Any] | None = None
    validate: Literal["exists", "manifest", "hashes", "schema"] = "manifest"


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


@dataclass(frozen=True, slots=True)
class H5ChildSpec:
    """
    Semantic customization for one discovered child artifact inside an HDF5 container.

    The spec is keyed by HDF5 dataset path and only affects how an already-selected
    child artifact is logged; it does not change dataset discovery behavior unless
    paired with ``child_selection="include_only"``.
    """

    key: Optional[str] = None
    description: Optional[str] = None
    facet: Optional[FacetLike] = None
    facet_schema_version: Optional[Union[str, int]] = None
    facet_index: bool = False
    metadata: Optional[Mapping[str, Any]] = None


# Known artifact drivers used by Consist loaders.
DriverLiteral: TypeAlias = Literal[
    "parquet",
    "csv",
    "gtfs",
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
    GTFS = "gtfs"
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
                cls.GTFS.value,
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
                cls.GTFS.value,
                cls.H5_TABLE.value,
                cls.JSON.value,
            }
        )

    @classmethod
    def table_path_drivers(cls) -> frozenset[str]:
        """Tabular drivers that require a child table path."""
        return frozenset({cls.GTFS.value, cls.H5_TABLE.value})

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
