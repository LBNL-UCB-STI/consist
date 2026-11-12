"""
Lightweight typing helpers and pure option types for Consist integrations.

This module is a strict Layer-1 leaf:
- Zero imports from ``consist.models.*`` or ``consist.core.*``.
- Zero imports from ``sqlalchemy`` or ``sqlmodel``.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
import inspect
import os
from pathlib import Path
import re
from typing import (
    Any,
    Callable,
    Iterable,
    Literal,
    Mapping,
    Optional,
    Sequence,
    TypeAlias,
    TypeVar,
    Union,
    cast,
)
from pydantic import BaseModel
from typing_extensions import Protocol, runtime_checkable

from consist.protocols import ArtifactLike, RunResultLike

FacetDict: TypeAlias = Mapping[str, Any]
FacetLike: TypeAlias = Union[FacetDict, BaseModel]
SchemaModel: TypeAlias = type[BaseModel]

PathLike: TypeAlias = Union[str, Path]

ArtifactRef: TypeAlias = Union[Any, PathLike]
RunInputRef: TypeAlias = Union[Any, PathLike]

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

VALID_MATERIALIZE_ON_MISSING: frozenset[str] = frozenset({"warn", "raise"})
VALID_MATERIALIZE_DB_FALLBACK: frozenset[str] = frozenset({"never", "if_ingested"})

_OutputT = TypeVar("_OutputT")


def resolve_output_from_mapping(
    outputs: Mapping[str, _OutputT],
    *,
    key: Optional[str] = None,
) -> _OutputT:
    """Resolve one output from an outputs mapping with unambiguous single-output shorthand."""
    output_keys = list(outputs.keys())
    available_keys = ", ".join(repr(item) for item in output_keys) or "<none>"

    if key is not None:
        if key in outputs:
            return outputs[key]
        raise KeyError(
            f"RunResult output key {key!r} was not found. Available keys: {available_keys}."
        )

    if len(output_keys) == 1:
        return outputs[output_keys[0]]

    if not output_keys:
        raise ValueError(
            "RunResult has no outputs. Select an explicit output with "
            "consist.ref(..., key='...')."
        )

    raise ValueError(
        "RunResult has multiple outputs. Select an explicit output with "
        "consist.ref(..., key='...'). "
        f"Available keys: {available_keys}."
    )


def normalize_materialize_output_keys(
    keys: Sequence[str] | None,
    *,
    caller: str,
) -> tuple[str, ...] | None:
    """Normalize key selection for historical output recovery APIs."""
    if keys is None:
        return None
    if isinstance(keys, (str, bytes)):
        raise TypeError(
            "keys must be a sequence of output-key strings, not a single str/bytes value."
        )

    normalized: list[str] = []
    for key in keys:
        if not isinstance(key, str):
            raise TypeError(
                "keys must contain only strings "
                f"(got {type(key).__name__!s} in {caller})."
            )
        normalized.append(key)
    return tuple(normalized)


def validate_materialize_option(
    *,
    name: str,
    value: str,
    allowed: set[str] | frozenset[str],
) -> None:
    """Validate a runtime option shared by hydration/materialization helpers."""
    if value not in allowed:
        allowed_display = ", ".join(repr(item) for item in sorted(allowed))
        raise ValueError(f"{name} must be one of: {allowed_display}")


class _NormalizedFrozenMeta(type):
    """Metaclass that normalizes constructor arguments before frozen slot assignment."""

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        sig = inspect.signature(cls.__init__)
        bound = sig.bind_partial(None, *args, **kwargs)
        bound.apply_defaults()
        raw_kwargs = dict(bound.arguments)
        raw_kwargs.pop("self", None)
        normalized = cls._normalize_fields(raw_kwargs)
        return super().__call__(**normalized)

    def _normalize_fields(cls, kwargs: dict[str, Any]) -> dict[str, Any]:
        return kwargs


@dataclass(frozen=True, slots=True)
class BindingResult:
    """Execution-envelope for resolved scenario bindings."""

    inputs: Optional[Mapping[str, RunInputRef]] = None
    input_keys: Optional[Union[Iterable[str], str]] = None
    optional_input_keys: Optional[Union[Iterable[str], str]] = None
    metadata: Optional[Mapping[str, Any]] = None

    def input_for(self, name: str) -> RunInputRef:
        """Return the bound input reference for ``name``."""
        if self.inputs is None or name not in self.inputs:
            raise KeyError(name)
        return self.inputs[name]

    def artifact_for(self, name: str) -> Any:
        """Return the bound ``Artifact`` for ``name``."""
        value = self.input_for(name)
        if isinstance(value, (str, Path)):
            raise TypeError(
                f"BindingResult input {name!r} is not an Artifact "
                f"(got {type(value).__name__})."
            )
        if isinstance(value, ArtifactLike):
            return value
        if isinstance(value, RunResultLike) and isinstance(value.outputs, Mapping):
            return resolve_output_from_mapping(value.outputs)
        raise TypeError(
            f"BindingResult input {name!r} is not an Artifact "
            f"(got {type(value).__name__})."
        )

    def path_for(self, name: str) -> Path:
        """Return the resolved filesystem ``Path`` for ``name``."""
        value = self.input_for(name)
        if isinstance(value, Path):
            return value
        if isinstance(value, str):
            return Path(value)
        return cast(Path, self.artifact_for(name).path)


@dataclass(frozen=True, slots=True)
class CacheOptions:
    """Configure cache lookup, hydration, validation, and code identity."""

    cache_mode: Optional[str] = None
    cache_hydration: Optional[str] = None
    cache_hydration_failure: Literal["warn", "miss"] = "warn"
    cache_version: Optional[int] = None
    cache_epoch: Optional[int] = None
    validate_cached_outputs: Optional[str] = None
    validate_materialized_inputs: Optional[bool] = None
    materialize_cached_outputs_source_root: Optional[PathLike] = None
    code_identity: Optional[CodeIdentityMode] = None
    code_identity_extra_deps: Optional[list[str]] = None


def _coerce_archive_root_path(raw_root: Any) -> Optional[Path]:
    if raw_root is None:
        return None
    if not isinstance(raw_root, (str, os.PathLike)):
        raise TypeError(
            "RunCompletionArchivePolicy.archive_root must resolve to a path-like value."
        )
    normalized_root = os.fspath(raw_root).strip()
    if not normalized_root:
        raise ValueError(
            "RunCompletionArchivePolicy.archive_root must be a non-empty path."
        )
    return Path(normalized_root).expanduser()


@dataclass(frozen=True, slots=True)
class RunCompletionArchivePolicy(metaclass=_NormalizedFrozenMeta):
    """Synchronous run-completion output archival policy."""

    archive_root: Union[
        str,
        os.PathLike[str],
        Callable[[], Optional[Union[str, os.PathLike[str]]]],
    ]
    mode: Literal["copy"] = "copy"
    prepend_source_mount_root: bool = True

    @classmethod
    def _normalize_fields(cls, kwargs: dict[str, Any]) -> dict[str, Any]:
        mode = kwargs.get("mode", "copy")
        if mode != "copy":
            raise ValueError(
                f"RunCompletionArchivePolicy.mode must be 'copy'; got {mode!r}."
            )
        prepend = kwargs.get("prepend_source_mount_root", True)
        if not isinstance(prepend, bool):
            raise TypeError(
                "RunCompletionArchivePolicy.prepend_source_mount_root must be a bool."
            )
        archive_root = kwargs.get("archive_root")
        if not callable(archive_root):
            resolved = _coerce_archive_root_path(archive_root)
            if resolved is None:
                raise TypeError(
                    "RunCompletionArchivePolicy.archive_root must resolve to a path-like value."
                )
            kwargs["archive_root"] = resolved
        return kwargs

    def resolve_archive_root(self) -> Optional[Path]:
        """Resolve ``archive_root`` at run completion, returning ``None`` when skipped."""
        raw_root = (
            self.archive_root() if callable(self.archive_root) else self.archive_root
        )
        return _coerce_archive_root_path(raw_root)


@dataclass(frozen=True, slots=True)
class OutputPolicyOptions:
    """Grouped output policy options for ``run(...)`` and ``trace(...)`` APIs."""

    output_mismatch: Optional[Literal["warn", "error", "ignore"]] = None
    output_missing: Optional[Literal["warn", "error", "ignore"]] = None
    run_completion_archive: Optional[
        Union[RunCompletionArchivePolicy, Literal[False]]
    ] = None


@dataclass(frozen=True, slots=True)
class OutputArtifactSpec(metaclass=_NormalizedFrozenMeta):
    """Declares one file or directory output with optional artifact logging metadata."""

    path: ArtifactRef
    schema: SchemaModel | None = None
    driver: str | None = None
    facet: FacetLike | None = None
    facet_schema_version: str | int | None = None
    facet_index: bool = False
    profile_file_schema: bool | Literal["if_changed"] | None = None
    file_schema_sample_rows: int | None = None
    meta: Mapping[str, Any] | None = None
    directory: bool = False
    allow_extra_files: bool = False

    @classmethod
    def _normalize_fields(cls, kwargs: dict[str, Any]) -> dict[str, Any]:
        directory = kwargs.get("directory", False)
        allow_extra_files = kwargs.get("allow_extra_files", False)
        if not isinstance(directory, bool):
            raise TypeError("OutputArtifactSpec.directory must be a bool.")
        if not isinstance(allow_extra_files, bool):
            raise TypeError("OutputArtifactSpec.allow_extra_files must be a bool.")

        raw_meta = kwargs.get("meta")
        merged_meta: dict[str, Any] = dict(raw_meta) if raw_meta is not None else {}
        is_directory = directory or (merged_meta.get("directory_artifact") is True)
        allows_extra = allow_extra_files or (
            merged_meta.get("directory_allow_extra_files") is True
        )
        path_val = kwargs.get("path")
        driver_val = kwargs.get("driver")
        is_zarr_output = driver_val == "zarr" or (
            isinstance(path_val, (str, os.PathLike))
            and str(os.fspath(path_val)).rstrip("/\\").endswith(".zarr")
        )
        if allows_extra and not (is_directory or is_zarr_output):
            raise ValueError(
                "OutputArtifactSpec.allow_extra_files=True requires directory=True "
                "or a Zarr directory output."
            )
        if is_directory:
            kwargs["directory"] = True
            merged_meta["directory_artifact"] = True
        if allows_extra:
            kwargs["allow_extra_files"] = True
            merged_meta["directory_allow_extra_files"] = True
        if merged_meta:
            kwargs["meta"] = merged_meta
        return kwargs

    @classmethod
    def for_directory(
        cls,
        path: ArtifactRef,
        *,
        allow_extra_files: bool = False,
        schema: SchemaModel | None = None,
        driver: str | None = None,
        facet: FacetLike | None = None,
        facet_schema_version: str | int | None = None,
        facet_index: bool = False,
        profile_file_schema: bool | Literal["if_changed"] | None = None,
        file_schema_sample_rows: int | None = None,
        meta: Mapping[str, Any] | None = None,
    ) -> "OutputArtifactSpec":
        """Construct a directory-backed ``OutputArtifactSpec`` (`directory=True`)."""
        return cls(
            path=path,
            schema=schema,
            driver=driver,
            facet=facet,
            facet_schema_version=facet_schema_version,
            facet_index=facet_index,
            profile_file_schema=profile_file_schema,
            file_schema_sample_rows=file_schema_sample_rows,
            meta=meta,
            directory=True,
            allow_extra_files=allow_extra_files,
        )


ArtifactSpec = OutputArtifactSpec
OutputPathRef: TypeAlias = Union[ArtifactRef, OutputArtifactSpec]


_SAFE_CAPTURE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_FILENAME_PATTERN_RESERVED_NAMES = {
    "artifact_set",
    "manifest_artifact_id",
    "member_count",
    "output_set_key",
    "output_set_kind",
    "output_set_member",
    "output_set_relative_path",
    "schema_name",
    "total_size_bytes",
}


def _validate_capture_name(name: str) -> None:
    if not _SAFE_CAPTURE_NAME_RE.fullmatch(name):
        raise ValueError(f"capture name must be a safe identifier (got {name!r})")
    if name in _FILENAME_PATTERN_RESERVED_NAMES:
        raise ValueError(f"capture name {name!r} is reserved")


def _capture_signature(capture: "FilenameCapture") -> tuple[str, tuple[str, ...]]:
    if isinstance(capture, IntCapture):
        return ("int", ())
    if isinstance(capture, EnumCapture):
        return ("enum", tuple(sorted(capture.allowed)))
    if isinstance(capture, StringCapture):
        return ("str", (capture.pattern,))
    raise TypeError(f"unsupported capture type: {type(capture)!r}")


@dataclass(frozen=True, slots=True)
class IntCapture:
    """Captures an integer facet from a 1-indexed ``*`` wildcard in a ``FilenamePattern``."""

    name: str
    wildcard: int

    def __post_init__(self) -> None:
        _validate_capture_name(self.name)
        if self.wildcard < 1:
            raise ValueError(f"wildcard must be 1-indexed (got {self.wildcard})")


@dataclass(frozen=True, slots=True)
class EnumCapture(metaclass=_NormalizedFrozenMeta):
    """Captures a string facet constrained to an allowed set of values."""

    name: str
    allowed: frozenset[str]
    wildcard: int

    @classmethod
    def _normalize_fields(cls, kwargs: dict[str, Any]) -> dict[str, Any]:
        name = kwargs["name"]
        wildcard = kwargs["wildcard"]
        _validate_capture_name(name)
        if wildcard < 1:
            raise ValueError(f"wildcard must be 1-indexed (got {wildcard})")
        allowed_values = frozenset(str(value) for value in kwargs["allowed"])
        if not allowed_values:
            raise ValueError("enum capture must define at least one allowed value")
        kwargs["allowed"] = allowed_values
        return kwargs


_DEFAULT_STRING_CAPTURE_PATTERN = r"[A-Za-z0-9_.-]+"


def _validate_string_capture_pattern(pattern: str) -> None:
    if not isinstance(pattern, str) or not pattern:
        raise ValueError("string capture pattern must be a non-empty string")
    try:
        compiled = re.compile(f"^(?:{pattern})$")
    except re.error as exc:
        raise ValueError(f"invalid string capture regex {pattern!r}: {exc}") from exc
    if compiled.groups != 0:
        raise ValueError(
            "string capture pattern must not contain capturing groups; use non-capturing '(?:...)' instead"
        )
    if compiled.fullmatch("") is not None:
        raise ValueError("string capture pattern must not match empty strings")
    if re.search(pattern, "/") or re.search(pattern, "\\"):
        raise ValueError(
            "string capture pattern must not match path separators ('/' or '\\\\')"
        )


@dataclass(frozen=True, slots=True)
class StringCapture:
    """Captures an open string token from a 1-indexed ``*`` wildcard in a ``FilenamePattern``."""

    name: str
    wildcard: int
    pattern: str = _DEFAULT_STRING_CAPTURE_PATTERN

    def __post_init__(self) -> None:
        _validate_capture_name(self.name)
        if self.wildcard < 1:
            raise ValueError(f"wildcard must be 1-indexed (got {self.wildcard})")
        _validate_string_capture_pattern(self.pattern)


FilenameCapture: TypeAlias = Union[IntCapture, EnumCapture, StringCapture]


@dataclass(frozen=True, slots=True)
class FilenamePattern:
    """Glob pattern for an ``OutputSet`` with optional typed wildcard captures."""

    pattern: str
    captures: tuple[FilenameCapture, ...] = ()

    def __post_init__(self) -> None:
        _validate_filename_pattern_glob(self.pattern)
        if self.captures:
            _validate_filename_pattern_captures(self)

    @classmethod
    def glob(cls, pattern: str) -> "FilenamePattern":
        """Construct a ``FilenamePattern`` for ``pattern`` before attaching captures."""
        return cls(pattern=pattern)

    def with_captures(self, *captures: FilenameCapture) -> "FilenamePattern":
        """Return a copy of this pattern with ``captures`` bound to its ``*`` wildcards."""
        return replace(self, captures=tuple(captures))


def _validate_filename_pattern_glob(pattern: str) -> None:
    if not pattern:
        raise ValueError("filename pattern must not be empty")
    if "\\" in pattern:
        raise ValueError(
            "capture-aware filename patterns must use '/' as the path separator"
        )
    if "**" in pattern:
        raise ValueError("capture-aware filename patterns cannot contain '**'")
    if any(token in pattern for token in ("?", "[", "]")):
        raise ValueError(
            "capture-aware filename patterns only support literal text and '*'"
        )


def _validate_filename_pattern_captures(pattern: FilenamePattern) -> None:
    wildcard_count = pattern.pattern.count("*")
    if not pattern.captures:
        raise ValueError("filename patterns must declare at least one capture")
    if wildcard_count != len(pattern.captures):
        raise ValueError(
            f"filename pattern has {wildcard_count} wildcard(s) but "
            f"{len(pattern.captures)} capture(s)"
        )

    seen_signatures: dict[str, tuple[str, tuple[str, ...]]] = {}
    seen_wildcards: set[int] = set()
    for capture in pattern.captures:
        signature = _capture_signature(capture)
        previous_signature = seen_signatures.get(capture.name)
        if previous_signature is not None and previous_signature != signature:
            raise ValueError(f"incompatible repeated capture {capture.name!r}")
        if capture.wildcard in seen_wildcards:
            raise ValueError(
                f"duplicate wildcard index {capture.wildcard} is not allowed"
            )
        if capture.wildcard < 1 or capture.wildcard > wildcard_count:
            raise ValueError(
                f"wildcard index {capture.wildcard} is out of range for pattern "
                f"with {wildcard_count} wildcard(s)"
            )
        seen_signatures[capture.name] = signature
        seen_wildcards.add(capture.wildcard)

    if seen_wildcards != set(range(1, wildcard_count + 1)):
        raise ValueError("every wildcard must be bound exactly once")


@dataclass(frozen=True, slots=True)
class OutputSet:
    """Declares a logical output artifact represented by multiple member files."""

    root: PathLike
    include: str | Sequence[str] | FilenamePattern
    exclude: str | Sequence[str] | None = None
    recursive: bool = False
    kind: str | None = None
    schema: SchemaModel | None = None
    profile_file_schema: bool | Literal["if_changed"] | None = None
    file_schema_sample_rows: int | None = None
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
    """Grouped execution options for ``run(...)`` APIs."""

    load_inputs: Optional[bool] = None
    input_binding: Optional[InputBindingMode] = None
    input_paths: Optional[Mapping[str, PathLike]] = None
    input_materialization: Optional[Literal["requested"]] = None
    input_materialization_mode: Optional[Literal["copy"]] = None
    requested_input_artifact_ids: Optional[Mapping[str, str]] = None
    executor: Optional[Literal["python", "container"]] = None
    container: Optional[Mapping[str, Any]] = None
    runtime_kwargs: Optional[Mapping[str, Any]] = None
    inject_context: Optional[Union[bool, str]] = None


@dataclass(frozen=True, slots=True)
class H5ChildSpec:
    """Semantic customization for one discovered child artifact inside an HDF5 container."""

    key: Optional[str] = None
    description: Optional[str] = None
    facet: Optional[FacetLike] = None
    facet_schema_version: Optional[Union[str, int]] = None
    facet_index: bool = False
    metadata: Optional[Mapping[str, Any]] = None


DriverLiteral: TypeAlias = Literal[
    "parquet",
    "csv",
    "gtfs",
    "zarr",
    "netcdf",
    "openmatrix",
    "geojson",
    "geoparquet",
    "shapefile",
    "geopackage",
    "json",
    "h5_table",
    "h5",
    "hdf5",
    "other",
]


class DriverType(str, Enum):
    """Known artifact format handlers for Consist loaders."""

    PARQUET = "parquet"
    CSV = "csv"
    GTFS = "gtfs"
    ZARR = "zarr"
    NETCDF = "netcdf"
    OPENMATRIX = "openmatrix"
    GEOJSON = "geojson"
    GEOPARQUET = "geoparquet"
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
        """Tabular drivers that require a child member path."""
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
        return frozenset(
            {
                cls.GEOJSON.value,
                cls.GEOPARQUET.value,
                cls.SHAPEFILE.value,
                cls.GEOPACKAGE.value,
            }
        )

    @classmethod
    def hdf_drivers(cls) -> frozenset[str]:
        """Drivers that load as pandas HDFStore."""
        return frozenset({cls.H5.value, cls.HDF5.value})


@dataclass(frozen=True, slots=True)
class ParameterizedSQL(metaclass=_NormalizedFrozenMeta):
    """Immutable parameterized read-only DuckDB SQL statement and bound parameters."""

    sql: str
    params: tuple[Any, ...] = ()

    @classmethod
    def _normalize_fields(cls, kwargs: dict[str, Any]) -> dict[str, Any]:
        sql = kwargs["sql"]
        params = kwargs.get("params", ())
        if not isinstance(sql, str):
            raise TypeError(f"sql must be a str, got {type(sql).__name__}")
        kwargs["params"] = params if isinstance(params, tuple) else tuple(params)
        return kwargs


@runtime_checkable
class HasConsistFacet(Protocol):
    def to_consist_facet(self) -> FacetLike: ...


@runtime_checkable
class HasFacetSchemaVersion(Protocol):
    facet_schema_version: Optional[Union[str, int]]
