"""
Internal helpers for resolving a ``Tracker.run``/``ScenarioContext.run`` call.

This module centralizes option flattening, validation, and metadata resolution so
both execution paths share the same normalization logic.
"""

from __future__ import annotations

from dataclasses import dataclass
import inspect
import os
from pathlib import Path, PurePath
import sys
import types
from typing import (
    Annotated,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Union,
    get_args,
    get_origin,
)
import weakref

import pandas as pd

from consist.core.error_messages import format_problem_cause_fix
from consist.core.metadata_resolver import MetadataResolver, ResolvedStepIdentity
from consist.core.run_options import merge_run_options
from consist.core.settings import ConsistSettings
from consist.models.artifact import Artifact
from consist.models.run import ConsistRecord
from consist.protocols import ArtifactLike
from consist.types import (
    CacheOptions,
    CodeIdentityMode,
    ExecutionOptions,
    FacetLike,
    InputBindingMode,
    IdentityInputs,
    OutputArtifactSpec,
    OutputPolicyOptions,
    OutputPathRef,
    OutputSet,
    PathLike,
    RunCompletionArchivePolicy,
    RunInputRef,
)


@dataclass(frozen=True, slots=True)
class DeclaredOutputSpec:
    """Normalized specification for a single ``outputs={role: path}`` entry."""

    role: str
    raw_value: Union[str, Path]
    is_dir: bool
    artifact_spec: Optional[OutputArtifactSpec] = None


def normalize_declared_outputs_mapping(
    outputs: Mapping[str, Any],
) -> Dict[str, DeclaredOutputSpec]:
    """Validate and normalize an ``outputs={role: path | OutputArtifactSpec}`` mapping."""
    specs: Dict[str, DeclaredOutputSpec] = {}
    for raw_role, raw_target in outputs.items():
        if (
            not isinstance(raw_role, str)
            or not raw_role
            or raw_role.strip() != raw_role
        ):
            raise ValueError(
                format_problem_cause_fix(
                    problem=(
                        "outputs mapping keys must be non-empty strings without "
                        f"surrounding whitespace. Received {raw_role!r}."
                    ),
                    cause="An invalid output role key was supplied.",
                    fix="Use clean string role keys such as outputs={'cleaned': out_path}.",
                )
            )
        artifact_spec: Optional[OutputArtifactSpec] = None
        path_target: Any = raw_target
        if isinstance(raw_target, OutputArtifactSpec):
            artifact_spec = raw_target
            path_target = raw_target.path
        if not isinstance(path_target, (str, Path, os.PathLike)):
            raise TypeError(
                format_problem_cause_fix(
                    problem=(
                        f"outputs[{raw_role!r}] must be a str, Path, or OutputArtifactSpec "
                        f"with a filesystem path/URI. Received {type(raw_target).__name__}."
                    ),
                    cause=(
                        "Symmetric outputs={role: path} mappings require filesystem "
                        "paths, URI strings, or OutputArtifactSpec instances."
                    ),
                    fix=(
                        f"Pass a Path, path string, or OutputArtifactSpec for outputs[{raw_role!r}], e.g. "
                        f"outputs={{'{raw_role}': Path('out.parquet')}}."
                    ),
                )
            )
        raw_str = (
            path_target if isinstance(path_target, str) else os.fspath(path_target)
        )
        is_dir = isinstance(raw_str, str) and raw_str.endswith(("/", "\\"))
        if (
            artifact_spec is not None
            and isinstance(artifact_spec.meta, Mapping)
            and artifact_spec.meta.get("directory_artifact") is True
        ):
            is_dir = True
        normalized_value: Union[str, Path] = (
            path_target
            if isinstance(path_target, (str, Path))
            else Path(os.fspath(path_target))
        )
        specs[raw_role] = DeclaredOutputSpec(
            role=raw_role,
            raw_value=normalized_value,
            is_dir=is_dir,
            artifact_spec=artifact_spec,
        )
    return specs


def resolve_declared_output_target_path(
    spec: DeclaredOutputSpec,
    *,
    default_base_dir: Path,
    resolve_uri_fn: Optional[Callable[[str], Optional[str]]] = None,
) -> Path:
    """Resolve a ``DeclaredOutputSpec`` into an absolute filesystem ``Path``."""
    raw_value = spec.raw_value
    if isinstance(raw_value, str) and "://" in raw_value and resolve_uri_fn is not None:
        resolved_uri = resolve_uri_fn(raw_value)
        target = Path(resolved_uri) if resolved_uri else Path(raw_value)
    else:
        target = Path(raw_value)
    if not target.is_absolute():
        target = default_base_dir / target
    return target.resolve()


def prepare_declared_output_targets(
    specs: Mapping[str, DeclaredOutputSpec],
    *,
    default_base_dir: Path,
    resolve_path_fn: Optional[Callable[[Union[str, Path]], Path]] = None,
    resolve_uri_fn: Optional[Callable[[str], Optional[str]]] = None,
) -> Dict[str, Path]:
    """Resolve and pre-create parent/target directories for ``DeclaredOutputSpec`` entries."""
    targets: Dict[str, Path] = {}
    for role, spec in specs.items():
        if resolve_path_fn is not None:
            target_path = resolve_path_fn(spec.raw_value)
        else:
            target_path = resolve_declared_output_target_path(
                spec,
                default_base_dir=default_base_dir,
                resolve_uri_fn=resolve_uri_fn,
            )
        if spec.is_dir:
            target_path.mkdir(parents=True, exist_ok=True)
        else:
            target_path.parent.mkdir(parents=True, exist_ok=True)
        targets[role] = target_path
    return targets


def build_declared_outputs_identity(
    specs: Mapping[str, DeclaredOutputSpec],
) -> Dict[str, Dict[str, Any]]:
    """
    Build the canonical step-identity payload for ``outputs={role: path}``.

    Includes sorted role keys, directory vs. file semantics (``is_dir``),
    full filename (``name``, preserving multi-dot stems like ``sim.2020.parquet``
    and distinguishing ``a.csv`` from ``b.csv``), and lowercase extensions (``ext``).
    Parent directory is excluded so relocating a workspace does not invalidate hits.
    """
    payload: Dict[str, Dict[str, Any]] = {}
    for role in sorted(specs.keys()):
        spec = specs[role]
        raw_str = str(spec.raw_value).rstrip("/\\")
        path_obj = Path(raw_str)
        ext = "".join(path_obj.suffixes).lower()
        payload[role] = {
            "is_dir": bool(spec.is_dir),
            "name": path_obj.name,
            "ext": ext,
        }
    return payload


ParameterBindingKind = Literal["artifact", "path", "loaded"]

_RESERVED_BINDING_PARAMS = frozenset(
    {"ctx", "_consist_ctx", "runtime_kwargs", "self", "cls"}
)

_ARTIFACT_ANNOTATION_STRINGS = frozenset(
    {
        "Artifact",
        "consist.Artifact",
        "consist.models.artifact.Artifact",
        "ArtifactLike",
        "consist.protocols.ArtifactLike",
    }
)

_PATH_ANNOTATION_STRINGS = frozenset(
    {
        "Path",
        "pathlib.Path",
        "PurePath",
        "pathlib.PurePath",
        "PosixPath",
        "pathlib.PosixPath",
        "WindowsPath",
        "pathlib.WindowsPath",
        "os.PathLike",
        "PathLike",
    }
)

_DATASET_ANNOTATION_STRINGS = frozenset(
    {
        "DataFrame",
        "pd.DataFrame",
        "pandas.DataFrame",
        "pandas.core.frame.DataFrame",
        "GeoDataFrame",
        "gpd.GeoDataFrame",
        "geopandas.GeoDataFrame",
        "geopandas.geodataframe.GeoDataFrame",
        "Dataset",
        "xr.Dataset",
        "xarray.Dataset",
        "xarray.core.dataset.Dataset",
        "Table",
        "pa.Table",
        "pyarrow.Table",
        "pyarrow.lib.Table",
    }
)


def _optional_loaded_dataset_types() -> tuple[type[Any], ...]:
    types_list: list[type[Any]] = [pd.DataFrame]
    if "geopandas" in sys.modules:
        try:
            import geopandas as gpd

            types_list.append(gpd.GeoDataFrame)
        except ImportError:
            pass
    if "xarray" in sys.modules:
        try:
            import xarray as xr

            types_list.append(xr.Dataset)
        except ImportError:
            pass
    if "pyarrow" in sys.modules:
        try:
            import pyarrow as pa

            types_list.append(pa.Table)
        except ImportError:
            pass
    return tuple(types_list)


def _unwrap_string_annotation(raw: str) -> str:
    cleaned = raw.strip()
    for prefix in ("Annotated[", "typing.Annotated["):
        if cleaned.startswith(prefix) and cleaned.endswith("]"):
            inner = cleaned[len(prefix) : -1]
            depth = 0
            for idx, ch in enumerate(inner):
                if ch in "([":
                    depth += 1
                elif ch in ")]":
                    depth -= 1
                elif ch == "," and depth == 0:
                    return _unwrap_string_annotation(inner[:idx])
    for prefix in ("Optional[", "typing.Optional["):
        if cleaned.startswith(prefix) and cleaned.endswith("]"):
            return _unwrap_string_annotation(cleaned[len(prefix) : -1])
    if "|" in cleaned:
        parts = [
            part.strip()
            for part in cleaned.split("|")
            if part.strip() not in {"None", "NoneType"}
        ]
        if len(parts) == 1:
            return _unwrap_string_annotation(parts[0])
    return cleaned


def _classify_annotation(annotation: Any) -> Optional[ParameterBindingKind]:
    if annotation is inspect.Parameter.empty or annotation is None:
        return None
    if isinstance(annotation, str):
        cleaned = _unwrap_string_annotation(annotation)
        if cleaned in _ARTIFACT_ANNOTATION_STRINGS:
            return "artifact"
        if (
            cleaned in _PATH_ANNOTATION_STRINGS
            or cleaned.startswith(("os.PathLike[", "PathLike["))
            and cleaned.endswith("]")
        ):
            return "path"
        if cleaned in _DATASET_ANNOTATION_STRINGS:
            return "loaded"
        return None

    origin = get_origin(annotation)
    if origin is Annotated:
        args = get_args(annotation)
        return _classify_annotation(args[0]) if args else None
    if origin is Union or origin is types.UnionType:
        non_none = [arg for arg in get_args(annotation) if arg is not type(None)]
        if not non_none:
            return None
        kinds = [_classify_annotation(arg) for arg in non_none]
        if kinds[0] is not None and all(k == kinds[0] for k in kinds):
            return kinds[0]
        return None

    if annotation in (Artifact, ArtifactLike):
        return "artifact"
    if isinstance(annotation, type) and issubclass(annotation, Artifact):
        return "artifact"

    if annotation in (Path, PurePath, os.PathLike):
        return "path"
    if isinstance(annotation, type) and issubclass(annotation, (PurePath, os.PathLike)):
        return "path"
    if origin in (Path, PurePath, os.PathLike) or (
        isinstance(origin, type) and issubclass(origin, (PurePath, os.PathLike))
    ):
        return "path"

    dataset_types = _optional_loaded_dataset_types()
    if annotation in dataset_types or (
        isinstance(annotation, type) and issubclass(annotation, dataset_types)
    ):
        return "loaded"
    if origin in dataset_types or (
        isinstance(origin, type) and issubclass(origin, dataset_types)
    ):
        return "loaded"
    return None


def _resolve_param_annotation(fn: Any, raw_annotation: Any) -> Any:
    if not isinstance(raw_annotation, str):
        return raw_annotation
    globalns: Dict[str, Any] = {}
    if isinstance(fn, types.FunctionType):
        globalns = fn.__globals__
    else:
        mod_name = type(fn).__module__ if not callable(fn) else fn.__module__
        mod = sys.modules.get(mod_name)
        if mod is not None:
            globalns = mod.__dict__
    try:
        return eval(raw_annotation, globalns, {})
    except Exception:
        return raw_annotation


_PARAM_BINDING_CACHE: weakref.WeakKeyDictionary[
    Any, tuple[Dict[str, ParameterBindingKind], frozenset[str]]
] = weakref.WeakKeyDictionary()


def _resolve_binding_info(
    fn: Any,
) -> tuple[Dict[str, ParameterBindingKind], frozenset[str]]:
    if fn is None or not callable(fn):
        return {}, frozenset()
    try:
        cached = _PARAM_BINDING_CACHE.get(fn)
        if cached is not None:
            return dict(cached[0]), cached[1]
    except TypeError:
        pass

    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return {}, frozenset()

    result: Dict[str, ParameterBindingKind] = {}
    pyarrow_params: set[str] = set()
    for param_name, param in sig.parameters.items():
        if param_name in _RESERVED_BINDING_PARAMS:
            continue
        if param.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue
        if param.annotation is inspect.Parameter.empty:
            continue
        resolved_ann = _resolve_param_annotation(fn, param.annotation)
        kind = _classify_annotation(resolved_ann)
        if kind is not None:
            result[param_name] = kind
            if kind == "loaded" and _matches_pyarrow_table_annotation(resolved_ann):
                pyarrow_params.add(param_name)

    frozen_pa = frozenset(pyarrow_params)
    try:
        _PARAM_BINDING_CACHE[fn] = (dict(result), frozen_pa)
    except TypeError:
        pass
    return result, frozen_pa


def infer_parameter_binding_kinds(fn: Any) -> Dict[str, ParameterBindingKind]:
    """
    Resolve step parameter binding kinds per parameter.

    Resolves each parameter independently so a closure ``NameError`` on one
    parameter does not abort type resolution for the remaining parameters.
    Unwraps ``Annotated[T, ...]`` and ``Optional[T]`` / ``T | None``, and
    excludes reserved/injected parameters (``ctx``, ``_consist_ctx``,
    ``runtime_kwargs``, ``self``, ``cls``).
    """
    kinds, _ = _resolve_binding_info(fn)
    return kinds


def infer_path_annotated_parameters(fn: Any) -> frozenset[str]:
    """Return parameter names of ``fn`` annotated as ``Path`` / ``os.PathLike``."""
    return frozenset(
        name
        for name, kind in infer_parameter_binding_kinds(fn).items()
        if kind == "path"
    )


def infer_dataframe_annotated_parameters(fn: Any) -> frozenset[str]:
    """Return parameter names of ``fn`` annotated as a loaded dataset type."""
    return frozenset(
        name
        for name, kind in infer_parameter_binding_kinds(fn).items()
        if kind == "loaded"
    )


def _matches_pyarrow_table_annotation(annotation: Any) -> bool:
    if isinstance(annotation, str):
        cleaned = _unwrap_string_annotation(annotation)
        return cleaned in {"Table", "pa.Table", "pyarrow.Table", "pyarrow.lib.Table"}
    origin = get_origin(annotation)
    if origin is Annotated:
        args = get_args(annotation)
        return _matches_pyarrow_table_annotation(args[0]) if args else False
    if origin is Union or origin is types.UnionType:
        non_none = [arg for arg in get_args(annotation) if arg is not type(None)]
        return bool(non_none) and all(
            _matches_pyarrow_table_annotation(arg) for arg in non_none
        )
    if "pyarrow" not in sys.modules:
        return False
    try:
        import pyarrow as pa
    except ImportError:
        return False
    return annotation is pa.Table or (
        isinstance(annotation, type) and issubclass(annotation, pa.Table)
    )


def is_pyarrow_table_annotation(fn: Any, param_name: str) -> bool:
    """Return True if ``param_name`` on ``fn`` is annotated as ``pyarrow.Table``."""
    _, pyarrow_params = _resolve_binding_info(fn)
    return param_name in pyarrow_params


@dataclass(frozen=True, slots=True)
class ResolvedRunInvocation:
    """
    Immutable container for normalized run invocation inputs.

    Instances are produced by :func:`resolve_run_invocation` and consumed by
    both ``Tracker.run`` and ``ScenarioContext.run`` to avoid duplicating
    option/default handling and metadata resolution.

    Attributes
    ----------
    name : str
        Resolved run name after decorator/default/template processing.
    model : str
        Resolved model name to persist on the run record.
    description : Optional[str]
        Resolved run description.
    config : Optional[Dict[str, Any]]
        Resolved configuration payload.
    adapter : Optional[Any]
        Resolved adapter metadata on the recommended path.
    identity_inputs : IdentityInputs
        Resolved identity inputs after recommended/legacy precedence.
    tags : Optional[List[str]]
        Resolved run tags.
    facet : Optional[FacetLike]
        Resolved facet payload.
    facet_from : Optional[List[str]]
        Resolved config keys to project into facets.
    facet_schema_version : Optional[Union[str, int]]
        Resolved facet schema version.
    facet_index : Optional[bool]
        Resolved facet indexing preference.
    inputs : Optional[Union[Mapping[str, RunInputRef], Iterable[RunInputRef]]]
        Resolved input references before tracker-level input materialization.
    input_keys : Optional[Iterable[str] | str]
        Resolved legacy input key spec.
    optional_input_keys : Optional[Iterable[str] | str]
        Resolved legacy optional-input key spec.
    outputs : Optional[List[str]]
        Resolved declared output keys.
    output_paths : Optional[Mapping[str, ArtifactRef]]
        Resolved declared output path mapping.
    output_sets : Optional[Mapping[str, OutputSet]]
        Resolved declared output-set mapping.
    cache_mode : str
        Effective cache mode with defaults applied.
    cache_hydration : Optional[str]
        Effective cache hydration mode.
    cache_version : Optional[int]
        Effective cache version.
    cache_epoch : Optional[int]
        Effective cache epoch override.
    validate_cached_outputs : str
        Effective cached-output validation mode.
    materialize_cached_outputs_source_root : Optional[PathLike]
        Optional archive/mirror root forwarded to cache-hit output hydration.
    code_identity : Optional[CodeIdentityMode]
        Effective code identity mode.
    code_identity_extra_deps : Optional[List[str]]
        Additional dependency paths included in code identity hashing.
    output_mismatch : Literal["warn", "error", "ignore"]
        Effective policy for output count/shape mismatch.
    output_missing : Literal["warn", "error", "ignore"]
        Effective policy for missing expected outputs.
    input_binding : {"loaded", "paths", "none"}
        Effective input binding mode for callable execution.
    load_inputs : Optional[bool]
        Effective legacy input auto-loading preference retained for compatibility.
    input_paths : Optional[Mapping[str, PathLike]]
        Requested destination paths for explicit input staging.
    input_materialization : Optional[Literal["requested"]]
        Requested input-staging policy.
    input_materialization_mode : Optional[Literal["copy"]]
        Requested staging transport mode.
    requested_input_artifact_ids : Optional[Mapping[str, str]]
        Internal strict-binding mapping from callable parameter to tracked
        artifact identifier for requested input staging.
    executor : Literal["python", "container"]
        Effective execution backend.
    container : Optional[Mapping[str, Any]]
        Container execution spec when ``executor="container"``.
    runtime_kwargs : Optional[Dict[str, Any]]
        Runtime-only kwargs forwarded to callable execution.
    inject_context : Union[bool, str]
        Effective context-injection behavior.
    """

    name: str
    model: str
    description: Optional[str]
    config: Optional[Dict[str, Any]]
    adapter: Optional[Any]
    identity_inputs: IdentityInputs
    tags: Optional[List[str]]
    facet: Optional[FacetLike]
    facet_from: Optional[List[str]]
    facet_schema_version: Optional[Union[str, int]]
    facet_index: Optional[bool]
    inputs: Optional[Union[Mapping[str, RunInputRef], Iterable[RunInputRef]]]
    input_keys: Optional[Iterable[str] | str]
    optional_input_keys: Optional[Iterable[str] | str]
    outputs: Optional[List[str]]
    output_paths: Optional[Mapping[str, OutputPathRef]]
    output_sets: Optional[Mapping[str, OutputSet]]
    cache_mode: str
    cache_hydration: Optional[str]
    cache_hydration_failure: Literal["warn", "miss"]
    cache_version: Optional[int]
    cache_epoch: Optional[int]
    validate_cached_outputs: str
    validate_materialized_inputs: bool
    materialize_cached_outputs_source_root: Optional[PathLike]
    code_identity: Optional[CodeIdentityMode]
    code_identity_extra_deps: Optional[List[str]]
    output_mismatch: Literal["warn", "error", "ignore"]
    output_missing: Literal["warn", "error", "ignore"]
    run_completion_archive: Optional[Union[RunCompletionArchivePolicy, Literal[False]]]
    input_binding: InputBindingMode
    load_inputs: Optional[bool]
    input_paths: Optional[Mapping[str, PathLike]]
    input_materialization: Optional[Literal["requested"]]
    input_materialization_mode: Optional[Literal["copy"]]
    requested_input_artifact_ids: Optional[Mapping[str, str]]
    executor: Literal["python", "container"]
    container: Optional[Mapping[str, Any]]
    runtime_kwargs: Optional[Dict[str, Any]]
    inject_context: Union[bool, str]
    declared_output_specs: Optional[Dict[str, DeclaredOutputSpec]] = None
    requested_input_binding: Optional[InputBindingMode] = None

    @property
    def outputs_for_delegation(
        self,
    ) -> Optional[Union[Sequence[str], Mapping[str, OutputPathRef]]]:
        """Return declared outputs in the shape expected when delegating to ``Tracker.run``/``trace``."""
        if self.declared_output_specs is not None:
            return {
                role: (
                    spec.artifact_spec
                    if spec.artifact_spec is not None
                    else spec.raw_value
                )
                for role, spec in self.declared_output_specs.items()
            }
        return self.outputs


def _resolve_input_binding(
    *,
    explicit_input_binding: Optional[InputBindingMode],
    load_inputs: Optional[bool],
    inputs: Optional[Union[Mapping[str, RunInputRef], Iterable[RunInputRef]]],
) -> InputBindingMode:
    if explicit_input_binding is not None and load_inputs is not None:
        raise ValueError(
            format_problem_cause_fix(
                problem=(
                    "ExecutionOptions cannot set both input_binding and load_inputs."
                ),
                cause=(
                    "These options both control how declared inputs bind into "
                    "callable parameters."
                ),
                fix=(
                    "Use input_binding='loaded' or 'paths' for new code, or keep "
                    "only load_inputs=... for legacy behavior."
                ),
            )
        )

    if explicit_input_binding is not None:
        if explicit_input_binding not in {"loaded", "paths", "none"}:
            raise ValueError(
                format_problem_cause_fix(
                    problem=(
                        "execution_options.input_binding must be one of "
                        "'loaded', 'paths', or 'none'. "
                        f"Received {explicit_input_binding!r}."
                    ),
                    cause="An unsupported input binding mode was configured.",
                    fix=(
                        "Choose input_binding='loaded' for object hydration, "
                        "'paths' for explicit file paths, or 'none' to disable "
                        "automatic input binding."
                    ),
                )
            )
        binding = explicit_input_binding
    elif load_inputs is True:
        binding = "loaded"
    elif load_inputs is False:
        binding = "none"
    elif isinstance(inputs, Mapping):
        binding = "loaded"
    else:
        binding = "none"

    return binding


def resolve_run_invocation(
    *,
    fn: Optional[Any],
    name: Optional[str],
    model: Optional[str],
    description: Optional[str],
    config: Optional[Dict[str, Any]],
    adapter: Optional[Any] = None,
    identity_inputs: IdentityInputs = None,
    inputs: Optional[Union[Mapping[str, RunInputRef], Iterable[RunInputRef]]],
    input_keys: Optional[Iterable[str] | str],
    optional_input_keys: Optional[Iterable[str] | str],
    tags: Optional[List[str]],
    facet: Optional[FacetLike],
    facet_from: Optional[List[str]],
    facet_schema_version: Optional[Union[str, int]],
    facet_index: Optional[bool],
    year: Optional[int],
    iteration: Optional[int],
    phase: Optional[str],
    stage: Optional[str],
    outputs: Optional[Union[Sequence[str], Mapping[str, OutputPathRef]]],
    output_paths: Optional[Mapping[str, OutputPathRef]],
    output_sets: Optional[Mapping[str, OutputSet]],
    cache_options: Optional[CacheOptions],
    output_policy: Optional[OutputPolicyOptions],
    execution_options: Optional[ExecutionOptions],
    default_name_template: Optional[str],
    allow_template: Optional[bool],
    apply_step_defaults: Optional[bool],
    consist_settings: ConsistSettings,
    consist_workspace: Path,
    consist_state: Optional[ConsistRecord],
    missing_name_error: str,
    python_missing_fn_error: str,
    allow_python_without_fn: bool = False,
    pre_resolved_step_identity: ResolvedStepIdentity | None = None,
) -> ResolvedRunInvocation:
    """
    Resolve and validate a run invocation into a normalized internal contract.

    This helper is intentionally internal and developer-focused. It centralizes
    three concerns that were previously duplicated across run entry points:

    1. Flatten grouped options objects into primitive execution controls.
    2. Validate cross-field constraints (executor/container/code-identity/policies).
    3. Resolve metadata defaults/templates via ``MetadataResolver``.

    Parameters
    ----------
    fn : Optional[Any]
        Callable to execute for python runs. May be ``None`` for container runs.
    name : Optional[str]
        Explicit run name override.
    model : Optional[str]
        Explicit model name override.
    description : Optional[str]
        Explicit description override.
    config : Optional[Dict[str, Any]]
        Optional configuration payload.
    adapter : Optional[Any]
        Optional config adapter on the recommended path.
    identity_inputs : IdentityInputs
        Optional identity inputs on the recommended path.
    inputs : Optional[Union[Mapping[str, RunInputRef], Iterable[RunInputRef]]]
        Optional input references prior to tracker-level coercion/materialization.
    input_keys : Optional[Iterable[str] | str]
        Legacy input key selector (retained for compatibility).
    optional_input_keys : Optional[Iterable[str] | str]
        Legacy optional input key selector (retained for compatibility).
    tags : Optional[List[str]]
        Optional run tags.
    facet : Optional[FacetLike]
        Optional facet payload.
    facet_from : Optional[List[str]]
        Optional config keys to extract as facets.
    facet_schema_version : Optional[Union[str, int]]
        Optional facet schema version.
    facet_index : Optional[bool]
        Optional facet indexing preference.
    year : Optional[int]
        Optional year metadata.
    iteration : Optional[int]
        Optional iteration metadata.
    phase : Optional[str]
        Optional phase metadata.
    stage : Optional[str]
        Optional stage metadata.
    outputs : Optional[List[str]]
        Optional declared output keys.
    output_paths : Optional[Mapping[str, ArtifactRef]]
        Optional declared output path mapping.
    output_sets : Optional[Mapping[str, OutputSet]]
        Optional declared output-set mapping.
    cache_options : Optional[CacheOptions]
        Grouped cache options object.
    output_policy : Optional[OutputPolicyOptions]
        Grouped output policy object.
    execution_options : Optional[ExecutionOptions]
        Grouped execution options object.
    default_name_template : Optional[str]
        Name template fallback when no explicit name is supplied.
    allow_template : Optional[bool]
        Whether template-based name expansion is permitted.
    apply_step_defaults : Optional[bool]
        Whether ``@define_step`` metadata defaults should be applied.
    consist_settings : ConsistSettings
        Active consist settings used for metadata context.
    consist_workspace : Path
        Active run workspace used for metadata context.
    consist_state : Optional[ConsistRecord]
        Active consist state/header context for metadata resolution.
    missing_name_error : str
        Error message used if no run name can be resolved.
    python_missing_fn_error : str
        Error message used when python execution is selected without a callable.
    allow_python_without_fn : bool, default False
        If True, allow python executor resolution when ``fn`` is omitted.
        This is used by ``trace()`` flows that execute inline block code
        rather than a direct callable.

    Returns
    -------
    ResolvedRunInvocation
        Fully normalized and validated invocation object used by run executors.

    Raises
    ------
    ValueError
        If executor selection or policy values are invalid, or required executor
        prerequisites are missing.
    TypeError
        If ``code_identity_extra_deps`` is not ``list[str]``.

    Notes
    -----
    This function currently combines validation and metadata resolution in one
    step to guarantee parity between tracker and scenario entry points.

    Suggested future improvement:
    split this into two pure helpers, one for option/executor validation and one
    for metadata resolution, then compose them here. That would reduce cognitive
    load and make unit tests more granular while preserving one call site.
    """

    merged_options = merge_run_options(
        cache_options=cache_options,
        output_policy=output_policy,
        execution_options=execution_options,
    )

    cache_mode = merged_options.cache_mode
    cache_hydration = merged_options.cache_hydration
    cache_hydration_failure = merged_options.cache_hydration_failure
    cache_version = merged_options.cache_version
    cache_epoch = merged_options.cache_epoch
    validate_cached_outputs = merged_options.validate_cached_outputs
    validate_materialized_inputs = merged_options.validate_materialized_inputs
    materialize_cached_outputs_source_root = (
        merged_options.materialize_cached_outputs_source_root
    )
    code_identity = merged_options.code_identity
    code_identity_extra_deps = merged_options.code_identity_extra_deps
    output_mismatch = merged_options.output_mismatch
    output_missing = merged_options.output_missing
    run_completion_archive = merged_options.run_completion_archive
    input_binding = merged_options.input_binding
    load_inputs = merged_options.load_inputs
    requested_input_paths = merged_options.input_paths
    requested_input_materialization = merged_options.input_materialization
    requested_input_materialization_mode = merged_options.input_materialization_mode
    requested_input_artifact_ids = merged_options.requested_input_artifact_ids
    executor = merged_options.executor
    container = merged_options.container
    runtime_kwargs = merged_options.runtime_kwargs
    inject_context = merged_options.inject_context

    if executor is None:
        executor = "python"
    if inject_context is None:
        inject_context = False
    if output_mismatch is None:
        output_mismatch = "warn"

    if executor not in {"python", "container"}:
        raise ValueError(
            format_problem_cause_fix(
                problem=(
                    "Tracker.run supports executor='python' or 'container'. "
                    f"Received executor={executor!r}."
                ),
                cause=(
                    "The executor value is outside the supported run backends for the "
                    "recommended path."
                ),
                fix=(
                    "Use execution_options=ExecutionOptions(executor='python') for "
                    "callables or executor='container' with a valid container spec."
                ),
            )
        )
    if code_identity not in {
        None,
        "repo_git",
        "callable_module",
        "callable_source",
    }:
        raise ValueError(
            format_problem_cause_fix(
                problem=(
                    "cache_options.code_identity must be one of "
                    "'repo_git', 'callable_module', or 'callable_source'. "
                    f"Received {code_identity!r}."
                ),
                cause="An unsupported code identity mode was configured.",
                fix=(
                    "Choose a supported mode in CacheOptions(...). For most workflows "
                    "on the recommended path, use 'repo_git'."
                ),
            )
        )
    if code_identity in {"callable_module", "callable_source"} and executor != "python":
        raise ValueError(
            format_problem_cause_fix(
                problem=(
                    "cache_options.code_identity callable modes require "
                    "executor='python'."
                ),
                cause=(
                    "Callable code identity needs a Python callable and cannot be "
                    "computed for container execution."
                ),
                fix=(
                    "Use executor='python' for callable_module/callable_source, or "
                    "switch code_identity to 'repo_git' for container runs."
                ),
            )
        )
    if code_identity_extra_deps is not None:
        if not isinstance(code_identity_extra_deps, list) or not all(
            isinstance(dep, str) for dep in code_identity_extra_deps
        ):
            raise TypeError(
                format_problem_cause_fix(
                    problem=(
                        "cache_options.code_identity_extra_deps must be a list[str]."
                    ),
                    cause=(
                        "The configured dependency list contains a non-string value or "
                        "is not a list."
                    ),
                    fix=(
                        "Pass paths as strings, for example: "
                        "CacheOptions(code_identity_extra_deps=['src/my_module.py'])."
                    ),
                )
            )

    if executor == "container":
        if container is None:
            raise ValueError(
                format_problem_cause_fix(
                    problem="executor='container' requires a container spec.",
                    cause="No container configuration was provided.",
                    fix=(
                        "Set execution_options=ExecutionOptions(executor='container', "
                        "container={...})."
                    ),
                )
            )
        if output_paths is None:
            raise ValueError(
                format_problem_cause_fix(
                    problem="executor='container' requires output_paths.",
                    cause=(
                        "Container runs cannot infer outputs from Python return values."
                    ),
                    fix=(
                        "Declare explicit output paths, for example "
                        "output_paths={'result': 'result.parquet'}."
                    ),
                )
            )
        if outputs is not None:
            raise ValueError(
                format_problem_cause_fix(
                    problem=(
                        "executor='container' does not accept outputs; use "
                        "output_paths."
                    ),
                    cause=(
                        "outputs=[...] is only valid for Python return-value logging."
                    ),
                    fix=(
                        "Replace outputs=[...] with output_paths={key: path} for "
                        "container execution."
                    ),
                )
            )
        if fn is None and name is None:
            raise ValueError(
                format_problem_cause_fix(
                    problem="executor='container' requires name when fn is None.",
                    cause=(
                        "No callable name can be inferred when container runs are "
                        "configured without fn."
                    ),
                    fix="Provide name='step_name' for container execution.",
                )
            )
    elif fn is None and not allow_python_without_fn:
        raise ValueError(python_missing_fn_error)

    runtime_kwargs_dict: Optional[Dict[str, Any]] = (
        dict(runtime_kwargs) if runtime_kwargs is not None else None
    )

    resolved_allow_template = (
        executor == "python" if allow_template is None else allow_template
    )
    resolved_apply_step_defaults = (
        executor == "python" if apply_step_defaults is None else apply_step_defaults
    )

    resolver = MetadataResolver(
        default_name_template=default_name_template,
        allow_template=resolved_allow_template,
        apply_step_defaults=resolved_apply_step_defaults,
    )
    resolved = resolver.resolve(
        fn=fn,
        name=name,
        model=model,
        description=description,
        config=config,
        adapter=adapter,
        identity_inputs=identity_inputs,
        inputs=inputs,
        input_keys=input_keys,
        optional_input_keys=optional_input_keys,
        tags=tags,
        facet=facet,
        facet_from=facet_from,
        facet_schema_version=facet_schema_version,
        facet_index=facet_index,
        year=year,
        iteration=iteration,
        phase=phase,
        stage=stage,
        consist_settings=consist_settings,
        consist_workspace=consist_workspace,
        consist_state=consist_state,
        runtime_kwargs=runtime_kwargs_dict,
        outputs=outputs,
        output_paths=output_paths,
        output_sets=output_sets,
        cache_mode=cache_mode,
        cache_hydration=cache_hydration,
        cache_version=cache_version,
        validate_cached_outputs=validate_cached_outputs,
        input_binding=input_binding,
        load_inputs=load_inputs,
        missing_name_error=missing_name_error,
        pre_resolved_identity=pre_resolved_step_identity,
    )

    resolved_cache_mode = resolved.cache_mode
    resolved_validate_cached_outputs = resolved.validate_cached_outputs
    if resolved_cache_mode is None:
        resolved_cache_mode = "reuse"
    if resolved_validate_cached_outputs is None:
        resolved_validate_cached_outputs = "lazy"

    declared_output_specs: Optional[Dict[str, DeclaredOutputSpec]] = None
    resolved_outputs_list: Optional[List[str]]
    if isinstance(resolved.outputs, Mapping):
        if resolved.output_paths is not None:
            raise ValueError(
                format_problem_cause_fix(
                    problem=(
                        "Cannot specify both outputs={...} mapping and output_paths={...}."
                    ),
                    cause=(
                        "outputs={role: path} is the unified symmetric replacement "
                        "for output_paths."
                    ),
                    fix="Use outputs={role: path} alone and remove output_paths.",
                )
            )
        declared_output_specs = normalize_declared_outputs_mapping(resolved.outputs)
        resolved_outputs_list = list(declared_output_specs.keys())
    else:
        resolved_outputs_list = (
            list(resolved.outputs) if resolved.outputs is not None else None
        )

    if output_missing is None:
        output_missing = "error" if declared_output_specs is not None else "warn"
    if cache_hydration_failure not in {"warn", "miss"}:
        raise ValueError("cache_hydration_failure must be one of: 'warn', 'miss'")
    if cache_hydration_failure == "miss" and resolved.cache_hydration not in {
        "outputs-requested",
        "outputs-all",
    }:
        raise ValueError(
            "cache_hydration_failure='miss' requires "
            "cache_hydration='outputs-requested' or 'outputs-all'"
        )
    resolved_validate_materialized_inputs = (
        False if validate_materialized_inputs is None else validate_materialized_inputs
    )
    if not isinstance(resolved_validate_materialized_inputs, bool):
        raise ValueError("validate_materialized_inputs must be a boolean")
    if (
        resolved_validate_materialized_inputs
        and resolved.cache_hydration != "inputs-missing"
    ):
        raise ValueError(
            "validate_materialized_inputs=True requires "
            "cache_hydration='inputs-missing'"
        )

    if requested_input_materialization is not None:
        if requested_input_materialization != "requested":
            raise ValueError(
                format_problem_cause_fix(
                    problem=(
                        "execution_options.input_materialization must be 'requested'. "
                        f"Received {requested_input_materialization!r}."
                    ),
                    cause="An unsupported input materialization policy was configured.",
                    fix=(
                        "Use execution_options=ExecutionOptions("
                        "input_materialization='requested', input_paths={...})."
                    ),
                )
            )
        if requested_input_paths is None:
            raise ValueError(
                format_problem_cause_fix(
                    problem=(
                        "execution_options.input_materialization='requested' "
                        "requires input_paths."
                    ),
                    cause=("Requested input staging needs explicit destination paths."),
                    fix=(
                        "Provide execution_options=ExecutionOptions("
                        "input_materialization='requested', input_paths={...})."
                    ),
                )
            )
    else:
        requested_input_paths = None

    if (
        requested_input_materialization_mode is not None
        and requested_input_materialization_mode != "copy"
    ):
        raise ValueError(
            format_problem_cause_fix(
                problem=(
                    "execution_options.input_materialization_mode must be 'copy'. "
                    f"Received {requested_input_materialization_mode!r}."
                ),
                cause="Only copy-based requested input staging is supported.",
                fix=(
                    "Use execution_options=ExecutionOptions("
                    "input_materialization_mode='copy')."
                ),
            )
        )

    if output_mismatch not in {"warn", "error", "ignore"}:
        raise ValueError(
            format_problem_cause_fix(
                problem=(
                    "output_mismatch must be one of: 'warn', 'error', 'ignore'. "
                    f"Received {output_mismatch!r}."
                ),
                cause="An unsupported output mismatch policy was configured.",
                fix=(
                    "Set output_policy=OutputPolicyOptions(output_mismatch='warn' | "
                    "'error' | 'ignore')."
                ),
            )
        )
    if output_missing not in {"warn", "error", "ignore"}:
        raise ValueError(
            format_problem_cause_fix(
                problem=(
                    "output_missing must be one of: 'warn', 'error', 'ignore'. "
                    f"Received {output_missing!r}."
                ),
                cause="An unsupported missing-output policy was configured.",
                fix=(
                    "Set output_policy=OutputPolicyOptions(output_missing='warn' | "
                    "'error' | 'ignore')."
                ),
            )
        )
    if (
        run_completion_archive is not None
        and run_completion_archive is not False
        and not isinstance(run_completion_archive, RunCompletionArchivePolicy)
    ):
        raise TypeError(
            format_problem_cause_fix(
                problem=(
                    "output_policy.run_completion_archive must be a "
                    "RunCompletionArchivePolicy instance, False, or None. "
                    f"Received {type(run_completion_archive).__name__}."
                ),
                cause=(
                    "An invalid object was supplied for run-completion output "
                    "archival policy."
                ),
                fix=(
                    "Pass output_policy=OutputPolicyOptions("
                    "run_completion_archive=RunCompletionArchivePolicy("
                    "archive_root=...)) or False to disable scenario defaults."
                ),
            )
        )

    resolved_input_binding = _resolve_input_binding(
        explicit_input_binding=resolved.input_binding,
        load_inputs=resolved.load_inputs,
        inputs=resolved.inputs,
    )

    if (
        requested_input_materialization == "requested"
        and requested_input_paths is not None
        and isinstance(resolved.inputs, Mapping)
    ):
        requested_keys = set(requested_input_paths.keys())
        resolved_keys = {str(key) for key in resolved.inputs.keys()}
        missing_keys = sorted(requested_keys - resolved_keys)
        if missing_keys:
            raise ValueError(
                format_problem_cause_fix(
                    problem=(
                        "execution_options.input_paths contains keys that are not "
                        "present in the resolved inputs: "
                        f"{missing_keys}."
                    ),
                    cause=(
                        "Requested input staging can only target resolved input "
                        "artifact keys."
                    ),
                    fix=(
                        "Use keys from the run inputs mapping, or remove the "
                        "missing entries from execution_options.input_paths."
                    ),
                )
            )

    return ResolvedRunInvocation(
        name=resolved.name,
        model=resolved.model,
        description=resolved.description,
        config=resolved.config,
        adapter=resolved.adapter,
        identity_inputs=resolved.identity_inputs,
        tags=resolved.tags,
        facet=resolved.facet,
        facet_from=resolved.facet_from,
        facet_schema_version=resolved.facet_schema_version,
        facet_index=resolved.facet_index,
        inputs=resolved.inputs,
        input_keys=resolved.input_keys,
        optional_input_keys=resolved.optional_input_keys,
        outputs=resolved_outputs_list,
        output_paths=resolved.output_paths,
        output_sets=(
            dict(resolved.output_sets) if resolved.output_sets is not None else None
        ),
        cache_mode=resolved_cache_mode,
        cache_hydration=resolved.cache_hydration,
        cache_hydration_failure=cache_hydration_failure,
        cache_version=resolved.cache_version,
        cache_epoch=cache_epoch,
        validate_cached_outputs=resolved_validate_cached_outputs,
        validate_materialized_inputs=resolved_validate_materialized_inputs,
        materialize_cached_outputs_source_root=(materialize_cached_outputs_source_root),
        code_identity=code_identity,
        code_identity_extra_deps=(
            list(code_identity_extra_deps)
            if code_identity_extra_deps is not None
            else None
        ),
        output_mismatch=output_mismatch,
        output_missing=output_missing,
        run_completion_archive=run_completion_archive,
        input_binding=resolved_input_binding,
        load_inputs=resolved.load_inputs,
        input_paths=(
            dict(requested_input_paths) if requested_input_paths is not None else None
        ),
        input_materialization=requested_input_materialization,
        input_materialization_mode=requested_input_materialization_mode,
        requested_input_artifact_ids=(
            dict(requested_input_artifact_ids)
            if requested_input_artifact_ids is not None
            else None
        ),
        executor=executor,
        container=container,
        runtime_kwargs=runtime_kwargs_dict,
        inject_context=inject_context,
        declared_output_specs=declared_output_specs,
        requested_input_binding=(
            resolved.input_binding
            if resolved.input_binding is not None
            else (
                "loaded"
                if resolved.load_inputs is True
                else ("none" if resolved.load_inputs is False else None)
            )
        ),
    )
