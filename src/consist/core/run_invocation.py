"""
Internal helpers for resolving a ``Tracker.run``/``ScenarioContext.run`` call.

This module centralizes option flattening, validation, and metadata resolution so
both execution paths share the same normalization logic.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Literal, Mapping, Optional, Union, cast

from consist.core.config_canonicalization import ConfigPlan
from consist.core.metadata_resolver import MetadataResolver
from consist.core.run_options import merge_run_options
from consist.core.settings import ConsistSettings
from consist.models.run import ConsistRecord
from consist.types import (
    ArtifactRef,
    CacheOptions,
    CodeIdentityMode,
    ExecutionOptions,
    FacetLike,
    HashInputs,
    OutputPolicyOptions,
    RunInputRef,
)


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
    config_plan : Optional[ConfigPlan]
        Resolved config plan to apply during execution.
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
    hash_inputs : HashInputs
        Resolved input hashing mode.
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
    code_identity : Optional[CodeIdentityMode]
        Effective code identity mode.
    code_identity_extra_deps : Optional[List[str]]
        Additional dependency paths included in code identity hashing.
    output_mismatch : Literal["warn", "error", "ignore"]
        Effective policy for output count/shape mismatch.
    output_missing : Literal["warn", "error", "ignore"]
        Effective policy for missing expected outputs.
    load_inputs : Optional[bool]
        Effective input auto-loading preference.
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
    config_plan: Optional[ConfigPlan]
    tags: Optional[List[str]]
    facet: Optional[FacetLike]
    facet_from: Optional[List[str]]
    facet_schema_version: Optional[Union[str, int]]
    facet_index: Optional[bool]
    hash_inputs: HashInputs
    inputs: Optional[Union[Mapping[str, RunInputRef], Iterable[RunInputRef]]]
    input_keys: Optional[Iterable[str] | str]
    optional_input_keys: Optional[Iterable[str] | str]
    outputs: Optional[List[str]]
    output_paths: Optional[Mapping[str, ArtifactRef]]
    cache_mode: str
    cache_hydration: Optional[str]
    cache_version: Optional[int]
    cache_epoch: Optional[int]
    validate_cached_outputs: str
    code_identity: Optional[CodeIdentityMode]
    code_identity_extra_deps: Optional[List[str]]
    output_mismatch: Literal["warn", "error", "ignore"]
    output_missing: Literal["warn", "error", "ignore"]
    load_inputs: Optional[bool]
    executor: Literal["python", "container"]
    container: Optional[Mapping[str, Any]]
    runtime_kwargs: Optional[Dict[str, Any]]
    inject_context: Union[bool, str]


def resolve_run_invocation(
    *,
    fn: Optional[Any],
    name: Optional[str],
    model: Optional[str],
    description: Optional[str],
    config: Optional[Dict[str, Any]],
    config_plan: Optional[ConfigPlan],
    inputs: Optional[Union[Mapping[str, RunInputRef], Iterable[RunInputRef]]],
    input_keys: Optional[Iterable[str] | str],
    optional_input_keys: Optional[Iterable[str] | str],
    tags: Optional[List[str]],
    facet: Optional[FacetLike],
    facet_from: Optional[List[str]],
    facet_schema_version: Optional[Union[str, int]],
    facet_index: Optional[bool],
    hash_inputs: HashInputs,
    year: Optional[int],
    iteration: Optional[int],
    phase: Optional[str],
    stage: Optional[str],
    outputs: Optional[List[str]],
    output_paths: Optional[Mapping[str, ArtifactRef]],
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
    config_plan : Optional[ConfigPlan]
        Optional canonicalized config plan.
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
    hash_inputs : HashInputs
        Input hashing mode.
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
    cache_version = merged_options.cache_version
    cache_epoch = merged_options.cache_epoch
    validate_cached_outputs = merged_options.validate_cached_outputs
    code_identity = merged_options.code_identity
    code_identity_extra_deps = merged_options.code_identity_extra_deps
    output_mismatch = merged_options.output_mismatch
    output_missing = merged_options.output_missing
    load_inputs = merged_options.load_inputs
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
    if output_missing is None:
        output_missing = "warn"

    if executor not in {"python", "container"}:
        raise ValueError("Tracker.run supports executor='python' or 'container'.")
    if code_identity not in {
        None,
        "repo_git",
        "callable_module",
        "callable_source",
    }:
        raise ValueError(
            "cache_options.code_identity must be one of: "
            "'repo_git', 'callable_module', 'callable_source'"
        )
    if code_identity in {"callable_module", "callable_source"} and executor != "python":
        raise ValueError(
            "cache_options.code_identity callable modes require executor='python'."
        )
    if code_identity_extra_deps is not None:
        if not isinstance(code_identity_extra_deps, list) or not all(
            isinstance(dep, str) for dep in code_identity_extra_deps
        ):
            raise TypeError(
                "cache_options.code_identity_extra_deps must be a list[str]."
            )

    if executor == "container":
        if container is None:
            raise ValueError("executor='container' requires a container spec.")
        if output_paths is None:
            raise ValueError("executor='container' requires output_paths.")
        if outputs is not None:
            raise ValueError(
                "executor='container' does not accept outputs; use output_paths."
            )
        if fn is None and name is None:
            raise ValueError("executor='container' requires name when fn is None.")
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
        config_plan=config_plan,
        inputs=inputs,
        input_keys=input_keys,
        optional_input_keys=optional_input_keys,
        tags=tags,
        facet=facet,
        facet_from=facet_from,
        facet_schema_version=facet_schema_version,
        facet_index=facet_index,
        hash_inputs=hash_inputs,
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
        cache_mode=cache_mode,
        cache_hydration=cache_hydration,
        cache_version=cache_version,
        validate_cached_outputs=validate_cached_outputs,
        load_inputs=load_inputs,
        missing_name_error=missing_name_error,
    )

    resolved_cache_mode = resolved.cache_mode
    resolved_validate_cached_outputs = resolved.validate_cached_outputs
    if resolved_cache_mode is None:
        resolved_cache_mode = "reuse"
    if resolved_validate_cached_outputs is None:
        resolved_validate_cached_outputs = "lazy"

    if output_mismatch not in {"warn", "error", "ignore"}:
        raise ValueError("output_mismatch must be one of: 'warn', 'error', 'ignore'")
    if output_missing not in {"warn", "error", "ignore"}:
        raise ValueError("output_missing must be one of: 'warn', 'error', 'ignore'")

    return ResolvedRunInvocation(
        name=resolved.name,
        model=resolved.model,
        description=resolved.description,
        config=resolved.config,
        config_plan=resolved.config_plan,
        tags=resolved.tags,
        facet=resolved.facet,
        facet_from=resolved.facet_from,
        facet_schema_version=resolved.facet_schema_version,
        facet_index=resolved.facet_index,
        hash_inputs=resolved.hash_inputs,
        inputs=resolved.inputs,
        input_keys=resolved.input_keys,
        optional_input_keys=resolved.optional_input_keys,
        outputs=resolved.outputs,
        output_paths=resolved.output_paths,
        cache_mode=resolved_cache_mode,
        cache_hydration=resolved.cache_hydration,
        cache_version=resolved.cache_version,
        cache_epoch=cache_epoch,
        validate_cached_outputs=resolved_validate_cached_outputs,
        code_identity=code_identity,
        code_identity_extra_deps=(
            list(code_identity_extra_deps)
            if code_identity_extra_deps is not None
            else None
        ),
        output_mismatch=cast(Literal["warn", "error", "ignore"], output_mismatch),
        output_missing=cast(Literal["warn", "error", "ignore"], output_missing),
        load_inputs=resolved.load_inputs,
        executor=cast(Literal["python", "container"], executor),
        container=container,
        runtime_kwargs=runtime_kwargs_dict,
        inject_context=inject_context,
    )
