from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Union

from consist.core.metadata_resolver import MetadataResolver
from consist.core.run_options import merge_run_options
from consist.core.settings import ConsistSettings
from consist.models.run import ConsistRecord
from consist.types import (
    ArtifactRef,
    CacheOptions,
    ExecutionOptions,
    FacetLike,
    IdentityInputs,
    InputBindingMode,
    OutputSet,
    RunInputRef,
)


@dataclass(frozen=True, slots=True)
class StepContract:
    """
    Resolved metadata contract for a ``@define_step`` function.

    This is the planning/introspection counterpart to the run invocation
    metadata used by ``Tracker.run`` and ``ScenarioContext.run``. It intentionally
    carries the declarative fields that orchestrators need before execution
    without owning execution, output registration, or domain-specific coupler
    policy.

    Attributes
    ----------
    name : str
        Resolved step run name after explicit overrides, decorator
        ``name_template``, and default name templates are applied.
    model : str
        Resolved model name that execution would persist on the run record.
        When no explicit model is provided, this falls back to ``name``.
    description : str or None
        Resolved human-readable step description.
    config : dict[str, Any] or None
        Resolved configuration payload that would contribute to run identity.
    adapter : Any or None
        Resolved config adapter, when decorator metadata or an explicit
        override provides one.
    identity_inputs : IdentityInputs
        Resolved hash-only identity inputs. These are paths or labeled paths
        that affect run identity without being logged as artifacts.
    tags : list[str] or None
        Resolved run tags.
    facet : FacetLike or None
        Resolved queryable facet payload.
    facet_from : list[str] or None
        Config keys that execution would project into the facet payload.
    facet_schema_version : str, int, or None
        Optional schema version for the resolved facet payload.
    facet_index : bool or None
        Whether scalar facet values should be indexed for query.
    inputs : mapping or iterable or None
        Resolved direct input references before execution-time materialization
        or coupler lookup.
    input_keys : iterable[str], str, or None
        Required coupler input keys resolved from decorator or call-site
        metadata.
    optional_input_keys : iterable[str], str, or None
        Optional coupler input keys resolved from decorator or call-site
        metadata.
    outputs : list[str] or None
        Resolved Python return-value output keys.
    output_paths : mapping[str, ArtifactRef] or None
        Resolved key-to-path mapping for file-writing steps.
    output_sets : mapping[str, OutputSet] or None
        Resolved logical multi-file outputs.
    cache_mode : str or None
        Resolved cache mode, when explicitly supplied by metadata or options.
    cache_hydration : str or None
        Resolved cache-hit output hydration policy.
    cache_version : int or None
        Resolved per-step cache version.
    validate_cached_outputs : str or None
        Resolved cached-output validation policy.
    input_binding : {"loaded", "paths", "none"} or None
        Resolved callable input binding mode.
    load_inputs : bool or None
        Resolved legacy input-loading preference.
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
    input_keys: Optional[Union[Iterable[str], str]]
    optional_input_keys: Optional[Union[Iterable[str], str]]
    outputs: Optional[List[str]]
    output_paths: Optional[Mapping[str, ArtifactRef]]
    output_sets: Optional[Mapping[str, OutputSet]]
    cache_mode: Optional[str]
    cache_hydration: Optional[str]
    cache_version: Optional[int]
    validate_cached_outputs: Optional[str]
    input_binding: Optional[InputBindingMode]
    load_inputs: Optional[bool]


def resolve_step_contract(
    step: Callable[..., Any],
    *,
    name: Optional[str] = None,
    model: Optional[str] = None,
    description: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    adapter: Optional[Any] = None,
    identity_inputs: IdentityInputs = None,
    inputs: Optional[Union[Mapping[str, RunInputRef], Iterable[RunInputRef]]] = None,
    input_keys: Optional[Union[Iterable[str], str]] = None,
    optional_input_keys: Optional[Union[Iterable[str], str]] = None,
    tags: Optional[List[str]] = None,
    facet: Optional[FacetLike] = None,
    facet_from: Optional[List[str]] = None,
    facet_schema_version: Optional[Union[str, int]] = None,
    facet_index: Optional[bool] = None,
    year: Optional[int] = None,
    iteration: Optional[int] = None,
    phase: Optional[str] = None,
    stage: Optional[str] = None,
    outputs: Optional[List[str]] = None,
    output_paths: Optional[Mapping[str, ArtifactRef]] = None,
    output_sets: Optional[Mapping[str, OutputSet]] = None,
    cache_options: Optional[CacheOptions] = None,
    execution_options: Optional[ExecutionOptions] = None,
    default_name_template: Optional[str] = None,
    allow_template: bool = True,
    apply_step_defaults: bool = True,
    consist_settings: Optional[ConsistSettings] = None,
    consist_workspace: Optional[Path] = None,
    consist_state: Optional[ConsistRecord] = None,
    runtime_kwargs: Optional[Mapping[str, Any]] = None,
    missing_name_error: str = "resolve_step_contract requires a step name.",
) -> StepContract:
    """
    Resolve a decorated step's declarative metadata without executing it.

    Explicit arguments use the same precedence as run execution: call-site
    values override decorator metadata, while callable decorator metadata is
    resolved with a ``StepContext`` populated from the supplied workflow context.

    Parameters
    ----------
    step : Callable[..., Any]
        Step function to inspect. If the function was decorated with
        ``@define_step``, its metadata is applied using the same precedence as
        ``Tracker.run`` and ``ScenarioContext.run``.
    name : str, optional
        Explicit step name override.
    model : str, optional
        Explicit model name override.
    description : str, optional
        Explicit description override.
    config : dict[str, Any], optional
        Explicit configuration payload override.
    adapter : Any, optional
        Explicit config adapter override.
    identity_inputs : IdentityInputs, optional
        Explicit hash-only identity inputs.
    inputs : mapping or iterable, optional
        Direct input references to include in the resolved contract.
    input_keys : iterable[str] or str, optional
        Required coupler input keys.
    optional_input_keys : iterable[str] or str, optional
        Optional coupler input keys.
    tags : list[str], optional
        Explicit run tags.
    facet : FacetLike, optional
        Explicit queryable facet payload.
    facet_from : list[str], optional
        Config keys to project into facets during execution.
    facet_schema_version : str or int, optional
        Facet schema version.
    facet_index : bool, optional
        Whether scalar facet values should be indexed.
    year : int, optional
        Workflow year made available to callable metadata and name templates.
    iteration : int, optional
        Workflow iteration made available to callable metadata and name
        templates.
    phase : str, optional
        Workflow phase made available to callable metadata and name templates.
    stage : str, optional
        Workflow stage made available to callable metadata and name templates.
    outputs : list[str], optional
        Explicit return-value output key override.
    output_paths : mapping[str, ArtifactRef], optional
        Explicit file output path mapping.
    output_sets : mapping[str, OutputSet], optional
        Explicit logical multi-file output mapping.
    cache_options : CacheOptions, optional
        Cache policy options whose fields are resolved into the contract.
    execution_options : ExecutionOptions, optional
        Execution options whose metadata-relevant fields are resolved into the
        contract. Runtime kwargs may be supplied here or through
        ``runtime_kwargs``, but not both.
    default_name_template : str, optional
        Name template used when the step decorator does not provide one.
    allow_template : bool, default True
        Whether name templates are applied.
    apply_step_defaults : bool, default True
        Whether ``@define_step`` metadata defaults are applied.
    consist_settings : ConsistSettings, optional
        Consist settings object exposed to callable metadata as
        ``ctx.consist_settings``.
    consist_workspace : pathlib.Path, optional
        Consist run workspace exposed to callable metadata as
        ``ctx.consist_workspace``.
    consist_state : ConsistRecord, optional
        Active Consist state exposed to callable metadata as
        ``ctx.consist_state``.
    runtime_kwargs : mapping[str, Any], optional
        Workflow runtime objects exposed to callable metadata through
        ``ctx.runtime_kwargs`` and the ``ctx.runtime_*`` convenience fields.
    missing_name_error : str, default "resolve_step_contract requires a step name."
        Error message used when neither an explicit name, function name, nor
        template can resolve a name.

    Returns
    -------
    StepContract
        Fully resolved declarative step metadata.

    Raises
    ------
    ValueError
        If runtime kwargs are provided both directly and through
        ``execution_options.runtime_kwargs``.
    ValueError
        If the step name cannot be resolved.

    Examples
    --------
    >>> import consist
    >>> @consist.define_step(
    ...     outputs=lambda ctx: [f"results_{ctx.year}"],
    ...     name_template="{func_name}__y{year}",
    ... )
    ... def simulate():
    ...     pass
    >>> contract = consist.resolve_step_contract(simulate, year=2030)
    >>> contract.name
    'simulate__y2030'
    >>> contract.outputs
    ['results_2030']
    """

    if runtime_kwargs is not None:
        if (
            execution_options is not None
            and execution_options.runtime_kwargs is not None
        ):
            raise ValueError(
                "resolve_step_contract received runtime_kwargs and "
                "execution_options.runtime_kwargs. Provide runtime kwargs once."
            )
        execution_options = ExecutionOptions(
            input_binding=execution_options.input_binding
            if execution_options
            else None,
            load_inputs=execution_options.load_inputs if execution_options else None,
            input_paths=execution_options.input_paths if execution_options else None,
            input_materialization=(
                execution_options.input_materialization if execution_options else None
            ),
            input_materialization_mode=(
                execution_options.input_materialization_mode
                if execution_options
                else None
            ),
            executor=execution_options.executor if execution_options else None,
            container=execution_options.container if execution_options else None,
            runtime_kwargs=runtime_kwargs,
            inject_context=execution_options.inject_context
            if execution_options
            else None,
        )

    resolved_options = merge_run_options(
        cache_options=cache_options,
        execution_options=execution_options,
    )
    runtime_kwargs_dict = (
        dict(resolved_options.runtime_kwargs)
        if resolved_options.runtime_kwargs is not None
        else None
    )

    resolver = MetadataResolver(
        default_name_template=default_name_template,
        allow_template=allow_template,
        apply_step_defaults=apply_step_defaults,
    )
    resolved = resolver.resolve(
        fn=step,
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
        cache_mode=resolved_options.cache_mode,
        cache_hydration=resolved_options.cache_hydration,
        cache_version=resolved_options.cache_version,
        validate_cached_outputs=resolved_options.validate_cached_outputs,
        input_binding=resolved_options.input_binding,
        load_inputs=resolved_options.load_inputs,
        missing_name_error=missing_name_error,
    )

    return StepContract(
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
        outputs=resolved.outputs,
        output_paths=resolved.output_paths,
        output_sets=resolved.output_sets,
        cache_mode=resolved.cache_mode,
        cache_hydration=resolved.cache_hydration,
        cache_version=resolved.cache_version,
        validate_cached_outputs=resolved.validate_cached_outputs,
        input_binding=resolved.input_binding,
        load_inputs=resolved.load_inputs,
    )


def collect_step_contracts(
    steps: Iterable[Callable[..., Any]],
    **context: Any,
) -> list[StepContract]:
    """
    Resolve a sequence of decorated steps with shared workflow context.

    Parameters
    ----------
    steps : iterable of Callable[..., Any]
        Step functions to inspect.
    **context : Any
        Shared keyword arguments forwarded to ``resolve_step_contract`` for
        each step, such as ``year``, ``iteration``, ``runtime_kwargs``,
        ``cache_options``, or ``default_name_template``.

    Returns
    -------
    list[StepContract]
        Resolved contracts in the same order as ``steps``.

    Examples
    --------
    >>> import consist
    >>> @consist.define_step(outputs=lambda ctx: [f"a_{ctx.year}"])
    ... def first():
    ...     pass
    >>> contracts = consist.collect_step_contracts([first], year=2040)
    >>> contracts[0].outputs
    ['a_2040']
    """

    return [resolve_step_contract(step, **context) for step in steps]
