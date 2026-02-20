from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Union,
    cast,
)

from consist.core.decorators import StepDefinition
from consist.core.error_messages import format_problem_cause_fix
from consist.core.step_context import StepContext, format_step_name, resolve_metadata
from consist.types import HashInputs, IdentityInputs

if TYPE_CHECKING:
    from consist.core.config_canonicalization import ConfigAdapter, ConfigPlan


@dataclass(frozen=True)
class ResolvedStepMetadata:
    name: str
    model: str
    description: Optional[str]
    config: Optional[Dict[str, Any]]
    adapter: Optional["ConfigAdapter"]
    identity_inputs: IdentityInputs
    config_plan: Optional["ConfigPlan"]
    tags: Optional[List[str]]
    facet: Optional[Any]
    facet_index: Optional[bool]
    outputs: Optional[List[str]]
    output_paths: Optional[Mapping[str, Any]]
    inputs: Optional[Union[Mapping[str, Any], Iterable[Any]]]
    input_keys: Optional[Union[Iterable[str], str]]
    optional_input_keys: Optional[Union[Iterable[str], str]]
    cache_mode: Optional[str]
    cache_hydration: Optional[str]
    cache_version: Optional[int]
    validate_cached_outputs: Optional[str]
    load_inputs: Optional[bool]
    hash_inputs: HashInputs
    facet_from: Optional[List[str]]
    facet_schema_version: Optional[Union[str, int]]


class MetadataResolver:
    def __init__(
        self,
        *,
        default_name_template: Optional[str] = None,
        allow_template: bool = True,
        apply_step_defaults: bool = True,
    ) -> None:
        self._default_name_template = default_name_template
        self._allow_template = allow_template
        self._apply_step_defaults = apply_step_defaults

    def resolve(
        self,
        *,
        fn: Optional[Any],
        name: Optional[str],
        model: Optional[str],
        description: Optional[str],
        config: Optional[Dict[str, Any]],
        config_plan: Optional["ConfigPlan"],
        inputs: Optional[Union[Mapping[str, Any], Iterable[Any]]],
        input_keys: Optional[Union[Iterable[str], str]],
        optional_input_keys: Optional[Union[Iterable[str], str]],
        tags: Optional[List[str]],
        facet: Optional[Any],
        facet_from: Optional[List[str]],
        facet_schema_version: Optional[Union[str, int]],
        facet_index: Optional[bool],
        hash_inputs: HashInputs,
        year: Optional[int],
        iteration: Optional[int],
        phase: Optional[str],
        stage: Optional[str],
        consist_settings: Optional[Any],
        consist_workspace: Optional[Path],
        consist_state: Optional[Any],
        runtime_kwargs: Optional[Dict[str, Any]],
        outputs: Optional[List[str]],
        output_paths: Optional[Mapping[str, Any]],
        cache_mode: Optional[str],
        cache_hydration: Optional[str],
        cache_version: Optional[int],
        validate_cached_outputs: Optional[str],
        load_inputs: Optional[bool],
        missing_name_error: str,
        adapter: Optional["ConfigAdapter"] = None,
        identity_inputs: IdentityInputs = None,
    ) -> ResolvedStepMetadata:
        func_name = getattr(fn, "__name__", None) if fn is not None else None
        step_def = StepDefinition()
        if self._apply_step_defaults and fn is not None:
            step_def = getattr(fn, "__consist_step__", StepDefinition())

        ctx: StepContext | None = None
        if fn is not None:
            runtime_map = runtime_kwargs or {}
            ctx = StepContext(
                func_name=func_name or "",
                model=model,
                year=year,
                iteration=iteration,
                phase=phase,
                stage=stage,
                consist_settings=consist_settings,
                consist_workspace=consist_workspace,
                consist_state=consist_state,
                runtime_settings=runtime_map.get("settings"),
                runtime_workspace=runtime_map.get("workspace"),
                runtime_state=runtime_map.get("state"),
                runtime_kwargs=runtime_map,
            )

        resolved_model = model
        if self._apply_step_defaults and model is None and ctx is not None:
            resolved_model = cast(Optional[str], resolve_metadata(step_def.model, ctx))
        if ctx is not None:
            ctx.model = resolved_model

        name_template = None
        if self._allow_template:
            if self._apply_step_defaults and step_def.name_template is not None:
                if ctx is None:
                    raise RuntimeError(
                        "Step context unavailable for metadata resolution."
                    )
                name_template = resolve_metadata(step_def.name_template, ctx)
            elif self._default_name_template is not None:
                name_template = self._default_name_template

        if name is not None:
            resolved_name = name
        elif name_template:
            if ctx is None:
                raise RuntimeError("Step context unavailable for name formatting.")
            resolved_name = format_step_name(str(name_template), ctx)
        else:
            resolved_name = func_name

        if resolved_name is None:
            raise ValueError(missing_name_error)

        if resolved_model is None:
            resolved_model = resolved_name
        if ctx is not None:
            ctx.model = resolved_model

        def _resolve_meta(explicit: Any, def_value: Any) -> Any:
            if explicit is not None:
                return explicit
            if def_value is None or ctx is None:
                return def_value
            return resolve_metadata(def_value, ctx)

        resolved_description = _resolve_meta(description, step_def.description)
        resolved_config = _resolve_meta(config, step_def.config)
        resolved_adapter = _resolve_meta(adapter, step_def.adapter)
        resolved_identity_inputs = _resolve_meta(
            identity_inputs, step_def.identity_inputs
        )
        # TODO(v0.1.0): Remove legacy config_plan/hash_inputs resolution path.
        resolved_config_plan_legacy = _resolve_meta(config_plan, step_def.config_plan)
        resolved_tags = _resolve_meta(tags, step_def.tags)
        if resolved_tags is not None:
            resolved_tags = list(resolved_tags)
        resolved_facet = _resolve_meta(facet, step_def.facet)
        resolved_facet_index = _resolve_meta(facet_index, step_def.facet_index)
        resolved_outputs = _resolve_meta(outputs, step_def.outputs)
        if resolved_outputs is not None:
            resolved_outputs = list(resolved_outputs)
        resolved_output_paths = _resolve_meta(output_paths, step_def.output_paths)
        resolved_inputs = _resolve_meta(inputs, step_def.inputs)
        resolved_input_keys = _resolve_meta(input_keys, step_def.input_keys)
        resolved_optional_input_keys = _resolve_meta(
            optional_input_keys, step_def.optional_input_keys
        )
        resolved_cache_mode = _resolve_meta(cache_mode, step_def.cache_mode)
        resolved_cache_hydration = _resolve_meta(
            cache_hydration, step_def.cache_hydration
        )
        resolved_cache_version = _resolve_meta(cache_version, step_def.cache_version)
        resolved_validate_cached_outputs = _resolve_meta(
            validate_cached_outputs, step_def.validate_cached_outputs
        )
        resolved_load_inputs = _resolve_meta(load_inputs, step_def.load_inputs)
        # TODO(v0.1.0): Remove legacy config_plan/hash_inputs resolution path.
        resolved_hash_inputs_legacy = _resolve_meta(hash_inputs, step_def.hash_inputs)
        if resolved_identity_inputs is not None and resolved_hash_inputs_legacy is not None:
            raise ValueError(
                format_problem_cause_fix(
                    problem="Pass either identity_inputs= or hash_inputs=, not both.",
                    cause=(
                        "Both new and legacy identity input options were provided, "
                        "which makes step identity ambiguous."
                    ),
                    fix=(
                        "Use the recommended path with identity_inputs=... and remove "
                        "hash_inputs=."
                    ),
                )
            )
        if resolved_adapter is not None and resolved_config_plan_legacy is not None:
            raise ValueError(
                format_problem_cause_fix(
                    problem="Pass either adapter= or config_plan=, not both.",
                    cause=(
                        "Both identity/config sources were provided, which makes step "
                        "configuration ambiguous."
                    ),
                    fix=(
                        "Use the recommended path with adapter=... and remove "
                        "config_plan=."
                    ),
                )
            )
        resolved_config_plan = (
            None if resolved_adapter is not None else resolved_config_plan_legacy
        )
        resolved_identity = (
            resolved_identity_inputs
            if resolved_identity_inputs is not None
            else resolved_hash_inputs_legacy
        )
        resolved_facet_from = _resolve_meta(facet_from, step_def.facet_from)
        if resolved_facet_from is not None:
            resolved_facet_from = list(resolved_facet_from)
        resolved_facet_schema_version = _resolve_meta(
            facet_schema_version, step_def.facet_schema_version
        )

        return ResolvedStepMetadata(
            name=resolved_name,
            model=resolved_model,
            description=resolved_description,
            config=resolved_config,
            adapter=resolved_adapter,
            identity_inputs=resolved_identity,
            config_plan=resolved_config_plan,
            tags=resolved_tags,
            facet=resolved_facet,
            facet_index=resolved_facet_index,
            outputs=resolved_outputs,
            output_paths=resolved_output_paths,
            inputs=resolved_inputs,
            input_keys=resolved_input_keys,
            optional_input_keys=resolved_optional_input_keys,
            cache_mode=resolved_cache_mode,
            cache_hydration=resolved_cache_hydration,
            cache_version=resolved_cache_version,
            validate_cached_outputs=resolved_validate_cached_outputs,
            load_inputs=resolved_load_inputs,
            # TODO(v0.1.0): Remove hash_inputs alias after legacy API removal.
            hash_inputs=resolved_identity,
            facet_from=resolved_facet_from,
            facet_schema_version=resolved_facet_schema_version,
        )
