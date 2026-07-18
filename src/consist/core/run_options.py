from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final, Literal, Mapping, Optional, Union

from consist.core.error_messages import format_problem_cause_fix
from consist.types import (
    CacheOptions,
    CodeIdentityMode,
    ExecutionOptions,
    InputBindingMode,
    OutputPolicyOptions,
    PathLike,
)


LEGACY_POLICY_KWARG_REPLACEMENTS: Final[dict[str, str]] = {
    "cache_mode": "cache_options=CacheOptions(cache_mode=...)",
    "cache_hydration": "cache_options=CacheOptions(cache_hydration=...)",
    "cache_version": "cache_options=CacheOptions(cache_version=...)",
    "cache_epoch": "cache_options=CacheOptions(cache_epoch=...)",
    "validate_cached_outputs": (
        "cache_options=CacheOptions(validate_cached_outputs=...)"
    ),
    "output_mismatch": "output_policy=OutputPolicyOptions(output_mismatch=...)",
    "output_missing": "output_policy=OutputPolicyOptions(output_missing=...)",
    "load_inputs": "execution_options=ExecutionOptions(load_inputs=...)",
    "executor": "execution_options=ExecutionOptions(executor=...)",
    "container": "execution_options=ExecutionOptions(container=...)",
    "input_binding": "execution_options=ExecutionOptions(input_binding=...)",
    "input_paths": "execution_options=ExecutionOptions(input_paths=...)",
    "input_materialization": (
        "execution_options=ExecutionOptions(input_materialization=...)"
    ),
    "input_materialization_mode": (
        "execution_options=ExecutionOptions(input_materialization_mode=...)"
    ),
    "runtime_kwargs": "execution_options=ExecutionOptions(runtime_kwargs=...)",
    "inject_context": "execution_options=ExecutionOptions(inject_context=...)",
}

LEGACY_POLICY_KWARGS: Final[frozenset[str]] = frozenset(
    LEGACY_POLICY_KWARG_REPLACEMENTS.keys()
)


@dataclass(frozen=True, slots=True)
class ResolvedRunOptions:
    """Flatten cache, output-policy, and execution options for one run.

    This immutable value preserves unset options as ``None`` so downstream
    invocation code can apply active defaults consistently.

    Attributes
    ----------
    cache_mode : str | None
        Requested cache lookup policy.
    cache_hydration : str | None
        Requested cache-hit hydration policy.
    cache_hydration_failure : {"warn", "miss"}
        Requested-output hydration failure policy.
    cache_version : int | None
        Optional cache-version candidate discriminator.
    cache_epoch : int | None
        Optional cache-epoch candidate discriminator.
    validate_cached_outputs : str | None
        Cached-output validation policy.
    validate_materialized_inputs : bool | None
        Whether materialized inputs must be validated.
    materialize_cached_outputs_source_root : PathLike | None
        Source-root override for cached-output recovery.
    code_identity : CodeIdentityMode | None
        Callable code-identity policy.
    code_identity_extra_deps : list[str] | None
        Additional dependency paths included in code identity.
    output_mismatch : str | None
        Policy applied when declared outputs differ from returned outputs.
    output_missing : str | None
        Policy applied when a declared output is absent after execution.
    input_binding : InputBindingMode | None
        Input-to-callable binding policy.
    load_inputs : bool | None
        Whether to load input artifacts before invoking the callable.
    input_paths : Mapping[str, Any] | None
        Explicit input artifact paths for the invocation.
    input_materialization : {"requested"} | None
        Input-materialization selection policy.
    input_materialization_mode : {"copy"} | None
        Input-materialization transfer mode.
    requested_input_artifact_ids : Mapping[str, str] | None
        Internal parameter-to-artifact mapping for strict input staging.
    strict_binding_identity : str | None
        Internal immutable strict-binding cache discriminator.
    strict_binding_json : str | None
        Internal canonical binding evidence for lifecycle persistence.
    executor : str | None
        Requested execution backend.
    container : Mapping[str, Any] | None
        Container execution configuration.
    runtime_kwargs : Mapping[str, Any] | None
        Additional runtime keyword arguments for the callable.
    inject_context : bool | str | None
        Whether and how to inject Consist run context into the callable.
    """

    cache_mode: Optional[str]
    cache_hydration: Optional[str]
    cache_hydration_failure: Literal["warn", "miss"]
    cache_version: Optional[int]
    cache_epoch: Optional[int]
    validate_cached_outputs: Optional[str]
    validate_materialized_inputs: Optional[bool]
    materialize_cached_outputs_source_root: Optional[PathLike]
    code_identity: Optional[CodeIdentityMode]
    code_identity_extra_deps: Optional[list[str]]
    output_mismatch: Optional[str]
    output_missing: Optional[str]
    input_binding: Optional[InputBindingMode]
    load_inputs: Optional[bool]
    input_paths: Optional[Mapping[str, Any]]
    input_materialization: Optional[Literal["requested"]]
    input_materialization_mode: Optional[Literal["copy"]]
    requested_input_artifact_ids: Optional[Mapping[str, str]]
    strict_binding_identity: Optional[str]
    strict_binding_json: Optional[str]
    executor: Optional[str]
    container: Optional[Mapping[str, Any]]
    runtime_kwargs: Optional[Mapping[str, Any]]
    inject_context: Optional[Union[bool, str]]


def raise_legacy_policy_kwargs_error(
    *, api_name: str, kwargs: Mapping[str, Any]
) -> None:
    """
    Fail fast when removed direct policy kwargs are used on public run APIs.
    """
    legacy_keys = sorted(key for key in kwargs if key in LEGACY_POLICY_KWARGS)
    if not legacy_keys:
        return

    quoted_keys = ", ".join(f"`{key}`" for key in legacy_keys)
    guidance = "; ".join(
        f"`{key}` -> `{LEGACY_POLICY_KWARG_REPLACEMENTS[key]}`" for key in legacy_keys
    )
    plural = "kwarg" if len(legacy_keys) == 1 else "kwargs"
    raise TypeError(
        f"{api_name} no longer accepts legacy policy {plural}: {quoted_keys}. "
        f"Use options objects instead: {guidance}."
    )


def resolve_runtime_kwargs_alias(
    *,
    api_name: str,
    execution_options: Optional[ExecutionOptions] = None,
    runtime_kwargs: Optional[Mapping[str, Any]] = None,
) -> Optional[ExecutionOptions]:
    """
    Normalize top-level ``runtime_kwargs`` into ``ExecutionOptions``.
    """
    if runtime_kwargs is None:
        return execution_options

    if execution_options is not None and execution_options.runtime_kwargs is not None:
        raise ValueError(
            format_problem_cause_fix(
                problem=(
                    f"{api_name} received both top-level runtime_kwargs and "
                    "execution_options.runtime_kwargs."
                ),
                cause=(
                    "Runtime kwargs were provided in two places, making intent "
                    "ambiguous."
                ),
                fix=(
                    "Provide runtime kwargs in exactly one place: either "
                    "runtime_kwargs={...} or "
                    "execution_options=ExecutionOptions(runtime_kwargs={...})."
                ),
            )
        )

    if execution_options is None:
        return ExecutionOptions(runtime_kwargs=runtime_kwargs)

    return ExecutionOptions(
        input_binding=execution_options.input_binding,
        load_inputs=execution_options.load_inputs,
        input_paths=execution_options.input_paths,
        input_materialization=execution_options.input_materialization,
        input_materialization_mode=execution_options.input_materialization_mode,
        requested_input_artifact_ids=execution_options.requested_input_artifact_ids,
        strict_binding_identity=execution_options.strict_binding_identity,
        strict_binding_json=execution_options.strict_binding_json,
        executor=execution_options.executor,
        container=execution_options.container,
        runtime_kwargs=runtime_kwargs,
        inject_context=execution_options.inject_context,
    )


def merge_run_options(
    *,
    cache_options: Optional[CacheOptions] = None,
    output_policy: Optional[OutputPolicyOptions] = None,
    execution_options: Optional[ExecutionOptions] = None,
) -> ResolvedRunOptions:
    """
    Resolve grouped options into a flat set of execution controls.
    """

    cache_obj = cache_options or CacheOptions()
    output_obj = output_policy or OutputPolicyOptions()
    exec_obj = execution_options or ExecutionOptions()

    return ResolvedRunOptions(
        cache_mode=cache_obj.cache_mode,
        cache_hydration=cache_obj.cache_hydration,
        cache_hydration_failure=cache_obj.cache_hydration_failure,
        cache_version=cache_obj.cache_version,
        cache_epoch=cache_obj.cache_epoch,
        validate_cached_outputs=cache_obj.validate_cached_outputs,
        validate_materialized_inputs=cache_obj.validate_materialized_inputs,
        materialize_cached_outputs_source_root=(
            cache_obj.materialize_cached_outputs_source_root
        ),
        code_identity=cache_obj.code_identity,
        code_identity_extra_deps=cache_obj.code_identity_extra_deps,
        output_mismatch=output_obj.output_mismatch,
        output_missing=output_obj.output_missing,
        input_binding=exec_obj.input_binding,
        load_inputs=exec_obj.load_inputs,
        input_paths=exec_obj.input_paths,
        input_materialization=exec_obj.input_materialization,
        input_materialization_mode=exec_obj.input_materialization_mode,
        requested_input_artifact_ids=exec_obj.requested_input_artifact_ids,
        strict_binding_identity=exec_obj.strict_binding_identity,
        strict_binding_json=exec_obj.strict_binding_json,
        executor=exec_obj.executor,
        container=exec_obj.container,
        runtime_kwargs=exec_obj.runtime_kwargs,
        inject_context=exec_obj.inject_context,
    )
