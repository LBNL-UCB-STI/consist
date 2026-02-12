from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final, Mapping, Optional, Union

from consist.types import CacheOptions, ExecutionOptions, OutputPolicyOptions


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
    "runtime_kwargs": "execution_options=ExecutionOptions(runtime_kwargs=...)",
    "inject_context": "execution_options=ExecutionOptions(inject_context=...)",
}

LEGACY_POLICY_KWARGS: Final[frozenset[str]] = frozenset(
    LEGACY_POLICY_KWARG_REPLACEMENTS.keys()
)


@dataclass(frozen=True, slots=True)
class ResolvedRunOptions:
    """
    Flattened run options after merging option objects and optional internal overrides.
    """

    cache_mode: Optional[str]
    cache_hydration: Optional[str]
    cache_version: Optional[int]
    cache_epoch: Optional[int]
    validate_cached_outputs: Optional[str]
    output_mismatch: Optional[str]
    output_missing: Optional[str]
    load_inputs: Optional[bool]
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


def raise_unexpected_run_kwargs_error(
    *, api_name: str, kwargs: Mapping[str, Any]
) -> None:
    if not kwargs:
        return

    sorted_keys = sorted(kwargs)
    if len(sorted_keys) == 1:
        raise TypeError(
            f"{api_name} got an unexpected keyword argument '{sorted_keys[0]}'."
        )

    key_list = ", ".join(f"'{key}'" for key in sorted_keys)
    raise TypeError(f"{api_name} got unexpected keyword arguments: {key_list}.")


def _coalesce(direct_value: Any, object_value: Any) -> Any:
    return direct_value if direct_value is not None else object_value


def merge_run_options(
    *,
    cache_options: Optional[CacheOptions] = None,
    output_policy: Optional[OutputPolicyOptions] = None,
    execution_options: Optional[ExecutionOptions] = None,
    cache_mode: Optional[str] = None,
    cache_hydration: Optional[str] = None,
    cache_version: Optional[int] = None,
    cache_epoch: Optional[int] = None,
    validate_cached_outputs: Optional[str] = None,
    output_mismatch: Optional[str] = None,
    output_missing: Optional[str] = None,
    load_inputs: Optional[bool] = None,
    executor: Optional[str] = None,
    container: Optional[Mapping[str, Any]] = None,
    runtime_kwargs: Optional[Mapping[str, Any]] = None,
    inject_context: Optional[Union[bool, str]] = None,
) -> ResolvedRunOptions:
    """
    Resolve grouped options into a flat set of execution controls.

    Optional primitive values are internal-only overrides used by non-public callsites.
    """

    cache_obj = cache_options or CacheOptions()
    output_obj = output_policy or OutputPolicyOptions()
    exec_obj = execution_options or ExecutionOptions()

    return ResolvedRunOptions(
        cache_mode=_coalesce(cache_mode, cache_obj.cache_mode),
        cache_hydration=_coalesce(cache_hydration, cache_obj.cache_hydration),
        cache_version=_coalesce(cache_version, cache_obj.cache_version),
        cache_epoch=_coalesce(cache_epoch, cache_obj.cache_epoch),
        validate_cached_outputs=_coalesce(
            validate_cached_outputs, cache_obj.validate_cached_outputs
        ),
        output_mismatch=_coalesce(output_mismatch, output_obj.output_mismatch),
        output_missing=_coalesce(output_missing, output_obj.output_missing),
        load_inputs=_coalesce(load_inputs, exec_obj.load_inputs),
        executor=_coalesce(executor, exec_obj.executor),
        container=_coalesce(container, exec_obj.container),
        runtime_kwargs=_coalesce(runtime_kwargs, exec_obj.runtime_kwargs),
        inject_context=_coalesce(inject_context, exec_obj.inject_context),
    )
