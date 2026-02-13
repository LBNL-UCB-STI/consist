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
    Flattened run options derived from option objects.
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
        cache_version=cache_obj.cache_version,
        cache_epoch=cache_obj.cache_epoch,
        validate_cached_outputs=cache_obj.validate_cached_outputs,
        output_mismatch=output_obj.output_mismatch,
        output_missing=output_obj.output_missing,
        load_inputs=exec_obj.load_inputs,
        executor=exec_obj.executor,
        container=exec_obj.container,
        runtime_kwargs=exec_obj.runtime_kwargs,
        inject_context=exec_obj.inject_context,
    )
