from __future__ import annotations

from dataclasses import dataclass
import warnings
from typing import Any, Mapping, Optional, Union

from consist.types import CacheOptions, ExecutionOptions, OutputPolicyOptions


@dataclass(frozen=True, slots=True)
class ResolvedRunOptions:
    """
    Flattened run options after merging direct kwargs with option objects.

    Resolution policy:
    - Direct kwargs win.
    - Option object values fill only missing direct kwargs.
    - Warn only when both sources provide different values for the same field.
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


def _merge_field(
    *,
    field_name: str,
    direct_value: Any,
    object_value: Any,
    object_label: str,
    warning_prefix: str,
    warning_stacklevel: int,
) -> Any:
    if direct_value is not None:
        if object_value is not None and direct_value != object_value:
            warnings.warn(
                (
                    f"{warning_prefix}: both `{field_name}` and "
                    f"`{object_label}.{field_name}` were provided with different "
                    f"values. Using `{field_name}`."
                ),
                UserWarning,
                stacklevel=warning_stacklevel,
            )
        return direct_value
    return object_value


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
    warning_prefix: str = "run",
    warning_stacklevel: int = 2,
) -> ResolvedRunOptions:
    """
    Resolve primitive run kwargs against grouped option objects.

    The return value is normalized and ready to feed into run orchestration code.
    """

    cache_obj = cache_options or CacheOptions()
    output_obj = output_policy or OutputPolicyOptions()
    exec_obj = execution_options or ExecutionOptions()

    return ResolvedRunOptions(
        cache_mode=_merge_field(
            field_name="cache_mode",
            direct_value=cache_mode,
            object_value=cache_obj.cache_mode,
            object_label="cache_options",
            warning_prefix=warning_prefix,
            warning_stacklevel=warning_stacklevel,
        ),
        cache_hydration=_merge_field(
            field_name="cache_hydration",
            direct_value=cache_hydration,
            object_value=cache_obj.cache_hydration,
            object_label="cache_options",
            warning_prefix=warning_prefix,
            warning_stacklevel=warning_stacklevel,
        ),
        cache_version=_merge_field(
            field_name="cache_version",
            direct_value=cache_version,
            object_value=cache_obj.cache_version,
            object_label="cache_options",
            warning_prefix=warning_prefix,
            warning_stacklevel=warning_stacklevel,
        ),
        cache_epoch=_merge_field(
            field_name="cache_epoch",
            direct_value=cache_epoch,
            object_value=cache_obj.cache_epoch,
            object_label="cache_options",
            warning_prefix=warning_prefix,
            warning_stacklevel=warning_stacklevel,
        ),
        validate_cached_outputs=_merge_field(
            field_name="validate_cached_outputs",
            direct_value=validate_cached_outputs,
            object_value=cache_obj.validate_cached_outputs,
            object_label="cache_options",
            warning_prefix=warning_prefix,
            warning_stacklevel=warning_stacklevel,
        ),
        output_mismatch=_merge_field(
            field_name="output_mismatch",
            direct_value=output_mismatch,
            object_value=output_obj.output_mismatch,
            object_label="output_policy",
            warning_prefix=warning_prefix,
            warning_stacklevel=warning_stacklevel,
        ),
        output_missing=_merge_field(
            field_name="output_missing",
            direct_value=output_missing,
            object_value=output_obj.output_missing,
            object_label="output_policy",
            warning_prefix=warning_prefix,
            warning_stacklevel=warning_stacklevel,
        ),
        load_inputs=_merge_field(
            field_name="load_inputs",
            direct_value=load_inputs,
            object_value=exec_obj.load_inputs,
            object_label="execution_options",
            warning_prefix=warning_prefix,
            warning_stacklevel=warning_stacklevel,
        ),
        executor=_merge_field(
            field_name="executor",
            direct_value=executor,
            object_value=exec_obj.executor,
            object_label="execution_options",
            warning_prefix=warning_prefix,
            warning_stacklevel=warning_stacklevel,
        ),
        container=_merge_field(
            field_name="container",
            direct_value=container,
            object_value=exec_obj.container,
            object_label="execution_options",
            warning_prefix=warning_prefix,
            warning_stacklevel=warning_stacklevel,
        ),
        runtime_kwargs=_merge_field(
            field_name="runtime_kwargs",
            direct_value=runtime_kwargs,
            object_value=exec_obj.runtime_kwargs,
            object_label="execution_options",
            warning_prefix=warning_prefix,
            warning_stacklevel=warning_stacklevel,
        ),
        inject_context=_merge_field(
            field_name="inject_context",
            direct_value=inject_context,
            object_value=exec_obj.inject_context,
            object_label="execution_options",
            warning_prefix=warning_prefix,
            warning_stacklevel=warning_stacklevel,
        ),
    )
