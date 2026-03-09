from __future__ import annotations

import pytest

from consist.core.run_options import (
    merge_run_options,
    raise_legacy_policy_kwargs_error,
    resolve_runtime_kwargs_alias,
)
from consist.types import CacheOptions, ExecutionOptions, OutputPolicyOptions


def test_merge_run_options_merges_options_objects() -> None:
    merged = merge_run_options(
        cache_options=CacheOptions(
            cache_mode="reuse",
            cache_hydration="metadata",
            code_identity="callable_module",
            code_identity_extra_deps=["helpers.py"],
        ),
        output_policy=OutputPolicyOptions(output_missing="error"),
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    assert merged.cache_mode == "reuse"
    assert merged.cache_hydration == "metadata"
    assert merged.code_identity == "callable_module"
    assert merged.code_identity_extra_deps == ["helpers.py"]
    assert merged.output_missing == "error"
    assert merged.input_binding is None
    assert merged.inject_context == "ctx"


def test_merge_run_options_defaults_to_empty_options() -> None:
    merged = merge_run_options()

    assert merged.cache_mode is None
    assert merged.cache_hydration is None
    assert merged.cache_version is None
    assert merged.cache_epoch is None
    assert merged.validate_cached_outputs is None
    assert merged.code_identity is None
    assert merged.code_identity_extra_deps is None
    assert merged.output_mismatch is None
    assert merged.output_missing is None
    assert merged.input_binding is None
    assert merged.load_inputs is None
    assert merged.executor is None
    assert merged.container is None
    assert merged.runtime_kwargs is None
    assert merged.inject_context is None


def test_raise_legacy_policy_kwargs_error_has_migration_guidance() -> None:
    with pytest.raises(
        TypeError, match="consist\\.run no longer accepts legacy policy"
    ):
        raise_legacy_policy_kwargs_error(
            api_name="consist.run",
            kwargs={
                "cache_mode": "reuse",
                "inject_context": True,
            },
        )


def test_resolve_runtime_kwargs_alias_merges_into_execution_options() -> None:
    resolved = resolve_runtime_kwargs_alias(
        api_name="Tracker.run",
        execution_options=ExecutionOptions(
            input_binding="paths", load_inputs=True, inject_context="ctx"
        ),
        runtime_kwargs={"threshold": 2},
    )

    assert resolved is not None
    assert resolved.input_binding == "paths"
    assert resolved.load_inputs is True
    assert resolved.inject_context == "ctx"
    assert resolved.runtime_kwargs == {"threshold": 2}


def test_resolve_runtime_kwargs_alias_rejects_conflicting_values() -> None:
    with pytest.raises(
        ValueError,
        match=("both top-level runtime_kwargs and execution_options\\.runtime_kwargs"),
    ):
        resolve_runtime_kwargs_alias(
            api_name="Tracker.run",
            execution_options=ExecutionOptions(runtime_kwargs={"a": 1}),
            runtime_kwargs={"a": 2},
        )
