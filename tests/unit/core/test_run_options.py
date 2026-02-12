from __future__ import annotations

import pytest

from consist.core.run_options import (
    merge_run_options,
    raise_legacy_policy_kwargs_error,
    raise_unexpected_run_kwargs_error,
)
from consist.types import CacheOptions, ExecutionOptions, OutputPolicyOptions


def test_merge_run_options_merges_options_objects() -> None:
    merged = merge_run_options(
        cache_options=CacheOptions(cache_mode="reuse", cache_hydration="metadata"),
        output_policy=OutputPolicyOptions(output_missing="error"),
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    assert merged.cache_mode == "reuse"
    assert merged.cache_hydration == "metadata"
    assert merged.output_missing == "error"
    assert merged.inject_context == "ctx"


def test_merge_run_options_supports_internal_overrides() -> None:
    merged = merge_run_options(
        cache_options=CacheOptions(cache_mode="reuse"),
        cache_mode="overwrite",
        execution_options=ExecutionOptions(load_inputs=True),
        load_inputs=False,
    )

    assert merged.cache_mode == "overwrite"
    assert merged.load_inputs is False


def test_raise_legacy_policy_kwargs_error_has_migration_guidance() -> None:
    with pytest.raises(
        TypeError, match="Tracker\\.run no longer accepts legacy policy"
    ):
        raise_legacy_policy_kwargs_error(
            api_name="Tracker.run",
            kwargs={
                "cache_mode": "reuse",
                "inject_context": True,
            },
        )


def test_raise_unexpected_run_kwargs_error_uses_typeerror_shape() -> None:
    with pytest.raises(
        TypeError,
        match="Tracker\\.run got an unexpected keyword argument 'unknown_flag'",
    ):
        raise_unexpected_run_kwargs_error(
            api_name="Tracker.run",
            kwargs={"unknown_flag": 1},
        )
