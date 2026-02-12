from __future__ import annotations

import warnings

from consist.core.run_options import merge_run_options
from consist.types import CacheOptions, ExecutionOptions, OutputPolicyOptions


def test_merge_run_options_warns_only_on_conflicting_field_values() -> None:
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        merged = merge_run_options(
            cache_options=CacheOptions(cache_mode="overwrite"),
            cache_mode="reuse",
            warning_prefix="test.run",
            warning_stacklevel=1,
        )

    assert merged.cache_mode == "reuse"
    assert len(captured) == 1
    assert "cache_mode" in str(captured[0].message)


def test_merge_run_options_does_not_warn_for_non_conflicting_mixed_usage() -> None:
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        merged = merge_run_options(
            cache_options=CacheOptions(cache_mode="reuse"),
            output_policy=OutputPolicyOptions(output_missing="error"),
            execution_options=ExecutionOptions(inject_context="ctx"),
            cache_hydration="metadata",
            warning_prefix="test.run",
            warning_stacklevel=1,
        )

    assert merged.cache_mode == "reuse"
    assert merged.cache_hydration == "metadata"
    assert merged.output_missing == "error"
    assert merged.inject_context == "ctx"
    assert captured == []


def test_merge_run_options_does_not_warn_when_duplicate_values_match() -> None:
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        merged = merge_run_options(
            output_policy=OutputPolicyOptions(output_mismatch="warn"),
            output_mismatch="warn",
            warning_prefix="test.run",
            warning_stacklevel=1,
        )

    assert merged.output_mismatch == "warn"
    assert captured == []
