from __future__ import annotations

from pathlib import Path

import pytest

from consist.core.run_invocation import resolve_run_invocation
from consist.core.run_options import (
    merge_run_options,
    raise_legacy_policy_kwargs_error,
    resolve_runtime_kwargs_alias,
)
from consist.core.settings import ConsistSettings
from consist.types import (
    CacheOptions,
    ExecutionOptions,
    OutputPolicyOptions,
    RunCompletionArchivePolicy,
)


def test_merge_run_options_merges_options_objects() -> None:
    merged = merge_run_options(
        cache_options=CacheOptions(
            cache_mode="reuse",
            cache_hydration="metadata",
            cache_hydration_failure="warn",
            validate_materialized_inputs=True,
            materialize_cached_outputs_source_root="/tmp/archive",
            code_identity="callable_module",
            code_identity_extra_deps=["helpers.py"],
        ),
        output_policy=OutputPolicyOptions(output_missing="error"),
        execution_options=ExecutionOptions(
            inject_context="ctx",
            input_binding="paths",
            input_paths={"raw": "/tmp/raw.csv"},
            input_materialization="requested",
            input_materialization_mode="copy",
        ),
    )

    assert merged.cache_mode == "reuse"
    assert merged.cache_hydration == "metadata"
    assert merged.cache_hydration_failure == "warn"
    assert merged.validate_materialized_inputs is True
    assert merged.materialize_cached_outputs_source_root == "/tmp/archive"
    assert merged.code_identity == "callable_module"
    assert merged.code_identity_extra_deps == ["helpers.py"]
    assert merged.output_missing == "error"
    assert merged.input_binding == "paths"
    assert merged.input_paths == {"raw": "/tmp/raw.csv"}
    assert merged.input_materialization == "requested"
    assert merged.input_materialization_mode == "copy"
    assert merged.inject_context == "ctx"


def test_merge_run_options_defaults_to_empty_options() -> None:
    merged = merge_run_options()

    assert merged.cache_mode is None
    assert merged.cache_hydration is None
    assert merged.cache_hydration_failure == "warn"
    assert merged.cache_version is None
    assert merged.cache_epoch is None
    assert merged.validate_cached_outputs is None
    assert merged.validate_materialized_inputs is None
    assert merged.materialize_cached_outputs_source_root is None
    assert merged.code_identity is None
    assert merged.code_identity_extra_deps is None
    assert merged.output_mismatch is None
    assert merged.output_missing is None
    assert merged.run_completion_archive is None
    assert merged.input_binding is None
    assert merged.load_inputs is None
    assert merged.input_paths is None
    assert merged.input_materialization is None
    assert merged.input_materialization_mode is None
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
            input_binding="paths",
            load_inputs=True,
            inject_context="ctx",
            input_paths={"raw": "/tmp/raw.csv"},
            input_materialization="requested",
            input_materialization_mode="copy",
        ),
        runtime_kwargs={"threshold": 2},
    )

    assert resolved is not None
    assert resolved.input_binding == "paths"
    assert resolved.load_inputs is True
    assert resolved.inject_context == "ctx"
    assert resolved.input_paths == {"raw": "/tmp/raw.csv"}
    assert resolved.input_materialization == "requested"
    assert resolved.input_materialization_mode == "copy"
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


def test_run_completion_archive_policy_validation_and_round_trip() -> None:
    with pytest.raises(ValueError, match="archive_root must be a non-empty"):
        RunCompletionArchivePolicy(archive_root="   ")

    with pytest.raises(ValueError, match="mode must be 'copy'"):
        RunCompletionArchivePolicy(archive_root="/tmp/archive", mode="move")  # type: ignore[arg-type]

    policy = RunCompletionArchivePolicy(archive_root="  /tmp/archive  ")
    assert policy.archive_root == Path("/tmp/archive")
    merged = merge_run_options(
        output_policy=OutputPolicyOptions(
            output_missing="error",
            run_completion_archive=policy,
        )
    )
    assert merged.run_completion_archive == policy

    invocation = resolve_run_invocation(
        fn=lambda: None,
        name="step",
        model=None,
        description=None,
        config=None,
        adapter=None,
        inputs=None,
        input_keys=None,
        optional_input_keys=None,
        tags=None,
        facet=None,
        facet_from=None,
        facet_schema_version=None,
        facet_index=None,
        identity_inputs=None,
        year=None,
        iteration=None,
        phase=None,
        stage=None,
        outputs=None,
        output_paths=None,
        output_sets=None,
        cache_options=None,
        output_policy=OutputPolicyOptions(
            output_missing="error",
            run_completion_archive=policy,
        ),
        execution_options=None,
        default_name_template=None,
        allow_template=False,
        apply_step_defaults=False,
        consist_settings=ConsistSettings(),
        consist_workspace=Path("/tmp"),
        consist_state=None,
        missing_name_error="missing name",
        python_missing_fn_error="missing fn",
    )
    assert invocation.run_completion_archive == policy

    with pytest.raises(
        TypeError,
        match="run_completion_archive must be a RunCompletionArchivePolicy",
    ):
        resolve_run_invocation(
            fn=lambda: None,
            name="step",
            model=None,
            description=None,
            config=None,
            adapter=None,
            inputs=None,
            input_keys=None,
            optional_input_keys=None,
            tags=None,
            facet=None,
            facet_from=None,
            facet_schema_version=None,
            facet_index=None,
            identity_inputs=None,
            year=None,
            iteration=None,
            phase=None,
            stage=None,
            outputs=None,
            output_paths=None,
            output_sets=None,
            cache_options=None,
            output_policy=OutputPolicyOptions(
                run_completion_archive="/tmp/not_a_policy",  # type: ignore[arg-type]
            ),
            execution_options=None,
            default_name_template=None,
            allow_template=False,
            apply_step_defaults=False,
            consist_settings=ConsistSettings(),
            consist_workspace=Path("/tmp"),
            consist_state=None,
            missing_name_error="missing name",
            python_missing_fn_error="missing fn",
        )
