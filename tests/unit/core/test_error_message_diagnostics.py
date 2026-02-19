from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

from consist.core.config_canonicalization import CanonicalConfig, ConfigPlan
from consist.types import CacheOptions, ExecutionOptions


def _assert_problem_cause_fix(message: str) -> None:
    assert "Problem:" in message
    assert "Cause:" in message
    assert "Fix:" in message


def test_run_rejects_mixed_identity_inputs_and_hash_inputs(tracker) -> None:
    with pytest.raises(ValueError) as excinfo:
        tracker.run(
            fn=lambda: None,
            name="identity_mix",
            identity_inputs=[],
            hash_inputs=[],
        )

    message = str(excinfo.value)
    _assert_problem_cause_fix(message)
    assert "identity_inputs= or hash_inputs=" in message


def test_trace_rejects_mixed_identity_inputs_and_hash_inputs(tracker) -> None:
    with pytest.raises(ValueError) as excinfo:
        with tracker.trace(
            name="trace_identity_mix",
            identity_inputs=[],
            hash_inputs=[],
        ):
            pass

    message = str(excinfo.value)
    _assert_problem_cause_fix(message)
    assert "identity_inputs= or hash_inputs=" in message


def test_run_rejects_mixed_adapter_and_config_plan(tracker, tmp_path: Path) -> None:
    config_root = tmp_path / "cfg"
    config_root.mkdir(parents=True, exist_ok=True)

    plan = ConfigPlan(
        adapter_name="dummy",
        adapter_version="1.0",
        canonical=CanonicalConfig(
            root_dirs=[config_root],
            primary_config=None,
            config_files=[],
            external_files=[],
            content_hash="hash",
        ),
        artifacts=[],
        ingestables=[],
    )

    class DummyAdapter:
        root_dirs = [config_root]

    with pytest.raises(ValueError) as excinfo:
        tracker.run(
            fn=lambda: None,
            name="mixed_adapter_config_plan",
            adapter=DummyAdapter(),
            config_plan=plan,
        )

    message = str(excinfo.value)
    _assert_problem_cause_fix(message)
    assert "adapter= or config_plan=" in message


def test_run_rejects_load_inputs_true_with_non_mapping_inputs(tracker) -> None:
    with pytest.raises(ValueError) as excinfo:
        tracker.run(
            fn=lambda data: None,
            name="bad_load_inputs",
            inputs=[Path("any.csv")],
            execution_options=ExecutionOptions(load_inputs=True),
        )

    message = str(excinfo.value)
    _assert_problem_cause_fix(message)
    assert "load_inputs=True requires inputs to be a dict" in message


def test_run_rejects_invalid_executor_message_shape(tracker) -> None:
    with pytest.raises(ValueError) as excinfo:
        tracker.run(
            fn=lambda: None,
            name="bad_executor",
            execution_options=ExecutionOptions(executor=cast(Any, "invalid")),
        )

    message = str(excinfo.value)
    _assert_problem_cause_fix(message)
    assert "supports executor='python' or 'container'" in message


def test_run_rejects_invalid_code_identity_message_shape(tracker) -> None:
    with pytest.raises(ValueError) as excinfo:
        tracker.run(
            fn=lambda: None,
            name="bad_code_identity",
            cache_options=CacheOptions(code_identity=cast(Any, "invalid")),
        )

    message = str(excinfo.value)
    _assert_problem_cause_fix(message)
    assert "cache_options.code_identity must be one of" in message


def test_run_requires_python_for_callable_code_identity_modes(tracker) -> None:
    with pytest.raises(ValueError) as excinfo:
        tracker.run(
            fn=None,
            name="container_callable_identity",
            output_paths={"out": "out.txt"},
            execution_options=ExecutionOptions(
                executor="container",
                container={"image": "alpine:latest", "command": ["echo", "ok"]},
            ),
            cache_options=CacheOptions(code_identity="callable_source"),
        )

    message = str(excinfo.value)
    _assert_problem_cause_fix(message)
    assert "callable modes require executor='python'" in message


def test_container_executor_requires_output_paths_message_shape(tracker) -> None:
    with pytest.raises(ValueError) as excinfo:
        tracker.run(
            fn=None,
            name="container_missing_outputs",
            execution_options=ExecutionOptions(
                executor="container",
                container={"image": "alpine:latest", "command": ["echo", "ok"]},
            ),
        )

    message = str(excinfo.value)
    _assert_problem_cause_fix(message)
    assert "executor='container' requires output_paths" in message


def test_outputs_requested_cache_hydration_requires_output_paths(tracker) -> None:
    with pytest.raises(ValueError) as excinfo:
        tracker.run(
            fn=lambda: None,
            name="outputs_requested_without_paths",
            cache_options=CacheOptions(cache_hydration="outputs-requested"),
        )

    message = str(excinfo.value)
    _assert_problem_cause_fix(message)
    assert "cache_hydration='outputs-requested' requires output_paths" in message


def test_identity_inputs_missing_path_raises_actionable_error(
    tracker, tmp_path: Path
) -> None:
    missing = tmp_path / "missing_identity.yaml"
    with pytest.raises(ValueError) as excinfo:
        tracker.run(
            fn=lambda: None,
            name="missing_identity_input",
            identity_inputs=[missing],
            cache_options=CacheOptions(cache_mode="overwrite"),
        )

    message = str(excinfo.value)
    _assert_problem_cause_fix(message)
    assert "Failed to compute identity input digests" in message


def test_identity_inputs_rejects_single_string_value(tracker) -> None:
    with pytest.raises(ValueError) as excinfo:
        tracker.run(
            fn=lambda: None,
            name="string_identity_input",
            identity_inputs=cast(Any, "config.yaml"),
        )

    message = str(excinfo.value)
    _assert_problem_cause_fix(message)
    assert "identity_inputs/hash_inputs must be a list of paths" in message


def test_scenario_input_string_error_message_shape(tracker) -> None:
    with tracker.scenario("scenario_missing_input") as sc:
        with pytest.raises(ValueError) as excinfo:
            sc.run(
                fn=lambda: None,
                name="consume",
                inputs={"data": "unknown_key_or_path"},
                execution_options=ExecutionOptions(load_inputs=False),
            )

    message = str(excinfo.value)
    _assert_problem_cause_fix(message)
    assert "Scenario input string did not resolve" in message
