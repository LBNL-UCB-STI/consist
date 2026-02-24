import pytest

import consist
from consist.types import ExecutionOptions


def test_require_runtime_kwargs_enforces_missing(tracker):
    @consist.require_runtime_kwargs("settings")
    def run_step(settings):
        return None

    with pytest.raises(ValueError, match="Missing runtime_kwargs"):
        tracker.run(fn=run_step, name="missing_runtime_kwargs")


def test_require_runtime_kwargs_allows_present(tracker):
    @consist.require_runtime_kwargs("settings", "state")
    def run_step(settings, state):
        return None

    result = tracker.run(
        fn=run_step,
        name="with_runtime_kwargs",
        execution_options=ExecutionOptions(
            runtime_kwargs={"settings": {"x": 1}, "state": {"y": 2}}
        ),
    )

    assert result.run.id.startswith("with_runtime_kwargs")


def test_require_runtime_kwargs_allows_top_level_alias(tracker):
    @consist.require_runtime_kwargs("settings", "state")
    def run_step(settings, state):
        return None

    result = tracker.run(
        fn=run_step,
        name="with_runtime_kwargs_alias",
        runtime_kwargs={"settings": {"x": 1}, "state": {"y": 2}},
    )

    assert result.run.id.startswith("with_runtime_kwargs_alias")


def test_runtime_kwargs_alias_conflicts_with_execution_options(tracker):
    @consist.require_runtime_kwargs("settings")
    def run_step(settings):
        return None

    with pytest.raises(
        ValueError,
        match=("both top-level runtime_kwargs and execution_options\\.runtime_kwargs"),
    ):
        tracker.run(
            fn=run_step,
            name="runtime_kwargs_conflict",
            runtime_kwargs={"settings": {"x": 1}},
            execution_options=ExecutionOptions(runtime_kwargs={"settings": {"x": 2}}),
        )
