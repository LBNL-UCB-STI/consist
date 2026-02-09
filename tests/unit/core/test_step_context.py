"""
Tests for StepContext runtime/internal separation.

These tests lock in the runtime precedence contract used by metadata callables:
runtime values should be read from runtime fields first, with deprecated aliases
still available during migration.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from consist.core.step_context import StepContext


def test_runtime_accessors_prefer_runtime_fields() -> None:
    ctx = StepContext(
        func_name="step",
        consist_settings={"internal": True},
        runtime_settings={"workflow": True},
        runtime_kwargs={"settings": {"from_kwargs": True}},
    )

    assert ctx.get_runtime("settings") == {"workflow": True}
    assert ctx["settings"] == {"workflow": True}
    assert ctx.get("settings") == {"workflow": True}


def test_get_runtime_falls_back_to_runtime_kwargs() -> None:
    ctx = StepContext(
        func_name="step",
        runtime_kwargs={"workspace": "/tmp/work", "custom": 123},
    )

    assert ctx.get_runtime("workspace") == "/tmp/work"
    assert ctx.get_runtime("custom") == 123
    assert ctx.get_runtime("missing", default="x") == "x"


def test_require_runtime_raises_for_missing_value() -> None:
    ctx = StepContext(func_name="step", runtime_kwargs={})

    with pytest.raises(ValueError, match="Missing runtime value 'settings'"):
        ctx.require_runtime("settings")


def test_deprecated_aliases_warn_and_prefer_runtime_value() -> None:
    ctx = StepContext(
        func_name="step",
        consist_settings={"internal": True},
        consist_workspace=Path("/tmp/internal"),
        consist_state={"internal": True},
        runtime_settings={"runtime": True},
        runtime_workspace="/tmp/runtime",
        runtime_state={"runtime": True},
    )

    with pytest.warns(DeprecationWarning, match="StepContext.settings is deprecated"):
        assert ctx.settings == {"runtime": True}
    with pytest.warns(DeprecationWarning, match="StepContext.workspace is deprecated"):
        assert ctx.workspace == "/tmp/runtime"
    with pytest.warns(DeprecationWarning, match="StepContext.state is deprecated"):
        assert ctx.state == {"runtime": True}


def test_deprecated_aliases_fall_back_to_internal_value() -> None:
    ctx = StepContext(
        func_name="step",
        consist_settings={"internal": True},
        consist_workspace=Path("/tmp/internal"),
        consist_state={"internal": True},
    )

    with pytest.warns(DeprecationWarning):
        assert ctx.settings == {"internal": True}
    with pytest.warns(DeprecationWarning):
        assert ctx.workspace == Path("/tmp/internal")
    with pytest.warns(DeprecationWarning):
        assert ctx.state == {"internal": True}
