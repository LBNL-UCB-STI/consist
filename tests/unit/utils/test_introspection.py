"""
Tests for collect_step_schema: ensures decorated outputs are discoverable,
callable outputs resolve with settings, and unresolvable callables warn.
The coupler schema builder and docs examples rely on this behavior.
"""

from __future__ import annotations

import pytest

from consist import define_step
from consist.utils import collect_step_schema


def test_collect_step_schema_static_outputs() -> None:
    @define_step(outputs=["alpha"], description="Alpha output")
    def step_alpha() -> None:
        return None

    schema = collect_step_schema([step_alpha])
    assert schema == {"alpha": "Alpha output"}


def test_collect_step_schema_callable_outputs_with_settings() -> None:
    @define_step(outputs=lambda ctx: [f"out_{ctx.runtime_settings['suffix']}"])
    def step_dynamic() -> None:
        return None

    schema = collect_step_schema([step_dynamic], settings={"suffix": "v1"})
    assert schema == {"out_v1": "Output from step_dynamic"}


def test_collect_step_schema_warns_when_unresolvable() -> None:
    @define_step(outputs=lambda ctx: [f"out_{ctx.runtime_settings['suffix']}"])
    def step_dynamic() -> None:
        return None

    with pytest.warns(UserWarning, match="Callable outputs for step_dynamic"):
        schema = collect_step_schema([step_dynamic], settings=None)
    assert schema == {}
