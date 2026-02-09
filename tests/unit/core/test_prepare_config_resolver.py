"""
Tests for Tracker.prepare_config_resolver.

This helper is used to keep `@define_step(config_plan=...)` declarations concise
while still deriving config plans at metadata time from runtime settings.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

from consist.core.step_context import StepContext


def test_prepare_config_resolver_uses_runtime_path(monkeypatch, tracker) -> None:
    captured: dict[str, object] = {}
    sentinel = object()

    def fake_prepare_config(**kwargs):
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(tracker, "prepare_config", fake_prepare_config)

    resolver = tracker.prepare_config_resolver(
        adapter=cast(Any, "adapter"),
        config_dirs_from="settings.config_dirs",
        strict=True,
    )
    ctx = StepContext(
        func_name="step",
        runtime_settings={"config_dirs": [Path("cfg_a"), "cfg_b"]},
        runtime_kwargs={"settings": {"config_dirs": [Path("cfg_a"), "cfg_b"]}},
    )

    plan = resolver(ctx)

    assert plan is sentinel
    assert captured["adapter"] == "adapter"
    assert list(captured["config_dirs"]) == [Path("cfg_a"), "cfg_b"]
    assert captured["strict"] is True


def test_prepare_config_resolver_uses_callable_source(monkeypatch, tracker) -> None:
    captured: dict[str, object] = {}
    sentinel = object()

    def fake_prepare_config(**kwargs):
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(tracker, "prepare_config", fake_prepare_config)

    resolver = tracker.prepare_config_resolver(
        adapter=cast(Any, "adapter"),
        config_dirs_from=lambda ctx: ctx.require_runtime("config_dirs"),
    )
    ctx = StepContext(
        func_name="step",
        runtime_kwargs={"config_dirs": [Path("cfg_c"), "cfg_d"]},
    )

    plan = resolver(ctx)

    assert plan is sentinel
    assert list(captured["config_dirs"]) == [Path("cfg_c"), "cfg_d"]


def test_prepare_config_resolver_validates_source_arguments(tracker) -> None:
    with pytest.raises(ValueError, match="exactly one of"):
        tracker.prepare_config_resolver(adapter=cast(Any, "adapter"))
    with pytest.raises(ValueError, match="exactly one of"):
        tracker.prepare_config_resolver(
            adapter=cast(Any, "adapter"),
            config_dirs=["cfg_a"],
            config_dirs_from="settings.config_dirs",
        )


def test_prepare_config_resolver_rejects_scalar_runtime_value(
    monkeypatch, tracker
) -> None:
    monkeypatch.setattr(tracker, "prepare_config", lambda **kwargs: object())

    resolver = tracker.prepare_config_resolver(
        adapter=cast(Any, "adapter"),
        config_dirs_from="settings.config_dirs",
    )
    ctx = StepContext(
        func_name="step",
        runtime_settings={"config_dirs": "cfg_a"},
        runtime_kwargs={"settings": {"config_dirs": "cfg_a"}},
    )

    with pytest.raises(ValueError, match="iterable of paths"):
        resolver(ctx)
