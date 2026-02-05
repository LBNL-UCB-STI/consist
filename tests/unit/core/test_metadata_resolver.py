"""
Tests for MetadataResolver: ensure decorator defaults, templates, and callables
resolve with the correct precedence. Tracker.run and ScenarioContext.run rely
on these guarantees to keep execution logic consistent.
"""

from __future__ import annotations

from typing import Any

import pytest

from consist.core.decorators import define_step
from consist.core.metadata_resolver import MetadataResolver


def test_resolver_precedence_overrides_decorator_defaults() -> None:
    @define_step(outputs=["decorator"], tags=["decorator"], cache_mode="reuse")
    def step() -> None:
        return None

    resolver = MetadataResolver()
    resolved = resolver.resolve(
        fn=step,
        name=None,
        model=None,
        description=None,
        inputs=None,
        input_keys=None,
        optional_input_keys=None,
        tags=["explicit"],
        facet_from=None,
        facet_schema_version=None,
        hash_inputs=None,
        year=None,
        iteration=None,
        phase=None,
        stage=None,
        settings=None,
        workspace=None,
        state=None,
        runtime_kwargs=None,
        outputs=["explicit"],
        output_paths=None,
        cache_mode="overwrite",
        cache_hydration=None,
        cache_version=None,
        validate_cached_outputs=None,
        load_inputs=None,
        missing_name_error="missing",
    )

    assert resolved.outputs == ["explicit"]
    assert resolved.tags == ["explicit"]
    assert resolved.cache_mode == "overwrite"


def test_resolver_name_template_precedence_and_fallback() -> None:
    @define_step(name_template="{func_name}__y{year}")
    def step() -> None:
        return None

    resolver = MetadataResolver(default_name_template="{func_name}__default")
    resolved = resolver.resolve(
        fn=step,
        name=None,
        model=None,
        description=None,
        inputs=None,
        input_keys=None,
        optional_input_keys=None,
        tags=None,
        facet_from=None,
        facet_schema_version=None,
        hash_inputs=None,
        year=2030,
        iteration=None,
        phase=None,
        stage=None,
        settings=None,
        workspace=None,
        state=None,
        runtime_kwargs=None,
        outputs=None,
        output_paths=None,
        cache_mode=None,
        cache_hydration=None,
        cache_version=None,
        validate_cached_outputs=None,
        load_inputs=None,
        missing_name_error="missing",
    )
    assert resolved.name == "step__y2030"

    resolver_default = MetadataResolver(default_name_template="{func_name}__default")
    resolved_default = resolver_default.resolve(
        fn=lambda: None,
        name=None,
        model=None,
        description=None,
        inputs=None,
        input_keys=None,
        optional_input_keys=None,
        tags=None,
        facet_from=None,
        facet_schema_version=None,
        hash_inputs=None,
        year=None,
        iteration=None,
        phase=None,
        stage=None,
        settings=None,
        workspace=None,
        state=None,
        runtime_kwargs=None,
        outputs=None,
        output_paths=None,
        cache_mode=None,
        cache_hydration=None,
        cache_version=None,
        validate_cached_outputs=None,
        load_inputs=None,
        missing_name_error="missing",
    )
    assert resolved_default.name == "<lambda>__default"


def test_resolver_disables_step_defaults_and_templates() -> None:
    @define_step(outputs=["decorator"], name_template="{func_name}__decorator")
    def step() -> None:
        return None

    resolver = MetadataResolver(allow_template=False, apply_step_defaults=False)
    resolved = resolver.resolve(
        fn=step,
        name=None,
        model=None,
        description=None,
        inputs=None,
        input_keys=None,
        optional_input_keys=None,
        tags=None,
        facet_from=None,
        facet_schema_version=None,
        hash_inputs=None,
        year=None,
        iteration=None,
        phase=None,
        stage=None,
        settings=None,
        workspace=None,
        state=None,
        runtime_kwargs=None,
        outputs=None,
        output_paths=None,
        cache_mode=None,
        cache_hydration=None,
        cache_version=None,
        validate_cached_outputs=None,
        load_inputs=None,
        missing_name_error="missing",
    )
    assert resolved.outputs is None
    assert resolved.name == "step"


def test_resolver_raises_missing_name() -> None:
    class CallableNoName:
        def __call__(self) -> None:
            return None

    resolver = MetadataResolver()
    with pytest.raises(ValueError, match="missing"):
        resolver.resolve(
            fn=CallableNoName(),
            name=None,
            model=None,
            description=None,
            inputs=None,
            input_keys=None,
            optional_input_keys=None,
            tags=None,
            facet_from=None,
            facet_schema_version=None,
            hash_inputs=None,
            year=None,
            iteration=None,
            phase=None,
            stage=None,
            settings=None,
            workspace=None,
            state=None,
            runtime_kwargs=None,
            outputs=None,
            output_paths=None,
            cache_mode=None,
            cache_hydration=None,
            cache_version=None,
            validate_cached_outputs=None,
            load_inputs=None,
            missing_name_error="missing",
        )


def test_resolver_callable_metadata() -> None:
    @define_step(
        model=lambda ctx: f"{ctx.func_name}_model",
        outputs=lambda ctx: [f"out_{ctx.year}"],
    )
    def step() -> None:
        return None

    resolver = MetadataResolver()
    resolved = resolver.resolve(
        fn=step,
        name=None,
        model=None,
        description=None,
        inputs=None,
        input_keys=None,
        optional_input_keys=None,
        tags=None,
        facet_from=None,
        facet_schema_version=None,
        hash_inputs=None,
        year=2040,
        iteration=None,
        phase=None,
        stage=None,
        settings=None,
        workspace=None,
        state=None,
        runtime_kwargs=None,
        outputs=None,
        output_paths=None,
        cache_mode=None,
        cache_hydration=None,
        cache_version=None,
        validate_cached_outputs=None,
        load_inputs=None,
        missing_name_error="missing",
    )

    assert resolved.model == "step_model"
    assert resolved.outputs == ["out_2040"]
