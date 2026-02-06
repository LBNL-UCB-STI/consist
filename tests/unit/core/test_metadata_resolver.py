"""
Tests for MetadataResolver: ensure decorator defaults, templates, and callables
resolve with the correct precedence. Tracker.run and ScenarioContext.run rely
on these guarantees to keep execution logic consistent.
"""

from __future__ import annotations


import pytest

from consist.core.config_canonicalization import CanonicalConfig, ConfigPlan
from consist.core.decorators import define_step
from consist.core.metadata_resolver import MetadataResolver


def _dummy_config_plan(*, adapter_version: str, content_hash: str) -> ConfigPlan:
    canonical = CanonicalConfig(
        root_dirs=[],
        primary_config=None,
        config_files=[],
        external_files=[],
        content_hash=content_hash,
    )
    return ConfigPlan(
        adapter_name="dummy",
        adapter_version=adapter_version,
        canonical=canonical,
        artifacts=[],
        ingestables=[],
    )


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
        config={"explicit": True},
        config_plan=None,
        inputs=None,
        input_keys=None,
        optional_input_keys=None,
        tags=["explicit"],
        facet={"explicit": True},
        facet_from=None,
        facet_schema_version=None,
        facet_index=True,
        hash_inputs=None,
        year=None,
        iteration=None,
        phase=None,
        stage=None,
        consist_settings=None,
        consist_workspace=None,
        consist_state=None,
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
    assert resolved.config == {"explicit": True}
    assert resolved.facet == {"explicit": True}
    assert resolved.facet_index is True


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
        config=None,
        config_plan=None,
        inputs=None,
        input_keys=None,
        optional_input_keys=None,
        tags=None,
        facet=None,
        facet_from=None,
        facet_schema_version=None,
        facet_index=None,
        hash_inputs=None,
        year=2030,
        iteration=None,
        phase=None,
        stage=None,
        consist_settings=None,
        consist_workspace=None,
        consist_state=None,
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
        config=None,
        config_plan=None,
        inputs=None,
        input_keys=None,
        optional_input_keys=None,
        tags=None,
        facet=None,
        facet_from=None,
        facet_schema_version=None,
        facet_index=None,
        hash_inputs=None,
        year=None,
        iteration=None,
        phase=None,
        stage=None,
        consist_settings=None,
        consist_workspace=None,
        consist_state=None,
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
        config=None,
        config_plan=None,
        inputs=None,
        input_keys=None,
        optional_input_keys=None,
        tags=None,
        facet=None,
        facet_from=None,
        facet_schema_version=None,
        facet_index=None,
        hash_inputs=None,
        year=None,
        iteration=None,
        phase=None,
        stage=None,
        consist_settings=None,
        consist_workspace=None,
        consist_state=None,
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
            config=None,
            config_plan=None,
            inputs=None,
            input_keys=None,
            optional_input_keys=None,
            tags=None,
            facet=None,
            facet_from=None,
            facet_schema_version=None,
            facet_index=None,
            hash_inputs=None,
            year=None,
            iteration=None,
            phase=None,
            stage=None,
            consist_settings=None,
            consist_workspace=None,
            consist_state=None,
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
        config=None,
        config_plan=None,
        inputs=None,
        input_keys=None,
        optional_input_keys=None,
        tags=None,
        facet=None,
        facet_from=None,
        facet_schema_version=None,
        facet_index=None,
        hash_inputs=None,
        year=2040,
        iteration=None,
        phase=None,
        stage=None,
        consist_settings=None,
        consist_workspace=None,
        consist_state=None,
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


def test_resolver_decorator_config_and_facet_defaults() -> None:
    @define_step(
        config=lambda ctx: {"year": ctx.year},
        facet=lambda ctx: {"scenario": ctx.runtime_kwargs.get("scenario")},
        facet_index=True,
    )
    def step() -> None:
        return None

    resolver = MetadataResolver()
    resolved = resolver.resolve(
        fn=step,
        name=None,
        model=None,
        description=None,
        config=None,
        config_plan=None,
        inputs=None,
        input_keys=None,
        optional_input_keys=None,
        tags=None,
        facet=None,
        facet_from=None,
        facet_schema_version=None,
        facet_index=None,
        hash_inputs=None,
        year=2030,
        iteration=None,
        phase=None,
        stage=None,
        consist_settings=None,
        consist_workspace=None,
        consist_state=None,
        runtime_kwargs={"scenario": "baseline"},
        outputs=None,
        output_paths=None,
        cache_mode=None,
        cache_hydration=None,
        cache_version=None,
        validate_cached_outputs=None,
        load_inputs=None,
        missing_name_error="missing",
    )

    assert resolved.config == {"year": 2030}
    assert resolved.facet == {"scenario": "baseline"}
    assert resolved.facet_index is True


def test_resolver_populates_runtime_fields_from_runtime_kwargs() -> None:
    @define_step(
        config=lambda ctx: {"scenario": ctx.runtime_settings["scenario"]},
        hash_inputs=lambda ctx: (
            "full" if ctx.runtime_workspace == "/workspace/runtime" else "fast"
        ),
    )
    def step() -> None:
        return None

    resolver = MetadataResolver()
    resolved = resolver.resolve(
        fn=step,
        name=None,
        model=None,
        description=None,
        config=None,
        config_plan=None,
        inputs=None,
        input_keys=None,
        optional_input_keys=None,
        tags=None,
        facet=None,
        facet_from=None,
        facet_schema_version=None,
        facet_index=None,
        hash_inputs=None,
        year=None,
        iteration=None,
        phase=None,
        stage=None,
        consist_settings=None,
        consist_workspace=None,
        consist_state=None,
        runtime_kwargs={
            "settings": {"scenario": "baseline"},
            "workspace": "/workspace/runtime",
        },
        outputs=None,
        output_paths=None,
        cache_mode=None,
        cache_hydration=None,
        cache_version=None,
        validate_cached_outputs=None,
        load_inputs=None,
        missing_name_error="missing",
    )

    assert resolved.config == {"scenario": "baseline"}
    assert resolved.hash_inputs == "full"


def test_resolver_decorator_config_plan_defaults() -> None:
    @define_step(
        config_plan=_dummy_config_plan(adapter_version="1.0", content_hash="h1")
    )
    def step() -> None:
        return None

    resolver = MetadataResolver()
    resolved = resolver.resolve(
        fn=step,
        name=None,
        model=None,
        description=None,
        config=None,
        config_plan=None,
        inputs=None,
        input_keys=None,
        optional_input_keys=None,
        tags=None,
        facet=None,
        facet_from=None,
        facet_schema_version=None,
        facet_index=None,
        hash_inputs=None,
        year=None,
        iteration=None,
        phase=None,
        stage=None,
        consist_settings=None,
        consist_workspace=None,
        consist_state=None,
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

    assert resolved.config_plan is not None
    assert resolved.config_plan.adapter_version == "1.0"
    assert resolved.config_plan.identity_hash == "h1"


def test_resolver_config_plan_callable_and_explicit_override() -> None:
    @define_step(
        config_plan=lambda ctx: _dummy_config_plan(
            adapter_version=str(ctx.year),
            content_hash=f"hash_{ctx.year}",
        )
    )
    def step() -> None:
        return None

    explicit_plan = _dummy_config_plan(
        adapter_version="explicit",
        content_hash="h_exp",
    )
    resolver = MetadataResolver()

    resolved_default = resolver.resolve(
        fn=step,
        name=None,
        model=None,
        description=None,
        config=None,
        config_plan=None,
        inputs=None,
        input_keys=None,
        optional_input_keys=None,
        tags=None,
        facet=None,
        facet_from=None,
        facet_schema_version=None,
        facet_index=None,
        hash_inputs=None,
        year=2030,
        iteration=None,
        phase=None,
        stage=None,
        consist_settings=None,
        consist_workspace=None,
        consist_state=None,
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
    assert resolved_default.config_plan is not None
    assert resolved_default.config_plan.adapter_version == "2030"
    assert resolved_default.config_plan.identity_hash == "hash_2030"

    resolved_override = resolver.resolve(
        fn=step,
        name=None,
        model=None,
        description=None,
        config=None,
        config_plan=explicit_plan,
        inputs=None,
        input_keys=None,
        optional_input_keys=None,
        tags=None,
        facet=None,
        facet_from=None,
        facet_schema_version=None,
        facet_index=None,
        hash_inputs=None,
        year=2030,
        iteration=None,
        phase=None,
        stage=None,
        consist_settings=None,
        consist_workspace=None,
        consist_state=None,
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
    assert resolved_override.config_plan is explicit_plan
