from __future__ import annotations

from pathlib import Path

from consist import ExecutionOptions, OutputSet, define_step
from consist.utils import (
    collect_step_contracts,
    collect_step_schema,
    resolve_step_contract,
)


def test_resolve_step_contract_resolves_callable_metadata() -> None:
    @define_step(
        model=lambda ctx: f"{ctx.func_name}_model",
        name_template="{func_name}__y{year}__i{iteration}__{phase}",
        description=lambda ctx: f"{ctx.stage}:{ctx.phase}",
        outputs=lambda ctx: [f"out_{ctx.year}"],
        output_paths=lambda ctx: {f"out_{ctx.year}": Path(f"out_{ctx.year}.csv")},
        config=lambda ctx: {
            "scenario": ctx.require_runtime("settings")["scenario"],
            "year": ctx.year,
        },
        identity_inputs=lambda ctx: [
            ("config", ctx.require_runtime("settings")["config_dir"])
        ],
        facet=lambda ctx: {"scenario": ctx.require_runtime("settings")["scenario"]},
        facet_index=True,
        cache_mode="overwrite",
        cache_hydration="outputs-all",
        cache_version=lambda ctx: ctx.require_runtime("settings")["cache_version"],
        validate_cached_outputs="strict",
        input_binding="paths",
        tags=lambda ctx: [f"stage:{ctx.stage}"],
        facet_from=["scenario"],
        facet_schema_version=2,
    )
    def step() -> None:
        return None

    contract = resolve_step_contract(
        step,
        year=2035,
        iteration=2,
        phase="run",
        stage="demand",
        runtime_kwargs={
            "settings": {
                "scenario": "baseline",
                "config_dir": Path("configs"),
                "cache_version": 7,
            }
        },
    )

    assert contract.name == "step__y2035__i2__run"
    assert contract.model == "step_model"
    assert contract.description == "demand:run"
    assert contract.outputs == ["out_2035"]
    assert contract.output_paths == {"out_2035": Path("out_2035.csv")}
    assert contract.config == {"scenario": "baseline", "year": 2035}
    assert contract.identity_inputs == [("config", Path("configs"))]
    assert contract.facet == {"scenario": "baseline"}
    assert contract.facet_index is True
    assert contract.cache_mode == "overwrite"
    assert contract.cache_hydration == "outputs-all"
    assert contract.cache_version == 7
    assert contract.validate_cached_outputs == "strict"
    assert contract.input_binding == "paths"
    assert contract.tags == ["stage:demand"]
    assert contract.facet_from == ["scenario"]
    assert contract.facet_schema_version == 2


def test_collect_step_contracts_accepts_shared_context_defaults() -> None:
    @define_step(outputs=lambda ctx: [f"a_{ctx.year}"])
    def first() -> None:
        return None

    @define_step(
        output_sets=lambda ctx: {
            "annual": OutputSet(root=f"annual/{ctx.year}", include="*.csv")
        },
        cache_hydration="outputs-requested",
    )
    def second() -> None:
        return None

    contracts = collect_step_contracts(
        [first, second],
        year=2040,
        default_name_template="{func_name}__{year}",
        execution_options=ExecutionOptions(input_binding="none"),
    )

    assert [contract.name for contract in contracts] == ["first__2040", "second__2040"]
    assert contracts[0].outputs == ["a_2040"]
    assert contracts[0].input_binding == "none"
    assert contracts[1].output_sets == {
        "annual": OutputSet(root="annual/2040", include="*.csv")
    }
    assert contracts[1].cache_hydration == "outputs-requested"


def test_collect_step_schema_uses_resolved_contract_outputs() -> None:
    @define_step(
        outputs=lambda ctx: [f"runtime_{ctx.runtime_settings['suffix']}"],
        schema_outputs=lambda ctx: [f"schema_{ctx.runtime_settings['suffix']}"],
        description=lambda ctx: f"Output for {ctx.runtime_settings['suffix']}",
    )
    def step() -> None:
        return None

    schema = collect_step_schema(
        [step],
        settings={"suffix": "v1"},
        extra_keys={"init": "Initialization output"},
    )

    assert schema == {
        "init": "Initialization output",
        "schema_v1": "Output for v1",
    }
