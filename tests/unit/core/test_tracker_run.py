from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pandas as pd
import pytest

import consist
from consist.core.config_canonicalization import CanonicalConfig, ConfigPlan
from consist.core.tracker import Tracker
from consist.types import CacheOptions, ExecutionOptions, OutputPolicyOptions


def test_tracker_run_load_inputs_requires_mapping(tracker, sample_csv):
    input_path = sample_csv("inputs.csv", rows=2)

    def step(data: pd.DataFrame) -> None:
        assert not data.empty

    with pytest.raises(
        ValueError, match="load_inputs=True requires inputs to be a dict"
    ):
        tracker.run(
            fn=step,
            inputs=[input_path],
            execution_options=ExecutionOptions(load_inputs=True),
        )


def test_tracker_run_loads_inputs_and_injects_context(tracker, sample_csv):
    input_path = sample_csv("data.csv", rows=4)

    def step(data: pd.DataFrame, threshold: int, ctx) -> None:
        assert len(data) == 4
        assert threshold == 2
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        out_path = ctx.run_dir / "filtered.csv"
        data[data["value"] >= threshold].to_csv(out_path, index=False)

    result = tracker.run(
        fn=step,
        inputs={"data": input_path},
        config={"threshold": 2},
        output_paths={"filtered": "filtered.csv"},
        execution_options=ExecutionOptions(load_inputs=True, inject_context="ctx"),
    )

    assert "filtered" in result.outputs
    assert result.outputs["filtered"].path.exists()


def test_tracker_run_cache_hit_skips_callable(tracker):
    calls: list[str] = []

    def step(ctx) -> None:
        calls.append("called")
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        out_path = ctx.run_dir / "out.txt"
        out_path.write_text(f"calls={len(calls)}\n")

    first = tracker.run(
        fn=step,
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )
    second = tracker.run(
        fn=step,
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    assert calls == ["called"]
    assert first.outputs["out"].path.read_text() == "calls=1\n"
    assert second.cache_hit is True


def test_tracker_run_cache_options_callable_code_identity(tracker):
    calls: list[str] = []

    def step() -> None:
        calls.append("called")

    with patch.object(
        tracker.identity, "compute_callable_hash", return_value="callable_hash"
    ) as mock_callable_hash:
        with patch.object(
            tracker.identity,
            "get_code_version",
            side_effect=RuntimeError("repo git hash should not be used"),
        ):
            first = tracker.run(
                fn=step,
                cache_options=CacheOptions(code_identity="callable_module"),
            )
            second = tracker.run(
                fn=step,
                cache_options=CacheOptions(code_identity="callable_module"),
            )

    assert calls == ["called"]
    assert first.run.git_hash == "callable_hash"
    assert second.cache_hit is True
    assert mock_callable_hash.call_count == 2


def test_run_context_run_dir_is_created(tracker):
    def step(ctx):
        out_path = ctx.run_dir / "out.txt"
        out_path.write_text("ok\n")
        return out_path

    result = tracker.run(
        fn=step,
        outputs=["out"],
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    assert result.outputs["out"].path.exists()


def test_run_with_config_overrides_requires_supporting_adapter(tracker, tmp_path):
    with pytest.raises(
        TypeError, match="does not support run_with_config_overrides delegation"
    ):
        tracker.run_with_config_overrides(
            adapter=object(),
            base_run_id="missing_run",
            overrides={},
            output_dir=tmp_path / "materialized",
            fn=lambda: None,
            name="override_step",
        )


def test_tracker_run_rejects_removed_config_plan_kwarg(tracker, tmp_path):
    @tracker.define_step(adapter=object())
    def step() -> None:
        return None

    with pytest.raises(TypeError, match="unexpected keyword argument 'config_plan'"):
        tracker.run(fn=step, name="step", config_plan=object())


def test_tracker_run_rejects_removed_hash_inputs_kwarg(tracker, tmp_path):
    dep = tmp_path / "tracker_identity_dep.yaml"
    dep.write_text("mode=test\n")

    @tracker.define_step(identity_inputs=[dep])
    def step() -> None:
        return None

    with pytest.raises(TypeError, match="unexpected keyword argument 'hash_inputs'"):
        tracker.run(fn=step, name="step", hash_inputs=[dep])


def test_start_run_overwrite_updates_cache_index(tracker, tmp_path):
    input_path = tmp_path / "input.txt"
    input_path.write_text("payload")
    config = {"param": 1}

    with tracker.start_run(
        "run_a_overwrite", "overwrite_model", config=config, inputs=[str(input_path)]
    ):
        pass

    with tracker.start_run(
        "run_b_overwrite",
        "overwrite_model",
        config=config,
        inputs=[str(input_path)],
        cache_mode="overwrite",
    ) as t:
        assert not t.is_cached

    with tracker.start_run(
        "run_c_overwrite",
        "overwrite_model",
        config=config,
        inputs=[str(input_path)],
        cache_mode="reuse",
    ) as t:
        assert t.is_cached
        assert t.current_consist.cached_run.id == "run_b_overwrite"


def test_begin_end_run_imperative_logs_output(tracker):
    tracker.begin_run("imperative_run", "imperative_model", config={"alpha": 1})
    assert tracker.current_consist is not None

    out_path = tracker.run_artifact_dir() / "out.txt"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("ok\n")
    output = tracker.log_artifact(out_path, key="out", direction="output")

    completed = tracker.end_run(status="completed")
    assert completed.id == "imperative_run"
    assert completed.status == "completed"
    assert output.key == "out"
    assert tracker.current_consist is None
    assert tracker.last_run.run.id == "imperative_run"
    assert any(artifact.key == "out" for artifact in tracker.last_run.outputs)


def test_trace_cache_hit_relogs_cached_output(tracker):
    first_artifact = None
    second_artifact = None
    calls: list[int] = []
    shared_out = tracker.run_dir / "trace_cached_output.txt"

    with tracker.trace("trace_cached_output") as t:
        calls.append(1)
        shared_out.parent.mkdir(parents=True, exist_ok=True)
        shared_out.write_text("value=1\n")
        first_artifact = t.log_artifact(shared_out, key="out", direction="output")

    with tracker.trace("trace_cached_output") as t:
        assert t.is_cached
        calls.append(1)
        shared_out.write_text("value=1\n")
        second_artifact = t.log_artifact(shared_out, key="out", direction="output")

    assert len(calls) == 2
    assert first_artifact is not None
    assert second_artifact is not None
    assert second_artifact.id == first_artifact.id


def test_tracker_run_output_mismatch_warns(tracker, caplog):
    def step():
        return ["only-one"]

    with caplog.at_level(logging.WARNING):
        result = tracker.run(
            fn=step,
            outputs=["a", "b"],
            output_policy=OutputPolicyOptions(
                output_mismatch="warn",
                output_missing="ignore",
            ),
        )

    assert any(
        "Output list length does not match declared outputs" in record.message
        for record in caplog.records
    )
    assert result.outputs == {}


def test_tracker_run_output_mismatch_error_raises(tracker):
    def step():
        return ["only-one"]

    with pytest.raises(RuntimeError, match="Output list length does not match"):
        tracker.run(
            fn=step,
            outputs=["a", "b"],
            output_policy=OutputPolicyOptions(
                output_mismatch="error",
                output_missing="ignore",
            ),
        )


def test_tracker_run_logs_dataframe_for_single_declared_output(tracker):
    def step():
        return pd.DataFrame({"id": [1, 2], "value": [10.0, 20.0]})

    result = tracker.run(fn=step, outputs=["table"])
    assert "table" in result.outputs

    artifact = result.outputs["table"]
    assert artifact.path.exists()
    loaded = consist.load_df(artifact, tracker=tracker)
    assert isinstance(loaded, pd.DataFrame)
    assert list(loaded.columns) == ["id", "value"]
    assert loaded.shape == (2, 2)


def test_tracker_run_logs_series_for_single_declared_output(tracker):
    def step():
        return pd.Series([3, 4], name="ignored_name")

    result = tracker.run(fn=step, outputs=["series_out"])
    assert "series_out" in result.outputs

    artifact = result.outputs["series_out"]
    assert artifact.path.exists()
    loaded = consist.load_df(artifact, tracker=tracker)
    assert isinstance(loaded, pd.DataFrame)
    assert list(loaded.columns) == ["series_out"]
    assert loaded["series_out"].tolist() == [3, 4]


def test_tracker_run_output_missing_error(tracker):
    def step() -> None:
        return None

    with pytest.raises(RuntimeError, match="missing outputs"):
        tracker.run(
            fn=step,
            outputs=["out"],
            output_policy=OutputPolicyOptions(output_missing="error"),
        )


def test_tracker_run_inject_context_requires_param(tracker):
    def step() -> None:
        return None

    with pytest.raises(ValueError, match="inject_context requested"):
        tracker.run(
            fn=step,
            execution_options=ExecutionOptions(inject_context=True),
        )


def test_consist_ref_selects_outputs_and_validates_keys(tracker):
    def multi_output_step(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "left.txt").write_text("left")
        (ctx.run_dir / "right.txt").write_text("right")

    result = tracker.run(
        fn=multi_output_step,
        output_paths={"left": "left.txt", "right": "right.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    selected = consist.ref(result, key="right")
    assert selected.key == "right"

    with pytest.raises(KeyError, match="Available keys: 'left', 'right'"):
        consist.ref(result, key="missing")

    with pytest.raises(ValueError, match="consist.ref\\(\\.\\.\\., key='\\.\\.\\.'\\)"):
        consist.ref(result)

    empty = tracker.run(fn=lambda: None)
    with pytest.raises(ValueError, match="consist.ref\\(\\.\\.\\., key='\\.\\.\\.'\\)"):
        consist.ref(empty)


def test_consist_refs_builds_and_combines_input_mappings(tracker):
    def produce_people(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "households.txt").write_text("households")
        (ctx.run_dir / "persons.txt").write_text("persons")

    def produce_network(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "skims.txt").write_text("skims")

    people = tracker.run(
        fn=produce_people,
        output_paths={"households": "households.txt", "persons": "persons.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )
    network = tracker.run(
        fn=produce_network,
        output_paths={"skims": "skims.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    single_result_mapping = consist.refs(people, "households", "persons")
    assert set(single_result_mapping.keys()) == {"households", "persons"}

    all_outputs = consist.refs(people)
    assert set(all_outputs.keys()) == {"households", "persons"}

    combined = consist.refs((people, "households"), (network, "skims"))
    assert set(combined.keys()) == {"households", "skims"}

    tuple_selector_all_outputs = consist.refs((people,))
    assert set(tuple_selector_all_outputs.keys()) == {"households", "persons"}

    aliased = consist.refs(hh=(people, "households"), travel_skims=(network, "skims"))
    assert aliased["hh"].key == "households"
    assert aliased["travel_skims"].key == "skims"

    single_output_alias = consist.refs(skims=network)
    assert single_output_alias["skims"].key == "skims"

    alias_map = consist.refs(people, {"hh": "households", "travel_persons": "persons"})
    assert alias_map["hh"].key == "households"
    assert alias_map["travel_persons"].key == "persons"

    tracker.run(
        fn=lambda: None,
        inputs=combined,
        execution_options=ExecutionOptions(load_inputs=False),
    )
    assert any(a.key == "households" for a in tracker.last_run.inputs)
    assert any(a.key == "skims" for a in tracker.last_run.inputs)


def test_consist_refs_validates_duplicates_and_empty_selectors(tracker):
    def produce(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "single.txt").write_text("single")

    result = tracker.run(
        fn=produce,
        output_paths={"single": "single.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    with pytest.raises(ValueError, match="requires at least one selector"):
        consist.refs()

    with pytest.raises(ValueError, match="Duplicate input key"):
        consist.refs((result, "single"), single=(result, "single"))

    with pytest.raises(ValueError, match="alias mapping cannot be empty"):
        consist.refs(result, {})

    with pytest.raises(TypeError, match="alias mapping keys must be strings"):
        cast(Any, consist.refs)(result, {1: "single"})

    with pytest.raises(TypeError, match="alias mapping values must be strings"):
        cast(Any, consist.refs)(result, {"x": 1})

    with pytest.raises(TypeError, match="remaining positional arguments must be"):
        cast(Any, consist.refs)(result, 1)

    with pytest.raises(TypeError, match="Positional selectors.*must be non-empty"):
        cast(Any, consist.refs)("single")

    with pytest.raises(TypeError, match="Positional selectors.*must be non-empty"):
        consist.refs(())

    with pytest.raises(TypeError, match="must start with RunResult"):
        consist.refs(("single",))

    with pytest.raises(TypeError, match="Keyword selectors.*RunResult or"):
        consist.refs(single=(result, 1))

    with pytest.raises(TypeError, match="Keyword selectors.*RunResult or"):
        consist.refs(single=(result, "single", "extra"))


def test_tracker_run_accepts_direct_single_output_run_result_input(tracker):
    def produce(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "single.txt").write_text("single")

    produced = tracker.run(
        fn=produce,
        output_paths={"single": "single.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    tracker.run(
        fn=lambda: None,
        inputs={"upstream": produced},
        execution_options=ExecutionOptions(load_inputs=False),
    )

    assert any(a.key == "single" for a in tracker.last_run.inputs)


def test_tracker_run_rejects_ambiguous_run_result_input(tracker):
    def produce(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "a.txt").write_text("a")
        (ctx.run_dir / "b.txt").write_text("b")

    produced = tracker.run(
        fn=produce,
        output_paths={"a": "a.txt", "b": "b.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    with pytest.raises(ValueError, match="consist.ref\\(\\.\\.\\., key='\\.\\.\\.'\\)"):
        tracker.run(
            fn=lambda: None,
            inputs={"upstream": produced},
            execution_options=ExecutionOptions(load_inputs=False),
        )


def test_tracker_get_run_result_returns_selected_outputs(tracker):
    def step(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "a.txt").write_text("a")
        (ctx.run_dir / "b.txt").write_text("b")

    produced = tracker.run(
        fn=step,
        output_paths={"a": "a.txt", "b": "b.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    historical = tracker.get_run_result(produced.run.id)
    assert historical.run.id == produced.run.id
    assert set(historical.outputs.keys()) == {"a", "b"}

    subset = tracker.get_run_result(produced.run.id, keys=["b"])
    assert list(subset.outputs.keys()) == ["b"]


def test_tracker_get_run_result_validates_keys_and_run_id(tracker):
    def step(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "out.txt").write_text("ok")

    produced = tracker.run(
        fn=step,
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    with pytest.raises(KeyError, match="missing requested output keys"):
        tracker.get_run_result(produced.run.id, keys=["missing"])

    with pytest.raises(KeyError, match="was not found"):
        tracker.get_run_result("missing-run-id")

    with pytest.raises(ValueError, match="validate must be one of"):
        tracker.get_run_result(produced.run.id, validate="bogus")


def test_tracker_get_run_result_strict_validation_checks_paths(tracker):
    def step(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        out_path = ctx.run_dir / "out.txt"
        out_path.write_text("ok")

    produced = tracker.run(
        fn=step,
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )
    produced.outputs["out"].path.unlink()

    with pytest.raises(FileNotFoundError, match="missing output files"):
        tracker.get_run_result(produced.run.id, validate="strict")


def test_tracker_get_config_bundle_returns_resolved_bundle_path(tracker):
    with tracker.start_run("bundle_run", "bundle_model"):
        bundle_path = tracker.run_artifact_dir() / "config_bundle.tar.gz"
        bundle_path.parent.mkdir(parents=True, exist_ok=True)
        bundle_path.write_text("bundle")
        tracker.log_artifact(
            bundle_path,
            key="config_bundle",
            direction="input",
            config_role="bundle",
        )

    resolved = tracker.get_config_bundle("bundle_run")
    assert resolved == bundle_path.resolve()


def test_tracker_get_config_bundle_adapter_filter_matches_and_misses(tracker):
    with tracker.start_run("bundle_adapter", "bundle_model"):
        tracker.log_meta(config_adapter="activitysim")
        bundle_path = tracker.run_artifact_dir() / "config_bundle.tar.gz"
        bundle_path.parent.mkdir(parents=True, exist_ok=True)
        bundle_path.write_text("bundle")
        tracker.log_artifact(
            bundle_path,
            key="config_bundle",
            direction="input",
            config_role="bundle",
        )

    assert (
        tracker.get_config_bundle("bundle_adapter", adapter="activitysim")
        == bundle_path.resolve()
    )
    assert (
        tracker.get_config_bundle(
            "bundle_adapter",
            adapter="beam",
            allow_missing=True,
        )
        is None
    )


def test_tracker_get_config_bundle_adapter_filter_uses_artifact_metadata(tracker):
    with tracker.start_run("bundle_artifact_adapter", "bundle_model"):
        bundle_path = tracker.run_artifact_dir() / "config_bundle.tar.gz"
        bundle_path.parent.mkdir(parents=True, exist_ok=True)
        bundle_path.write_text("bundle")
        tracker.log_artifact(
            bundle_path,
            key="config_bundle",
            direction="input",
            config_role="bundle",
            adapter="beam",
        )

    resolved = tracker.get_config_bundle("bundle_artifact_adapter", adapter="beam")
    assert resolved == bundle_path.resolve()


def test_tracker_get_config_bundle_allow_missing_returns_none(tracker):
    with tracker.start_run("bundle_missing_allow", "bundle_model"):
        pass

    assert tracker.get_config_bundle("bundle_missing_allow", allow_missing=True) is None


def test_tracker_get_config_bundle_missing_raises_file_not_found(tracker):
    with tracker.start_run("bundle_missing_error", "bundle_model"):
        pass

    with pytest.raises(FileNotFoundError, match="No config artifact found"):
        tracker.get_config_bundle("bundle_missing_error")


def test_tracker_get_config_bundle_selects_deterministic_match(tracker):
    with tracker.start_run("bundle_multiple", "bundle_model"):
        first = tracker.run_artifact_dir() / "z_config_bundle.tar.gz"
        second = tracker.run_artifact_dir() / "a_config_bundle.tar.gz"
        first.parent.mkdir(parents=True, exist_ok=True)
        first.write_text("z")
        second.write_text("a")

        tracker.log_artifact(
            first,
            key="z_bundle",
            direction="input",
            config_role="bundle",
        )
        tracker.log_artifact(
            second,
            key="a_bundle",
            direction="input",
            config_role="bundle",
        )

    resolved = tracker.get_config_bundle("bundle_multiple")
    assert resolved == second.resolve()


def test_tracker_get_config_bundle_ignores_output_artifacts(tracker):
    with tracker.start_run("bundle_output_only", "bundle_model"):
        bundle_path = tracker.run_artifact_dir() / "config_bundle.tar.gz"
        bundle_path.parent.mkdir(parents=True, exist_ok=True)
        bundle_path.write_text("bundle")
        tracker.log_artifact(
            bundle_path,
            key="config_bundle",
            direction="output",
            config_role="bundle",
        )

    assert tracker.get_config_bundle("bundle_output_only", allow_missing=True) is None


def test_cache_epoch_affects_config_hash(tmp_path):
    def step(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "out.txt").write_text("ok")

    tracker_a = Tracker(run_dir=tmp_path / "epoch_a", cache_epoch=1)
    tracker_a.run(
        fn=step,
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
        cache_options=CacheOptions(cache_mode="overwrite"),
    )
    hash_a = tracker_a.last_run.run.config_hash

    tracker_b = Tracker(run_dir=tmp_path / "epoch_b", cache_epoch=2)
    tracker_b.run(
        fn=step,
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
        cache_options=CacheOptions(cache_mode="overwrite"),
    )
    hash_b = tracker_b.last_run.run.config_hash

    assert hash_a != hash_b


def test_cache_version_affects_config_hash(tracker):
    def step(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "out.txt").write_text("ok")

    tracker.run(
        fn=step,
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
        cache_options=CacheOptions(cache_version=1, cache_mode="overwrite"),
    )
    hash_v1 = tracker.last_run.run.config_hash

    tracker.run(
        fn=step,
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
        cache_options=CacheOptions(cache_version=2, cache_mode="overwrite"),
    )
    hash_v2 = tracker.last_run.run.config_hash

    assert hash_v1 != hash_v2


def test_tracker_run_adapter_identity_populates_meta(tracker, tmp_path, monkeypatch):
    dep_path = tmp_path / "identity_dep.yaml"
    dep_path.write_text("threshold: 0.5\n")

    config_root = tmp_path / "adapter_config"
    config_root.mkdir(parents=True, exist_ok=True)

    adapter_plan = ConfigPlan(
        adapter_name="dummy_adapter",
        adapter_version="1.0",
        canonical=CanonicalConfig(
            root_dirs=[config_root],
            primary_config=None,
            config_files=[],
            external_files=[],
            content_hash="adapter_identity_hash",
        ),
        artifacts=[],
        ingestables=[],
    )

    class DummyAdapter:
        model_name = "dummy_adapter"
        root_dirs = [config_root]

    dummy_adapter = DummyAdapter()

    calls: list[list[Path]] = []

    def fake_prepare_config(adapter, config_dirs, **kwargs):
        del kwargs
        assert adapter is dummy_adapter
        resolved_dirs = [Path(p) for p in config_dirs]
        calls.append(resolved_dirs)
        return adapter_plan

    monkeypatch.setattr(tracker, "prepare_config", fake_prepare_config)

    def step() -> None:
        return None

    adapter_run = tracker.run(
        fn=step,
        name="identity_step",
        adapter=dummy_adapter,
        identity_inputs=[dep_path],
        cache_options=CacheOptions(cache_mode="overwrite"),
    )
    assert calls == [[config_root]]
    assert adapter_run.run.meta["config_adapter"] == "dummy_adapter"
    assert adapter_run.run.meta["config_bundle_hash"] == "adapter_identity_hash"
    assert adapter_run.run.meta["consist_hash_inputs"]


def test_tracker_run_rejects_legacy_policy_kwargs(tracker):
    def step() -> None:
        return None

    with pytest.raises(
        TypeError,
        match="unexpected keyword argument 'cache_mode'",
    ):
        tracker.run(fn=step, cache_mode="reuse")


def test_tracker_run_output_policy_options_enforced(tracker):
    def step() -> None:
        return None

    with pytest.raises(RuntimeError, match="missing outputs"):
        tracker.run(
            fn=step,
            outputs=["out"],
            output_policy=OutputPolicyOptions(output_missing="error"),
        )


def test_tracker_run_container_forces_overwrite_and_errors_on_missing_outputs(
    tracker, monkeypatch, caplog
):
    from types import SimpleNamespace

    from consist.integrations import containers

    captured: dict[str, Any] = {}

    def _fake_run_container(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(artifacts={}, cache_hit=False)

    monkeypatch.setattr(containers, "run_container", _fake_run_container)

    with (
        caplog.at_level(logging.WARNING),
        pytest.raises(RuntimeError, match=r"Run 'container_step' missing outputs"),
    ):
        tracker.run(
            fn=None,
            name="container_step",
            output_paths={"out": "container_out.txt"},
            cache_options=CacheOptions(cache_mode="reuse"),
            output_policy=OutputPolicyOptions(output_missing="error"),
            execution_options=ExecutionOptions(
                executor="container",
                container={
                    "image": "ghcr.io/example/test:latest",
                    "command": ["python", "-V"],
                },
            ),
        )

    assert any(
        "forcing cache_mode='overwrite'" in record.message for record in caplog.records
    )
    assert "out" in captured["outputs"]
    out_path = Path(str(captured["outputs"]["out"]))
    assert out_path.name == "container_out.txt"
    assert Path(tracker.run_dir) in out_path.parents


def test_tracker_run_delegates_invocation_defaults_and_validation(tracker, monkeypatch):
    from consist.core import tracker_orchestration
    from consist.core.run_invocation import resolve_run_invocation as _resolve

    captured: dict[str, Any] = {}

    def _spy_resolve_run_invocation(**kwargs):
        captured.update(kwargs)
        return _resolve(**kwargs)

    monkeypatch.setattr(
        tracker_orchestration,
        "resolve_run_invocation",
        _spy_resolve_run_invocation,
    )

    with pytest.raises(
        ValueError, match="Tracker.run supports executor='python' or 'container'."
    ):
        tracker.run(
            fn=lambda: None,
            execution_options=ExecutionOptions(executor=cast(Any, "invalid")),
        )

    assert captured["allow_template"] is None
    assert captured["apply_step_defaults"] is None
    assert captured["missing_name_error"] == "Tracker.run requires a run name."
    assert captured["python_missing_fn_error"] == "Tracker.run requires a callable fn."
