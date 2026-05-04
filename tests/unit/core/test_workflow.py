from pathlib import Path

import pandas as pd
import pytest
from consist.core.coupler import Coupler
from consist.core.config_canonicalization import (
    CanonicalConfig,
    ConfigPlan,
    canonical_identity_from_config,
)
from consist.core.tracker import Tracker
from consist.core.workflow import RunContext
from consist.types import CacheOptions, ExecutionOptions


def _dummy_config_plan(
    *, adapter_version: str, content_hash: str = "plan_hash"
) -> ConfigPlan:
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
        identity=canonical_identity_from_config(
            adapter_name="dummy",
            adapter_version=adapter_version,
            config=canonical,
        ),
    )


def test_scenario_lifecycle(tracker: Tracker):
    """Test the header run creation, suspension, and restoration."""

    with tracker.scenario("scen_A", config={"global": 1}) as sc:
        # 1. Inside Scenario (Header Active but suspended)
        assert sc.run_id == "scen_A"
        assert tracker.current_consist is None  # Should be suspended

        # 2. Add Input (Temporarily restores header)
        inp = sc.add_input("input.csv", key="base_data")

        # Verify input was logged by checking IDs (robust against instance differences)
        logged_inputs = tracker.get_artifacts_for_run("scen_A").inputs
        assert inp.id in [a.id for a in logged_inputs.values()]
        assert inp.key == "base_data"

        # 3. Step execution
        with sc.trace("step_1", config={"local": 2}) as step_tracker:
            assert step_tracker.current_consist.run.id == "scen_A_step_1"
            assert step_tracker.current_consist.run.parent_run_id == "scen_A"
            assert step_tracker.current_consist.config["local"] == 2

            # Verify active state
            assert tracker.current_consist is not None

        # 4. Back in header (suspended)
        assert tracker.current_consist is None

    # 5. Context Exit
    # Header should be completed
    assert tracker.get_run("scen_A") is not None
    assert tracker.get_run("scen_A").status == "completed"


def test_scenario_input_locking(tracker: Tracker):
    """Ensure inputs cannot be added after steps start."""
    with tracker.scenario("scen_B") as sc:
        with sc.trace("s1"):
            pass

        with pytest.raises(RuntimeError, match="Cannot add scenario inputs"):
            sc.add_input("late.csv", key="late")


def test_scenario_context(tracker: Tracker):
    with tracker.scenario("header", config={"a": 1}) as sc:
        assert sc.run_id == "header"
        assert tracker.current_consist is None  # Suspended

        with sc.trace("step1") as step_t:
            assert step_t.current_consist.run.parent_run_id == "header"

    run = tracker.get_run("header")
    assert run is not None
    assert run.status == "completed"


def test_scenario_parent_links_use_bulk_helper(
    tracker: Tracker, tmp_path: Path, monkeypatch
):
    input_path = tmp_path / "input.csv"
    input_path.write_text("x\n1\n", encoding="utf-8")

    persistence_bulk_calls: list[tuple[str, tuple[str, ...], str]] = []
    db_bulk_calls: list[tuple[str, tuple[str, ...], str]] = []
    single_parent_calls: list[tuple[str, str, str]] = []

    original_persistence_bulk = tracker.persistence.sync_run_with_links
    original_db_bulk = tracker.db.link_artifacts_to_run_bulk
    original_single = tracker.db.link_artifact_to_run

    def counting_persistence_bulk(run, *, artifact_ids, direction) -> None:
        persistence_bulk_calls.append(
            (run.id, tuple(str(a) for a in artifact_ids), direction)
        )
        original_persistence_bulk(run, artifact_ids=artifact_ids, direction=direction)

    def counting_db_bulk(*, artifact_ids, run_id, direction) -> None:
        db_bulk_calls.append((run_id, tuple(str(a) for a in artifact_ids), direction))
        original_db_bulk(artifact_ids=artifact_ids, run_id=run_id, direction=direction)

    def counting_single(artifact_id, run_id, direction) -> None:
        if run_id == "parent_bulk":
            single_parent_calls.append((str(artifact_id), run_id, direction))
        original_single(artifact_id, run_id, direction)

    monkeypatch.setattr(
        tracker.persistence, "sync_run_with_links", counting_persistence_bulk
    )
    monkeypatch.setattr(tracker.db, "link_artifacts_to_run_bulk", counting_db_bulk)
    monkeypatch.setattr(tracker.db, "link_artifact_to_run", counting_single)

    def step(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        out_path = ctx.run_dir / "output.csv"
        out_path.write_text("y\n2\n", encoding="utf-8")

    with tracker.scenario("parent_bulk") as sc:
        sc.run(
            fn=step,
            inputs=[input_path],
            output_paths={"output": "output.csv"},
            execution_options=ExecutionOptions(inject_context="ctx"),
        )

    parent_persistence_calls = [
        call for call in persistence_bulk_calls if call[0] == "parent_bulk"
    ]
    parent_db_bulk_calls = [call for call in db_bulk_calls if call[0] == "parent_bulk"]
    assert [direction for _, _, direction in parent_persistence_calls] == ["output"]
    assert [direction for _, _, direction in parent_db_bulk_calls] == ["input"]
    assert single_parent_calls == []


def test_scenario_exit_does_not_double_sync_header_run(tracker: Tracker, monkeypatch):
    sync_run_calls = 0

    original_sync_run = tracker.persistence.sync_run

    def counting_sync_run(run) -> None:
        nonlocal sync_run_calls
        sync_run_calls += 1
        original_sync_run(run)

    with tracker.scenario("scenario_exit_sync_count") as _sc:
        monkeypatch.setattr(tracker.persistence, "sync_run", counting_sync_run)
        pass

    assert sync_run_calls == 1


def test_scenario_coupler_kw_not_serialized(tracker: Tracker):
    """
    Ensure a coupler passed to scenario(...) is treated as runtime-only and
    not serialized into run metadata.
    """
    with tracker.scenario(
        "scen_coupler_meta",
        coupler=Coupler(),
    ):
        pass

    run = tracker.get_run("scen_coupler_meta")
    assert run is not None
    meta = run.meta or {}
    assert "coupler" not in meta


def test_scenario_trace_input_keys_declares_inputs(tracker: Tracker):
    """
    `ScenarioContext.trace(..., input_keys=[...])` should declare inputs before begin_run()
    computes the cache signature (so caching and `db_fallback='inputs-only'` stay correct).
    """
    with tracker.scenario("scen_input_keys") as sc:
        with sc.trace("produce") as t:
            produced = t.log_artifact("raw.csv", key="raw", direction="output")

        with sc.trace("consume", input_keys=["raw"], cache_mode="overwrite") as t:
            assert t.current_consist is not None
            assert any(a.id == produced.id for a in t.current_consist.inputs)


def test_scenario_trace_optional_input_keys_skips_missing(tracker: Tracker):
    with tracker.scenario("scen_optional_inputs") as sc:
        with sc.trace("produce") as t:
            produced = t.log_artifact("raw.csv", key="raw", direction="output")

        with sc.trace(
            "consume",
            optional_input_keys=["raw", "missing"],
            cache_mode="overwrite",
        ) as t:
            assert t.current_consist is not None
            assert any(a.id == produced.id for a in t.current_consist.inputs)


def test_trace_live_sync_outputs(tracker: Tracker, tmp_path: Path) -> None:
    with tracker.scenario("scen_live_sync") as sc:
        with sc.trace("produce") as t:
            out_path = tmp_path / "data.csv"
            out_path.write_text("value\n1\n")
            t.log_artifact(out_path, key="data", direction="output")

            assert "data" in sc.coupler
            assert sc.coupler.require("data").key == "data"


def test_trace_cache_hit_pre_sync(tracker: Tracker, tmp_path: Path) -> None:
    with tracker.scenario("scen_cache_hit") as sc:
        with sc.trace("step", cache_mode="overwrite") as t:
            out_path = tmp_path / "data.csv"
            out_path.write_text("value\n1\n")
            t.log_artifact(out_path, key="data", direction="output")

        sc.coupler._artifacts.clear()
        assert "data" not in sc.coupler

        with sc.trace("step", run_id="scen_cache_hit_step_cached") as t:
            assert t.is_cached
            assert "data" in sc.coupler
            assert sc.coupler.require("data").key == "data"


def test_inputs_list_promotion_loads_from_coupler(
    tracker: Tracker, tmp_path: Path
) -> None:
    with tracker.scenario("scen_inputs_promo") as sc:
        with sc.trace("produce") as t:
            out_path = tmp_path / "raw.csv"
            out_path.write_text("value\n1\n")
            t.log_artifact(out_path, key="raw", direction="output")

        def consume(raw: pd.DataFrame) -> None:
            assert isinstance(raw, pd.DataFrame)

        sc.run(
            fn=consume,
            name="consume",
            inputs=["raw"],
            execution_options=ExecutionOptions(load_inputs=True),
        )


def test_scenario_trace_facet_from_config(tracker: Tracker):
    np = pytest.importorskip("numpy")
    with tracker.scenario("scen_facet_from") as sc:
        with sc.trace(
            "step_1",
            config={"alpha": np.float64(0.5), "beta": np.int64(2), "extra": 9},
            facet_from=["alpha", "beta"],
            facet={"note": "ok"},
        ):
            pass

    run = tracker.get_run("scen_facet_from_step_1")
    assert run is not None
    facet_id = run.meta["config_facet_id"]
    facet = tracker.get_config_facet(facet_id)
    assert facet is not None
    assert facet.facet_json["alpha"] == pytest.approx(0.5)
    assert facet.facet_json["beta"] == pytest.approx(2)
    assert facet.facet_json["note"] == "ok"
    assert "extra" not in facet.facet_json


def test_scenario_step_defaults_apply_to_child_steps_only(tracker: Tracker):
    def step() -> None:
        return None

    with tracker.scenario(
        "scen_step_defaults",
        step_tags=["scenario:baseline", "seed:7"],
        step_facet={"scenario_id": "baseline", "seed": 7},
    ) as sc:
        sc.run(
            fn=step,
            name="run_step",
            run_id="scen_step_defaults_run_step",
            tags=["custom", "scenario:baseline"],
            facet={"seed": 8, "local": "run"},
            cache_options=CacheOptions(cache_mode="overwrite"),
        )

        with sc.trace(
            "trace_step",
            run_id="scen_step_defaults_trace_step",
            tags=["trace", "scenario:baseline"],
            facet={"seed": 9, "local": "trace"},
            cache_mode="overwrite",
        ):
            pass

    header = tracker.get_run("scen_step_defaults")
    assert header is not None
    assert header.tags == ["scenario_header"]
    assert "config_facet_id" not in (header.meta or {})

    run_step = tracker.get_run("scen_step_defaults_run_step")
    assert run_step is not None
    assert run_step.tags == ["custom", "scenario:baseline", "seed:7"]
    run_step_facet_id = run_step.meta["config_facet_id"]
    run_step_facet = tracker.get_config_facet(run_step_facet_id)
    assert run_step_facet is not None
    assert run_step_facet.facet_json["scenario_id"] == "baseline"
    assert run_step_facet.facet_json["seed"] == 8
    assert run_step_facet.facet_json["local"] == "run"

    trace_step = tracker.get_run("scen_step_defaults_trace_step")
    assert trace_step is not None
    assert trace_step.tags == ["trace", "scenario:baseline", "seed:7"]
    trace_step_facet_id = trace_step.meta["config_facet_id"]
    trace_step_facet = tracker.get_config_facet(trace_step_facet_id)
    assert trace_step_facet is not None
    assert trace_step_facet.facet_json["scenario_id"] == "baseline"
    assert trace_step_facet.facet_json["seed"] == 9
    assert trace_step_facet.facet_json["local"] == "trace"


def test_scenario_step_facet_wins_over_facet_from_on_conflict(tracker: Tracker):
    with tracker.scenario(
        "scen_step_facet_conflict",
        step_facet={"scenario_id": "baseline", "seed": 7},
    ) as sc:
        with sc.trace(
            "trace_step",
            run_id="scen_step_facet_conflict_trace_step",
            config={"seed": 99, "sample": 0.2},
            facet_from=["seed", "sample"],
            cache_mode="overwrite",
        ):
            pass

    trace_step = tracker.get_run("scen_step_facet_conflict_trace_step")
    assert trace_step is not None
    trace_step_facet_id = trace_step.meta["config_facet_id"]
    trace_step_facet = tracker.get_config_facet(trace_step_facet_id)
    assert trace_step_facet is not None
    assert trace_step_facet.facet_json["scenario_id"] == "baseline"
    assert trace_step_facet.facet_json["seed"] == 7
    assert trace_step_facet.facet_json["sample"] == 0.2


def test_step_log_dataframe_defaults_to_run_dir(tracker: Tracker):
    df = pd.DataFrame({"value": [1, 2, 3]})
    artifact = None
    with tracker.scenario("scen_df") as sc:
        with sc.trace("write_df", iteration=0) as t:
            artifact = t.log_dataframe(df, key="series")

    assert artifact is not None
    assert artifact.key == "series"
    assert artifact.abs_path is not None
    abs_path = Path(artifact.abs_path)
    assert abs_path.exists()
    assert (
        abs_path.parent
        == tracker.run_dir / "outputs" / "scen_df" / "write_df" / "iteration_0"
    )
    assert abs_path.name == "series.parquet"


def test_scenario_step_cache_hydration_default(tracker: Tracker):
    with tracker.scenario(
        "scen_cache_default",
        step_cache_hydration="inputs-missing",
    ) as sc:
        with sc.trace("step_a") as t:
            assert t._active_run_cache_options is not None
            assert t._active_run_cache_options.cache_hydration == "inputs-missing"

        with sc.trace("step_b", cache_hydration="metadata") as t:
            assert t._active_run_cache_options is not None
            assert t._active_run_cache_options.cache_hydration == "metadata"


def test_run_artifact_dir_overrides(tracker: Tracker, tmp_path: Path):
    df = pd.DataFrame({"value": [1, 2, 3]})

    with tracker.start_run(
        "override_rel",
        model="override_model",
        artifact_dir="custom/dir",
        cache_mode="overwrite",
    ) as t:
        artifact = t.log_dataframe(df, key="series")

    assert artifact is not None
    assert artifact.abs_path is not None
    abs_path = Path(artifact.abs_path)
    assert abs_path.parent == tracker.run_dir / "outputs" / "custom" / "dir"

    absolute_dir = tmp_path / "absolute_outputs"
    with pytest.raises(ValueError, match="artifact_dir must remain within"):
        with tracker.start_run(
            "override_abs",
            model="override_model",
            artifact_dir=absolute_dir,
            cache_mode="overwrite",
        ) as t:
            t.run_artifact_dir()


def test_run_context_output_dir_default_and_namespace(tracker: Tracker) -> None:
    with tracker.start_run("ctx_output_dir", model="demo") as t:
        ctx = RunContext(t)

        base_dir = ctx.output_dir()
        assert base_dir == t.run_artifact_dir()
        assert base_dir.exists()

        namespaced_dir = ctx.output_dir("tables/daily")
        assert namespaced_dir == base_dir / "tables" / "daily"
        assert namespaced_dir.exists()


def test_run_context_output_path_normalizes_ext_and_creates_dirs(
    tracker: Tracker,
) -> None:
    with tracker.start_run("ctx_output_path", model="demo") as t:
        ctx = RunContext(t)

        path = ctx.output_path("reports/summary", ext=".CSV")
        assert path == t.run_artifact_dir() / "reports" / "summary.csv"
        assert path.parent.exists()


def test_run_context_output_path_respects_artifact_dir_override(
    tracker: Tracker,
) -> None:
    with tracker.start_run(
        "ctx_output_override",
        model="demo",
        artifact_dir="custom/managed",
    ) as t:
        ctx = RunContext(t)

        path = ctx.output_path("result")
        assert (
            path
            == tracker.run_dir / "outputs" / "custom" / "managed" / "result.parquet"
        )
        assert path.parent.exists()


def test_step_default_path_includes_model_name(tracker: Tracker):
    df = pd.DataFrame({"value": [1, 2, 3]})
    artifact = None
    with tracker.scenario("scen_model_dir") as sc:
        with sc.trace("write_df", model="model_x", iteration=2) as t:
            artifact = t.log_dataframe(df, key="series")

    assert artifact is not None
    assert artifact.abs_path is not None
    abs_path = Path(artifact.abs_path)
    assert (
        abs_path.parent
        == tracker.run_dir / "outputs" / "scen_model_dir" / "model_x" / "iteration_2"
    )


def test_run_adapter_includes_adapter_version(
    tracker: Tracker, tmp_path: Path, monkeypatch
):
    config_root = tmp_path / "adapter_cfg_run"
    config_root.mkdir(parents=True, exist_ok=True)

    class DummyAdapter:
        root_dirs = [config_root]

    adapter = DummyAdapter()
    plan_v1 = _dummy_config_plan(adapter_version="1.0")
    plan_v2 = _dummy_config_plan(adapter_version="2.0")
    plans = iter([plan_v1, plan_v2])

    def fake_prepare_config(*, adapter, config_dirs, **kwargs):
        del kwargs
        assert adapter is not None
        assert list(config_dirs) == [config_root]
        return next(plans)

    monkeypatch.setattr(tracker, "prepare_config", fake_prepare_config)

    tracker.run(
        fn=lambda: None,
        name="plan_run_v1",
        adapter=adapter,
        cache_options=CacheOptions(cache_mode="overwrite"),
    )
    record_v1 = tracker.last_run
    assert record_v1 is not None
    assert record_v1.config["__consist_config_plan__"]["adapter_version"] == "1.0"
    hash_v1 = record_v1.run.config_hash

    tracker.run(
        fn=lambda: None,
        name="plan_run_v2",
        adapter=adapter,
        cache_options=CacheOptions(cache_mode="overwrite"),
    )
    record_v2 = tracker.last_run
    assert record_v2 is not None
    hash_v2 = record_v2.run.config_hash

    assert hash_v1 != hash_v2


def test_scenario_run_with_adapter(tracker: Tracker, tmp_path: Path, monkeypatch):
    config_root = tmp_path / "adapter_cfg_scenario"
    config_root.mkdir(parents=True, exist_ok=True)

    class DummyAdapter:
        root_dirs = [config_root]

    adapter = DummyAdapter()
    plan = _dummy_config_plan(adapter_version="3.1")

    def fake_prepare_config(*, adapter, config_dirs, **kwargs):
        del kwargs
        assert adapter is not None
        assert list(config_dirs) == [config_root]
        return plan

    monkeypatch.setattr(tracker, "prepare_config", fake_prepare_config)

    with tracker.scenario("scen_plan") as sc:
        sc.run(
            fn=lambda: None,
            name="step",
            adapter=adapter,
            cache_options=CacheOptions(cache_mode="overwrite"),
        )
        record = tracker.last_run
        assert record is not None
        assert record.config["__consist_config_plan__"]["adapter_version"] == "3.1"


def test_scenario_run_uses_decorator_adapter_default(
    tracker: Tracker, tmp_path: Path, monkeypatch
) -> None:
    config_root = tmp_path / "adapter_cfg_decorator"
    config_root.mkdir(parents=True, exist_ok=True)

    class DummyAdapter:
        root_dirs = [config_root]

    adapter = DummyAdapter()

    @tracker.define_step(
        adapter=lambda ctx: adapter,
    )
    def step() -> None:
        return None

    def fake_prepare_config(*, adapter, config_dirs, **kwargs):
        del kwargs
        assert adapter is not None
        assert list(config_dirs) == [config_root]
        return _dummy_config_plan(
            adapter_version="2042",
            content_hash="hash_2042",
        )

    monkeypatch.setattr(tracker, "prepare_config", fake_prepare_config)

    with tracker.scenario("scen_plan_default") as sc:
        sc.run(fn=step, year=2042, cache_options=CacheOptions(cache_mode="overwrite"))
        record = tracker.last_run
        assert record is not None
        assert record.config["__consist_config_plan__"]["adapter_version"] == "2042"


def test_scenario_run_uses_decorator_identity_inputs_default(
    tracker: Tracker, tmp_path: Path
) -> None:
    dep = tmp_path / "scenario_identity_dep.yaml"
    dep.write_text("mode=test\n")

    @tracker.define_step(identity_inputs=[dep])
    def step() -> None:
        return None

    with tracker.scenario("scen_identity_inputs_default") as sc:
        sc.run(fn=step, cache_options=CacheOptions(cache_mode="overwrite"))
        record = tracker.last_run
        assert record is not None
        digest_map = record.run.meta.get("consist_hash_inputs")
        assert isinstance(digest_map, dict)
        assert len(digest_map) == 1
