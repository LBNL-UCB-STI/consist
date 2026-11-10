from pathlib import Path

import pandas as pd
import pytest

import consist
from consist.core.coupler import Coupler
from consist.core.config_canonicalization import (
    CanonicalConfig,
    CanonicalizationSnapshot,
    ConfigPlan,
    canonical_identity_from_config,
)
from consist.core.tracker import Tracker
from consist.core.workflow import RunContext
from consist.types import (
    CacheOptions,
    ExecutionOptions,
    OutputPolicyOptions,
    OutputSet,
    RunCompletionArchivePolicy,
)


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
        == tracker.run_dir / "outputs" / "scen_df" / "write_df" / "scen_df_write_df"
    )
    assert abs_path.name == "series.parquet"


def test_log_dataframe_preserves_explicit_driver_for_extensionless_paths(
    tracker: Tracker, tmp_path: Path
):
    df = pd.DataFrame({"value": [1, 2, 3]})

    with tracker.start_run("tracker_extensionless_csv", "test_model"):
        tracker_artifact = tracker.log_dataframe(
            df,
            key="tracker_series",
            path=tmp_path / "tracker_series_data",
            driver="csv",
        )

    with consist.use_tracker(tracker):
        with tracker.start_run("api_extensionless_csv", "test_model"):
            api_artifact = consist.log_dataframe(
                df,
                key="api_series",
                path=tmp_path / "api_series_data",
                driver="csv",
            )

    assert tracker_artifact.driver == "csv"
    assert api_artifact.driver == "csv"


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
        == tracker.run_dir
        / "outputs"
        / "scen_model_dir"
        / "model_x"
        / "scen_model_dir_write_df"
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


def test_run_injects_applied_canonicalization_snapshot(
    tracker: Tracker, tmp_path: Path, monkeypatch
) -> None:
    config_root = tmp_path / "snapshot_config"
    config_root.mkdir()
    canonical = CanonicalConfig(
        root_dirs=[config_root],
        primary_config=None,
        config_files=[],
        external_files=[],
        content_hash="snapshot",
    )
    identity = canonical_identity_from_config(
        adapter_name="snapshot_adapter",
        adapter_version="1",
        config=canonical,
    )
    snapshot = CanonicalizationSnapshot(
        adapter_name="snapshot_adapter",
        adapter_version="1",
        identity_hash=identity.identity_hash,
    )
    plan = ConfigPlan(
        adapter_name="snapshot_adapter",
        adapter_version="1",
        canonical=canonical,
        artifacts=[],
        ingestables=[],
        identity=identity,
        canonicalization=snapshot,
    )

    class Adapter:
        root_dirs = [config_root]

    monkeypatch.setattr(tracker, "prepare_config", lambda **_: plan)
    observed = []

    def step(ctx: RunContext) -> None:
        observed.append(ctx.canonicalization)

    tracker.run(
        fn=step,
        name="snapshot_run",
        adapter=Adapter(),
        cache_options=CacheOptions(cache_mode="overwrite"),
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    assert observed == [snapshot]


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


def test_scenario_run_accepts_output_sets(tracker: Tracker) -> None:
    def step(ctx) -> None:
        annual_dir = ctx.run_dir / "annual"
        annual_dir.mkdir(parents=True)
        (annual_dir / "annual_2030.csv").write_text("year\n2030\n")

    with tracker.scenario("scen_output_sets") as sc:
        result = sc.run(
            fn=step,
            name="annual_step",
            output_sets={
                "annual_outputs": OutputSet(root="annual", include="annual_*.csv")
            },
            execution_options=ExecutionOptions(inject_context="ctx"),
        )

        assert result.outputs["annual_outputs"].driver == "artifact_set"
        assert sc.coupler["annual_outputs"].driver == "artifact_set"


def test_scenario_step_run_completion_archive_default_and_override(
    tracker: Tracker, tmp_path: Path
) -> None:
    default_archive = tmp_path / "scenario_default_archive"
    override_archive = tmp_path / "step_override_archive"

    with pytest.raises(
        TypeError,
        match="step_run_completion_archive must be a RunCompletionArchivePolicy",
    ):
        with tracker.scenario(
            "bad_archive_scen",
            step_run_completion_archive="/tmp/not_policy",  # type: ignore[arg-type]
        ):
            pass

    def step_one(ctx) -> dict[str, Path]:
        p = ctx.run_dir / "step_one.csv"
        p.write_text("a\n1\n", encoding="utf-8")
        return {"one": p}

    def step_two(ctx) -> dict[str, Path]:
        p = ctx.run_dir / "step_two.csv"
        p.write_text("b\n2\n", encoding="utf-8")
        return {"two": p}

    with tracker.scenario(
        "scen_archive_default",
        step_run_completion_archive=RunCompletionArchivePolicy(
            archive_root=default_archive
        ),
    ) as sc:
        res1 = sc.run(
            fn=step_one,
            name="step_one",
            outputs=["one"],
            execution_options=ExecutionOptions(inject_context="ctx"),
        )
        res2 = sc.run(
            fn=step_two,
            name="step_two",
            outputs=["two"],
            execution_options=ExecutionOptions(inject_context="ctx"),
            output_policy=OutputPolicyOptions(
                run_completion_archive=RunCompletionArchivePolicy(
                    archive_root=override_archive
                )
            ),
        )

        assert res1.outputs["one"].meta.get("recovery_roots") == [
            str(default_archive.resolve())
        ]
        assert res2.outputs["two"].meta.get("recovery_roots") == [
            str(override_archive.resolve())
        ]
        assert sc.coupler["one"].meta.get("recovery_roots") == [
            str(default_archive.resolve())
        ]
        assert sc.coupler["two"].meta.get("recovery_roots") == [
            str(override_archive.resolve())
        ]

    assert Path(
        tracker.fs.resolve_historical_path(
            res1.outputs["one"].container_uri, default_archive
        )
    ).exists()
    assert Path(
        tracker.fs.resolve_historical_path(
            res2.outputs["two"].container_uri, override_archive
        )
    ).exists()
    assert tracker.last_run is not None
    header_outputs = {art.key: art for art in tracker.last_run.outputs}
    assert header_outputs["one"].meta.get("recovery_roots") == [
        str(default_archive.resolve())
    ]
    assert header_outputs["two"].meta.get("recovery_roots") == [
        str(override_archive.resolve())
    ]


def test_run_completion_archive_policy_failure_aborts_run_and_prevents_coupler_update(
    tracker: Tracker, tmp_path: Path, monkeypatch
) -> None:
    archive_root = tmp_path / "failing_archive"

    def step(ctx) -> dict[str, Path]:
        p = ctx.run_dir / "will_fail_archive.csv"
        p.write_text("x\n1\n", encoding="utf-8")
        return {"failing_out": p}

    def boom_archive(*args, **kwargs):
        raise RuntimeError("simulated archive storage failure")

    monkeypatch.setattr(tracker, "archive_current_run_outputs", boom_archive)

    with pytest.raises(RuntimeError, match="simulated archive storage failure"):
        with tracker.scenario(
            "scen_archive_fail",
            step_run_completion_archive=RunCompletionArchivePolicy(
                archive_root=archive_root
            ),
        ) as sc:
            sc.run(
                fn=step,
                name="fail_step",
                run_id="scen_archive_fail_step",
                outputs=["failing_out"],
                execution_options=ExecutionOptions(inject_context="ctx"),
            )

    assert "failing_out" not in sc.coupler
    failed_run = tracker.get_run("scen_archive_fail_step")
    assert failed_run is not None
    assert failed_run.status == "failed"
    post_snapshot = tracker.get_coupler_snapshot("scen_archive_fail", phase="post")
    assert "failing_out" not in post_snapshot


def test_scenario_log_meta_persists_to_header_run_while_suspended(
    tracker: Tracker, tmp_path: Path
) -> None:
    import json
    from types import MappingProxyType

    def write_step(ctx) -> dict[str, Path]:
        out_path = ctx.run_dir / "step_out.csv"
        out_path.write_text("col\n1\n", encoding="utf-8")
        return {"step_out": out_path}

    with tracker.scenario("sc_meta_test") as sc:
        assert sc._header_record is not None
        sc._header_record.run.meta = None
        assert isinstance(sc.meta, MappingProxyType)
        assert dict(sc.meta) == {}
        assert sc._header_record.run.meta is None
        with pytest.raises(TypeError):
            sc.meta["illegal"] = "value"  # type: ignore[index]

        sc.log_meta(
            admission_policy="v1",
            preflight={"ok": True, "artifact_path": tmp_path / "input.csv"},
        )
        # Empty call is a safe no-op
        sc.log_meta()

        assert isinstance(sc.meta, MappingProxyType)
        assert sc.meta["admission_policy"] == "v1"
        assert sc.meta["preflight"] == {
            "ok": True,
            "artifact_path": str(tmp_path / "input.csv"),
        }

        # Scenario is suspended while inside the with-block; verify persistence
        # before any child step runs or scenario completion occurs.
        assert tracker.current_consist is None

        persisted_while_running = tracker.get_run("sc_meta_test")
        assert persisted_while_running is not None
        assert persisted_while_running.status == "running"
        assert persisted_while_running.meta["admission_policy"] == "v1"
        assert persisted_while_running.meta["preflight"] == {
            "ok": True,
            "artifact_path": str(tmp_path / "input.csv"),
        }

        consist_json = json.loads(
            (tracker.run_dir / "consist.json").read_text(encoding="utf-8")
        )
        assert consist_json["run"]["id"] == "sc_meta_test"
        assert consist_json["run"]["status"] == "running"
        assert consist_json["run"]["meta"]["admission_policy"] == "v1"
        assert consist_json["run"]["meta"]["preflight"] == {
            "ok": True,
            "artifact_path": str(tmp_path / "input.csv"),
        }

        snapshot_json = json.loads(
            (tracker.run_dir / "consist_runs" / "sc_meta_test.json").read_text(
                encoding="utf-8"
            )
        )
        assert snapshot_json["run"]["meta"]["admission_policy"] == "v1"

        step_result = sc.run(
            fn=write_step,
            name="child_step",
            run_id="sc_meta_test_child_step",
            outputs=["step_out"],
            execution_options=ExecutionOptions(inject_context="ctx"),
        )
        assert "admission_policy" not in step_result.run.meta
        assert "preflight" not in step_result.run.meta

    completed_header = tracker.get_run("sc_meta_test")
    assert completed_header is not None
    assert completed_header.status == "completed"
    assert completed_header.meta["admission_policy"] == "v1"
    assert completed_header.meta["preflight"] == {
        "ok": True,
        "artifact_path": str(tmp_path / "input.csv"),
    }
    assert len(completed_header.meta["steps"]) == 1
    assert completed_header.meta["steps"][0]["id"] == "sc_meta_test_child_step"
    post_snapshot = tracker.get_coupler_snapshot(
        "sc_meta_test_child_step", phase="post"
    )
    assert "step_out" in post_snapshot


def test_scenario_log_meta_outside_active_scenario_raises_runtime_error(
    tracker: Tracker,
) -> None:
    sc = tracker.scenario("sc_inactive")

    with pytest.raises(RuntimeError, match="Scenario not active"):
        _ = sc.meta

    with pytest.raises(RuntimeError, match="Scenario not active"):
        sc.log_meta(stage="before_enter")

    with sc:
        sc.log_meta(stage="inside")
        assert sc.meta["stage"] == "inside"

    with pytest.raises(RuntimeError, match="Scenario not active"):
        _ = sc.meta

    with pytest.raises(RuntimeError, match="Scenario not active"):
        sc.log_meta(stage="after_exit")


def test_scenario_positional_arguments_preserve_legacy_positions(
    tracker: Tracker,
) -> None:
    with tracker.scenario(
        "sc_pos_args",
        {"k": 1},
        ["tag_a"],
        "custom_model",
        ["step_tag"],
        {"step_f": 2},
        "inputs-missing",
        "{step}_tmpl",
        7,
    ) as sc:
        assert sc.name_template == "{step}_tmpl"
        assert sc.cache_epoch == 7
        assert sc.step_cache_hydration == "inputs-missing"
        assert sc.step_run_completion_archive is None


def test_set_output_meta_attaches_metadata_to_declared_outputs_across_run_and_trace(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    import json

    archive_root = tmp_path / "meta_archive"

    def step_with_meta(ctx) -> dict[str, Path]:
        out_a = ctx.run_dir / "out_a.csv"
        out_b = ctx.run_dir / "out_b.csv"
        out_a.write_text("x\n1\n", encoding="utf-8")
        out_b.write_text("y\n2\n", encoding="utf-8")

        ctx.set_output_meta("a", {"sidecar": {"v": 1}, "tag": "initial"})
        consist.set_output_meta("a", {"extra": "merged"})
        ctx.set_output_meta("b", {"will_be": "replaced"})
        ctx.set_output_meta("b", {"final": True}, merge=False)
        return {"a": out_a, "b": out_b}

    with tracker.scenario(
        "sc_output_meta",
        step_run_completion_archive=RunCompletionArchivePolicy(
            archive_root=archive_root
        ),
    ) as sc:
        res = sc.run(
            fn=step_with_meta,
            name="step_with_meta",
            run_id="run_with_meta_1",
            outputs=["a", "b"],
            execution_options=ExecutionOptions(inject_context="ctx"),
        )
        assert res.cache_hit is False
        assert res.outputs["a"].meta["sidecar"] == {"v": 1}
        assert res.outputs["a"].meta["tag"] == "initial"
        assert res.outputs["a"].meta["extra"] == "merged"
        assert res.outputs["a"].meta.get("recovery_roots") == [
            str(archive_root.resolve())
        ]
        assert "will_be" not in res.outputs["b"].meta
        assert res.outputs["b"].meta["final"] is True

        # Coupler and DB also reflect the merged output metadata
        assert sc.coupler.require("a").meta["sidecar"] == {"v": 1}
        assert sc.coupler.require("b").meta["final"] is True

        with sc.trace(
            "trace_with_meta",
            run_id="trace_with_meta_1",
            outputs=["traced_out"],
        ) as t:
            traced_path = t.run_dir / "traced_out.csv"
            traced_path.write_text("z\n3\n", encoding="utf-8")
            t.log_output(traced_path, key="traced_out", initial=1)
            t.set_output_meta("traced_out", {"added_in_trace": 2})

        assert sc.coupler.require("traced_out").meta["initial"] == 1
        assert sc.coupler.require("traced_out").meta["added_in_trace"] == 2
        assert sc.coupler.require("traced_out").meta.get("recovery_roots") == [
            str(archive_root.resolve())
        ]

    db_outputs = tracker.get_artifacts_for_run("run_with_meta_1").outputs
    assert db_outputs["a"].meta["sidecar"] == {"v": 1}
    assert db_outputs["a"].meta["extra"] == "merged"
    assert db_outputs["b"].meta["final"] is True

    trace_db_outputs = tracker.get_artifacts_for_run("trace_with_meta_1").outputs
    assert trace_db_outputs["traced_out"].meta["added_in_trace"] == 2

    snapshot = json.loads(
        (tracker.run_dir / "consist_runs" / "run_with_meta_1.json").read_text(
            encoding="utf-8"
        )
    )
    snap_by_key = {art["key"]: art for art in snapshot["outputs"]}
    assert snap_by_key["a"]["meta"]["sidecar"] == {"v": 1}
    assert snap_by_key["b"]["meta"]["final"] is True


def test_scenario_trace_archive_opt_out_and_step_factory(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    archive_root = tmp_path / "factory_archive"

    def policy_factory(step_name: str) -> RunCompletionArchivePolicy | None:
        if step_name == "skip_by_factory":
            return None
        return RunCompletionArchivePolicy(archive_root=archive_root / step_name)

    def write_csv(ctx) -> dict[str, Path]:
        assert tracker.current_consist is not None
        run_id = tracker.current_consist.run.id
        p = ctx.run_dir / f"{run_id}.csv"
        p.write_text("v\n1\n", encoding="utf-8")
        return {"out": p}

    with tracker.scenario(
        "sc_factory_and_opt_out",
        step_run_completion_archive=policy_factory,
    ) as sc:
        # 1. Archived via factory in sc.run
        r_archived = sc.run(
            fn=write_csv,
            name="archived_step",
            run_id="run_archived_step",
            outputs=["out"],
            execution_options=ExecutionOptions(inject_context="ctx"),
        )
        assert r_archived.outputs["out"].meta.get("recovery_roots") == [
            str((archive_root / "archived_step").resolve())
        ]

        # 2. Skipped via factory returning None
        r_skipped_factory = sc.run(
            fn=write_csv,
            name="skip_by_factory",
            run_id="run_skip_by_factory",
            outputs=["out"],
            execution_options=ExecutionOptions(inject_context="ctx"),
        )
        assert "recovery_roots" not in r_skipped_factory.outputs["out"].meta

        # 3. Explicitly opted out via OutputPolicyOptions(run_completion_archive=False)
        r_opted_out = sc.run(
            fn=write_csv,
            name="opted_out_step",
            run_id="run_opted_out_step",
            outputs=["out"],
            execution_options=ExecutionOptions(inject_context="ctx"),
            output_policy=OutputPolicyOptions(run_completion_archive=False),
        )
        assert "recovery_roots" not in r_opted_out.outputs["out"].meta

        # 4. Archived via factory in sc.trace
        with sc.trace(
            "traced_archived",
            run_id="run_traced_archived",
            outputs=["traced_file"],
        ) as t:
            tp = t.run_dir / "traced_file.csv"
            tp.write_text("t\n1\n", encoding="utf-8")
            t.log_output(tp, key="traced_file")

        assert sc.coupler.require("traced_file").meta.get("recovery_roots") == [
            str((archive_root / "traced_archived").resolve())
        ]
        assert sc.coupler.require("traced_file").abs_path == str(tp.resolve())

        # 5. Explicitly opted out in sc.trace via run_completion_archive=False
        with sc.trace(
            "traced_opt_out",
            run_id="run_traced_opt_out",
            outputs=["traced_no_archive"],
            run_completion_archive=False,
        ) as t:
            tp2 = t.run_dir / "traced_no_archive.csv"
            tp2.write_text("t\n2\n", encoding="utf-8")
            t.log_output(tp2, key="traced_no_archive")

        assert "recovery_roots" not in sc.coupler.require("traced_no_archive").meta


def test_set_output_meta_reserved_keys_trace_demotion_and_post_call_preservation(
    tracker: Tracker,
) -> None:
    # 1. Reserved log_artifact parameter names in set_output_meta with output_paths + return dict
    def step_reserved(ctx) -> dict[str, Path]:
        out = ctx.run_dir / "reserved.csv"
        out.write_text("id\n1\n", encoding="utf-8")
        ctx.set_output_meta(
            "reserved",
            {
                "key": "user_meta_key",
                "direction": "north",
                "path": "/logical/route",
                "driver": "custom_meta_driver",
                "schema": "v2",
                "content_hash": "not_an_override",
            },
        )
        return {"reserved": out}

    res = tracker.run(
        fn=step_reserved,
        name="step_reserved_meta",
        run_id="run_reserved_meta",
        outputs=["reserved"],
        output_paths={"reserved": "reserved.csv"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )
    art = res.outputs["reserved"]
    assert art.key == "reserved"
    assert art.driver == "csv"
    assert art.meta["key"] == "user_meta_key"
    assert art.meta["direction"] == "north"
    assert art.meta["path"] == "/logical/route"
    assert art.meta["driver"] == "custom_meta_driver"
    assert art.meta["schema"] == "v2"
    assert art.meta["content_hash"] == "not_an_override"

    # 2. merge=False on an already-logged output does not wipe subsequent metadata additions
    def step_post_meta(ctx) -> None:
        p = ctx.run_dir / "post.csv"
        p.write_text("id\n2\n", encoding="utf-8")
        logged = tracker.log_output(p, key="post", initial="drop_me")
        ctx.set_output_meta("post", {"replaced": True}, merge=False)
        logged.meta["after_replace"] = "keep_me"

    res_post = tracker.run(
        fn=step_post_meta,
        name="step_post_meta",
        run_id="run_post_meta",
        outputs=["post"],
        execution_options=ExecutionOptions(inject_context="ctx"),
    )
    post_meta = res_post.outputs["post"].meta
    assert "initial" not in post_meta
    assert post_meta.get("replaced") is True
    assert post_meta.get("after_replace") == "keep_me"

    # 3. tracker.trace() cache-hit demotion to miss when set_output_meta is called before log_output
    with tracker.trace("trace_demote", run_id="trace_demote_1", outputs=["t_out"]) as t:
        tp = t.run_dir / "t_out.csv"
        tp.write_text("v\n1\n", encoding="utf-8")
        t.log_output(tp, key="t_out")

    with tracker.trace("trace_demote", run_id="trace_demote_2", outputs=["t_out"]) as t:
        # Initially a cache hit before log_output is called
        assert t.is_cached is True
        t.set_output_meta("t_out", {"from_demoted_trace": 42})
        tp2 = t.run_dir / "t_out_demoted.csv"
        tp2.write_text("v\n2\n", encoding="utf-8")
        t.log_output(tp2, key="t_out")
        assert t.is_cached is False

    demoted_out = tracker.get_run_outputs("trace_demote_2")["t_out"]
    assert demoted_out.meta.get("from_demoted_trace") == 42


def test_lazy_callable_archive_root_and_in_run_set_archive_root(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True)
    db_path = str(tmp_path / "provenance.duckdb")
    tracker = Tracker(
        run_dir=workspace,
        db_path=db_path,
        mounts={"workspace": str(workspace)},
    )

    state: dict[str, Path | None] = {"archive_dir": None}

    def beam_like_step() -> None:
        # Discover archive root during execution
        state["archive_dir"] = tmp_path / "discovered_beam_archive"
        out_file = workspace / "beam_out" / "events.csv"
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text("time,type\n1,departure\n", encoding="utf-8")

    res = tracker.run(
        fn=beam_like_step,
        name="beam_like_step",
        run_id="run_lazy_callable_archive",
        outputs=["events"],
        output_paths={"events": workspace / "beam_out" / "events.csv"},
        output_policy=OutputPolicyOptions(
            run_completion_archive=RunCompletionArchivePolicy(
                archive_root=lambda: state["archive_dir"],
                mode="copy",
            )
        ),
    )
    discovered = state["archive_dir"]
    assert discovered is not None
    assert (discovered / "beam_out" / "events.csv").exists()
    assert res.outputs["events"].meta["recovery_roots"] == [
        str(workspace.resolve()),
        str(discovered.resolve()),
    ]
    assert res.outputs["events"].abs_path == str(
        (workspace / "beam_out" / "events.csv").resolve()
    )

    # Callable returning None skips archiving cleanly
    def skipped_archive_step() -> None:
        out_file = workspace / "beam_out" / "skipped.csv"
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text("time\n1\n", encoding="utf-8")

    res_skipped = tracker.run(
        fn=skipped_archive_step,
        name="skipped_archive_step",
        run_id="run_skipped_callable_archive",
        outputs=["skipped"],
        output_paths={"skipped": workspace / "beam_out" / "skipped.csv"},
        output_policy=OutputPolicyOptions(
            run_completion_archive=RunCompletionArchivePolicy(
                archive_root=lambda: None,
                mode="copy",
            )
        ),
    )
    assert "recovery_roots" not in (res_skipped.outputs["skipped"].meta or {})

    # In-run ctx.set_archive_root override and disable
    in_run_archive = tmp_path / "in_run_override_archive"

    def step_with_ctx_archive(ctx) -> None:
        out_file = workspace / "beam_out" / "ctx_events.csv"
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text("time\n2\n", encoding="utf-8")
        ctx.set_archive_root(in_run_archive, prepend_source_mount_root=False)

    res_ctx = tracker.run(
        fn=step_with_ctx_archive,
        name="step_with_ctx_archive",
        run_id="run_ctx_set_archive_root",
        outputs=["ctx_events"],
        output_paths={"ctx_events": workspace / "beam_out" / "ctx_events.csv"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )
    assert (in_run_archive / "beam_out" / "ctx_events.csv").exists()
    assert res_ctx.outputs["ctx_events"].meta["recovery_roots"] == [
        str(in_run_archive.resolve())
    ]

    def step_disable_archive(ctx) -> None:
        out_file = workspace / "beam_out" / "disabled.csv"
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text("time\n3\n", encoding="utf-8")
        ctx.set_archive_root(None)

    res_disabled = tracker.run(
        fn=step_disable_archive,
        name="step_disable_archive",
        run_id="run_ctx_disable_archive_root",
        outputs=["disabled"],
        output_paths={"disabled": workspace / "beam_out" / "disabled.csv"},
        output_policy=OutputPolicyOptions(
            run_completion_archive=RunCompletionArchivePolicy(
                archive_root=tmp_path / "unused_static_archive",
                mode="copy",
            )
        ),
        execution_options=ExecutionOptions(inject_context="ctx"),
    )
    assert "recovery_roots" not in (res_disabled.outputs["disabled"].meta or {})


def test_output_artifact_spec_typed_directory_options(
    tmp_path: Path,
) -> None:
    from consist.types import OutputArtifactSpec

    run_dir = tmp_path / "runs"
    db_path = str(tmp_path / "provenance.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)

    spec_kw = OutputArtifactSpec(
        path=run_dir / "dir_kw",
        directory=True,
        allow_extra_files=True,
        meta={"custom_tag": "v1"},
    )
    assert spec_kw.directory is True
    assert spec_kw.allow_extra_files is True
    assert spec_kw.meta == {
        "custom_tag": "v1",
        "directory_artifact": True,
        "directory_allow_extra_files": True,
    }

    spec_factory = OutputArtifactSpec.for_directory(
        run_dir / "dir_factory.zarr",
        allow_extra_files=True,
        driver="zarr",
    )
    assert spec_factory.directory is True
    assert spec_factory.allow_extra_files is True
    assert spec_factory.driver == "zarr"
    assert spec_factory.meta == {
        "directory_artifact": True,
        "directory_allow_extra_files": True,
    }

    with pytest.raises(
        ValueError, match="allow_extra_files=True requires directory=True"
    ):
        OutputArtifactSpec(path=run_dir / "scalar.csv", allow_extra_files=True)

    def write_dirs() -> None:
        d1 = run_dir / "dir_kw"
        d1.mkdir(parents=True, exist_ok=True)
        (d1 / "part.txt").write_text("hello", encoding="utf-8")

    res = tracker.run(
        fn=write_dirs,
        name="write_dirs",
        run_id="run_typed_dir_spec",
        outputs=["dir_kw"],
        output_paths={"dir_kw": spec_kw},
    )
    logged = res.outputs["dir_kw"]
    assert logged.meta["directory_artifact"] is True
    assert logged.meta["directory_allow_extra_files"] is True
    assert logged.meta["custom_tag"] == "v1"
    assert "directory_manifest" in logged.meta


def test_open_h5py_read_only_falls_back_when_locking_flags_conflict(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import types
    from consist.digest import open_h5py_read_only

    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def fake_file(*args: object, **kwargs: object) -> str:
        calls.append((args, kwargs))
        if kwargs.get("locking") is False:
            raise OSError(
                "Unable to synchronously open file (file locking flag values don't match)"
            )
        return "opened-with-default-locking"

    fake_h5py = types.SimpleNamespace(File=fake_file)
    h5_path = tmp_path / "model_data.h5"
    result = open_h5py_read_only(h5_path, h5py_module=fake_h5py)
    assert result == "opened-with-default-locking"
    assert calls == [
        ((h5_path, "r"), {"locking": False}),
        ((h5_path, "r"), {}),
    ]


def test_output_policy_error_honors_runtime_output_paths_narrowing_decorator_outputs(
    tmp_path: Path,
) -> None:
    from consist.types import CacheOptions, OutputPolicyOptions

    run_dir = tmp_path / "runs"
    db_path = str(tmp_path / "provenance.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)

    @tracker.define_step(outputs=["out_a", "out_b"])
    def conditional_step() -> None:
        out_a = run_dir / "out_a.txt"
        out_a.parent.mkdir(parents=True, exist_ok=True)
        out_a.write_text("only a", encoding="utf-8")

    # 1. Cache miss: runtime output_paths narrows expected outputs to out_a
    with tracker.scenario("scen_narrow_miss") as sc:
        res_miss = sc.run(
            fn=conditional_step,
            output_paths={"out_a": run_dir / "out_a.txt"},
            output_policy=OutputPolicyOptions(output_missing="error"),
        )
        assert res_miss.cache_hit is False
        assert set(res_miss.outputs.keys()) == {"out_a"}
        assert "out_a" in sc.coupler

    # 2. Cache hit: cached run also validates only out_a without failing on out_b
    with tracker.scenario("scen_narrow_hit") as sc_hit:
        res_hit = sc_hit.run(
            fn=conditional_step,
            output_paths={"out_a": run_dir / "out_a.txt"},
            output_policy=OutputPolicyOptions(output_missing="error"),
        )
        assert res_hit.cache_hit is True
        assert set(res_hit.outputs.keys()) == {"out_a"}

    # 3. Missing declared runtime output_paths key still raises RuntimeError
    @tracker.define_step(outputs=["out_a", "out_b"])
    def missing_step() -> None:
        pass

    with pytest.raises(RuntimeError, match="out_a"):
        tracker.run(
            fn=missing_step,
            output_paths={"out_a": run_dir / "never_written.txt"},
            cache_options=CacheOptions(cache_mode="overwrite"),
            output_policy=OutputPolicyOptions(output_missing="error"),
        )


def test_scenario_log_meta_promotes_stage_and_phase_columns(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs"
    db_path = str(tmp_path / "provenance.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)

    with tracker.scenario("scen_stage_phase") as sc:
        sc.log_meta(stage="supply_demand", phase="iteration", custom_key="val")
        assert sc.meta["stage"] == "supply_demand"
        assert sc.meta["phase"] == "iteration"
        assert sc.meta["custom_key"] == "val"
        assert sc._header_record is not None
        assert sc._header_record.run.stage == "supply_demand"
        assert sc._header_record.run.phase == "iteration"

    persisted = tracker.get_run("scen_stage_phase")
    assert persisted is not None
    assert persisted.stage == "supply_demand"
    assert persisted.phase == "iteration"
    assert persisted.meta["custom_key"] == "val"


def test_scenario_multi_year_output_sets_do_not_collide_in_run_artifact_dir(
    tmp_path: Path,
) -> None:
    from consist.types import OutputSet

    run_dir = tmp_path / "runs"
    db_path = str(tmp_path / "provenance.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)

    def run_atlas(year: int) -> None:
        out_root = run_dir / "atlas_out" / f"y{year}"
        out_root.mkdir(parents=True, exist_ok=True)
        (out_root / f"vehicles_{year}.csv").write_text(
            f"year\n{year}\n", encoding="utf-8"
        )

    with tracker.scenario("multi_year_scen") as sc:
        res_2015 = sc.run(
            fn=run_atlas,
            name="atlas_run",
            model="atlas",
            year=2015,
            iteration=0,
            config={"year": 2015},
            output_sets={
                "atlas_outputs": OutputSet(
                    root=run_dir / "atlas_out" / "y2015",
                    include="vehicles_*.csv",
                )
            },
        )
        res_2017 = sc.run(
            fn=run_atlas,
            name="atlas_run",
            model="atlas",
            year=2017,
            iteration=0,
            config={"year": 2017},
            output_sets={
                "atlas_outputs": OutputSet(
                    root=run_dir / "atlas_out" / "y2017",
                    include="vehicles_*.csv",
                )
            },
        )

    assert tracker.run_artifact_dir(res_2015.run) != tracker.run_artifact_dir(
        res_2017.run
    )
    manifest_2015 = (
        tracker.run_artifact_dir(res_2015.run)
        / "atlas_outputs.output_set_manifest.json"
    )
    manifest_2017 = (
        tracker.run_artifact_dir(res_2017.run)
        / "atlas_outputs.output_set_manifest.json"
    )
    assert manifest_2015.exists()
    assert manifest_2017.exists()
    assert "vehicles_2015.csv" in manifest_2015.read_text(encoding="utf-8")
    assert "vehicles_2017.csv" in manifest_2017.read_text(encoding="utf-8")


def test_event_hooks_snapshot_allows_self_unregistration(tracker: Tracker) -> None:
    seen_start: list[str] = []
    seen_complete: list[str] = []
    seen_failed: list[str] = []

    def one_shot_start(run) -> None:
        seen_start.append(f"one_shot:{run.id}")
        tracker.off_run_start(one_shot_start)

    def adjacent_start(run) -> None:
        seen_start.append(f"adjacent:{run.id}")

    def one_shot_complete(run, _outputs) -> None:
        seen_complete.append(f"one_shot:{run.id}")
        tracker.off_run_complete(one_shot_complete)

    def adjacent_complete(run, _outputs) -> None:
        seen_complete.append(f"adjacent:{run.id}")

    def one_shot_failed(run, _err) -> None:
        seen_failed.append(f"one_shot:{run.id}")
        tracker.off_run_failed(one_shot_failed)

    def adjacent_failed(run, _err) -> None:
        seen_failed.append(f"adjacent:{run.id}")

    tracker.on_run_start(one_shot_start)
    tracker.on_run_start(adjacent_start)
    tracker.on_run_complete(one_shot_complete)
    tracker.on_run_complete(adjacent_complete)
    tracker.on_run_failed(one_shot_failed)
    tracker.on_run_failed(adjacent_failed)

    with tracker.start_run("hook_run_1", model="demo"):
        pass

    with pytest.raises(RuntimeError, match="boom"):
        with tracker.start_run("hook_run_fail", model="demo"):
            raise RuntimeError("boom")

    assert seen_start == [
        "one_shot:hook_run_1",
        "adjacent:hook_run_1",
        "adjacent:hook_run_fail",
    ]
    assert seen_complete == [
        "one_shot:hook_run_1",
        "adjacent:hook_run_1",
    ]
    assert seen_failed == [
        "one_shot:hook_run_fail",
        "adjacent:hook_run_fail",
    ]
    tracker.off_run_start(adjacent_start)
    tracker.off_run_complete(adjacent_complete)
    tracker.off_run_failed(adjacent_failed)
