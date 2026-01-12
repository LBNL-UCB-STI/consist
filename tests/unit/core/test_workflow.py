from pathlib import Path

import pandas as pd
import pytest
from consist.core.coupler import SchemaValidatingCoupler
from consist.core.config_canonicalization import CanonicalConfig, ConfigPlan
from consist.core.tracker import Tracker


def _dummy_config_plan(*, adapter_version: str) -> ConfigPlan:
    canonical = CanonicalConfig(
        root_dirs=[],
        primary_config=None,
        config_files=[],
        external_files=[],
        content_hash="plan_hash",
    )
    return ConfigPlan(
        adapter_name="dummy",
        adapter_version=adapter_version,
        canonical=canonical,
        artifacts=[],
        ingestables=[],
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
    # assert header_run.status == "completed" # TODO: Figure out why this is still "running"


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
    # assert run.status == "completed" # TODO: Figure out why this is still "running"


def test_scenario_coupler_kw_not_serialized(tracker: Tracker):
    """
    Ensure a coupler passed to scenario(...) is treated as runtime-only and
    not serialized into run metadata.
    """
    with tracker.scenario(
        "scen_coupler_meta",
        coupler=SchemaValidatingCoupler(schema={"demo": "doc"}),
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
            sc.coupler.set("raw", produced)

        with sc.trace("consume", input_keys=["raw"], cache_mode="overwrite") as t:
            assert t.current_consist is not None
            assert any(a.id == produced.id for a in t.current_consist.inputs)


def test_scenario_trace_optional_input_keys_skips_missing(tracker: Tracker):
    with tracker.scenario("scen_optional_inputs") as sc:
        with sc.trace("produce") as t:
            produced = t.log_artifact("raw.csv", key="raw", direction="output")
            sc.coupler.set("raw", produced)

        with sc.trace(
            "consume",
            optional_input_keys=["raw", "missing"],
            cache_mode="overwrite",
        ) as t:
            assert t.current_consist is not None
            assert any(a.id == produced.id for a in t.current_consist.inputs)


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
    with tracker.start_run(
        "override_abs",
        model="override_model",
        artifact_dir=absolute_dir,
        cache_mode="overwrite",
    ) as t:
        artifact = t.log_dataframe(df, key="series_abs")

    assert artifact is not None
    assert artifact.abs_path is not None
    abs_path = Path(artifact.abs_path)
    assert abs_path.parent == absolute_dir


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


def test_run_config_plan_includes_adapter_version(tracker: Tracker):
    plan_v1 = _dummy_config_plan(adapter_version="1.0")
    tracker.run(
        fn=lambda: None,
        name="plan_run_v1",
        config_plan=plan_v1,
        cache_mode="overwrite",
    )
    record_v1 = tracker.last_run
    assert record_v1 is not None
    assert record_v1.config["__consist_config_plan__"]["adapter_version"] == "1.0"
    hash_v1 = record_v1.run.config_hash

    plan_v2 = _dummy_config_plan(adapter_version="2.0")
    tracker.run(
        fn=lambda: None,
        name="plan_run_v2",
        config_plan=plan_v2,
        cache_mode="overwrite",
    )
    record_v2 = tracker.last_run
    assert record_v2 is not None
    hash_v2 = record_v2.run.config_hash

    assert hash_v1 != hash_v2


def test_scenario_run_with_config_plan(tracker: Tracker):
    plan = _dummy_config_plan(adapter_version="3.1")
    with tracker.scenario("scen_plan") as sc:
        sc.run(
            fn=lambda: None,
            name="step",
            config_plan=plan,
            cache_mode="overwrite",
        )
        record = tracker.last_run
        assert record is not None
        assert record.config["__consist_config_plan__"]["adapter_version"] == "3.1"
