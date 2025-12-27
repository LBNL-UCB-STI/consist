from pathlib import Path

import pandas as pd
import pytest
from consist.core.tracker import Tracker


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
        with sc.step("step_1", config={"local": 2}) as step_tracker:
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
        with sc.step("s1"):
            pass

        with pytest.raises(RuntimeError, match="Cannot add scenario inputs"):
            sc.add_input("late.csv", key="late")


def test_scenario_context(tracker: Tracker):
    with tracker.scenario("header", config={"a": 1}) as sc:
        assert sc.run_id == "header"
        assert tracker.current_consist is None  # Suspended

        with sc.step("step1") as step_t:
            assert step_t.current_consist.run.parent_run_id == "header"

    run = tracker.get_run("header")
    assert run is not None
    # assert run.status == "completed" # TODO: Figure out why this is still "running"


def test_scenario_step_input_keys_declares_inputs(tracker: Tracker):
    """
    `ScenarioContext.step(..., input_keys=[...])` should declare inputs before begin_run()
    computes the cache signature (so caching and `db_fallback='inputs-only'` stay correct).
    """
    with tracker.scenario("scen_input_keys") as sc:
        with sc.step("produce") as t:
            produced = t.log_artifact("raw.csv", key="raw", direction="output")
            sc.coupler.set("raw", produced)

        with sc.step("consume", input_keys=["raw"], cache_mode="overwrite") as t:
            assert t.current_consist is not None
            assert any(a.id == produced.id for a in t.current_consist.inputs)


def test_scenario_step_optional_input_keys_skips_missing(tracker: Tracker):
    with tracker.scenario("scen_optional_inputs") as sc:
        with sc.step("produce") as t:
            produced = t.log_artifact("raw.csv", key="raw", direction="output")
            sc.coupler.set("raw", produced)

        with sc.step(
            "consume",
            optional_input_keys=["raw", "missing"],
            cache_mode="overwrite",
        ) as t:
            assert t.current_consist is not None
            assert any(a.id == produced.id for a in t.current_consist.inputs)


def test_scenario_step_facet_from_config(tracker: Tracker):
    np = pytest.importorskip("numpy")
    with tracker.scenario("scen_facet_from") as sc:
        with sc.step(
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
        with sc.step("write_df", iteration=0) as t:
            artifact = t.log_dataframe(df, key="series")

    assert artifact is not None
    assert artifact.key == "series"
    assert artifact.abs_path is not None
    abs_path = Path(artifact.abs_path)
    assert abs_path.exists()
    assert abs_path.parent == tracker.run_dir / "scen_df" / "iteration_0"
    assert abs_path.name == "series.parquet"


def test_scenario_step_cache_hydration_default(tracker: Tracker):
    with tracker.scenario(
        "scen_cache_default",
        step_cache_hydration="inputs-missing",
    ) as sc:
        with sc.step("step_a") as t:
            assert t._active_run_cache_options is not None
            assert t._active_run_cache_options.cache_hydration == "inputs-missing"

        with sc.step("step_b", cache_hydration="metadata") as t:
            assert t._active_run_cache_options is not None
            assert t._active_run_cache_options.cache_hydration == "metadata"
