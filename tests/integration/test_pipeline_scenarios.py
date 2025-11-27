import pytest
import pandas as pd
import time
from pathlib import Path
from typing import List
from consist.core.tracker import Tracker


class PipelineContext:
    def __init__(self):
        self.execution_log = []

    def log_execution(self, step_name: str):
        self.execution_log.append(step_name)

    def clear_log(self):
        self.execution_log = []


@pytest.fixture
def ctx():
    return PipelineContext()


@pytest.fixture
def tracker_setup(tmp_path):
    run_dir = tmp_path / "runs"
    db_path = str(tmp_path / "provenance.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)
    # FIX: Force stable identity
    tracker.identity.get_code_version = lambda: "stable_v1"
    return tracker, run_dir


def mock_model_step(
    tracker: Tracker,
    ctx: PipelineContext,
    step_name: str,
    inputs: List[str],
    config: dict = None,
    cache_mode: str = "reuse",  # <--- NEW ARGUMENT
) -> str:
    if config is None:
        config = {}

    resolved_inputs = []
    for i in inputs:
        resolved_inputs.append(i)

    # Unique Run ID
    run_id = f"run_{step_name}_{int(time.time() * 1000)}"

    # Pass cache_mode to start_run
    with tracker.start_run(
        run_id,
        model=step_name,
        config=config,
        inputs=resolved_inputs,
        cache_mode=cache_mode,
    ) as t:
        if t.is_cached:
            assert (
                len(t.current_consist.outputs) > 0
            ), f"Step {step_name}: Cached run missing outputs!"
            output_artifact = t.current_consist.outputs[0]
            return t.resolve_uri(output_artifact.uri)
        else:
            ctx.log_execution(step_name)
            df = pd.DataFrame({"id": [1], "val": [step_name], "param": [str(config)]})
            # Unique Filename per Run
            filename = f"{step_name}_{run_id}.parquet"
            out_path = t.run_dir / filename
            df.to_parquet(out_path)

            t.log_artifact(out_path, key=step_name)
            return str(out_path)


def test_pipeline_forking_cold_storage(tracker_setup, ctx):
    tracker, _ = tracker_setup

    print("\n--- Phase 1: Full Run ---")
    out_a = mock_model_step(tracker, ctx, "A", [], {"p": 1})
    out_b = mock_model_step(tracker, ctx, "B", [out_a], {"p": 1})
    out_c = mock_model_step(tracker, ctx, "C", [out_b], {"p": 1})
    out_d = mock_model_step(tracker, ctx, "D", [out_c], {"p": 1})

    assert ctx.execution_log == ["A", "B", "C", "D"]
    ctx.clear_log()

    print("\n--- Phase 2: Forked Run ---")
    out_a_2 = mock_model_step(tracker, ctx, "A", [], {"p": 1})
    out_b_2 = mock_model_step(tracker, ctx, "B", [out_a_2], {"p": 1})
    out_c_2 = mock_model_step(tracker, ctx, "C", [out_b_2], {"p": 999})
    out_d_2 = mock_model_step(tracker, ctx, "D", [out_c_2], {"p": 1})

    assert "A" not in ctx.execution_log
    assert "B" not in ctx.execution_log
    assert "C" in ctx.execution_log
    assert "D" in ctx.execution_log
    assert out_b_2 == out_b
    assert out_c_2 != out_c


def test_cache_overwrite_mode(tracker_setup, ctx):
    """
    SCENARIO 5: Explicit Overwrite.

    1. Run A (Normal).
    2. Run A (Again) with cache_mode="overwrite".

    Expectation:
    - Run 1: Executes.
    - Run 2: Executes (skips cache check), produces NEW artifact.
    """
    tracker, _ = tracker_setup

    # 1. Run A (Normal)
    print("\n--- Phase 1: Normal Run ---")
    out_a_1 = mock_model_step(tracker, ctx, "A", [], {"p": 1})
    assert "A" in ctx.execution_log
    ctx.clear_log()

    # 2. Run A (Overwrite)
    # Even though Config/Inputs/Code are identical, we force execution.
    print("\n--- Phase 2: Overwrite Run ---")
    out_a_2 = mock_model_step(tracker, ctx, "A", [], {"p": 1}, cache_mode="overwrite")

    # ASSERTIONS
    assert (
        "A" in ctx.execution_log
    ), "Step A should have executed despite matching previous run"
    assert out_a_2 != out_a_1, "Overwrite run should produce a physically new artifact"

    # 3. Verify Database State
    # The 'overwrite' run should now be the 'latest' one found by default lookups.
    with tracker.engine.connect() as conn:
        # We should see 2 completed runs for model A
        count = pd.read_sql(
            "SELECT count(*) as c FROM run WHERE model_name='A'", conn
        ).iloc[0]["c"]
        assert count == 2


def test_ghost_mode_ingested(tracker_setup, ctx):
    tracker, _ = tracker_setup

    out_a = mock_model_step(tracker, ctx, "A", [], {"p": 1})
    with tracker.start_run("ingest_ops", model="sys"):
        art_obj = tracker.log_artifact(out_a, key="test_data_a", direction="input")
        tracker.ingest(art_obj)

    ctx.clear_log()
    Path(out_a).unlink()

    out_a_ghost = mock_model_step(tracker, ctx, "A", [], {"p": 1})
    assert "A" not in ctx.execution_log
    assert out_a_ghost == out_a

    with tracker.engine.connect() as conn:
        tables = pd.read_sql(
            "SELECT table_schema, table_name FROM information_schema.tables", conn
        )
        # Robust search for the table name (dlt normalization)
        target_table = None
        for name in tables[tables["table_schema"] == "global_tables"]["table_name"]:
            if "test_data_a" in name or "a" == name:
                target_table = name
                break

        if not target_table:
            raise ValueError(f"Table not found in {tables}")

        df_db = pd.read_sql(f"SELECT * FROM global_tables.{target_table}", conn)
        assert not df_db.empty
        assert df_db.iloc[0]["val"] == "A"


def test_broken_cache_recovery(tracker_setup, ctx):
    tracker, _ = tracker_setup
    out_a = mock_model_step(tracker, ctx, "A", [], {"p": 1})
    ctx.clear_log()
    Path(out_a).unlink()
    out_a_retry = mock_model_step(tracker, ctx, "A", [], {"p": 1})
    assert "A" in ctx.execution_log
    assert Path(out_a_retry).exists()
