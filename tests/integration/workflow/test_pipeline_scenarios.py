"""
Consist Pipeline Scenarios Integration Tests

This module contains integration tests for various pipeline scenarios, focusing on
how Consist handles complex workflows, caching behaviors, and error recovery.

It uses a `mock_model_step` helper function to simulate individual steps within a pipeline
and verify Consist's tracking, caching, and lineage capabilities across these steps.
These tests validate Consist's ability to ensure reproducibility and efficiency
in multi-stage data processing and modeling pipelines.
"""

import logging

import pytest
import pandas as pd
import time
from pathlib import Path
from typing import List, Optional
from consist.core.tracker import Tracker


class PipelineContext:
    """
    A simple context object used in pipeline tests to track which mock steps
    have actually been executed (i.e., were not a cache hit).

    This helps in verifying the caching logic by providing an observable side effect
    that indicates whether a particular step's code block was run.

    Attributes
    ----------
    execution_log : List[str]
        A list of strings, where each string represents the `step_name` of a
        `mock_model_step` that was actually executed (not cached).
    """

    def __init__(self):
        self.execution_log = []

    def log_execution(self, step_name: str) -> None:
        """
        Records that a specific pipeline step was executed (i.e., was not a cache hit).

        Parameters
        ----------
        step_name : str
            The name of the pipeline step that was executed.
        """
        self.execution_log.append(step_name)

    def clear_log(self) -> None:
        """
        Clears the execution log.

        This method is typically called between test phases to ensure that
        execution counts are reset for subsequent assertions.
        """
        self.execution_log = []


@pytest.fixture
def ctx() -> PipelineContext:
    """
    Pytest fixture that provides a `PipelineContext` instance for tracking mock step executions.

    This fixture ensures that each test function has a fresh `PipelineContext`
    object, allowing for accurate tracking of which simulated pipeline steps
    were actually run versus those that were cached.

    Returns
    -------
    PipelineContext
        A new `PipelineContext` instance.
    """
    return PipelineContext()


@pytest.fixture
def tracker_setup(tmp_path: Path):
    """
    Pytest fixture that sets up a Consist `Tracker` instance with a stable identity for testing.

    This fixture creates a `Tracker` configured with a temporary run directory and a
    DuckDB database. Crucially, it patches the `IdentityManager.get_code_version`
    method to return a stable string ("stable_v1"). This prevents spurious cache
    misses in tests that rely on code identity, especially if the local Git repository
    is "dirty" or changes between test runs.

    Parameters
    ----------
    tmp_path : Path
        The built-in pytest fixture for creating unique temporary directories.

    Returns
    -------
    Tuple[Tracker, Path]
        A tuple containing:
        - `Tracker`: An initialized Consist `Tracker` instance with a stable code identity.
        - `Path`: The path to the temporary run directory.
    """
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
    config: Optional[dict] = None,
    cache_mode: str = "reuse",
    validate_cached_outputs: str = "lazy",
) -> str:
    """
    Simulates a single step in a data processing pipeline within the Consist framework.

    This helper function encapsulates the common pattern of a Consist-managed pipeline step:
    it starts a run, logs inputs, checks for cached results, and either executes
    a dummy operation (logging its execution) or returns a path to a cached output.
    It is crucial for building and testing multi-step pipelines and their caching behavior.

    Parameters
    ----------
    tracker : Tracker
        The Consist `Tracker` instance managing the pipeline's provenance.
    ctx : PipelineContext
        A `PipelineContext` object used to log whether this step actually executed
        or was a cache hit.
    step_name : str
        The descriptive name of the simulated pipeline step. This is used as the
        `model_name` for the Consist run and as the key for the output artifact.
    inputs : List[str]
        A list of file paths (as strings) that serve as inputs to this step. These
        will be passed to `tracker.start_run` to establish lineage.
    config : Optional[dict], optional
        A dictionary representing the configuration for this step. This contributes
        to the run's unique identity for caching. Defaults to an empty dictionary.
    cache_mode : str, default "reuse"
        Defines the caching behavior for this specific step. Options are "reuse"
        (default), "overwrite", or "readonly".

    Returns
    -------
    str
        The resolved host filesystem path to the primary output artifact generated
        or retrieved by this step.
    """
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
        validate_cached_outputs=validate_cached_outputs,
    ) as t:
        if t.is_cached:
            assert len(t.current_consist.outputs) > 0, (
                f"Step {step_name}: Cached run missing outputs!"
            )
            output_artifact = t.current_consist.outputs[0]
            return t.resolve_uri(output_artifact.container_uri)
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
    """
    Tests Consist's pipeline forking mechanism and caching for "cold storage" workflows.

    This test demonstrates how Consist ensures that an entire downstream pipeline
    can be reused (cache hit) if an upstream change does not affect its inputs,
    and how only the affected parts of the pipeline are re-executed (cache miss)
    if a change occurs in an upstream step.

    What happens:
    1. A `Tracker` and `run_dir` are set up, and the `IdentityManager.get_code_version`
       is mocked for stable hashes.
    2. **Phase 1 (Full Run)**: A four-step pipeline (A -> B -> C -> D) is executed
       using `mock_model_step`. Each step generates an output that serves as an input
       to the next.
    3. **Phase 2 (Forked Run)**: The pipeline is re-executed, but this time, the
       configuration for step "C" is intentionally changed (`p=999`).

    What's checked:
    -   **Phase 1**: All four steps (A, B, C, D) are logged in `ctx.execution_log`,
        confirming actual execution.
    -   **Phase 2**:
        - Steps "A" and "B" are *not* in `ctx.execution_log`, indicating they
          were cache hits.
        - Steps "C" and "D" *are* in `ctx.execution_log`, confirming they were
          re-executed due to the change in "C"'s configuration (cache miss for C,
          and subsequent re-execution of its downstream D).
        - The output path of "B" from both runs is identical, confirming cache hit.
        - The output path of "C" from both runs is different, confirming re-execution.
    """
    tracker, _ = tracker_setup

    logging.info("\n--- Phase 1: Full Run ---")
    out_a = mock_model_step(
        tracker, ctx, "A", [], {"p": 1}, validate_cached_outputs="eager"
    )
    out_b = mock_model_step(tracker, ctx, "B", [out_a], {"p": 1})
    out_c = mock_model_step(tracker, ctx, "C", [out_b], {"p": 1})
    mock_model_step(tracker, ctx, "D", [out_c], {"p": 1})

    assert ctx.execution_log == ["A", "B", "C", "D"]
    ctx.clear_log()

    logging.info("\n--- Phase 2: Forked Run ---")
    out_a_2 = mock_model_step(tracker, ctx, "A", [], {"p": 1})
    out_b_2 = mock_model_step(tracker, ctx, "B", [out_a_2], {"p": 1})
    out_c_2 = mock_model_step(tracker, ctx, "C", [out_b_2], {"p": 999})
    mock_model_step(tracker, ctx, "D", [out_c_2], {"p": 1})

    assert "A" not in ctx.execution_log
    assert "B" not in ctx.execution_log
    assert "C" in ctx.execution_log
    assert "D" in ctx.execution_log
    assert out_b_2 == out_b
    assert out_c_2 != out_c


def test_cache_overwrite_mode(tracker_setup, ctx):
    """
    Tests the `cache_mode="overwrite"` functionality within a pipeline step.

    This test verifies that when `cache_mode` is set to "overwrite", Consist
    will always execute the step, ignoring any existing cache entries for the
    same signature, and will update the cache with the results of the current run.
    It ensures that new results can forcibly replace older cached ones.

    What happens:
    1. A `Tracker` and `run_dir` are set up.
    2. **Phase 1 (Normal Run)**: `mock_model_step` "A" is executed with default
       `cache_mode="reuse"`. It executes and populates the cache.
    3. **Phase 2 (Overwrite Run)**: `mock_model_step` "A" is executed again with
       identical parameters but with `cache_mode="overwrite"`.

    What's checked:
    - After Phase 1, step "A" is in `ctx.execution_log`.
    - After Phase 2, step "A" is *again* in `ctx.execution_log`, confirming it
      re-executed despite a potential cache hit.
    - The output path from Phase 2 is different from Phase 1, proving that a
      physically new artifact was generated, and thus the cache was updated.
    - The database eventually contains two completed runs for model "A".
    """
    tracker, _ = tracker_setup

    # 1. Run A (Normal)
    logging.info("\n--- Phase 1: Normal Run ---")
    out_a_1 = mock_model_step(tracker, ctx, "A", [], {"p": 1})
    assert "A" in ctx.execution_log
    ctx.clear_log()

    # 2. Run A (Overwrite)
    # Even though Config/Inputs/Code are identical, we force execution.
    logging.info("\n--- Phase 2: Overwrite Run ---")
    out_a_2 = mock_model_step(tracker, ctx, "A", [], {"p": 1}, cache_mode="overwrite")

    # ASSERTIONS
    assert "A" in ctx.execution_log, (
        "Step A should have executed despite matching previous run"
    )
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
    """
    Tests Consist's "Ghost Mode" for ingested artifacts.

    This test verifies that if a physical output file is deleted from disk
    but its content was previously ingested into the DuckDB database, Consist
    can still "hydrate" the output (i.e., provide a valid path to the artifact)
    and successfully cache future runs that depend on it.

    What happens:
    1. A `mock_model_step` "A" is executed, producing an output file.
    2. This output artifact is then explicitly ingested into the Consist DuckDB.
    3. The physical output file generated by step "A" is deleted from disk.
    4. `mock_model_step` "A" is executed again.

    What's checked:
    - The second execution of `mock_model_step` "A" is a cache hit (i.e., "A"
      is *not* in `ctx.execution_log`), even though its physical output file
      is missing. This confirms Ghost Mode.
    - The output path returned by the "ghosted" step is still valid (it's the
      original path where the file *would* be, even if empty).
    - The DuckDB contains the ingested data, confirming data persistence.
    """
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
    """
    Tests Consist's "Self-Healing" capability: automatically re-executing a run
    if its cached output files are missing from disk (and not ingested).

    This test ensures that Consist will detect a broken cache state (where a cached
    run claims outputs exist, but they are physically missing and not recoverable
    from the DB) and correctly force a re-execution to regenerate the missing data.

    What happens:
    1. A `mock_model_step` "A" is executed, producing an output file.
    2. The physical output file generated by step "A" is deleted from disk.
       (Crucially, this artifact was *not* ingested into the DB, so there's no "Ghost Mode" fallback).
    3. `mock_model_step` "A" is executed again.

    What's checked:
    - The second execution of `mock_model_step` "A" is *not* a cache hit (i.e., "A"
      is in `ctx.execution_log`), confirming that the cache was invalidated and
      the step re-executed.
    - The output file is regenerated and exists on disk after the re-execution.
    """
    tracker, _ = tracker_setup
    out_a = mock_model_step(tracker, ctx, "A", [], {"p": 1})
    ctx.clear_log()
    Path(out_a).unlink()
    out_a_retry = mock_model_step(
        tracker, ctx, "A", [], {"p": 1}, validate_cached_outputs="eager"
    )
    assert "A" in ctx.execution_log
    assert Path(out_a_retry).exists()
