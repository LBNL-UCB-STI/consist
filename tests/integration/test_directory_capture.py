import pytest
from pathlib import Path
from typing import List, Dict
from pydantic import BaseModel
import pandas as pd
import logging

from consist.core.tracker import Tracker
from consist.models.artifact import Artifact


# --- Helpers ---

class ModelConfig(BaseModel):
    learning_rate: float
    iterations: int = 100


# --- Tests ---

def test_typed_configuration(tracker: Tracker):
    """
    Verifies that a Pydantic model passed to start_run is correctly
    serialized into a dictionary.
    """
    config_obj = ModelConfig(learning_rate=0.05)

    with tracker.start_run("run_typed", model="test_model", config=config_obj):
        current_config = tracker.current_consist.config
        assert isinstance(current_config, dict)
        assert current_config["learning_rate"] == 0.05
        assert current_config["iterations"] == 100
        assert tracker.current_consist.run.config_hash is not None


# --- The 3 Strict Modes of @task ---

def test_task_mode_pipe(tracker: Tracker, run_dir: Path):
    """
    Mode 1: The 'Pipe' (1-in, 1-out).
    Contract: Function returns Path -> Decorator returns Artifact.
    """

    @tracker.task()
    def clean_csv(content: str) -> Path:
        out = run_dir / "clean.csv"
        out.write_text(content)
        return out

    # Execution
    result = clean_csv("raw_data")

    # Verification
    assert isinstance(result, Artifact)
    assert result.key == "clean"
    assert result.uri.endswith("clean.csv")

    # USE CASE: Introspection
    # Verify artifacts persist in last_run after execution finishes
    assert tracker.last_run is not None
    assert len(tracker.last_run.outputs) == 1
    assert tracker.last_run.outputs[0].id == result.id


def test_task_mode_splitter(tracker: Tracker, run_dir: Path):
    """
    Mode 2: The 'Splitter' (Explicit Dictionary).
    Contract: Function returns Dict[str, Path] -> Decorator returns Dict[str, Artifact].
    """

    @tracker.task()
    def split_data() -> Dict[str, Path]:
        train = run_dir / "train.csv"
        test = run_dir / "test.csv"
        train.write_text("train")
        test.write_text("test")
        return {"train_set": train, "test_set": test}

    # Execution
    result = split_data()

    # Verification
    assert isinstance(result, dict)
    assert "train_set" in result
    assert isinstance(result["train_set"], Artifact)

    # Introspection check
    assert tracker.last_run.run.model_name == "split_data"


def test_task_mode_wrapper(tracker: Tracker, run_dir: Path):
    """
    Mode 3: The 'Wrapper' (Side-Effect Capture).
    Contract: capture_dir set, returns None -> Decorator returns List[Artifact].
    """
    capture_dir = run_dir / "legacy_out"
    capture_dir.mkdir()

    @tracker.task(capture_dir=capture_dir, capture_pattern="*.txt")
    def run_legacy_model():
        # Implicitly writes files
        (capture_dir / "a.txt").write_text("A")
        (capture_dir / "b.txt").write_text("B")
        # Must return None

    # Execution
    results = run_legacy_model()

    # Verification
    assert isinstance(results, list)
    assert len(results) == 2
    keys = sorted([a.key for a in results])
    assert keys == ["a", "b"]


def test_task_strictness_errors(tracker: Tracker, run_dir: Path):
    """
    Verifies that breaking the strict return type contracts raises errors.
    """

    # Case A: Returning a list (Ambiguous - not allowed)
    @tracker.task()
    def bad_return_type():
        return [Path("a"), Path("b")]

    with pytest.raises(TypeError, match="unsupported type"):
        bad_return_type()

    # Case B: Returning value when capture_dir is set
    @tracker.task(capture_dir=run_dir)
    def bad_capture_return():
        return Path("file.txt")

    with pytest.raises(ValueError, match="must return None"):
        bad_capture_return()


# --- Fallback to Context Manager ---

def test_complex_manual_workflow(tracker: Tracker, run_dir: Path):
    """
    Case 4: The 'Manual/Complex' Fallback.
    Demonstrates a scenario where @task is insufficient.
    """
    config = {"mode": "verbose"}

    # User manually controls the scope
    with tracker.start_run("complex_run", model="manual_step", config=config):

        # 1. Do some logic
        out_path = run_dir / "complex.parquet"
        out_path.write_text("data")

        # 2. Conditional Logic unavailable in simple decorator
        if config["mode"] == "verbose":
            # FIX: Unpack kwargs properly
            tracker.log_artifact(
                out_path,
                key="main_output",
                quality="high",
                rows=1000
            )
        else:
            tracker.log_artifact(out_path, key="main_output")

        # 3. Explicit logging
        metric_path = run_dir / "metrics.json"
        metric_path.write_text("{}")
        tracker.log_artifact(metric_path, key="metrics")

    # Verification
    # We can inspect the DB to see if the custom metadata landed
    art = tracker.get_artifact("main_output")
    assert art is not None
    assert art.meta["quality"] == "high"


# --- Introspection Features ---

def test_tracker_introspection_and_history(tracker: Tracker, run_dir: Path):
    """
    Verifies tracker.last_run and tracker.history().
    """

    # 1. Run a few tasks
    @tracker.task()
    def task_a():
        return Path(run_dir / "a").write_text("a") and Path(run_dir / "a")

    @tracker.task()
    def task_b():
        return Path(run_dir / "b").write_text("b") and Path(run_dir / "b")

    task_a()
    # Check last_run immediately
    assert tracker.last_run.run.model_name == "task_a"

    task_b()
    # Check last_run updated
    assert tracker.last_run.run.model_name == "task_b"

    # 2. Check History DataFrame
    df = tracker.history()

    assert isinstance(df, pd.DataFrame)
    # We expect 2 runs
    assert len(df) == 2

    # Verify content (ordered by created_at DESC)
    assert df.iloc[0]["model_name"] == "task_b"
    assert df.iloc[1]["model_name"] == "task_a"
    assert df.iloc[0]["status"] == "completed"


def test_log_meta_and_reprs(tracker: Tracker, run_dir: Path):
    """
    Verifies runtime metadata logging (log_meta) and object string representations.
    """
    with tracker.start_run("meta_test_run", model="repr_checker"):
        # 1. Log runtime metadata
        tracker.log_meta(accuracy=0.98, stage="validation")

        # 2. Log an artifact
        p = run_dir / "data.csv"
        p.write_text("a,b")
        art = tracker.log_artifact(p, key="dataset")

        # 3. Verify Representations (just ensure they don't crash and contain key info)
        run_repr = repr(tracker.current_consist.run)
        art_repr = repr(art)

        print(f"\n[DEBUG] Run Repr: {run_repr}")
        print(f"[DEBUG] Art Repr: {art_repr}")

        assert "meta_test_run" in run_repr
        assert "repr_checker" in run_repr
        assert "dataset" in art_repr

        # 4. Verify in-memory update
        assert tracker.current_consist.run.meta["accuracy"] == 0.98

    # 5. Verify Persistence in last_run
    assert tracker.last_run.run.meta["accuracy"] == 0.98
    assert tracker.last_run.run.meta["stage"] == "validation"