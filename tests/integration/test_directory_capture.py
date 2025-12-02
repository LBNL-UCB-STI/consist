import pytest
from pathlib import Path
from typing import Dict
from pydantic import BaseModel
import pandas as pd

from consist.core.tracker import Tracker
from consist.models.artifact import Artifact


# --- Helpers ---


class ModelConfig(BaseModel):
    learning_rate: float
    iterations: int = 100


# --- Tests ---


def test_typed_configuration(tracker: Tracker):
    """
    Tests that a Pydantic BaseModel instance provided as `config` to `tracker.start_run`
    is correctly serialized into a dictionary and used for config hashing.

    This test ensures that Consist can handle structured configuration objects,
    converting them into a canonical dictionary format for provenance tracking
    and identity computation.

    What happens:
    1. A `ModelConfig` Pydantic object is created with specific parameters.
    2. A `tracker.start_run` context is initiated, passing this Pydantic object
       as the `config`.

    What's checked:
    - The `tracker.current_consist.config` (the in-memory representation) is a dictionary.
    - The values from the `ModelConfig` object are correctly reflected in the `current_consist.config` dictionary.
    - The `config_hash` for the run is computed and is not None.
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
    Tests the "Pipe" mode for a `@tracker.task` decorated function.

    This mode represents a common scenario where a task function takes some input(s)
    and produces a single output file (represented as a `Path` object). Consist
    is expected to automatically convert this returned `Path` into an `Artifact`
    and log it as an output of the run.

    What happens:
    1. A `@tracker.task` decorated function `clean_csv` is defined. It simulates
       processing some content and writing it to a new file, returning the `Path` to this file.
    2. `clean_csv` is called with sample data.

    What's checked:
    - The return value of the decorated function is an `Artifact` object.
    - The `Artifact`'s `key` and `uri` correctly reflect the created file.
    - The `tracker.last_run` object correctly reflects that one output artifact was logged.
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
    Tests the "Splitter" mode for a `@tracker.task` decorated function.

    This mode handles scenarios where a task function produces multiple output files,
    returning them as a dictionary where keys are semantic names and values are
    `Path` objects. Consist is expected to convert each `Path` in the dictionary
    into an `Artifact` and log them all as outputs of the run.

    What happens:
    1. A `@tracker.task` decorated function `split_data` is defined. It simulates
       splitting data into training and testing sets, writing each to a file,
       and returning a dictionary mapping semantic names to `Path` objects.
    2. `split_data` is called.

    What's checked:
    - The return value of the decorated function is a dictionary.
    - The dictionary contains the expected keys.
    - The values in the returned dictionary are `Artifact` objects.
    - The `tracker.last_run` object correctly reflects the model name of the task.
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
    Tests the "Wrapper" mode for a `@tracker.task` decorated function, which uses `capture_dir`.

    This mode is designed for tasks that produce outputs as side-effects (e.g., legacy code,
    third-party tools) rather than returning them explicitly. Consist monitors a specified
    directory and automatically logs any new or modified files within it as output artifacts.

    What happens:
    1. A `capture_dir` is created.
    2. A `@tracker.task` decorated function `run_legacy_model` is defined with `capture_dir`
       and `capture_pattern` specified. It simulates writing two `.txt` files into the `capture_dir`.
       Crucially, it returns `None`.
    3. `run_legacy_model` is called.

    What's checked:
    - The return value of the decorated function is a list of `Artifact` objects.
    - The list contains two artifacts, corresponding to the two files written.
    - The `key` attribute of the captured artifacts matches the filenames.
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
    Verifies that the `@tracker.task` decorator enforces strict return type contracts
    and raises appropriate errors when these contracts are violated.

    This ensures that task functions adhere to expected patterns for clarity and
    correct provenance logging, preventing ambiguous or unsupported return types.

    What happens:
    1.  **Case A**: A task function `bad_return_type` is defined to return a list of `Path` objects,
        which is an unsupported return type for the decorator's automatic logging.
        It is then called within a `pytest.raises` context.
    2.  **Case B**: A task function `bad_capture_return` is defined with `capture_dir` enabled
        but incorrectly returns a `Path` object instead of `None`. It is then called
        within a `pytest.raises` context.

    What's checked:
    -   **Case A**: A `TypeError` is raised with a message indicating an unsupported return type.
    -   **Case B**: A `ValueError` is raised with a message indicating that a function
        with `capture_dir` must return `None`.
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
    Demonstrates using `tracker.start_run` as a context manager for manual, complex workflows.

    This test highlights scenarios where the `@tracker.task` decorator might be
    insufficient due to complex conditional logic, multiple granular logging steps,
    or other custom requirements within a single logical run. It shows how users
    can manually control the run lifecycle and artifact logging.

    What happens:
    1. A `tracker.start_run` context is manually initiated.
    2. Inside the run, an output file `complex.parquet` is created.
    3. Based on a `config` value (`mode="verbose"`), the artifact is logged with
       additional metadata (`quality` and `rows`).
    4. A second artifact (`metrics.json`) is explicitly logged.

    What's checked:
    - The `main_output` artifact is successfully retrieved from the tracker after the run.
    - The `main_output` artifact's metadata (`quality`) is correctly persisted and accessible.
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
            tracker.log_artifact(out_path, key="main_output", quality="high", rows=1000)
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
    Tests the introspection capabilities of the `Tracker`, specifically `tracker.last_run`
    and `tracker.history()`.

    This test verifies that Consist correctly maintains a record of the most
    recently completed run and can provide a history of past runs, which are
    essential for debugging, monitoring, and understanding workflow execution.

    What happens:
    1. Two simple `@tracker.task` functions (`task_a` and `task_b`) are defined and executed sequentially.
    2. After each task, `tracker.last_run` is immediately checked.
    3. After both tasks complete, `tracker.history()` is called to retrieve a DataFrame of past runs.

    What's checked:
    - `tracker.last_run` correctly reflects the `model_name` of the most recently completed task.
    - `tracker.history()` returns a Pandas DataFrame.
    - The DataFrame contains two entries (for `task_a` and `task_b`).
    - The order of runs in the history DataFrame is correct (most recent first).
    - The status of the runs in the history DataFrame is "completed".
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
    Verifies the functionality of `tracker.log_meta` for adding runtime metadata
    and ensures that the string representations (`__repr__`) of `Run` and `Artifact`
    objects are informative and do not raise errors.

    This test confirms that dynamic, runtime-generated metadata can be associated
    with a run and that core Consist objects provide useful debugging information.

    What happens:
    1. A `tracker.start_run` context is initiated.
    2. Inside the run, `tracker.log_meta` is used to add `accuracy` and `stage`
       to the run's metadata.
    3. An artifact (`data.csv`) is logged.
    4. The `repr()` of the current `Run` and the logged `Artifact` are captured.

    What's checked:
    - The `__repr__` output for `Run` and `Artifact` contains expected key information.
    - The `tracker.current_consist.run.meta` dictionary is updated with the logged metadata.
    - After the run completes, `tracker.last_run.run.meta` also contains the persisted metadata.
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
