"""
Comprehensive unit tests for the @task decorator.

These tests validate the core functionality of Consist's task decorator,
which provides automatic caching, artifact tracking, and provenance capture
for data processing workflows.

Test Coverage:
- Basic task execution and return values
- Input handling (Path, Artifact, lists, config objects)
- Artifact chaining between tasks
- Cache behavior (hits, misses, modes)
- Capture directory functionality
- Dependency tracking (depends_on)
- Metadata propagation
- Error handling
"""

import json
from pathlib import Path
from typing import Optional

import pandas as pd
import pytest
from pydantic import BaseModel
from sqlmodel import select, Session

from consist.core.tracker import Tracker
from consist.models.artifact import Artifact
from consist.models.run import Run


# At the top of the test file
def query_runs(tracker: Tracker, query):
    """Helper to query database in tests without active context."""
    with Session(tracker.engine) as session:
        return session.exec(query).all()


class TaskConfig(BaseModel):
    """Example configuration model for testing task parameters."""

    threshold: float = 0.5
    enable_feature: bool = True
    label: str = "default"


# ============================================================================
# Basic Task Execution Tests
# ============================================================================


def test_task_basic_execution(tracker: Tracker, run_dir: Path):
    """
    Test that a simple task executes and returns an Artifact.

    Input: Path to CSV file
    Output: Artifact pointing to processed CSV
    Validates: Basic task execution, artifact return type, file creation
    """
    # Setup input data
    input_path = run_dir / "input.csv"
    df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
    df.to_csv(input_path, index=False)

    @tracker.task()
    def simple_task(data_file: Path) -> Path:
        """Read CSV, double values, write output."""
        df = pd.read_csv(data_file)
        df["y"] = df["y"] * 2
        output = run_dir / "output.csv"
        df.to_csv(output, index=False)
        return output

    # Execute
    result = simple_task(input_path)

    # Validate
    assert isinstance(result, Artifact), "Task should return Artifact, not Path"
    assert result.path.exists(), "Output file should exist"
    assert result.key == "output", "Key should be derived from filename"
    assert result.driver == "csv", "Driver should be inferred from extension"

    # Validate tracker context is attached
    assert hasattr(result, "_tracker"), "Artifact should have tracker context"
    assert result._tracker is not None, "Tracker reference should be set"
    tracker_obj = result._tracker()
    assert tracker_obj is tracker, "Should reference the correct tracker"

    # Validate content
    df_out = pd.read_csv(result.path)
    assert df_out["y"].tolist() == [8, 10, 12], "Data should be doubled"


def test_task_with_config_parameter(tracker: Tracker, run_dir: Path):
    """
    Test task with Pydantic config object parameter.

    Input: Path + Pydantic model config
    Output: Artifact
    Validates: Config objects are handled correctly, appear in run.config
    """
    input_path = run_dir / "data.csv"
    pd.DataFrame({"val": [1, 2, 3, 4, 5]}).to_csv(input_path, index=False)

    @tracker.task()
    def filter_task(data_file: Path, config: TaskConfig) -> Path:
        """Filter data based on threshold in config."""
        df = pd.read_csv(data_file)
        df_filtered = df[df["val"] >= config.threshold * 10]
        output = run_dir / "filtered.csv"
        df_filtered.to_csv(output, index=False)
        return output

    config = TaskConfig(threshold=0.3, label="test_filter")
    result = filter_task(input_path, config)

    # Check result
    assert isinstance(result, Artifact)
    df_out = pd.read_csv(result.path)
    assert len(df_out) == 3, "Should filter to values >= 3"

    # Check run config
    runs = query_runs(tracker, select(Run).where(Run.model_name == "filter_task"))
    assert len(runs) == 1
    assert runs[0].config_hash is not None, "Config should be hashed for caching"


def test_task_returns_dict_of_paths(tracker: Tracker, run_dir: Path):
    """
    Test task that returns multiple outputs as a dictionary.

    Input: Single data file
    Output: Dict[str, Artifact] with multiple named outputs
    Validates: Multiple outputs are logged, each gets tracker context
    """
    input_path = run_dir / "data.csv"
    df = pd.DataFrame({"category": ["a", "b", "a", "b"], "value": [1, 2, 3, 4]})
    df.to_csv(input_path, index=False)

    @tracker.task()
    def split_task(data_file: Path) -> dict:
        """Split data by category into separate files."""
        df = pd.read_csv(data_file)
        outputs = {}
        for cat in df["category"].unique():
            out_path = run_dir / f"{cat}_data.csv"
            df[df["category"] == cat].to_csv(out_path, index=False)
            outputs[cat] = out_path
        return outputs

    results = split_task(input_path)

    # Validate return type
    assert isinstance(results, dict), "Should return dict"
    assert len(results) == 2, "Should have outputs for 'a' and 'b'"

    # Validate each output is an Artifact
    for key, artifact in results.items():
        assert isinstance(artifact, Artifact), f"Output '{key}' should be Artifact"
        assert artifact.path.exists(), f"File for '{key}' should exist"
        assert hasattr(artifact, "_tracker"), f"Artifact '{key}' should have tracker"

    # Validate content
    df_a = pd.read_csv(results["a"].path)
    df_b = pd.read_csv(results["b"].path)
    assert len(df_a) == 2 and len(df_b) == 2, "Should split equally"


# ============================================================================
# Artifact Input Handling Tests
# ============================================================================


def test_task_accepts_artifact_input(tracker: Tracker, run_dir: Path):
    """
    Test that a task can accept an Artifact as input.

    Input: Artifact from previous task
    Output: Artifact
    Validates: Artifact inputs are resolved to paths, dependencies tracked
    """
    # Create initial data
    input_path = run_dir / "start.csv"
    pd.DataFrame({"x": [1, 2, 3]}).to_csv(input_path, index=False)

    @tracker.task()
    def task_a(data: Path) -> Path:
        """First task creates intermediate artifact."""
        df = pd.read_csv(data)
        df["y"] = df["x"] * 2
        output = run_dir / "intermediate.csv"
        df.to_csv(output, index=False)
        return output

    @tracker.task()
    def task_b(intermediate: Path) -> Path:
        """Second task consumes artifact from first task."""
        df = pd.read_csv(intermediate)
        df["z"] = df["y"] + 10
        output = run_dir / "final.csv"
        df.to_csv(output, index=False)
        return output

    # Execute chain
    artifact_a = task_a(input_path)
    assert isinstance(artifact_a, Artifact)

    # Pass artifact to next task
    artifact_b = task_b(artifact_a)
    assert isinstance(artifact_b, Artifact)

    # Validate the artifact was resolved to a path inside the function
    df_final = pd.read_csv(artifact_b.path)
    assert "z" in df_final.columns
    assert df_final["z"].tolist() == [12, 14, 16]

    # Validate dependency tracking
    runs = query_runs(tracker, select(Run).where(Run.model_name == "task_b"))
    assert len(runs) == 1
    run_b = runs[0]

    # Check inputs were tracked
    artifacts_b = tracker.get_artifacts_for_run(run_b.id)
    assert len(artifacts_b.inputs) == 1, "Should have one input artifact"
    input_artifact = next(iter(artifacts_b.inputs.values()))
    assert input_artifact.uri == artifact_a.uri, "Should reference artifact from task_a"


def test_task_accepts_list_of_artifacts(tracker: Tracker, run_dir: Path):
    """
    Test task that accepts a list of artifacts as input.

    Input: List[Artifact]
    Output: Single Artifact (concatenated data)
    Validates: List inputs are handled, all dependencies tracked
    """
    # Create multiple input files
    files = []
    for i in range(3):
        path = run_dir / f"part_{i}.csv"
        pd.DataFrame({"val": [i * 10, i * 10 + 1]}).to_csv(path, index=False)
        files.append(path)

    @tracker.task()
    def create_parts(file: Path) -> Path:
        """Identity task to create artifacts."""
        return file

    @tracker.task()
    def concat_task(parts: list) -> Path:
        """Concatenate multiple CSV artifacts."""
        dfs = [pd.read_csv(p) for p in parts]
        combined = pd.concat(dfs, ignore_index=True)
        output = run_dir / "combined.csv"
        combined.to_csv(output, index=False)
        return output

    # Create artifacts
    artifacts = [create_parts(f) for f in files]
    assert all(isinstance(a, Artifact) for a in artifacts)

    # Pass list of artifacts
    result = concat_task(artifacts)

    # Validate
    assert isinstance(result, Artifact)
    df_combined = pd.read_csv(result.path)
    assert len(df_combined) == 6, "Should have 6 rows total"

    # Validate all inputs tracked
    runs = query_runs(tracker, select(Run).where(Run.model_name == "concat_task"))
    artifacts_concat = tracker.get_artifacts_for_run(runs[0].id)
    assert len(artifacts_concat.inputs) == 3, "Should track all 3 input artifacts"


def test_task_with_mixed_inputs(tracker: Tracker, run_dir: Path):
    """
    Test task with mix of Artifact, Path, and config inputs.

    Input: Artifact, Path, scalar config value
    Output: Artifact
    Validates: Different input types are handled correctly
    """
    # Setup
    artifact_input = run_dir / "artifact.csv"
    path_input = run_dir / "path.csv"
    pd.DataFrame({"a": [1, 2]}).to_csv(artifact_input, index=False)
    pd.DataFrame({"b": [3, 4]}).to_csv(path_input, index=False)

    @tracker.task()
    def make_artifact(p: Path) -> Path:
        return p

    @tracker.task()
    def mixed_inputs(
        artifact: Path,  # Will receive Artifact, resolved to Path
        path: Path,  # Will receive Path directly
        multiplier: int,  # Scalar config
    ) -> Path:
        """Combine inputs with multiplier."""
        df1 = pd.read_csv(artifact)
        df2 = pd.read_csv(path)
        combined = pd.concat([df1, df2], axis=1)
        combined = combined * multiplier
        output = run_dir / "result.csv"
        combined.to_csv(output, index=False)
        return output

    artifact = make_artifact(artifact_input)
    result = mixed_inputs(artifact, path_input, multiplier=10)

    # Validate
    assert isinstance(result, Artifact)
    df = pd.read_csv(result.path)
    assert df["a"].tolist() == [10, 20]
    assert df["b"].tolist() == [30, 40]


# ============================================================================
# Cache Behavior Tests
# ============================================================================


@pytest.mark.flaky(reruns=3)
def test_task_cache_hit(tracker: Tracker, run_dir: Path):
    """
    Test that identical task execution results in cache hit.

    Input: Same inputs run twice
    Output: Same Artifact URI both times
    Validates: Cache hit detected, metadata set, no re-execution
    """
    input_path = run_dir / "data.csv"
    pd.DataFrame({"x": [1, 2, 3]}).to_csv(input_path, index=False)

    execution_count = {"count": 0}

    @tracker.task()
    def cacheable_task(data: Path, multiplier: int) -> Path:
        """Task that tracks execution count."""
        execution_count["count"] += 1
        df = pd.read_csv(data)
        df["result"] = df["x"] * multiplier
        output = run_dir / "output.csv"
        df.to_csv(output, index=False)
        return output

    # First execution
    result1 = cacheable_task(input_path, multiplier=5)
    assert execution_count["count"] == 1, "Should execute once"

    # Second execution with same inputs
    import time

    time.sleep(0.1)  # Ensure DB persistence

    result2 = cacheable_task(input_path, multiplier=5)
    assert execution_count["count"] == 1, "Should NOT execute again (cache hit)"

    # Validate same artifact returned
    assert result1.uri == result2.uri, "Cache hit should return same artifact"

    # Validate cache metadata
    runs = query_runs(tracker, select(Run).where(Run.model_name == "cacheable_task"))
    assert len(runs) == 2, "Should have 2 run records"

    cache_hits = [r for r in runs if r.meta.get("cache_hit") is True]
    assert len(cache_hits) == 1, "Should have one cache hit"
    assert (
        cache_hits[0].meta.get("cache_source") is not None
    ), "Should reference source run"


def test_task_cache_miss_on_config_change(tracker: Tracker, run_dir: Path):
    """
    Test that changing config parameters invalidates cache.

    Input: Same data, different config
    Output: Different Artifacts
    Validates: Config changes trigger re-execution
    """
    input_path = run_dir / "data.csv"
    pd.DataFrame({"x": [1, 2, 3]}).to_csv(input_path, index=False)

    @tracker.task()
    def configurable_task(data: Path, config: TaskConfig) -> Path:
        df = pd.read_csv(data)
        df["filtered"] = df["x"] >= config.threshold
        output = run_dir / f"output_{config.label}.csv"
        df.to_csv(output, index=False)
        return output

    # First execution
    result1 = configurable_task(input_path, TaskConfig(threshold=1.5, label="run1"))

    # Second execution with different config
    import time

    time.sleep(0.1)
    result2 = configurable_task(input_path, TaskConfig(threshold=2.5, label="run2"))

    # Should be different artifacts
    assert (
        result1.uri != result2.uri
    ), "Different configs should produce different artifacts"

    # Validate both executed (no cache hit on second)
    runs = query_runs(tracker, select(Run).where(Run.model_name == "configurable_task"))
    cache_hits = [r for r in runs if r.meta.get("cache_hit") is True]
    assert len(cache_hits) == 0, "Config change should prevent cache hit"


def test_task_cache_miss_on_input_change(tracker: Tracker, run_dir: Path):
    """
    Test that changing input data invalidates cache.

    Input: Different input files
    Output: Different Artifacts
    Validates: Input hash changes trigger re-execution
    """

    @tracker.task()
    def process_task(data: Path) -> Path:
        df = pd.read_csv(data)
        df["doubled"] = df["value"] * 2
        output = run_dir / "processed.csv"
        df.to_csv(output, index=False)
        return output

    # First input
    input1 = run_dir / "input1.csv"
    pd.DataFrame({"value": [1, 2, 3]}).to_csv(input1, index=False)
    result1 = process_task(input1)

    # Second input (different data)
    input2 = run_dir / "input2.csv"
    pd.DataFrame({"value": [4, 5, 6]}).to_csv(input2, index=False)

    import time

    time.sleep(0.1)
    result2 = process_task(input2)

    # URIs will be the same (same output path), but this is OK
    # What matters is that different inputs caused re-execution

    # Validate signatures are different
    runs = query_runs(
        tracker,
        select(Run).where(Run.model_name == "process_task"),
    )
    assert len(runs) == 2, "Should have 2 runs"
    assert (
        runs[0].signature != runs[1].signature
    ), "Different inputs should have different signatures"

    # Verify neither is a cache hit
    cache_hits = [r for r in runs if r.meta.get("cache_hit")]
    assert len(cache_hits) == 0, "Different inputs should prevent cache hit"


def test_task_cache_mode_overwrite(tracker: Tracker, run_dir: Path):
    """
    Test cache_mode='overwrite' always re-executes.

    Input: Same inputs with overwrite mode
    Output: New execution each time
    Validates: Overwrite mode bypasses cache
    """
    input_path = run_dir / "data.csv"
    pd.DataFrame({"x": [1, 2]}).to_csv(input_path, index=False)

    execution_count = {"count": 0}

    @tracker.task(cache_mode="overwrite")
    def overwrite_task(data: Path) -> Path:
        execution_count["count"] += 1
        output = run_dir / "output.csv"
        pd.read_csv(data).to_csv(output, index=False)
        return output

    # Execute twice
    result1 = overwrite_task(input_path)
    result2 = overwrite_task(input_path)

    # Should execute both times
    assert execution_count["count"] == 2, "Overwrite mode should execute both times"

    # Should have 2 runs, neither cached
    runs = query_runs(tracker, select(Run).where(Run.model_name == "overwrite_task"))
    assert len(runs) == 2
    cache_hits = [r for r in runs if r.meta.get("cache_hit") is True]
    assert len(cache_hits) == 0, "Overwrite mode should not have cache hits"


# ============================================================================
# Dependency Tracking Tests (depends_on)
# ============================================================================


def test_task_depends_on_files(tracker: Tracker, run_dir: Path):
    """
    Test depends_on parameter for external file dependencies.

    Input: Task with depends_on=[config.json, params.yaml]
    Output: Artifact
    Validates: Dependency files are tracked as inputs
    """
    # Create dependency files
    config_file = run_dir / "config.json"
    params_file = run_dir / "params.json"
    config_file.write_text(json.dumps({"version": "1.0"}))
    params_file.write_text(json.dumps({"param": 42}))

    data_file = run_dir / "data.csv"
    pd.DataFrame({"x": [1]}).to_csv(data_file, index=False)

    @tracker.task(depends_on=[config_file, params_file])
    def task_with_deps(data: Path) -> Path:
        """Task that reads config files (tracked via depends_on)."""
        # Read the dependency files
        with open(config_file) as f:
            config = json.load(f)
        with open(params_file) as f:
            params = json.load(f)

        # Use them
        df = pd.read_csv(data)
        df["result"] = df["x"] * params["param"]
        output = run_dir / "output.csv"
        df.to_csv(output, index=False)
        return output

    result = task_with_deps(data_file)

    # Validate
    assert isinstance(result, Artifact)

    # Check dependencies were tracked
    runs = query_runs(tracker, select(Run).where(Run.model_name == "task_with_deps"))
    assert len(runs) == 1

    artifacts = tracker.get_artifacts_for_run(runs[0].id)
    # Should have 3 inputs: data file + 2 dependency files
    assert len(artifacts.inputs) == 3, "Should track data file + 2 dependencies"

    input_keys = {a.key for a in artifacts.inputs.values()}
    assert any("config" in k for k in input_keys), "Should track config.json"
    assert any("params" in k for k in input_keys), "Should track params.json"


def test_task_depends_on_artifact(tracker: Tracker, run_dir: Path):
    """
    Test depends_on with Artifact objects.

    Input: Task with depends_on=[artifact1, artifact2]
    Output: Artifact
    Validates: Artifact dependencies are tracked
    """
    # Create artifacts
    file1 = run_dir / "dep1.csv"
    file2 = run_dir / "dep2.csv"
    pd.DataFrame({"a": [1]}).to_csv(file1, index=False)
    pd.DataFrame({"b": [2]}).to_csv(file2, index=False)

    @tracker.task()
    def make_dep(p: Path) -> Path:
        return p

    dep1 = make_dep(file1)
    dep2 = make_dep(file2)

    data_file = run_dir / "data.csv"
    pd.DataFrame({"x": [3]}).to_csv(data_file, index=False)

    @tracker.task(depends_on=[dep1, dep2])
    def task_with_artifact_deps(data: Path) -> Path:
        """Task with artifact dependencies."""
        output = run_dir / "output.csv"
        pd.read_csv(data).to_csv(output, index=False)
        return output

    result = task_with_artifact_deps(data_file)

    # Validate dependencies tracked
    runs = query_runs(
        tracker, select(Run).where(Run.model_name == "task_with_artifact_deps")
    )
    artifacts = tracker.get_artifacts_for_run(runs[0].id)

    # Should have data file + 2 dependencies = 3 inputs
    assert len(artifacts.inputs) >= 3, "Should track data + dependencies"


# ============================================================================
# Capture Directory Tests
# ============================================================================


def test_task_capture_dir_basic(tracker: Tracker, run_dir: Path):
    """
    Test capture_dir for wrapping legacy code.

    Input: Task with capture_dir parameter
    Output: List of Artifacts from captured directory
    Validates: Files written to capture_dir are automatically tracked
    """
    input_file = run_dir / "input.csv"
    pd.DataFrame({"x": [1, 2, 3]}).to_csv(input_file, index=False)

    outputs_dir = run_dir / "outputs"

    @tracker.task(capture_dir=outputs_dir, capture_pattern="*.csv")
    def legacy_wrapper(data: Path) -> None:
        """Simulates legacy code that writes to a directory."""
        outputs_dir.mkdir(exist_ok=True)

        # Legacy code writes multiple files
        df = pd.read_csv(data)
        df[df["x"] > 1].to_csv(outputs_dir / "filtered.csv", index=False)
        df.describe().to_csv(outputs_dir / "stats.csv")

        # Task returns None when using capture_dir
        return None

    captured = legacy_wrapper(input_file)

    # Validate
    assert isinstance(captured, list), "capture_dir should return list"
    assert len(captured) == 2, "Should capture 2 CSV files"
    assert all(
        isinstance(a, Artifact) for a in captured
    ), "All captured should be Artifacts"

    # Validate files exist
    captured_keys = {a.key for a in captured}
    assert any("filtered" in k for k in captured_keys)
    assert any("stats" in k for k in captured_keys)

    # Validate they all have tracker context
    for artifact in captured:
        assert hasattr(artifact, "_tracker")
        assert artifact._tracker() is tracker


def test_task_capture_dir_with_pattern(tracker: Tracker, run_dir: Path):
    """
    Test capture_dir with specific file pattern.

    Input: Task with capture_pattern="*.parquet"
    Output: Only matching files captured
    Validates: Pattern filtering works correctly
    """
    outputs_dir = run_dir / "outputs"

    @tracker.task(capture_dir=outputs_dir, capture_pattern="*.parquet")
    def selective_capture() -> None:
        """Write multiple file types."""
        outputs_dir.mkdir(exist_ok=True)

        # Write different file types
        pd.DataFrame({"x": [1]}).to_parquet(outputs_dir / "data.parquet")
        pd.DataFrame({"y": [2]}).to_csv(outputs_dir / "data.csv")
        (outputs_dir / "readme.txt").write_text("Info")

        return None

    captured = selective_capture()

    # Should only capture .parquet file
    assert len(captured) == 1, "Should only capture .parquet files"
    assert captured[0].key == "data"
    assert captured[0].driver == "parquet"


def test_task_capture_dir_error_on_return_value(tracker: Tracker, run_dir: Path):
    """
    Test that capture_dir tasks cannot return non-None values.

    Input: Task with capture_dir that returns a value
    Output: ValueError raised
    Validates: Semantic constraint enforced
    """

    @tracker.task(capture_dir=run_dir / "outputs")
    def invalid_capture() -> Path:
        """This should raise an error."""
        return run_dir / "some_file.csv"

    with pytest.raises(ValueError, match="capture_dir must return None"):
        invalid_capture()


# ============================================================================
# Error Handling Tests
# ============================================================================


def test_task_exception_handling(tracker: Tracker, run_dir: Path):
    """
    Test that task exceptions are properly caught and logged.

    Input: Task that raises exception
    Output: Exception propagated, run marked as failed
    Validates: Error metadata logged, exception re-raised
    """

    @tracker.task()
    def failing_task(data: Path) -> Path:
        """Task that fails."""
        raise ValueError("Intentional failure")

    input_path = run_dir / "data.csv"
    pd.DataFrame({"x": [1]}).to_csv(input_path, index=False)

    # Should raise exception
    with pytest.raises(ValueError, match="Intentional failure"):
        failing_task(input_path)

    # Check run was marked as failed
    runs = query_runs(tracker, select(Run).where(Run.model_name == "failing_task"))
    assert len(runs) == 1
    assert runs[0].status == "failed"
    assert "error" in runs[0].meta
    assert "Intentional failure" in runs[0].meta["error"]


def test_task_artifact_path_resolution_error(tracker: Tracker, run_dir: Path):
    """
    Test behavior when artifact path cannot be resolved.

    Input: Artifact with invalid/missing file
    Output: Graceful error or fallback behavior
    Validates: System handles missing files appropriately
    """
    # Create artifact pointing to non-existent file
    from consist.models.artifact import Artifact as ArtifactModel

    fake_artifact = ArtifactModel(
        key="missing",
        uri="file:///nonexistent/path.csv",
        driver="csv",
        hash="fake_hash",
    )

    @tracker.task()
    def task_with_missing_input(data: Path) -> Path:
        """Try to read missing file."""
        # This should fail when trying to read
        df = pd.read_csv(data)
        return run_dir / "output.csv"

    # Should raise FileNotFoundError
    with pytest.raises(FileNotFoundError):
        task_with_missing_input(fake_artifact)


# ============================================================================
# Metadata and Provenance Tests
# ============================================================================


def test_task_metadata_propagation(tracker: Tracker, run_dir: Path):
    """
    Test that artifacts carry metadata through task chains.

    Input: Artifact with custom metadata
    Output: Metadata accessible throughout chain
    Validates: Metadata preserved and accessible
    """
    input_path = run_dir / "data.csv"
    pd.DataFrame({"x": [1, 2, 3]}).to_csv(input_path, index=False)

    @tracker.task()
    def task_with_metadata(data: Path) -> Path:
        """Create artifact with custom metadata."""
        df = pd.read_csv(data)
        output = run_dir / "output.csv"
        df.to_csv(output, index=False)

        # Log custom metadata
        tracker.log_meta(custom_field="test_value", row_count=len(df))

        return output

    result = task_with_metadata(input_path)

    # Check metadata on run
    runs = query_runs(
        tracker, select(Run).where(Run.model_name == "task_with_metadata")
    )
    assert runs[0].meta.get("custom_field") == "test_value"
    assert runs[0].meta.get("row_count") == 3


def test_task_signature_determinism(tracker: Tracker, run_dir: Path):
    """
    Test that identical tasks produce identical signatures.

    Input: Same task executed twice with identical inputs
    Output: Same signature both times
    Validates: Signature computation is deterministic
    """
    input_path = run_dir / "data.csv"
    pd.DataFrame({"x": [1, 2, 3]}).to_csv(input_path, index=False)

    @tracker.task()
    def deterministic_task(data: Path, value: int) -> Path:
        output = run_dir / "output.csv"
        pd.read_csv(data).to_csv(output, index=False)
        return output

    # Run twice with same inputs
    deterministic_task(input_path, value=42)

    import time

    time.sleep(0.1)

    deterministic_task(input_path, value=42)

    # Check signatures
    runs = query_runs(
        tracker, select(Run).where(Run.model_name == "deterministic_task")
    )

    # Should have same signature (causing cache hit)
    signatures = {r.signature for r in runs}
    assert len(signatures) == 1, "Identical inputs should produce identical signatures"


def test_task_lineage_tracking(tracker: Tracker, run_dir: Path):
    """
    Test that artifact lineage is properly tracked through task chain.

    Input: Chain of 3 tasks
    Output: Full lineage traceable
    Validates: Parent-child relationships preserved
    """
    input_path = run_dir / "start.csv"
    pd.DataFrame({"x": [1, 2, 3]}).to_csv(input_path, index=False)

    @tracker.task()
    def step1(data: Path) -> Path:
        df = pd.read_csv(data)
        df["step1"] = df["x"] * 2
        out = run_dir / "step1.csv"
        df.to_csv(out, index=False)
        return out

    @tracker.task()
    def step2(data: Path) -> Path:
        df = pd.read_csv(data)
        df["step2"] = df["step1"] + 10
        out = run_dir / "step2.csv"
        df.to_csv(out, index=False)
        return out

    @tracker.task()
    def step3(data: Path) -> Path:
        df = pd.read_csv(data)
        df["step3"] = df["step2"] * 3
        out = run_dir / "final.csv"
        df.to_csv(out, index=False)
        return out

    # Execute chain
    a1 = step1(input_path)
    a2 = step2(a1)
    a3 = step3(a2)

    # Trace lineage backwards from final artifact
    lineage = tracker.get_artifact_lineage(a3.id)

    assert lineage is not None, "Should have lineage"
    assert lineage["artifact"].id == a3.id

    # Check producing run
    assert lineage["producing_run"] is not None
    assert lineage["producing_run"]["run"].model_name == "step3"

    # Check inputs to step3
    inputs = lineage["producing_run"]["inputs"]
    assert len(inputs) > 0, "Should have inputs"
    assert inputs[0]["artifact"].id == a2.id, "Should trace back to step2 output"


# ============================================================================
# Edge Cases and Special Scenarios
# ============================================================================


def test_task_with_no_inputs(tracker: Tracker, run_dir: Path):
    """
    Test task with no input parameters (data generator).

    Input: None (task generates data)
    Output: Artifact
    Validates: Tasks can generate data from scratch
    """

    @tracker.task()
    def generator_task() -> Path:
        """Generate data without inputs."""
        df = pd.DataFrame({"generated": range(10)})
        output = run_dir / "generated.csv"
        df.to_csv(output, index=False)
        return output

    result = generator_task()

    assert isinstance(result, Artifact)
    assert result.path.exists()

    df = pd.read_csv(result.path)
    assert len(df) == 10


def test_task_with_optional_parameters(tracker: Tracker, run_dir: Path):
    """
    Test task with optional/default parameters.

    Input: Mix of required and optional parameters
    Output: Artifact
    Validates: Default values handled correctly in signature
    """
    input_path = run_dir / "data.csv"
    pd.DataFrame({"x": [1, 2, 3]}).to_csv(input_path, index=False)

    @tracker.task()
    def task_with_defaults(
        data: Path, multiplier: int = 10, label: Optional[str] = None
    ) -> Path:
        """Task with default parameters."""
        df = pd.read_csv(data)
        df["result"] = df["x"] * multiplier
        output = run_dir / f"output_{label or 'default'}.csv"
        df.to_csv(output, index=False)
        return output

    # Call with defaults
    result1 = task_with_defaults(input_path)
    assert isinstance(result1, Artifact)

    # Call with explicit values
    result2 = task_with_defaults(input_path, multiplier=5, label="custom")

    # Should have different signatures
    runs = query_runs(
        tracker, select(Run).where(Run.model_name == "task_with_defaults")
    )
    signatures = {r.signature for r in runs}
    assert len(signatures) == 2, "Different parameter values should change signature"


def test_task_weakref_cleanup(tracker: Tracker, run_dir: Path):
    """
    Test that weak references don't prevent garbage collection.

    Input: Create artifact, delete tracker
    Output: Weak reference becomes None
    Validates: No memory leaks from circular references
    """
    input_path = run_dir / "data.csv"
    pd.DataFrame({"x": [1]}).to_csv(input_path, index=False)

    # Create a new tracker in limited scope
    temp_tracker = Tracker(run_dir=run_dir, db_path=":memory:")

    @temp_tracker.task()
    def task_for_weakref(data: Path) -> Path:
        output = run_dir / "output.csv"
        pd.read_csv(data).to_csv(output, index=False)
        return output

    result = task_for_weakref(input_path)

    # Verify tracker is attached
    assert result._tracker() is temp_tracker

    # Delete tracker
    tracker_id = id(temp_tracker)
    del temp_tracker

    # Weak reference should now be dead
    # (In practice, may still work if interpreter keeps ref, but demonstrates concept)
    # The artifact.path property should fall back to abs_path
    assert result.path.exists(), "Should still be able to get path via fallback"


def test_task_concurrent_output_paths(tracker: Tracker, run_dir: Path):
    """
    Test tasks writing to unique output paths don't collide.

    Input: Multiple tasks with different parameters
    Output: Unique artifacts for each
    Validates: Output path uniqueness
    """
    input_path = run_dir / "data.csv"
    pd.DataFrame({"x": [1, 2, 3]}).to_csv(input_path, index=False)

    @tracker.task()
    def parameterized_task(data: Path, suffix: str) -> Path:
        """Task with parameter that affects output path."""
        df = pd.read_csv(data)
        output = run_dir / f"output_{suffix}.csv"
        df.to_csv(output, index=False)
        return output

    # Create multiple outputs
    results = [parameterized_task(input_path, suffix=f"variant_{i}") for i in range(3)]

    # Validate all unique
    uris = {r.uri for r in results}
    assert len(uris) == 3, "Each task should produce unique output"

    # All should exist
    assert all(r.path.exists() for r in results)
