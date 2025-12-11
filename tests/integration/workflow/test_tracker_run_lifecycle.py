"""Imperative run lifecycle helpers: begin_run/end_run contracts and config propagation."""

import json
from pathlib import Path

import pytest

from consist.models.run import Run


class TestBeginEndRun:
    """Tests for begin_run() and end_run() imperative methods."""

    def test_begin_end_run_basic_lifecycle(self, tracker, run_dir: Path):
        test_file = run_dir / "imperative_test.csv"
        test_file.write_text("data\n")

        run = tracker.begin_run("run_imperative_001", "test_model")
        assert run.id == "run_imperative_001"
        assert run.status in ("started", "running")

        tracker.log_artifact(str(test_file), key="test_data")
        completed_run = tracker.end_run()
        assert completed_run.status == "completed"

        json_log = tracker.run_dir / "consist.json"
        assert json_log.exists()
        with open(json_log) as f:
            data = json.load(f)
        assert data["run"]["status"] == "completed"

    def test_begin_end_run_with_error_handling(self, tracker, run_dir: Path):
        test_file = run_dir / "error_test.csv"
        test_file.write_text("data\n")

        tracker.begin_run("run_error_test", "test_model")
        tracker.log_artifact(str(test_file), key="test_data")

        error = ValueError("Something went wrong")
        failed_run = tracker.end_run("failed", error=error)

        assert failed_run.status == "failed"
        assert "Something went wrong" in failed_run.meta.get("error", "")

        with open(tracker.run_dir / "consist.json") as f:
            data = json.load(f)
        assert data["run"]["status"] == "failed"

    def test_begin_run_twice_raises_error(self, tracker):
        run1 = tracker.begin_run("run_double_begin", "model_1")
        assert run1.id == "run_double_begin"

        try:
            with pytest.raises(RuntimeError, match="already active"):
                tracker.begin_run("run_double_begin_2", "model_2")
        finally:
            if tracker.current_consist:
                tracker.end_run()

    def test_end_run_without_begin_raises_error(self, tracker):
        with pytest.raises(RuntimeError, match="No active run"):
            tracker.end_run()

    def test_begin_run_with_config_and_metadata(self, tracker, run_dir: Path):
        test_file = run_dir / "config_test.csv"
        test_file.write_text("data\n")

        tracker.begin_run(
            "run_with_config",
            "test_model",
            config={"param1": "value1", "param2": 42},
            tags=["test", "imperative"],
            description="Testing imperative API with config",
            year=2024,
            custom_field="custom_value",
        )

        tracker.log_artifact(str(test_file), key="data")
        tracker.end_run()

        with open(tracker.run_dir / "consist.json") as f:
            data = json.load(f)

        assert data["run"]["tags"] == ["test", "imperative"]
        assert data["run"]["description"] == "Testing imperative API with config"
        assert data["config"]["param1"] == "value1"
        assert data["config"]["param2"] == 42

    def test_begin_run_with_inputs(self, tracker, run_dir: Path):
        input_files = []
        for i in range(2):
            f = run_dir / f"input_{i}.csv"
            f.write_text(f"data_{i}\n")
            input_files.append(str(f))

        tracker.begin_run(
            "run_with_inputs",
            "test_model",
            inputs=input_files,
        )

        assert len(tracker.current_consist.inputs) >= 2
        tracker.end_run()

    def test_end_run_idempotent(self, tracker, run_dir: Path):
        test_file = run_dir / "idempotent_test.csv"
        test_file.write_text("data\n")

        tracker.begin_run("run_idempotent", "test_model")
        tracker.log_artifact(str(test_file), key="data")

        run1 = tracker.end_run()
        assert run1.status == "completed"

        with pytest.raises(RuntimeError, match="No active run"):
            tracker.end_run()

    def test_begin_run_returns_run_object(self, tracker):
        run = tracker.begin_run("run_return_test", "model_name")

        assert isinstance(run, Run)
        assert run.id == "run_return_test"
        assert run.model_name == "model_name"
        assert run.status in ("started", "running")
        assert run.started_at is not None

        tracker.end_run()
