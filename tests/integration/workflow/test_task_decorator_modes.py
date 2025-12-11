from pathlib import Path
from typing import Dict

import pytest

from consist.core.tracker import Tracker
from consist.models.artifact import Artifact


def test_task_mode_pipe(tracker: Tracker, run_dir: Path):
    @tracker.task()
    def clean_csv(content: str) -> Path:
        out = run_dir / "clean.csv"
        out.write_text(content)
        return out

    result = clean_csv("raw_data")

    assert isinstance(result, Artifact)
    assert result.key == "clean"
    assert result.uri.endswith("clean.csv")
    assert tracker.last_run is not None
    assert len(tracker.last_run.outputs) == 1
    assert tracker.last_run.outputs[0].id == result.id


def test_task_mode_splitter(tracker: Tracker, run_dir: Path):
    @tracker.task()
    def split_data() -> Dict[str, Path]:
        train = run_dir / "train.csv"
        test = run_dir / "test.csv"
        train.write_text("train")
        test.write_text("test")
        return {"train_set": train, "test_set": test}

    result = split_data()

    assert isinstance(result, dict)
    assert "train_set" in result
    assert isinstance(result["train_set"], Artifact)
    assert tracker.last_run.run.model_name == "split_data"


def test_task_mode_wrapper(tracker: Tracker, run_dir: Path):
    capture_dir = run_dir / "legacy_out"
    capture_dir.mkdir()

    @tracker.task(capture_dir=capture_dir, capture_pattern="*.txt")
    def run_legacy_model():
        (capture_dir / "a.txt").write_text("A")
        (capture_dir / "b.txt").write_text("B")

    results = run_legacy_model()

    assert isinstance(results, list)
    assert len(results) == 2
    keys = sorted([a.key for a in results])
    assert keys == ["a", "b"]


def test_task_strictness_errors(tracker: Tracker, run_dir: Path):
    @tracker.task()
    def bad_return_type():
        return [Path("a"), Path("b")]

    with pytest.raises(TypeError, match="unsupported type"):
        bad_return_type()

    @tracker.task(capture_dir=run_dir)
    def bad_capture_return():
        return Path("file.txt")

    with pytest.raises(ValueError, match="must return None"):
        bad_capture_return()
