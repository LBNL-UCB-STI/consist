from pathlib import Path

import pandas as pd
from pydantic import BaseModel

from consist.core.tracker import Tracker


class ModelConfig(BaseModel):
    learning_rate: float
    iterations: int = 100


def test_typed_configuration(tracker: Tracker):
    config_obj = ModelConfig(learning_rate=0.05)

    with tracker.start_run("run_typed", model="test_model", config=config_obj):
        current_config = tracker.current_consist.config
        assert isinstance(current_config, dict)
        assert current_config["learning_rate"] == 0.05
        assert current_config["iterations"] == 100
        assert tracker.current_consist.run.config_hash is not None


def test_complex_manual_workflow(tracker: Tracker, run_dir: Path):
    config = {"mode": "verbose"}

    with tracker.start_run("complex_run", model="manual_step", config=config):
        out_path = run_dir / "complex.parquet"
        out_path.write_text("data")

        if config["mode"] == "verbose":
            tracker.log_artifact(out_path, key="main_output", quality="high", rows=1000)
        else:
            tracker.log_artifact(out_path, key="main_output")

        metric_path = run_dir / "metrics.json"
        metric_path.write_text("{}")
        tracker.log_artifact(metric_path, key="metrics")

    art = tracker.get_artifact("main_output")
    assert art is not None
    assert art.meta["quality"] == "high"


def test_tracker_introspection_and_history(tracker: Tracker, run_dir: Path):
    @tracker.task()
    def task_a():
        return Path(run_dir / "a").write_text("a") and Path(run_dir / "a")

    @tracker.task()
    def task_b():
        return Path(run_dir / "b").write_text("b") and Path(run_dir / "b")

    task_a()
    assert tracker.last_run.run.model_name == "task_a"

    task_b()
    assert tracker.last_run.run.model_name == "task_b"

    df = tracker.history()

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert df.iloc[0]["model_name"] == "task_b"
    assert df.iloc[1]["model_name"] == "task_a"
    assert df.iloc[0]["status"] == "completed"


def test_log_meta_and_reprs(tracker: Tracker, run_dir: Path):
    with tracker.start_run("meta_test_run", model="repr_checker"):
        tracker.log_meta(accuracy=0.98, stage="validation")

        p = run_dir / "data.csv"
        p.write_text("a,b")
        art = tracker.log_artifact(p, key="dataset")

        run_repr = repr(tracker.current_consist.run)
        art_repr = repr(art)

        assert "meta_test_run" in run_repr
        assert "repr_checker" in run_repr
        assert "dataset" in art_repr
        assert tracker.current_consist.run.meta["accuracy"] == 0.98

    assert tracker.last_run.run.meta["accuracy"] == 0.98
    assert tracker.last_run.run.meta["stage"] == "validation"
