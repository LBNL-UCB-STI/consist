from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from consist.types import ExecutionOptions


def test_tracker_stage_artifact_method_stages_to_requested_destination(
    tracker, sample_csv
) -> None:
    source = sample_csv("stage_source.csv", rows=2)
    artifact = tracker.artifacts.create_artifact(
        source,
        run_id=None,
        key="data",
        direction="input",
    )
    destination = tracker.run_dir / "staged" / "data.csv"

    result = tracker.stage_artifact(artifact, destination=destination)

    assert result.status == "staged"
    assert result.path == destination.resolve()
    assert destination.exists()
    assert pd.read_csv(destination)["value"].tolist() == [0, 1]


def test_tracker_run_requested_input_materialization_stages_before_execution(
    tracker, sample_csv
) -> None:
    source = sample_csv("runtime_stage.csv", rows=3)
    staged_path = tracker.run_dir / "workspace" / "runtime_stage.csv"
    seen: dict[str, Path] = {}

    def step(data: Path) -> None:
        seen["data"] = data
        assert data == staged_path
        assert pd.read_csv(data)["value"].tolist() == [0, 1, 2]

    result = tracker.run(
        fn=step,
        name="requested_input_stage_runtime",
        inputs={"data": source},
        execution_options=ExecutionOptions(
            input_binding="paths",
            input_materialization="requested",
            input_paths={"data": staged_path},
        ),
    )

    assert result.cache_hit is False
    assert seen["data"] == staged_path
    assert staged_path.exists()
    assert result.run.meta["staged_inputs"]["data"] == str(staged_path.resolve())


def test_tracker_run_requested_input_materialization_runs_on_cache_hit(
    tracker, sample_csv
) -> None:
    source = sample_csv("cache_hit_stage.csv", rows=2)
    staged_path = tracker.run_dir / "workspace" / "cache_hit_stage.csv"
    calls: list[Path] = []

    def step(data: Path, ctx) -> None:
        calls.append(data)
        assert data == staged_path
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "out.txt").write_text("ok\n", encoding="utf-8")

    first = tracker.run(
        fn=step,
        name="requested_input_stage_cache_hit",
        inputs={"data": source},
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(
            inject_context="ctx",
            input_binding="paths",
            input_materialization="requested",
            input_paths={"data": staged_path},
        ),
    )
    assert first.cache_hit is False
    assert staged_path.exists()

    staged_path.unlink()

    second = tracker.run(
        fn=step,
        name="requested_input_stage_cache_hit",
        inputs={"data": source},
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(
            inject_context="ctx",
            input_binding="paths",
            input_materialization="requested",
            input_paths={"data": staged_path},
        ),
    )

    assert second.cache_hit is True
    assert calls == [staged_path]
    assert staged_path.exists()
    assert second.run.meta["staged_inputs"]["data"] == str(staged_path.resolve())


def test_scenario_run_requested_input_materialization_stages_coupler_input(
    tracker,
) -> None:
    staged_path = tracker.run_dir / "workspace" / "scenario_data.csv"
    seen: dict[str, Path] = {}

    def produce(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        output = ctx.run_dir / "data.csv"
        pd.DataFrame({"value": [4, 5]}).to_csv(output, index=False)

    def consume(data: Path) -> None:
        seen["data"] = data
        assert data == staged_path
        assert pd.read_csv(data)["value"].tolist() == [4, 5]

    with tracker.scenario("scenario_requested_input_stage") as sc:
        sc.run(
            fn=produce,
            output_paths={"data": "data.csv"},
            execution_options=ExecutionOptions(inject_context="ctx"),
        )
        result = sc.run(
            fn=consume,
            inputs=["data"],
            execution_options=ExecutionOptions(
                input_binding="paths",
                input_materialization="requested",
                input_paths={"data": staged_path},
            ),
        )

    assert result.cache_hit is False
    assert seen["data"] == staged_path
    assert staged_path.exists()


def test_tracker_run_rejects_requested_input_materialization_for_unknown_key(
    tracker, sample_csv
) -> None:
    source = sample_csv("unknown_key_stage.csv", rows=1)

    with pytest.raises(
        ValueError, match="input_paths contains keys that are not present"
    ):
        tracker.run(
            fn=lambda data: None,
            name="requested_input_stage_unknown_key",
            inputs={"data": source},
            execution_options=ExecutionOptions(
                input_binding="paths",
                input_materialization="requested",
                input_paths={"other": tracker.run_dir / "workspace" / "other.csv"},
            ),
        )
