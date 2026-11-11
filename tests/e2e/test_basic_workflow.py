import errno
import json
from pathlib import Path
from typing import Optional
from unittest.mock import patch

import pandas as pd
import pytest
from typer.testing import CliRunner

import consist
from consist.api import to_df
from consist.artifact import Artifact
from consist.cli import app as cli_app
from consist.core.identity import IdentityManager
from consist.core.tracker import Tracker
from consist.core.workflow import RunContext
from consist.models.artifact_schema import (
    ArtifactSchema,
    ArtifactSchemaField,
    ArtifactSchemaObservation,
)
from consist.models.run import Run, RunResult
from consist.types import CacheOptions, ExecutionOptions, OutputPolicyOptions


def _write_csv(path: Path, rows: int = 5) -> None:
    """
    Helper to generate a tiny, deterministic CSV.

    This keeps the workflow readable by avoiding inline DataFrame setup in the test.
    """
    df = pd.DataFrame({"value": range(rows), "category": ["a"] * rows})
    df.to_csv(path, index=False)


def test_dual_write_workflow(tracker: Tracker, run_dir: Path):
    """
    Basic Consist workflow template (end-to-end).
    """
    runner = CliRunner()
    tracker.identity.hashing_strategy = "fast"

    scenario_id = "demo_scenario"
    ingest_run_id = f"{scenario_id}_ingest"
    transform_run_id = f"{scenario_id}_transform"
    scenario_cfg_path = run_dir / "scenario_config.json"
    scenario_cfg_path.write_text(json.dumps({"seed": 7, "note": "external config"}))
    features_artifact: Optional[Artifact] = None
    raw_artifact: Optional[Artifact] = None

    with tracker.scenario(
        scenario_id,
        config={"seed": 7},
        facet_from=["seed"],
        facet={"region": "demo"},
        hash_inputs=[("scenario_config", scenario_cfg_path)],
        tags=["e2e", "scenario_header"],
    ) as scenario:
        scenario.declare_outputs("raw_table", "features", required=True)

        def ingest_step():
            df = pd.DataFrame({"value": range(6), "category": ["a"] * 6})
            consist.log_dataframe(
                df,
                key="raw_table",
                driver="csv",
                meta={"rows": 6},
            )

        ingest_result: RunResult = scenario.run(
            name="ingest",
            run_id=ingest_run_id,
            fn=ingest_step,
            tags=["ingest"],
            year=2024,
            config={"step": "ingest", "rows_written": 6},
            facet_from=["step", "rows_written"],
            outputs=["raw_table"],
        )
        raw_artifact = ingest_result.outputs["raw_table"]
        assert scenario.coupler.require("raw_table").id == raw_artifact.id

        def transform_step(raw_table: pd.DataFrame):
            df_raw = raw_table
            df_raw["value_doubled"] = df_raw["value"] * 2
            consist.log_dataframe(
                df_raw,
                key="features",
                driver="csv",
                meta={"rows": len(df_raw)},
            )

        transform_result: RunResult = scenario.run(
            name="transform",
            run_id=transform_run_id,
            fn=transform_step,
            tags=["transform"],
            year=2025,
            inputs={"raw_table": "raw_table"},
            config={"step": "transform", "multiplier": 2},
            facet_from=["step", "multiplier"],
            outputs=["features"],
            execution_options=ExecutionOptions(load_inputs=True),
        )
        features_artifact = transform_result.outputs["features"]
        assert scenario.coupler.require("features").id == features_artifact.id

    # JSON snapshot
    json_file = tracker.run_dir / "consist.json"
    assert json_file.exists(), "JSON log was not created!"
    with open(json_file) as f:
        data = json.load(f)
    assert data["run"]["id"] == scenario_id
    assert data["run"]["status"] == "completed"
    assert len(data["outputs"]) >= 1
    assert data["config"]["seed"] == 7
    assert data["facet"]["seed"] == 7
    assert data["run"]["meta"]["consist_hash_inputs"]["scenario_config"] is not None
    assert isinstance(data["run"]["meta"]["mounts"], dict)
    assert data["run"]["config_hash"] == IdentityManager().compute_config_hash(
        {
            "seed": 7,
            "__consist_hash_inputs__": data["run"]["meta"]["consist_hash_inputs"],
            "__consist_run_fields__": {
                "model": "scenario",
                "year": None,
                "iteration": None,
            },
        }
    )
    assert data["run"]["git_hash"] is not None

    # Database snapshot
    runs = tracker.query_rows("SELECT * FROM main.run", model=Run)
    run_ids = {r.id for r in runs}
    assert {scenario_id, ingest_run_id, transform_run_id} <= run_ids
    assert all(r.status == "completed" for r in runs)

    artifacts_df = tracker.query_df("SELECT key FROM main.artifact")
    keys = set(artifacts_df["key"].tolist())
    assert {"raw_table", "features"} <= keys
    assert raw_artifact is not None
    outputs = tracker.get_run_outputs(transform_run_id)
    assert "features" in outputs
    loaded_features = to_df(tracker.load_run_output(transform_run_id, "features"))
    assert "value_doubled" in loaded_features.columns

    # Ensure parent linkage captured by scenario header
    child_parents = {r.id: r.parent_run_id for r in runs if r.id != scenario_id}
    assert child_parents[ingest_run_id] == scenario_id
    assert child_parents[transform_run_id] == scenario_id

    # Schema persistence: ingest an artifact and verify schema discovery is persisted.
    assert features_artifact is not None
    features_artifact = tracker.ingest(features_artifact)
    assert features_artifact.schema_id is not None
    assert "schema_summary" in features_artifact.meta

    schema_id = features_artifact.schema_id
    schemas = tracker.query_rows(
        "SELECT * FROM main.artifact_schema WHERE id = ?",
        [schema_id],
        model=ArtifactSchema,
    )
    assert len(schemas) == 1
    fields = tracker.query_rows(
        "SELECT * FROM main.artifact_schema_field WHERE schema_id = ?",
        [schema_id],
        model=ArtifactSchemaField,
    )
    assert {f.name for f in fields} >= {"value", "category", "value_doubled"}
    observations = tracker.query_rows(
        "SELECT * FROM main.artifact_schema_observation WHERE schema_id = ?",
        [schema_id],
        model=ArtifactSchemaObservation,
    )
    assert len(observations) >= 1

    # CLI sanity: runs/show/artifacts/summary/preview
    db_path = str(tracker.db_path)
    with patch("consist.cli.get_tracker", return_value=tracker):
        runs_result = runner.invoke(
            cli_app, ["runs", "--db-path", db_path, "--limit", "5", "--json"]
        )
        assert runs_result.exit_code == 0
        runs_payload = json.loads(runs_result.stdout)
        run_ids = {r.get("id") for r in runs_payload}
        assert scenario_id in run_ids
        assert transform_run_id in run_ids

        show_result = runner.invoke(
            cli_app, ["show", transform_run_id, "--db-path", db_path]
        )
        assert show_result.exit_code == 0
        assert transform_run_id in show_result.stdout
        assert "Status" in show_result.stdout

        artifacts_result = runner.invoke(
            cli_app, ["artifacts", transform_run_id, "--db-path", db_path]
        )
        assert artifacts_result.exit_code == 0
        assert "features" in artifacts_result.stdout
        assert "raw_table" in artifacts_result.stdout

        summary_result = runner.invoke(cli_app, ["summary", "--db-path", db_path])
        assert summary_result.exit_code == 0
        assert "Runs" in summary_result.stdout
        assert "Artifacts" in summary_result.stdout

        preview_result = runner.invoke(
            cli_app, ["preview", "features", "--db-path", db_path, "--rows", "2"]
        )
        assert preview_result.exit_code == 0
        assert "value" in preview_result.stdout
        assert "value_do" in preview_result.stdout

        schema_result = runner.invoke(
            cli_app,
            [
                "schema",
                "export",
                "--artifact-id",
                str(features_artifact.id),
                "--db-path",
                db_path,
            ],
        )
        assert schema_result.exit_code == 0
        assert "class " in schema_result.stdout
        assert "__tablename__" in schema_result.stdout
        assert schema_id in schema_result.stdout
        assert "value_doubled" in schema_result.stdout


def test_resume_after_failure_uses_cache_and_ghost_mode(
    tracker: Tracker, run_dir: Path
):
    """
    Regression test: resume a multi-step workflow after a failure, using cache.
    """
    tracker.identity.hashing_strategy = "fast"

    execution_counts = {"prepare_data": 0, "train_model": 0, "evaluate_model": 0}
    train_attempts = {"count": 0}

    prepared_path = run_dir / "prepared_data.csv"
    model_path = run_dir / "model.json"
    report_path = run_dir / "report.json"

    def prepare_data(rows: int = 8) -> Path:
        execution_counts["prepare_data"] += 1
        _write_csv(prepared_path, rows=rows)
        return prepared_path

    def train_model(_consist_ctx: RunContext) -> Path:
        execution_counts["train_model"] += 1

        df = to_df(_consist_ctx.load("prepared_data"))

        train_attempts["count"] += 1
        if train_attempts["count"] == 1:
            raise OSError(errno.ENOSPC, "No space left on device")

        model_payload = {"rows": int(len(df)), "mean_value": float(df["value"].mean())}
        model_path.write_text(json.dumps(model_payload))
        return model_path

    def evaluate_model(_consist_ctx: RunContext) -> Path:
        execution_counts["evaluate_model"] += 1

        model_artifact = _consist_ctx.inputs["model"]
        model_payload = json.loads(Path(model_artifact.path).read_text())
        df = to_df(_consist_ctx.load("prepared_data"))

        report_path.write_text(
            json.dumps(
                {
                    "ok": True,
                    "rows": int(len(df)),
                    "model_rows": int(model_payload["rows"]),
                }
            )
        )
        return report_path

    # --- First execution: predecessor completes, training fails ---
    prepared_result: RunResult = tracker.run(
        fn=prepare_data,
        name="prepare_data",
        outputs=["prepared_data"],
        execution_options=ExecutionOptions(runtime_kwargs={"rows": 8}),
    )
    prepared_artifact = prepared_result.outputs["prepared_data"]
    prepared_orig_path = prepared_artifact.path
    prepared_artifact = tracker.ingest(prepared_artifact)

    # Delete the on-disk file to force `consist.load(...)` to go through DuckDB.
    prepared_orig_path.unlink()
    assert not prepared_orig_path.exists()

    with pytest.raises(OSError) as exc_info:
        tracker.run(
            fn=train_model,
            name="train_model",
            inputs={"prepared_data": prepared_artifact},
            outputs=["model"],
            execution_options=ExecutionOptions(load_inputs=False, inject_context=True),
        )
    assert "No space left on device" in str(exc_info.value)

    prepare_runs = tracker.query_rows(
        "SELECT * FROM main.run WHERE model_name = ?",
        ["prepare_data"],
        model=Run,
    )
    assert len(prepare_runs) == 1
    assert prepare_runs[0].status == "completed"

    train_runs = tracker.query_rows(
        "SELECT * FROM main.run WHERE model_name = ?",
        ["train_model"],
        model=Run,
    )
    assert len(train_runs) == 1
    assert train_runs[0].status == "failed"
    assert "No space left on device" in train_runs[0].meta.get("error", "")

    eval_runs = tracker.query_rows(
        "SELECT * FROM main.run WHERE model_name = ?",
        ["evaluate_model"],
        model=Run,
    )
    assert len(eval_runs) == 0

    # --- Second execution: should reuse cached predecessor and succeed downstream ---
    prepared_result_2: RunResult = tracker.run(
        fn=prepare_data,
        name="prepare_data",
        outputs=["prepared_data"],
        execution_options=ExecutionOptions(runtime_kwargs={"rows": 8}),
    )
    prepared_artifact_2 = prepared_result_2.outputs["prepared_data"]
    assert prepared_artifact_2.identity.anchor == prepared_artifact.identity.anchor

    model_result: RunResult = tracker.run(
        fn=train_model,
        name="train_model",
        inputs={"prepared_data": prepared_artifact_2},
        outputs=["model"],
        execution_options=ExecutionOptions(load_inputs=False, inject_context=True),
    )
    report_result: RunResult = tracker.run(
        fn=evaluate_model,
        name="evaluate_model",
        inputs={
            "model": model_result.outputs["model"],
            "prepared_data": prepared_artifact_2,
        },
        outputs=["report"],
        execution_options=ExecutionOptions(load_inputs=False, inject_context=True),
    )
    report_artifact = report_result.outputs["report"]

    assert report_artifact.path.exists()
    assert execution_counts["prepare_data"] == 1, (
        "Predecessor should be reused via cache"
    )
    assert execution_counts["train_model"] == 2
    assert execution_counts["evaluate_model"] == 1

    prepare_runs = tracker.query_rows(
        "SELECT * FROM main.run WHERE model_name = ?",
        ["prepare_data"],
        model=Run,
    )
    assert len(prepare_runs) == 2
    assert any(r.meta.get("cache_hit") is True for r in prepare_runs)

    train_runs = tracker.query_rows(
        "SELECT * FROM main.run WHERE model_name = ?",
        ["train_model"],
        model=Run,
    )
    assert len(train_runs) == 2
    assert [r.status for r in train_runs].count("failed") == 1
    assert [r.status for r in train_runs].count("completed") == 1

    eval_runs = tracker.query_rows(
        "SELECT * FROM main.run WHERE model_name = ?",
        ["evaluate_model"],
        model=Run,
    )
    assert len(eval_runs) == 1
    assert eval_runs[0].status == "completed"


def test_scenario_run_skips_callable_on_cache_hit(tracker: Tracker):
    """
    End-to-end demonstration of `ScenarioContext.run(...)` cache skipping.
    """
    tracker.identity.hashing_strategy = "fast"

    calls: list[str] = []
    rel_out = Path("run_out.txt")

    def expensive_step(_consist_ctx: RunContext) -> None:
        calls.append("called")
        out_path = _consist_ctx.run_dir / rel_out
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(f"calls={len(calls)}\n")

    with tracker.scenario("run_demo_A") as sc:
        sc.declare_outputs("out", required=True)
        result: RunResult = sc.run(
            name="produce",
            fn=expensive_step,
            config={"v": 1},
            output_paths={"out": rel_out},
            execution_options=ExecutionOptions(inject_context=True),
        )
        assert result.outputs["out"].key == "out"
        assert sc.coupler.require("out").id == result.outputs["out"].id

    assert calls == ["called"]
    assert result.outputs["out"].path.read_text() == "calls=1\n"

    with tracker.scenario("run_demo_B") as sc:
        sc.declare_outputs("out", required=True)
        result = sc.run(
            name="produce",
            fn=expensive_step,
            config={"v": 1},
            output_paths={"out": rel_out},
            execution_options=ExecutionOptions(inject_context=True),
        )
        assert result.outputs["out"].key == "out"
        assert sc.coupler.require("out").id == result.outputs["out"].id

    assert calls == ["called"]
    assert result.outputs["out"].path.read_text() == "calls=1\n"


def test_run_options_objects_end_to_end(tracker: Tracker) -> None:
    calls: list[str] = []

    def step(ctx: RunContext) -> None:
        calls.append("called")
        out_path = ctx.run_dir / "out.txt"
        out_path.write_text("ok\n")

    first = tracker.run(
        fn=step,
        output_paths={"out": "out.txt"},
        cache_options=CacheOptions(cache_mode="reuse"),
        output_policy=OutputPolicyOptions(output_missing="error"),
        execution_options=ExecutionOptions(inject_context="ctx"),
    )
    second = tracker.run(
        fn=step,
        output_paths={"out": "out.txt"},
        cache_options=CacheOptions(cache_mode="reuse"),
        output_policy=OutputPolicyOptions(output_missing="error"),
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    assert first.cache_hit is False
    assert second.cache_hit is True
    assert calls == ["called"]
