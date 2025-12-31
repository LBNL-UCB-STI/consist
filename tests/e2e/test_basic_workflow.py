import errno
import json
from pathlib import Path
from typing import Optional
from unittest.mock import patch

import pandas as pd
import pytest
from sqlmodel import select
from typer.testing import CliRunner

import consist
from consist.cli import app as cli_app
from consist.core.identity import IdentityManager
from consist.core.tracker import Tracker
from consist.core.workflow import RunContext
from consist.models.artifact import Artifact
from consist.models.artifact_schema import (
    ArtifactSchema,
    ArtifactSchemaField,
    ArtifactSchemaObservation,
)
from consist.models.run import Run, RunResult


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

    This test is meant to be a readable starting point for new workflows. It shows:

    1) **Scenario header run**
       Use `tracker.scenario(...)` to create a parent run that groups multiple steps
       under a single scenario identifier (useful for multi-step pipelines/simulations).

    2) **Step runs + artifact lineage**
       Each `scenario.run(...)` creates a child run. Within a step:
       - write some data
       - `ctx.log_dataframe(...)` to register outputs (and optionally inputs)
       - use `scenario.coupler` to pass artifacts between steps (auto-updated)

    3) **Dual-write provenance**
       Consist writes a human-inspectable JSON snapshot (`consist.json`) and (when
       configured with `db_path`) persists runs/artifacts to DuckDB for querying.

    4) **Hot-data ingestion + schema persistence**
       Calling `tracker.ingest(artifact)` materializes the artifact into DuckDB (via dlt)
       and (as of this feature) persists a deduped schema profile referenced from
       `artifact.meta` (`schema_id`, `schema_summary`).

    5) **CLI introspection**
       Demonstrates how developers can inspect the resulting provenance via the CLI.

    Developers can adapt this by:
    - swapping CSV for Parquet/Zarr/HDF5 artifacts,
    - adding more steps and richer facets/tags,
    - ingesting selected outputs for query performance and schema tracking.
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
        config={"seed": 7},  # identity config (hashed)
        facet_from=["seed"],
        facet={"region": "demo"},  # queryable config facet (optional)
        hash_inputs=[("scenario_config", scenario_cfg_path)],  # hash-only attachment(s)
        tags=["e2e", "scenario_header"],
    ) as scenario:

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
            load_inputs=True,
        )
        features_artifact = transform_result.outputs["features"]

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
    # For tests, `consist.run_query(...)` is a convenient wrapper around a Session.
    runs = consist.run_query(select(Run), tracker=tracker)
    run_ids = {r.id for r in runs}
    assert {scenario_id, ingest_run_id, transform_run_id} <= run_ids
    assert all(r.status == "completed" for r in runs)

    artifacts = consist.run_query(select(Artifact), tracker=tracker)
    keys = {a.key for a in artifacts}
    assert {"raw_table", "features"} <= keys
    assert raw_artifact is not None

    # Ensure parent linkage captured by scenario header
    child_parents = {r.id: r.parent_run_id for r in runs if r.id != scenario_id}
    assert child_parents[ingest_run_id] == scenario_id
    assert child_parents[transform_run_id] == scenario_id

    # Schema persistence: ingest an artifact and verify schema discovery is persisted.
    assert features_artifact is not None
    tracker.ingest(features_artifact)
    assert "schema_id" in features_artifact.meta
    assert "schema_summary" in features_artifact.meta

    # `consist.db_session(...)` is the ergonomic way to open a Session for ad-hoc queries.
    with consist.db_session(tracker=tracker) as session:
        schema_id = features_artifact.meta["schema_id"]
        assert session.get(ArtifactSchema, schema_id) is not None
        fields = session.exec(
            select(ArtifactSchemaField).where(
                ArtifactSchemaField.schema_id == schema_id
            )
        ).all()
        assert {f.name for f in fields} >= {"value", "category", "value_doubled"}
        observations = session.exec(
            select(ArtifactSchemaObservation).where(
                ArtifactSchemaObservation.schema_id == schema_id
            )
        ).all()
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
        assert "value_doubled" in preview_result.stdout

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
        assert features_artifact.meta["schema_id"] in schema_result.stdout
        assert "value_doubled" in schema_result.stdout


def test_resume_after_failure_uses_cache_and_ghost_mode(
    tracker: Tracker, run_dir: Path
):
    """
    Regression test: resume a multi-step workflow after a failure, using cache.

    This test is intentionally narrative and "readable first" because it documents a
    common pain point in real pipelines: a long workflow fails late (e.g., out-of-disk),
    and you want to re-run without repeating expensive earlier steps.

    What we validate
    ----------------
    1) A predecessor step (`prepare_data`) completes successfully and its output is
       cached by Consist (keyed by signature = code hash + config hash + input hash).

    2) A downstream step (`train_model`) fails on the first attempt with an ENOSPC-like
       exception. Consist should persist the run with `status="failed"` and an error
       message in `run.meta["error"]`. Failed runs should never be reused as cache hits.

    3) On a second execution of the workflow with the same inputs/config/code:
       - `prepare_data` should be a cache hit (not re-executed).
       - `train_model` should re-execute (since the previous attempt failed).
       - the pipeline should be able to continue to `evaluate_model`.

    Bonus: Ghost Mode
    -----------------
    After we ingest `prepare_data` into DuckDB, we delete the original CSV file.
    Downstream steps then call `consist.load(...)` on the input artifact; since the file
    is missing but `is_ingested=True`, Consist transparently loads the data from DuckDB.
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

        df = _consist_ctx.load("prepared_data")

        train_attempts["count"] += 1
        if train_attempts["count"] == 1:
            # Simulate an OS-level failure (e.g., writing model artifacts runs out of space).
            # The tracker should mark this run "failed" and record the exception string.
            raise OSError(errno.ENOSPC, "No space left on device")

        model_payload = {"rows": int(len(df)), "mean_value": float(df["value"].mean())}
        model_path.write_text(json.dumps(model_payload))
        return model_path

    def evaluate_model(_consist_ctx: RunContext) -> Path:
        execution_counts["evaluate_model"] += 1

        model_artifact = _consist_ctx.inputs["model"]
        model_payload = json.loads(Path(model_artifact.path).read_text())
        df = _consist_ctx.load("prepared_data")

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
        fn_args={"rows": 8},
    )
    prepared_artifact = prepared_result.outputs["prepared_data"]
    # Materialize the predecessor output into DuckDB so downstream steps can still
    # read it even if the underlying file disappears (ghost mode).
    tracker.ingest(prepared_artifact)

    # Delete the on-disk file to force `consist.load(...)` to go through DuckDB.
    prepared_artifact.path.unlink()
    assert not prepared_artifact.path.exists()

    with pytest.raises(OSError) as exc_info:
        tracker.run(
            fn=train_model,
            name="train_model",
            inputs={"prepared_data": prepared_artifact},
            outputs=["model"],
            load_inputs=False,
            inject_context=True,
        )
    assert "No space left on device" in str(exc_info.value)

    prepare_runs = consist.run_query(
        select(Run).where(Run.model_name == "prepare_data"), tracker=tracker
    )
    assert len(prepare_runs) == 1
    assert prepare_runs[0].status == "completed"

    train_runs = consist.run_query(
        select(Run).where(Run.model_name == "train_model"), tracker=tracker
    )
    assert len(train_runs) == 1
    assert train_runs[0].status == "failed"
    assert "No space left on device" in train_runs[0].meta.get("error", "")

    eval_runs = consist.run_query(
        select(Run).where(Run.model_name == "evaluate_model"), tracker=tracker
    )
    assert len(eval_runs) == 0

    # --- Second execution: should reuse cached predecessor and succeed downstream ---
    prepared_result_2: RunResult = tracker.run(
        fn=prepare_data,
        name="prepare_data",
        outputs=["prepared_data"],
        fn_args={"rows": 8},
    )
    prepared_artifact_2 = prepared_result_2.outputs["prepared_data"]
    assert prepared_artifact_2.uri == prepared_artifact.uri

    model_result: RunResult = tracker.run(
        fn=train_model,
        name="train_model",
        inputs={"prepared_data": prepared_artifact_2},
        outputs=["model"],
        load_inputs=False,
        inject_context=True,
    )
    report_result: RunResult = tracker.run(
        fn=evaluate_model,
        name="evaluate_model",
        inputs={
            "model": model_result.outputs["model"],
            "prepared_data": prepared_artifact_2,
        },
        outputs=["report"],
        load_inputs=False,
        inject_context=True,
    )
    report_artifact = report_result.outputs["report"]

    assert report_artifact.path.exists()
    assert execution_counts["prepare_data"] == 1, (
        "Predecessor should be reused via cache"
    )
    assert execution_counts["train_model"] == 2
    assert execution_counts["evaluate_model"] == 1

    prepare_runs = consist.run_query(
        select(Run).where(Run.model_name == "prepare_data"), tracker=tracker
    )
    assert len(prepare_runs) == 2
    assert any(r.meta.get("cache_hit") is True for r in prepare_runs)

    train_runs = consist.run_query(
        select(Run).where(Run.model_name == "train_model"), tracker=tracker
    )
    assert len(train_runs) == 2
    assert [r.status for r in train_runs].count("failed") == 1
    assert [r.status for r in train_runs].count("completed") == 1

    eval_runs = consist.run_query(
        select(Run).where(Run.model_name == "evaluate_model"), tracker=tracker
    )
    assert len(eval_runs) == 1
    assert eval_runs[0].status == "completed"


def test_scenario_run_skips_callable_on_cache_hit(tracker: Tracker):
    """
    End-to-end demonstration of `ScenarioContext.run(...)` cache skipping.

    `scenario.run(...)` can skip executing the callable on cache hits while still
    hydrating cached outputs and populating the scenario Coupler.
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
        result: RunResult = sc.run(
            name="produce",
            fn=expensive_step,
            config={"v": 1},
            output_paths={"out": rel_out},
            inject_context=True,
        )
        assert result.outputs["out"].key == "out"
        assert sc.coupler.require("out").id == result.outputs["out"].id

    assert calls == ["called"]
    assert result.outputs["out"].path.read_text() == "calls=1\n"

    # Repeat with the same signature (code/config/inputs). This should be a cache hit
    # and should NOT execute `expensive_step()` again.
    with tracker.scenario("run_demo_B") as sc:
        result: RunResult = sc.run(
            name="produce",
            fn=expensive_step,
            config={"v": 1},
            output_paths={"out": rel_out},
            inject_context=True,
        )
        assert result.outputs["out"].key == "out"
        assert sc.coupler.require("out").id == result.outputs["out"].id

    assert calls == ["called"]
    assert result.outputs["out"].path.read_text() == "calls=1\n"
