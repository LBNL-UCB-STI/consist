import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from sqlmodel import Session, select
from typer.testing import CliRunner

from consist.cli import app as cli_app
from consist.core.identity import IdentityManager
from consist.core.tracker import Tracker
from consist.models.artifact import Artifact
from consist.models.run import Run


def _write_csv(path: Path, rows: int = 5) -> None:
    df = pd.DataFrame({"value": range(rows), "category": ["a"] * rows})
    df.to_csv(path, index=False)


def test_dual_write_workflow(tracker: Tracker, run_dir: Path):
    """
    End-to-end regression: scenario header + two steps, dual-write to JSON/DB,
    and CLI introspection on the resulting provenance.
    """
    runner = CliRunner()
    tracker.identity.hashing_strategy = "fast"

    scenario_id = "demo_scenario"
    ingest_run_id = f"{scenario_id}_ingest"
    transform_run_id = f"{scenario_id}_transform"
    raw_path = run_dir / "raw.csv"
    features_path = run_dir / "features.csv"
    scenario_cfg_path = run_dir / "scenario_config.json"
    scenario_cfg_path.write_text(json.dumps({"seed": 7, "note": "external config"}))

    with tracker.scenario(
        scenario_id,
        config={"seed": 7},  # identity config (hashed)
        facet={"seed": 7, "region": "demo"},  # queryable config facet (optional)
        hash_inputs=[("scenario_config", scenario_cfg_path)],  # hash-only attachment(s)
        tags=["e2e", "scenario_header"],
    ) as scenario:
        with scenario.step(
            name="ingest",
            run_id=ingest_run_id,
            tags=["ingest"],
            year=2024,
            facet={"step": "ingest", "rows_written": 6},
        ):
            _write_csv(raw_path, rows=6)
            raw_art = tracker.log_artifact(
                path=raw_path,
                key="raw_table",
                direction="output",
                driver="csv",
                meta={"rows": 6},
            )
            scenario.coupler.set("raw", raw_art)

        with scenario.step(
            name="transform",
            run_id=transform_run_id,
            tags=["transform"],
            year=2025,
            inputs=[scenario.coupler.get("raw")],
            facet={"step": "transform", "multiplier": 2},
        ):
            df_raw = pd.read_csv(raw_path)
            df_raw["value_doubled"] = df_raw["value"] * 2
            df_raw.to_csv(features_path, index=False)
            features_art = tracker.log_artifact(
                path=features_path,
                key="features",
                direction="output",
                driver="csv",
                meta={"rows": len(df_raw)},
            )
            scenario.coupler.set("features", features_art)

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
    assert data["run"]["config_hash"] == IdentityManager().compute_config_hash(
        {
            "seed": 7,
            "__consist_hash_inputs__": data["run"]["meta"]["consist_hash_inputs"],
        }
    )
    assert data["run"]["git_hash"] is not None

    # Database snapshot
    with Session(tracker.engine) as session:
        runs = session.exec(select(Run)).all()
        run_ids = {r.id for r in runs}
        assert {scenario_id, ingest_run_id, transform_run_id} <= run_ids
        assert all(r.status == "completed" for r in runs)

        artifacts = session.exec(select(Artifact)).all()
        keys = {a.key for a in artifacts}
        assert {"raw_table", "features"} <= keys

        # Ensure parent linkage captured by scenario header
        child_parents = {r.id: r.parent_run_id for r in runs if r.id != scenario_id}
        assert child_parents[ingest_run_id] == scenario_id
        assert child_parents[transform_run_id] == scenario_id

    # CLI sanity: runs/show/artifacts/summary/preview
    db_path = str(tracker.db_path)
    with patch("consist.cli.get_tracker", return_value=tracker):
        runs_result = runner.invoke(
            cli_app, ["runs", "--db-path", db_path, "--limit", "5"]
        )
        assert runs_result.exit_code == 0
        # Rich tables may truncate IDs; check the stable prefixes and model names.
        assert scenario_id[:10] in runs_result.stdout
        assert "transform" in runs_result.stdout

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
