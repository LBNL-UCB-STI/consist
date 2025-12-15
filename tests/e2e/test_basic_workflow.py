import json
from pathlib import Path
from typing import Optional
from unittest.mock import patch

import pandas as pd
from sqlmodel import Session, select
from typer.testing import CliRunner

from consist.cli import app as cli_app
from consist.core.identity import IdentityManager
from consist.core.tracker import Tracker
from consist.models.artifact import Artifact
from consist.models.artifact_schema import (
    ArtifactSchema,
    ArtifactSchemaField,
    ArtifactSchemaObservation,
)
from consist.models.run import Run


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
       Each `scenario.step(...)` creates a child run. Within a step:
       - write some data
       - `tracker.log_artifact(...)` to register outputs (and optionally inputs)
       - use `scenario.coupler` to pass artifacts between steps

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
    raw_path = run_dir / "raw.csv"
    features_path = run_dir / "features.csv"
    scenario_cfg_path = run_dir / "scenario_config.json"
    scenario_cfg_path.write_text(json.dumps({"seed": 7, "note": "external config"}))
    features_artifact: Optional[Artifact] = None

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
            features_artifact = features_art
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
    assert isinstance(data["run"]["meta"]["mounts"], dict)
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

    # Schema persistence: ingest an artifact and verify schema discovery is persisted.
    assert features_artifact is not None
    tracker.ingest(features_artifact)
    assert "schema_id" in features_artifact.meta
    assert "schema_summary" in features_artifact.meta

    with Session(tracker.engine) as session:
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

        schema_result = runner.invoke(
            cli_app, ["schema", "features", "--db-path", db_path, "--json"]
        )
        assert schema_result.exit_code == 0
        schema_payload = json.loads(schema_result.stdout)
        assert schema_payload["type"] == "dataframe"
        assert schema_payload["driver"] == "csv"
        assert schema_payload["columns"] >= 2
        assert "value" in schema_payload["dtypes"]
        assert "value_doubled" in schema_payload["dtypes"]
