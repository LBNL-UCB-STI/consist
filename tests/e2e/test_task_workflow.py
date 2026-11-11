import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from pydantic import BaseModel
from typer.testing import CliRunner

import consist
from consist.artifact import Artifact
from consist.cli import app as cli_app
from consist.core.indexing import index_by_facet
from consist.core.tracker import Tracker
from consist.core.workflow import RunContext
from consist.models.run import Run
from consist.types import CacheOptions, ExecutionOptions


class CleaningConfig(BaseModel):
    """Configuration for data cleaning task."""

    threshold: float = 0.5
    remove_outliers: bool = True
    min_value: float = 0.0

    def to_consist_facet(self) -> dict:
        return {
            "threshold": self.threshold,
            "remove_outliers": self.remove_outliers,
            "min_value": self.min_value,
        }


class ManualConfigDemo(BaseModel):
    """
    Example of a "real world" config model where only a small subset should be queryable.
    """

    mode: str = "strict"
    purpose: str = "demo"
    notes: str = "This field is intentionally NOT indexed via facet."
    facet_schema_version: int = 1

    def to_consist_facet(self) -> dict:
        return {"mode": self.mode, "purpose": self.purpose}


def test_task_decorator_workflow(tracker: Tracker, run_dir: Path):
    """
    End-to-end validation of @define_step decorator workflow.
    """
    runner = CliRunner()

    raw_data_path = run_dir / "raw_data.csv"
    config_path = run_dir / "cleaning_config.json"
    params_path = run_dir / "params.json"

    df_raw = pd.DataFrame(
        {
            "id": range(10),
            "value": [1.2, 0.3, 2.5, -0.1, 3.7, 0.8, 4.2, 0.2, 5.1, 1.9],
            "category": ["a", "b", "a", "b", "a"] * 2,
        }
    )
    df_raw.to_csv(raw_data_path, index=False)

    with open(config_path, "w") as f:
        json.dump({"threshold": 0.5, "min_value": 0.0}, f)

    with open(params_path, "w") as f:
        json.dump({"version": "1.0", "mode": "strict"}, f)

    @tracker.define_step(outputs=["cleaned"], tags=["clean"])
    def clean_data(raw_file: Path, config: CleaningConfig) -> Path:
        """Clean data by removing outliers and applying threshold."""
        output_path = run_dir / "cleaned.csv"
        df = pd.read_csv(raw_file)

        df_clean = df[df["value"] >= config.min_value].copy()
        if config.remove_outliers:
            mean = df_clean["value"].mean()
            std = df_clean["value"].std()
            df_clean = df_clean[
                (df_clean["value"] >= mean - 2 * std)
                & (df_clean["value"] <= mean + 2 * std)
            ]

        df_clean.to_csv(output_path, index=False)
        return output_path

    @tracker.define_step(outputs=["analysis"], tags=["analysis"])
    def analyze_data(cleaned_file: Path, multiplier: float = 1.0) -> Path:
        """Analyze cleaned data with a multiplier."""
        output_path = run_dir / "analysis.csv"
        df = pd.read_csv(cleaned_file)
        df["analyzed_value"] = df["value"] * multiplier
        df.to_csv(output_path, index=False)
        return output_path

    def run_legacy_analysis(cleaned_file: Path, _consist_ctx: RunContext) -> None:
        """Wrapper around legacy analysis code that reads config files."""
        output_dir = run_dir / "outputs"
        output_dir.mkdir(exist_ok=True)

        with _consist_ctx.capture_outputs(output_dir, pattern="*.csv"):
            with open(config_path) as f:
                config_data = json.load(f)

            df = pd.read_csv(cleaned_file)
            df["processed_value"] = df["value"] * config_data["threshold"]
            df.to_csv(output_dir / "analysis_results.csv", index=False)

            summary = pd.DataFrame(
                {
                    "metric": ["count", "mean", "max"],
                    "value": [len(df), df["value"].mean(), df["value"].max()],
                }
            )
            summary.to_csv(output_dir / "summary.csv", index=False)

        return None

    # === First execution: everything runs ===
    config = CleaningConfig(threshold=0.5, remove_outliers=True, min_value=0.0)

    cleaned_result = tracker.run(
        fn=clean_data,
        inputs={"raw_file": raw_data_path},
        config=config,
        execution_options=ExecutionOptions(
            runtime_kwargs={"raw_file": raw_data_path},
            load_inputs=False,
        ),
    )
    cleaned_artifact = cleaned_result.outputs["cleaned"]
    assert isinstance(cleaned_artifact, Artifact), "Run should return output artifacts"

    assert cleaned_artifact.path.exists(), "Cleaned data should exist"
    df_cleaned = pd.read_csv(cleaned_artifact.path)
    assert len(df_cleaned) < len(df_raw), "Cleaning should remove some rows"

    df_cleaned_loaded = consist.load_df(cleaned_artifact)
    assert len(df_cleaned_loaded) == len(df_cleaned)

    analysis_result = tracker.run(
        fn=analyze_data,
        inputs={"cleaned_file": cleaned_artifact},
        execution_options=ExecutionOptions(
            runtime_kwargs={"cleaned_file": cleaned_artifact.path, "multiplier": 2.0},
            load_inputs=False,
        ),
    )
    analysis_artifact = analysis_result.outputs["analysis"]
    assert isinstance(analysis_artifact, Artifact)
    assert analysis_artifact.path.exists()

    df_analysis = pd.read_csv(analysis_artifact.path)
    assert "analyzed_value" in df_analysis.columns
    assert df_analysis["analyzed_value"][0] == df_cleaned["value"][0] * 2.0

    legacy_result = tracker.run(
        fn=run_legacy_analysis,
        inputs={"cleaned_file": cleaned_artifact},
        execution_options=ExecutionOptions(
            runtime_kwargs={"cleaned_file": cleaned_artifact.path},
            inject_context=True,
            load_inputs=False,
        ),
        identity_inputs=[("cleaning_config", config_path), ("params", params_path)],
    )
    assert legacy_result.outputs, "Legacy analysis should capture outputs"

    analysis_output = run_dir / "outputs" / "analysis_results.csv"
    summary_output = run_dir / "outputs" / "summary.csv"
    assert analysis_output.exists(), "Legacy analysis should produce results"
    assert summary_output.exists(), "Legacy analysis should produce summary"

    # === Verify DB state after first execution ===
    runs = tracker.query_rows("SELECT * FROM main.run", model=Run)
    assert len(runs) == 3, "Should have 3 task runs"

    clean_run = next((r for r in runs if r.model_name == "clean_data"), None)
    analyze_run = next((r for r in runs if r.model_name == "analyze_data"), None)
    legacy_run = next((r for r in runs if r.model_name == "run_legacy_analysis"), None)

    assert clean_run is not None, "clean_data run should exist"
    assert analyze_run is not None, "analyze_data run should exist"
    assert legacy_run is not None, "run_legacy_analysis run should exist"
    assert all(r.status == "completed" for r in [clean_run, analyze_run, legacy_run])

    assert "cache_hit" not in clean_run.meta or clean_run.meta["cache_hit"] is False
    assert "cache_hit" not in analyze_run.meta or analyze_run.meta["cache_hit"] is False

    artifacts_df = tracker.query_df("SELECT key FROM main.artifact")
    artifact_keys = set(artifacts_df["key"].tolist())
    assert any("cleaned" in k for k in artifact_keys)
    assert any("analysis" in k for k in artifact_keys)

    with tracker.start_run(
        run_id="manual_config_demo",
        model="manual_config_demo",
        config=ManualConfigDemo(mode="strict", purpose="demo"),
        hash_inputs=[("cleaning_config", config_path), ("params", params_path)],
    ):
        tracker.log_artifact(raw_data_path, key="raw_data", direction="input")

    matching = tracker.find_runs_by_facet_kv(
        namespace="manual_config_demo",
        key="mode",
        value_str="strict",
    )
    assert any(r.id == "manual_config_demo" for r in matching)

    facets = tracker.get_config_facets(namespace="manual_config_demo")
    assert facets and facets[0].facet_json == {"mode": "strict", "purpose": "demo"}

    by_mode = tracker.find_runs(
        model="manual_config_demo", index_by=index_by_facet("mode")
    )
    assert by_mode["strict"].id == "manual_config_demo"

    # === Second execution: same inputs should hit cache ===
    cleaned_result_2 = tracker.run(
        fn=clean_data,
        inputs={"raw_file": raw_data_path},
        config=config,
        execution_options=ExecutionOptions(
            runtime_kwargs={"raw_file": raw_data_path},
            load_inputs=False,
        ),
    )

    assert (
        cleaned_result_2.outputs["cleaned"].primary_location
        == cleaned_artifact.primary_location
    )

    runs = tracker.query_rows(
        "SELECT * FROM main.run WHERE model_name = ? ORDER BY created_at ASC",
        ["clean_data"],
        model=Run,
    )
    assert len(runs) == 2, "Should have original run + cache hit run"

    cache_hits = [r for r in runs if r.meta.get("cache_hit") is True]
    assert len(cache_hits) == 1, "Should have exactly one cache hit run"
    assert cache_hits[0].meta.get("cache_source") is not None, (
        "Cache hit should reference source"
    )

    originals = [r for r in runs if not r.meta.get("cache_hit")]
    assert len(originals) == 1, "Should have exactly one original run"

    # === Third execution: different config should re-execute ===
    config_new = CleaningConfig(threshold=0.8, remove_outliers=True, min_value=0.0)
    tracker.run(
        fn=clean_data,
        inputs={"raw_file": raw_data_path},
        config=config_new,
        execution_options=ExecutionOptions(
            runtime_kwargs={"raw_file": raw_data_path},
            load_inputs=False,
        ),
    )

    runs = tracker.query_rows(
        "SELECT * FROM main.run WHERE model_name = ? ORDER BY created_at ASC",
        ["clean_data"],
        model=Run,
    )
    assert len(runs) == 3, "Should have original run + cache hit + new config run"

    sigs = {r.signature for r in runs}
    assert len(sigs) == 2, "Different configs should produce different signatures"

    latest_run = runs[-1]
    assert not latest_run.meta.get("cache_hit"), (
        "New config should execute, not hit cache"
    )

    # === Fourth execution: test cache_mode="overwrite" ===
    @tracker.define_step(outputs=["cleaned_overwrite"])
    def clean_data_overwrite(raw_file: Path, config: CleaningConfig) -> Path:
        """Same logic but always re-executes."""
        output_path = run_dir / "cleaned_overwrite.csv"
        df = pd.read_csv(raw_file)
        df_clean = df[df["value"] >= config.min_value]
        df_clean.to_csv(output_path, index=False)
        return output_path

    tracker.run(
        fn=clean_data_overwrite,
        inputs={"raw_file": raw_data_path},
        config=config,
        execution_options=ExecutionOptions(
            runtime_kwargs={"raw_file": raw_data_path},
            load_inputs=False,
        ),
        cache_options=CacheOptions(cache_mode="overwrite"),
    )
    tracker.run(
        fn=clean_data_overwrite,
        inputs={"raw_file": raw_data_path},
        config=config,
        execution_options=ExecutionOptions(
            runtime_kwargs={"raw_file": raw_data_path},
            load_inputs=False,
        ),
        cache_options=CacheOptions(cache_mode="overwrite"),
    )

    overwrite_runs = tracker.query_rows(
        "SELECT * FROM main.run WHERE model_name = ?",
        ["clean_data_overwrite"],
        model=Run,
    )
    assert len(overwrite_runs) == 2

    # === CLI validation using --json for robust testing ===
    db_path = str(tracker.db_path)
    with patch("consist.cli.get_tracker", return_value=tracker):
        runs_result = runner.invoke(
            cli_app, ["runs", "--db-path", db_path, "--json", "--limit", "10"]
        )
        assert runs_result.exit_code == 0
        runs_data = json.loads(runs_result.stdout)

        models = {r["model"] for r in runs_data}
        assert "clean_data" in models
        assert "analyze_data" in models
        assert "run_legacy_analysis" in models

        cache_hits = [
            r for r in runs_data if r.get("meta", {}).get("cache_hit") is True
        ]
        assert len(cache_hits) > 0, "Should have some cache hits recorded"

        analyze_run_id = next(
            r["id"] for r in runs_data if r["model"] == "analyze_data"
        )
        show_result = runner.invoke(
            cli_app, ["show", analyze_run_id, "--db-path", db_path]
        )
        assert show_result.exit_code == 0
        assert "analyze_data" in show_result.stdout
        assert "completed" in show_result.stdout.lower()

        artifacts_result = runner.invoke(
            cli_app, ["artifacts", analyze_run_id, "--db-path", db_path]
        )
        assert artifacts_result.exit_code == 0
        assert "Input" in artifacts_result.stdout
        assert "Output" in artifacts_result.stdout

        summary_result = runner.invoke(cli_app, ["summary", "--db-path", db_path])
        assert summary_result.exit_code == 0
        assert "Runs" in summary_result.stdout
        assert "Artifacts" in summary_result.stdout

        scenarios_result = runner.invoke(cli_app, ["scenarios", "--db-path", db_path])
        assert scenarios_result.exit_code == 0

    # === JSON snapshot validation ===
    json_files = list(tracker.run_dir.glob("**/consist.json"))
    assert len(json_files) > 0, "Should have JSON snapshots"

    with open(json_files[0]) as f:
        json_data = json.load(f)

    assert "run" in json_data
    assert "config" in json_data
    assert json_data["run"]["status"] == "completed"
    assert json_data["run"]["signature"] is not None

    # === Test artifact chaining with modified data ===
    df_modified = df_raw.copy()
    df_modified["value"] = df_modified["value"] * 1.5
    modified_path = run_dir / "raw_modified.csv"
    df_modified.to_csv(modified_path, index=False)

    cleaned_modified_result = tracker.run(
        fn=clean_data,
        inputs={"raw_file": modified_path},
        config=config,
        execution_options=ExecutionOptions(
            runtime_kwargs={"raw_file": modified_path},
            load_inputs=False,
        ),
    )

    clean_runs = tracker.query_rows(
        "SELECT * FROM main.run WHERE model_name = ?",
        ["clean_data"],
        model=Run,
    )
    assert len(clean_runs) == 4, (
        "Should have original, cache hit, new config, and modified input runs"
    )

    df_cleaned_modified = consist.load_df(cleaned_modified_result.outputs["cleaned"])
    assert not df_cleaned_modified.equals(df_cleaned)
