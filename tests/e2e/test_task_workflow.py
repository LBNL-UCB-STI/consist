import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from pydantic import BaseModel
from sqlmodel import select
from typer.testing import CliRunner

import consist
from consist.cli import app as cli_app
from consist.core.tracker import Tracker
from consist.core.workflow import RunContext
from consist.models.artifact import Artifact
from consist.models.run import Run


class CleaningConfig(BaseModel):
    """Configuration for data cleaning task."""

    threshold: float = 0.5
    remove_outliers: bool = True
    min_value: float = 0.0

    def to_consist_facet(self) -> dict:
        # Only expose the knobs that are useful for filtering/diffing.
        return {
            "threshold": self.threshold,
            "remove_outliers": self.remove_outliers,
            "min_value": self.min_value,
        }


class ManualConfigDemo(BaseModel):
    """
    Example of a "real world" config model where only a small subset should be queryable.

    In practice this might be a pipeline config containing large nested structures,
    secrets, or non-queryable blobs; `to_consist_facet()` lets you keep the DB index slim.
    """

    mode: str = "strict"
    purpose: str = "demo"
    notes: str = "This field is intentionally NOT indexed via facet."
    facet_schema_version: int = 1

    def to_consist_facet(self) -> dict:
        return {"mode": self.mode, "purpose": self.purpose}


def test_task_decorator_workflow(tracker: Tracker, run_dir: Path):
    """
    End-to-end validation of @task decorator workflow:
    - Task with simple caching
    - Task with hash_inputs (config file dependency)
    - Task with capture_outputs (legacy code wrapper)
    - Cache hit behavior on re-execution
    - CLI introspection
    - Artifact chaining between tasks
    """
    runner = CliRunner()
    tracker.identity.hashing_strategy = "fast"

    # Setup: create input data and config files
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

    # Task 1: Simple data cleaning with caching
    @tracker.define_step(outputs=["cleaned"], tags=["clean"])
    def clean_data(raw_file: Path, config: CleaningConfig) -> Path:
        """Clean data by removing outliers and applying threshold."""
        output_path = run_dir / "cleaned.csv"
        df = pd.read_csv(raw_file)

        # Apply cleaning rules
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

    # Task 2: Analysis that takes cleaned data as input
    @tracker.define_step(outputs=["analysis"], tags=["analysis"])
    def analyze_data(cleaned_file: Path, multiplier: float = 1.0) -> Path:
        """Analyze cleaned data with a multiplier."""
        output_path = run_dir / "analysis.csv"
        df = pd.read_csv(cleaned_file)
        df["analyzed_value"] = df["value"] * multiplier
        df.to_csv(output_path, index=False)
        return output_path

    # Task 3: Legacy code wrapper with hash_inputs + capture_outputs
    def run_legacy_analysis(cleaned_file: Path, _consist_ctx: RunContext) -> None:
        """Wrapper around legacy analysis code that reads config files."""
        # Simulate legacy code that reads config files and writes to ./outputs
        output_dir = run_dir / "outputs"
        output_dir.mkdir(exist_ok=True)

        with _consist_ctx.capture_outputs(output_dir, pattern="*.csv"):
            # Legacy code reads the config files (captured by hash_inputs)
            with open(config_path) as f:
                config_data = json.load(f)

            # Legacy code processes data
            df = pd.read_csv(cleaned_file)
            df["processed_value"] = df["value"] * config_data["threshold"]

            # Legacy code writes to outputs directory (captured by capture_outputs)
            df.to_csv(output_dir / "analysis_results.csv", index=False)

            # Summary report
            summary = pd.DataFrame(
                {
                    "metric": ["count", "mean", "max"],
                    "value": [len(df), df["value"].mean(), df["value"].max()],
                }
            )
            summary.to_csv(output_dir / "summary.csv", index=False)

        # Task returns None when using capture_outputs
        return None

    # === First execution: everything runs ===
    config = CleaningConfig(threshold=0.5, remove_outliers=True, min_value=0.0)

    cleaned_result = tracker.run(
        fn=clean_data,
        inputs={"raw_file": raw_data_path},
        config=config,
        runtime_kwargs={"raw_file": raw_data_path},
        load_inputs=False,
    )
    cleaned_artifact = cleaned_result.outputs["cleaned"]
    assert isinstance(cleaned_artifact, Artifact), "Run should return output artifacts"

    # Access the resolved path via .path property
    assert cleaned_artifact.path.exists(), "Cleaned data should exist"
    df_cleaned = pd.read_csv(cleaned_artifact.path)
    assert len(df_cleaned) < len(df_raw), "Cleaning should remove some rows"

    # Alternatively, use consist.load_df() which now works without explicit tracker
    df_cleaned_loaded = consist.load_df(cleaned_artifact)
    assert len(df_cleaned_loaded) == len(df_cleaned)

    # Chain tasks: pass artifact directly to next task
    analysis_result = tracker.run(
        fn=analyze_data,
        inputs={"cleaned_file": cleaned_artifact},
        runtime_kwargs={"cleaned_file": cleaned_artifact.path, "multiplier": 2.0},
        load_inputs=False,
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
        runtime_kwargs={"cleaned_file": cleaned_artifact.path},
        hash_inputs=[("cleaning_config", config_path), ("params", params_path)],
        inject_context=True,
        load_inputs=False,
    )
    assert legacy_result.outputs, "Legacy analysis should capture outputs"

    # Verify captured outputs exist
    analysis_output = run_dir / "outputs" / "analysis_results.csv"
    summary_output = run_dir / "outputs" / "summary.csv"
    assert analysis_output.exists(), "Legacy analysis should produce results"
    assert summary_output.exists(), "Legacy analysis should produce summary"

    # === Verify DB state after first execution ===
    runs = consist.run_query(select(Run), tracker=tracker)
    assert len(runs) == 3, "Should have 3 task runs"

    # Find the runs by model name
    clean_run = next((r for r in runs if r.model_name == "clean_data"), None)
    analyze_run = next((r for r in runs if r.model_name == "analyze_data"), None)
    legacy_run = next((r for r in runs if r.model_name == "run_legacy_analysis"), None)

    assert clean_run is not None, "clean_data run should exist"
    assert analyze_run is not None, "analyze_data run should exist"
    assert legacy_run is not None, "run_legacy_analysis run should exist"
    assert all(r.status == "completed" for r in [clean_run, analyze_run, legacy_run])

    # First runs should not have cache_hit metadata
    assert "cache_hit" not in clean_run.meta or clean_run.meta["cache_hit"] is False
    assert "cache_hit" not in analyze_run.meta or analyze_run.meta["cache_hit"] is False

    # Verify artifacts were logged
    artifacts = consist.run_query(select(Artifact), tracker=tracker)
    artifact_keys = {a.key for a in artifacts}
    assert any("cleaned" in k for k in artifact_keys)
    assert any("analysis" in k for k in artifact_keys)

    # === Example: new config pattern for "legacy reads config files" workflows ===
    # The config files should affect the run identity (hash_inputs) without being shoved into DB meta,
    # while a small facet stays queryable.
    with tracker.start_run(
        run_id="manual_config_demo",
        model="manual_config_demo",
        config=ManualConfigDemo(mode="strict", purpose="demo"),
        hash_inputs=[("cleaning_config", config_path), ("params", params_path)],
    ):
        tracker.log_artifact(raw_data_path, key="raw_data", direction="input")

    # Facet value: find runs by a queryable knob without loading/parsing full configs.
    matching = tracker.find_runs_by_facet_kv(
        namespace="manual_config_demo",
        key="mode",
        value_str="strict",
    )
    assert any(r.id == "manual_config_demo" for r in matching)

    facets = tracker.get_config_facets(namespace="manual_config_demo")
    assert facets and facets[0].facet_json == {"mode": "strict", "purpose": "demo"}

    # `find_runs(..., index_by=...)` can return a "runs by knob" map.
    # Prefer the typed helper for IDE/type-checker friendliness.
    by_mode = tracker.find_runs(
        model="manual_config_demo", index_by=consist.index_by_facet("mode")
    )
    assert by_mode["strict"].id == "manual_config_demo"

    if tracker.db:
        tracker.db.session.commit() if hasattr(tracker.db, "session") else None

    # === Second execution: same inputs should hit cache ===
    cleaned_result_2 = tracker.run(
        fn=clean_data,
        inputs={"raw_file": raw_data_path},
        config=config,
        runtime_kwargs={"raw_file": raw_data_path},
        load_inputs=False,
    )

    # Should get the same artifact back (cache hit)
    assert (
        cleaned_result_2.outputs["cleaned"].container_uri
        == cleaned_artifact.container_uri
    )

    # Check cache hit metadata
    runs = consist.run_query(
        select(Run).where(Run.model_name == "clean_data"), tracker=tracker
    )
    assert len(runs) == 2, "Should have original run + cache hit run"

    # One of the runs should be a cache hit
    cache_hits = [r for r in runs if r.meta.get("cache_hit") is True]
    print([r.meta for r in runs])
    print(runs)
    assert len(cache_hits) == 1, "Should have exactly one cache hit run"
    assert cache_hits[0].meta.get("cache_source") is not None, (
        "Cache hit should reference source"
    )

    # The other run should be the original (no cache_hit metadata)
    originals = [r for r in runs if not r.meta.get("cache_hit")]
    assert len(originals) == 1, "Should have exactly one original run"

    # === Third execution: different config should re-execute ===
    config_new = CleaningConfig(threshold=0.8, remove_outliers=True, min_value=0.0)
    tracker.run(
        fn=clean_data,
        inputs={"raw_file": raw_data_path},
        config=config_new,
        runtime_kwargs={"raw_file": raw_data_path},
        load_inputs=False,
    )

    # Should be a new execution (different config = different signature)
    runs = consist.run_query(
        select(Run).where(Run.model_name == "clean_data"), tracker=tracker
    )
    # Should have 3 runs: original + cache hit + new config
    assert len(runs) == 3, "Should have original run + cache hit + new config run"

    # Verify signatures are different
    sigs = {r.signature for r in runs}
    # Should have 2 unique signatures (original config used twice, new config once)
    assert len(sigs) == 2, "Different configs should produce different signatures"

    # Verify the new config run is NOT a cache hit
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
        runtime_kwargs={"raw_file": raw_data_path},
        load_inputs=False,
        cache_mode="overwrite",
    )
    tracker.run(
        fn=clean_data_overwrite,
        inputs={"raw_file": raw_data_path},
        config=config,
        runtime_kwargs={"raw_file": raw_data_path},
        load_inputs=False,
        cache_mode="overwrite",
    )

    # Both should execute (overwrite mode)
    overwrite_runs = consist.run_query(
        select(Run).where(Run.model_name == "clean_data_overwrite"), tracker=tracker
    )
    # Should have 2 executions even with same inputs
    assert len(overwrite_runs) == 2

    # === CLI validation using --json for robust testing ===
    db_path = str(tracker.db_path)
    with patch("consist.cli.get_tracker", return_value=tracker):
        # Test runs command with JSON output
        runs_result = runner.invoke(
            cli_app, ["runs", "--db-path", db_path, "--json", "--limit", "10"]
        )
        assert runs_result.exit_code == 0
        runs_data = json.loads(runs_result.stdout)

        # Should have runs for all our tasks
        models = {r["model"] for r in runs_data}
        assert "clean_data" in models
        assert "analyze_data" in models
        assert "run_legacy_analysis" in models

        # Verify some runs have cache hits
        cache_hits = [
            r for r in runs_data if r.get("meta", {}).get("cache_hit") is True
        ]
        assert len(cache_hits) > 0, "Should have some cache hits recorded"

        # Test show command on analyze task
        analyze_run_id = next(
            r["id"] for r in runs_data if r["model"] == "analyze_data"
        )
        show_result = runner.invoke(
            cli_app, ["show", analyze_run_id, "--db-path", db_path]
        )
        assert show_result.exit_code == 0
        assert "analyze_data" in show_result.stdout
        assert "completed" in show_result.stdout.lower()

        # Test artifacts command
        artifacts_result = runner.invoke(
            cli_app, ["artifacts", analyze_run_id, "--db-path", db_path]
        )
        assert artifacts_result.exit_code == 0
        # Should show input and output artifacts
        assert "Input" in artifacts_result.stdout
        assert "Output" in artifacts_result.stdout

        # Test summary
        summary_result = runner.invoke(cli_app, ["summary", "--db-path", db_path])
        assert summary_result.exit_code == 0
        assert "Runs" in summary_result.stdout
        assert "Artifacts" in summary_result.stdout

        # Test scenarios command (tasks don't create scenarios but command should work)
        scenarios_result = runner.invoke(cli_app, ["scenarios", "--db-path", db_path])
        # Should succeed even if no scenarios exist
        assert scenarios_result.exit_code == 0

    # === JSON snapshot validation ===
    json_files = list(tracker.run_dir.glob("**/consist.json"))
    assert len(json_files) > 0, "Should have JSON snapshots"

    # Load one of the JSON files and validate structure
    with open(json_files[0]) as f:
        json_data = json.load(f)

    assert "run" in json_data
    assert "config" in json_data
    assert json_data["run"]["status"] == "completed"
    assert json_data["run"]["signature"] is not None

    # === Test artifact chaining with modified data ===
    # Verify that changing upstream input invalidates downstream cache
    df_modified = df_raw.copy()
    df_modified["value"] = df_modified["value"] * 1.5
    modified_path = run_dir / "raw_modified.csv"
    df_modified.to_csv(modified_path, index=False)

    # Clean the modified data
    cleaned_modified_result = tracker.run(
        fn=clean_data,
        inputs={"raw_file": modified_path},
        config=config,
        runtime_kwargs={"raw_file": modified_path},
        load_inputs=False,
    )

    # Should be a new run (different input)
    clean_runs = consist.run_query(
        select(Run).where(Run.model_name == "clean_data"), tracker=tracker
    )
    # Should have original, new config, and modified input = 3 runs
    assert len(clean_runs) == 4, (
        "Should have original, cache hit, new config, and modified input runs"
    )

    # Verify the cleaned data is different
    df_cleaned_modified = consist.load_df(cleaned_modified_result.outputs["cleaned"])
    assert not df_cleaned_modified.equals(df_cleaned)
