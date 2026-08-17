import subprocess
import sys
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).parents[2]
SCRIPT_PATH = PROJECT_ROOT / "examples" / "scripts" / "research_pipeline_native.py"


def test_research_pipeline_native_runs_without_checked_in_input(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT_PATH)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "Pipeline complete." in result.stdout

    example_workspace = tmp_path / "examples" / "runs" / "research_pipeline_native"
    assert (example_workspace / "raw_data.csv").is_file()
    assert not (tmp_path / "examples" / "data" / "raw_data.csv").exists()

    with duckdb.connect(
        str(example_workspace / "research_provenance.duckdb"), read_only=True
    ) as connection:
        runs = connection.execute(
            "SELECT model_name, git_hash FROM run ORDER BY started_at"
        ).fetchall()

    assert {model_name for model_name, _git_hash in runs} == {
        "climate_analysis",
        "preprocess",
        "simulate",
    }
    assert all(git_hash != "unknown_code_version" for _model_name, git_hash in runs)
