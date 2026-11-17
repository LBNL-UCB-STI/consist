from __future__ import annotations

import json
from pathlib import Path
import pandas as pd
from typer.testing import CliRunner

from consist.cli import app


runner = CliRunner()


def test_cli_schemas_empty(cli_runner, tracker):
    result = cli_runner.invoke(app, ["schemas", "--db-path", str(tracker.db_path)])
    assert result.exit_code == 0
    assert "No artifact schemas found" in result.stdout


def test_cli_schemas_and_views_suggest(cli_runner, tracker, tmp_path: Path):
    file1 = tmp_path / "linkstats1.parquet"
    pd.DataFrame({"link": [1, 2], "volume": [10.0, 20.0]}).to_parquet(file1)
    file2 = tmp_path / "linkstats2.parquet"
    pd.DataFrame({"link": [3, 4], "volume": [30.0, 40.0]}).to_parquet(file2)

    with tracker.start_run(
        "run_1", "beam", year=2018, iteration=0, cache_mode="overwrite"
    ):
        art1 = tracker.log_artifact(
            str(file1),
            key="beam.linkstats",
            driver="parquet",
            profile_file_schema=True,
            facet={"year": 2018},
            facet_index=True,
        )

    with tracker.start_run(
        "run_2", "beam", year=2019, iteration=0, cache_mode="overwrite"
    ):
        art2 = tracker.log_artifact(
            str(file2),
            key="beam.linkstats",
            driver="parquet",
            profile_file_schema=True,
            facet={"year": 2019},
            facet_index=True,
        )

    assert art1.meta.get("schema_id") == art2.meta.get("schema_id")
    sid = art1.meta.get("schema_id")

    # 1. Test `consist schemas` tabular output
    res = cli_runner.invoke(app, ["schemas", "--db-path", str(tracker.db_path)])
    assert res.exit_code == 0
    assert "beam.linkstats" in res.stdout
    assert "parquet" in res.stdout
    assert "2" in res.stdout  # 2 runs

    # 2. Test `consist schemas --json`
    res_json = cli_runner.invoke(
        app, ["schemas", "--db-path", str(tracker.db_path), "--json"]
    )
    assert res_json.exit_code == 0
    data = json.loads(res_json.stdout)
    assert len(data) == 1
    assert data[0]["key"] == "beam.linkstats"
    assert data[0]["driver"] == "parquet"
    assert data[0]["schema_id"] == sid
    assert data[0]["run_count"] == 2
    assert data[0]["artifact_count"] == 2

    # 3. Test `consist schemas --key` and `--driver` filters
    res_filtered = cli_runner.invoke(
        app,
        [
            "schemas",
            "--db-path",
            str(tracker.db_path),
            "--key",
            "beam.linkstats",
            "--json",
        ],
    )
    assert res_filtered.exit_code == 0
    assert len(json.loads(res_filtered.stdout)) == 1

    res_nomatch = cli_runner.invoke(
        app,
        ["schemas", "--db-path", str(tracker.db_path), "--key", "nonexistent"],
    )
    assert res_nomatch.exit_code == 0
    assert "No artifact schemas found" in res_nomatch.stdout

    # 4. Test `consist views suggest`
    res_suggest = cli_runner.invoke(
        app,
        ["views", "suggest", "--db-path", str(tracker.db_path)],
        env={"COLUMNS": "160"},
    )
    assert res_suggest.exit_code == 0
    assert "Suggested Grouped Views" in res_suggest.stdout
    assert "beam.linkstats" in res_suggest.stdout
    assert (
        "consist views create v_beam_linkstats --key beam.linkstats"
        in res_suggest.stdout
    )

    # 5. Test `consist views create` using `--key`
    res_create = cli_runner.invoke(
        app,
        [
            "views",
            "create",
            "v_linkstats",
            "--key",
            "beam.linkstats",
            "--db-path",
            str(tracker.db_path),
            "--mode",
            "cold_only",
        ],
    )
    assert res_create.exit_code == 0
    assert "Created grouped view 'v_linkstats'" in res_create.stdout

    df_view = tracker.query_df("SELECT COUNT(*) AS cnt FROM v_linkstats")
    assert int(df_view.iloc[0]["cnt"]) == 4


def test_cli_views_create_key_validation(cli_runner, tracker, tmp_path: Path):
    # Both --schema-id and --key
    res_both = cli_runner.invoke(
        app,
        [
            "views",
            "create",
            "v_err",
            "--schema-id",
            "sid123",
            "--key",
            "k123",
            "--db-path",
            str(tracker.db_path),
        ],
    )
    assert res_both.exit_code != 0
    assert "Provide either --schema-id or --key, not both" in res_both.stdout

    # Neither --schema-id nor --key
    res_neither = cli_runner.invoke(
        app,
        [
            "views",
            "create",
            "v_err",
            "--db-path",
            str(tracker.db_path),
        ],
    )
    assert res_neither.exit_code != 0
    assert (
        "Provide either --schema-id or --key to select artifacts" in res_neither.stdout
    )

    # --key not found
    res_not_found = cli_runner.invoke(
        app,
        [
            "views",
            "create",
            "v_err",
            "--key",
            "missing_key",
            "--db-path",
            str(tracker.db_path),
        ],
    )
    assert res_not_found.exit_code != 0
    assert "No schemas found for artifact key 'missing_key'" in res_not_found.stdout


def test_cli_views_create_ambiguous_key(cli_runner, tracker, tmp_path: Path):
    f1 = tmp_path / "art1.parquet"
    pd.DataFrame({"col_a": [1]}).to_parquet(f1)
    f2 = tmp_path / "art2.parquet"
    pd.DataFrame({"col_b": ["text"]}).to_parquet(f2)

    with tracker.start_run("r1", "model_a", cache_mode="overwrite"):
        tracker.log_artifact(
            str(f1), key="ambiguous_key", driver="parquet", profile_file_schema=True
        )

    with tracker.start_run("r2", "model_b", cache_mode="overwrite"):
        tracker.log_artifact(
            str(f2), key="ambiguous_key", driver="parquet", profile_file_schema=True
        )

    res_ambiguous = cli_runner.invoke(
        app,
        [
            "views",
            "create",
            "v_ambiguous",
            "--key",
            "ambiguous_key",
            "--db-path",
            str(tracker.db_path),
        ],
    )
    assert res_ambiguous.exit_code != 0
    assert (
        "Multiple schema IDs found for artifact key 'ambiguous_key'"
        in res_ambiguous.stdout
    )
    assert "disambiguate" in res_ambiguous.stdout

    # Test that views suggest suggests --schema-id for ambiguous keys
    res_suggest_ambiguous = cli_runner.invoke(
        app,
        [
            "views",
            "suggest",
            "--db-path",
            str(tracker.db_path),
            "--key",
            "ambiguous_key",
        ],
        env={"COLUMNS": "160"},
    )
    assert res_suggest_ambiguous.exit_code == 0
    assert "--schema-id" in res_suggest_ambiguous.stdout
    assert "--key" not in res_suggest_ambiguous.stdout
