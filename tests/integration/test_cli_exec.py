from __future__ import annotations

import json
import re
import sys
from pathlib import Path

from typer.testing import CliRunner

from consist.cli import app

runner = CliRunner()


def test_cli_init_creates_project_structure(tmp_path: Path):
    res = runner.invoke(app, ["init", "--path", str(tmp_path)])
    assert res.exit_code == 0
    assert "Initialized Consist project" in res.output

    consist_dir = tmp_path / ".consist"
    runs_dir = tmp_path / "runs"
    db_file = consist_dir / "provenance.duckdb"

    assert consist_dir.is_dir()
    assert runs_dir.is_dir()
    assert db_file.is_file()


def test_cli_exec_basic(tmp_path: Path):
    # Initialize project in tmp_path
    init_res = runner.invoke(app, ["init", "--path", str(tmp_path)])
    assert init_res.exit_code == 0

    in_file = tmp_path / "data.txt"
    in_file.write_text("input content")
    out_file = tmp_path / "output.txt"

    cmd = [
        "exec",
        "--db-path",
        str(tmp_path / ".consist" / "provenance.duckdb"),
        "--run-dir",
        str(tmp_path / "runs"),
        "--name",
        "cli_basic_run",
        "-i",
        f"raw={in_file}",
        "-o",
        f"out={out_file}",
        "--",
        sys.executable,
        "-c",
        f"import pathlib; pathlib.Path('{out_file}').write_text('processed'); print('live streaming stdout')",
    ]

    res = runner.invoke(app, cmd)
    assert res.exit_code == 0
    assert "live streaming stdout" in res.stdout
    assert "Consist Exec Summary" in res.stdout
    assert "COMPLETED" in res.stdout
    assert out_file.read_text() == "processed"


def test_cli_exec_quiet_suppresses_stdout(tmp_path: Path):
    init_res = runner.invoke(app, ["init", "--path", str(tmp_path)])
    assert init_res.exit_code == 0

    cmd = [
        "exec",
        "--db-path",
        str(tmp_path / ".consist" / "provenance.duckdb"),
        "--run-dir",
        str(tmp_path / "runs"),
        "--name",
        "cli_quiet_run",
        "-q",
        "--",
        sys.executable,
        "-c",
        "print('child output')",
    ]

    res = runner.invoke(app, cmd)
    assert res.exit_code == 0
    assert "child output" in res.stdout
    assert "Consist Exec Summary" not in res.stdout
    assert "COMPLETED" not in res.stdout


def test_cli_exec_dry_run_and_json(tmp_path: Path):
    init_res = runner.invoke(app, ["init", "--path", str(tmp_path)])
    assert init_res.exit_code == 0

    in_file = tmp_path / "input.csv"
    in_file.write_text("a,b\n1,2\n")
    out_file = tmp_path / "out.csv"

    # 1. Dry run with --json
    cmd_dry = [
        "exec",
        "--db-path",
        str(tmp_path / ".consist" / "provenance.duckdb"),
        "--run-dir",
        str(tmp_path / "runs"),
        "--name",
        "dry_run_test",
        "-i",
        f"raw={in_file}",
        "-o",
        f"out={out_file}",
        "--dry-run",
        "--json",
        "--",
        sys.executable,
        "-c",
        "pass",
    ]

    res_dry = runner.invoke(app, cmd_dry)
    assert res_dry.exit_code == 0
    plan = json.loads(res_dry.stdout)
    assert plan["name"] == "dry_run_test"
    assert "raw" in plan["inputs"]
    assert "out" in plan["outputs"]
    assert plan["outputs"]["out"]["exists"] is False

    # 2. Real run with --json
    cmd_run = [
        "exec",
        "--db-path",
        str(tmp_path / ".consist" / "provenance.duckdb"),
        "--run-dir",
        str(tmp_path / "runs"),
        "--name",
        "json_run_test",
        "-i",
        f"raw={in_file}",
        "-o",
        f"out={out_file}",
        "--json",
        "--",
        sys.executable,
        "-c",
        f"import pathlib; pathlib.Path('{out_file}').write_text('json output')",
    ]

    res_run = runner.invoke(app, cmd_run)
    assert res_run.exit_code == 0
    result_data = json.loads(res_run.stdout)
    assert result_data["name"] == "json_run_test"
    assert result_data["exit_code"] == 0
    assert result_data["status"] == "completed"
    assert "raw" in result_data["inputs"]
    assert "out" in result_data["outputs"]
    assert "artifacts" in result_data
    assert len(result_data["artifacts"]) == 2
    art_keys = {a["key"] for a in result_data["artifacts"]}
    assert "raw" in art_keys
    assert "out" in art_keys


def test_cli_exec_command_not_found_exits_127(tmp_path: Path):
    init_res = runner.invoke(app, ["init", "--path", str(tmp_path)])
    assert init_res.exit_code == 0

    cmd = [
        "exec",
        "--db-path",
        str(tmp_path / ".consist" / "provenance.duckdb"),
        "--run-dir",
        str(tmp_path / "runs"),
        "--name",
        "missing_bin_run",
        "--",
        "nonexistent_command_binary_12345",
    ]

    res = runner.invoke(app, cmd)
    assert res.exit_code == 127
    assert "Command not found: nonexistent_command_binary_12345" in res.stdout


def test_cli_exec_non_zero_exit_propagates(tmp_path: Path):
    init_res = runner.invoke(app, ["init", "--path", str(tmp_path)])
    assert init_res.exit_code == 0

    cmd = [
        "exec",
        "--db-path",
        str(tmp_path / ".consist" / "provenance.duckdb"),
        "--run-dir",
        str(tmp_path / "runs"),
        "--name",
        "failing_run",
        "--",
        sys.executable,
        "-c",
        "import sys; sys.exit(7)",
    ]

    res = runner.invoke(app, cmd)
    assert res.exit_code == 7
    assert "FAILED" in res.stdout


def test_cli_exec_missing_declared_output_fails(tmp_path: Path):
    init_res = runner.invoke(app, ["init", "--path", str(tmp_path)])
    assert init_res.exit_code == 0

    missing_out = tmp_path / "never_created.csv"
    cmd = [
        "exec",
        "--db-path",
        str(tmp_path / ".consist" / "provenance.duckdb"),
        "--run-dir",
        str(tmp_path / "runs"),
        "--name",
        "missing_out_run",
        "-o",
        f"target={missing_out}",
        "--",
        sys.executable,
        "-c",
        "print('done without creating file')",
    ]

    res = runner.invoke(app, cmd)
    assert res.exit_code == 1
    assert "FAILED" in res.stdout
    cleaned = re.sub(r"\s*│\s*", "", res.stdout)
    assert "Declared output 'target'" in cleaned
    assert missing_out.name in cleaned
    assert "was not created" in cleaned


def test_cli_exec_upward_discovery(tmp_path: Path, monkeypatch):
    # Initialize project in tmp_path
    init_res = runner.invoke(app, ["init", "--path", str(tmp_path)])
    assert init_res.exit_code == 0

    # Create nested subdirectories
    nested_dir = tmp_path / "nested" / "sub" / "folder"
    nested_dir.mkdir(parents=True)

    monkeypatch.chdir(nested_dir)

    out_file = nested_dir / "out.txt"
    cmd = [
        "exec",
        "--name",
        "nested_run",
        "-o",
        f"out={out_file}",
        "--",
        sys.executable,
        "-c",
        f"import pathlib; pathlib.Path('{out_file}').write_text('nested done')",
    ]

    res = runner.invoke(app, cmd)
    assert res.exit_code == 0
    assert "COMPLETED" in res.stdout

    # Verify that the run was logged into the root .consist/provenance.duckdb
    from consist.core.tracker import Tracker

    db_file = tmp_path / ".consist" / "provenance.duckdb"
    tr = Tracker(db_path=str(db_file), run_dir=tmp_path / "runs")
    runs = tr.find_runs()
    model_names = [r.model_name for r in runs]
    assert "nested_run" in model_names


def test_cli_exec_accept_suggested(tmp_path: Path):
    init_res = runner.invoke(app, ["init", "--path", str(tmp_path)])
    assert init_res.exit_code == 0

    in_file = tmp_path / "suggested_input.csv"
    in_file.write_text("1,2,3")
    out_file = tmp_path / "out.txt"

    cmd = [
        "exec",
        "--db-path",
        str(tmp_path / ".consist" / "provenance.duckdb"),
        "--run-dir",
        str(tmp_path / "runs"),
        "--name",
        "suggested_run",
        "--accept-suggested",
        "-o",
        f"out={out_file}",
        "--json",
        "--",
        sys.executable,
        "-c",
        f"import pathlib; pathlib.Path('{out_file}').write_text('done')",
        str(in_file),
    ]

    res = runner.invoke(app, cmd)
    assert res.exit_code == 0
    data = json.loads(res.stdout)
    assert data["exit_code"] == 0
    # Input was auto-admitted
    assert len(data["inputs"]) >= 1
    input_paths = [inp["path"] for inp in data["inputs"].values()]
    assert str(in_file.resolve()) in input_paths


def test_cli_exec_shell_compound_command(tmp_path: Path):
    init_res = runner.invoke(app, ["init", "--path", str(tmp_path)])
    assert init_res.exit_code == 0

    out_file = tmp_path / "piped.txt"
    cmd = [
        "exec",
        "--db-path",
        str(tmp_path / ".consist" / "provenance.duckdb"),
        "--run-dir",
        str(tmp_path / "runs"),
        "--shell",
        "-o",
        f"piped={out_file}",
        "--",
        f"{sys.executable} -c \"print('piped output')\" > {out_file}",
    ]
    res = runner.invoke(app, cmd)
    assert res.exit_code == 0
    assert "COMPLETED" in res.stdout
    assert out_file.read_text().strip() == "piped output"


def test_cli_exec_keyboard_interrupt(tmp_path: Path, monkeypatch):
    init_res = runner.invoke(app, ["init", "--path", str(tmp_path)])
    assert init_res.exit_code == 0

    from consist.core.tracker import Tracker

    def raise_interrupt(*args, **kwargs):
        raise KeyboardInterrupt()

    monkeypatch.setattr(Tracker, "exec", raise_interrupt)

    cmd = [
        "exec",
        "--db-path",
        str(tmp_path / ".consist" / "provenance.duckdb"),
        "--run-dir",
        str(tmp_path / "runs"),
        "--",
        sys.executable,
        "-c",
        "pass",
    ]
    res = runner.invoke(app, cmd)
    assert res.exit_code == 130


def test_cli_exec_quiet_error_to_stderr(tmp_path: Path):
    init_res = runner.invoke(app, ["init", "--path", str(tmp_path)])
    assert init_res.exit_code == 0

    missing_out = tmp_path / "not_created.txt"
    cmd = [
        "exec",
        "--db-path",
        str(tmp_path / ".consist" / "provenance.duckdb"),
        "--run-dir",
        str(tmp_path / "runs"),
        "--quiet",
        "-o",
        f"target={missing_out}",
        "--",
        sys.executable,
        "-c",
        "pass",
    ]
    res = runner.invoke(app, cmd)
    assert res.exit_code == 1
    assert "Consist Exec Summary" not in res.stdout
    assert "Error:" in res.stderr
    assert "not_created.txt" in res.stderr
