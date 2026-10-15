from __future__ import annotations

import sys
import time
from pathlib import Path
from unittest.mock import patch

import pytest

import consist
from consist.core.command_exec import (
    CommandExecutionError,
    CommandExecutionResult,
    compute_env_digest,
    discover_suggested_inputs,
)
from consist.core.tracker import Tracker


def test_compute_env_digest():
    digest1, env1 = compute_env_digest(custom_env={"FOO": "BAR", "BAZ": "123"})
    assert isinstance(digest1, str)
    assert len(digest1) == 64
    assert env1["FOO"] == "BAR"
    assert env1["BAZ"] == "123"

    # Determinism
    digest2, env2 = compute_env_digest(custom_env={"BAZ": "123", "FOO": "BAR"})
    assert digest1 == digest2
    assert env1 == env2

    # Different env produces different digest
    digest3, _ = compute_env_digest(custom_env={"FOO": "DIFFERENT"})
    assert digest1 != digest3


def test_discover_suggested_inputs(tmp_path: Path):
    file1 = tmp_path / "data.csv"
    file1.write_text("a,b,c\n1,2,3\n")
    file2 = tmp_path / "model.bin"
    file2.write_text("model data")
    out_file = tmp_path / "out.csv"

    argv = [
        "python",
        "script.py",
        "--input",
        str(file1),
        "--threshold",
        "42.5",
        "http://example.com/test",
        str(file2),
        str(out_file),
    ]

    suggested = discover_suggested_inputs(
        argv,
        base_dir=tmp_path,
        declared_outputs={out_file.resolve()},
    )

    suggested_paths = [p for _, p in suggested]
    assert file1.resolve() in suggested_paths
    assert file2.resolve() in suggested_paths
    # out_file is in declared_outputs so it must NOT be suggested
    assert out_file.resolve() not in suggested_paths


def test_tracker_exec_basic_success(tracker: Tracker, tmp_path: Path):
    in_file = tmp_path / "input.txt"
    in_file.write_text("hello consist")
    out_file = tmp_path / "output.txt"

    py_cmd = [
        sys.executable,
        "-c",
        f"import pathlib; p = pathlib.Path('{out_file}'); p.write_text(pathlib.Path('{in_file}').read_text() + ' processed'); print('done streaming')",
    ]

    res = tracker.exec(
        name="test_basic",
        argv=py_cmd,
        inputs={"raw": in_file},
        outputs={"proc": out_file},
        stream_output=False,
    )

    assert isinstance(res, CommandExecutionResult)
    assert res.is_success
    assert res.exit_code == 0
    assert "done streaming" in res.stdout
    assert res.run is not None
    assert res.run.status == "completed"
    assert "raw" in res.inputs
    assert "proc" in res.outputs
    assert out_file.read_text() == "hello consist processed"

    # Verify log sidecars exist
    assert res.stdout_path is not None and res.stdout_path.is_file()
    assert res.stderr_path is not None and res.stderr_path.is_file()
    assert "done streaming" in res.stdout_path.read_text()


def test_tracker_exec_non_zero_exit_marks_failed(tracker: Tracker):
    py_cmd = [
        sys.executable,
        "-c",
        "import sys; sys.stderr.write('fatal crash\\n'); sys.exit(42)",
    ]

    res = tracker.exec(
        name="test_failure",
        argv=py_cmd,
        stream_output=False,
        check=False,
    )

    assert not res.is_success
    assert res.exit_code == 42
    assert "fatal crash" in res.stderr
    assert res.run is not None
    assert res.run.status == "failed"
    assert "Command failed with exit code 42" in res.error

    # With check=True, raises CommandExecutionError
    with pytest.raises(CommandExecutionError) as exc_info:
        tracker.exec(
            name="test_failure_check",
            argv=py_cmd,
            stream_output=False,
            check=True,
        )
    assert exc_info.value.exit_code == 42
    assert "Command failed with exit code 42" in str(exc_info.value)


def test_tracker_exec_missing_declared_output_fails(tracker: Tracker):
    missing_path = Path("/tmp/nonexistent_output_never_created.csv")
    if missing_path.exists():
        missing_path.unlink()

    py_cmd = [sys.executable, "-c", "print('did not create output')"]

    res = tracker.exec(
        name="test_missing_output",
        argv=py_cmd,
        outputs={"important": missing_path},
        stream_output=False,
        check=False,
    )

    expected_msg = f"Declared output 'important' at '{missing_path}' was not created."
    assert not res.is_success
    assert res.exit_code == 1
    assert res.run is not None
    assert res.run.status == "failed"
    assert res.error == expected_msg

    # Verify check=True raises CommandExecutionError with exact message
    with pytest.raises(CommandExecutionError) as exc_info:
        tracker.exec(
            name="test_missing_output_check",
            argv=py_cmd,
            outputs={"important": missing_path},
            stream_output=False,
            check=True,
        )
    assert exc_info.value.exit_code == 1
    assert str(exc_info.value) == expected_msg


def test_tracker_exec_missing_input_raises_filenotfound(tracker: Tracker):
    nonexistent = Path("/tmp/definitely_missing_input_file.csv")
    if nonexistent.exists():
        nonexistent.unlink()

    with pytest.raises(FileNotFoundError, match="Declared input 'missing'"):
        tracker.exec(
            name="test_missing_input",
            argv=[sys.executable, "-c", "pass"],
            inputs={"missing": nonexistent},
        )


def test_tracker_exec_input_output_overlap_raises(tracker: Tracker, tmp_path: Path):
    shared_file = tmp_path / "shared.csv"
    shared_file.write_text("content")

    with pytest.raises(
        ValueError, match="Path cannot be declared as both an input and an output"
    ):
        tracker.exec(
            name="test_overlap",
            argv=[sys.executable, "-c", "pass"],
            inputs={"in": shared_file},
            outputs={"out": shared_file},
        )


def test_tracker_exec_dry_run(tracker: Tracker, tmp_path: Path):
    in_file = tmp_path / "data.csv"
    in_file.write_text("col1,col2\n10,20\n")
    out_file = tmp_path / "predictions.csv"

    res = tracker.exec(
        name="test_dry_run",
        argv=["python", "train.py", str(in_file), "--out", str(out_file)],
        inputs={"train_data": in_file},
        outputs={"preds": out_file},
        dry_run=True,
    )

    assert res.dry_run
    assert res.is_success
    assert res.run is None
    assert res.plan is not None
    assert res.plan["name"] == "test_dry_run"
    assert "train_data" in res.plan["inputs"]
    assert "preds" in res.plan["outputs"]
    assert res.plan["outputs"]["preds"]["exists"] is False
    assert len(res.plan["inputs"]["train_data"]["hash"]) == 64


def test_tracker_exec_accept_suggested(tracker: Tracker, tmp_path: Path):
    in_file = tmp_path / "data.csv"
    in_file.write_text("a,b\n1,2\n")
    out_file = tmp_path / "result.txt"

    py_cmd = [
        sys.executable,
        "-c",
        f"import pathlib; pathlib.Path('{out_file}').write_text('done')",
        str(in_file),
    ]

    res = tracker.exec(
        name="test_suggested",
        argv=py_cmd,
        outputs={"res": out_file},
        accept_suggested=True,
        stream_output=False,
    )

    assert res.is_success
    # The in_file should be auto-admitted as an input
    input_paths = [str(art.path) for art in res.inputs.values()]
    assert str(in_file.resolve()) in input_paths


def test_tracker_exec_outputs_dir_monitoring(tracker: Tracker, tmp_path: Path):
    out_dir = tmp_path / "generated"
    out_dir.mkdir()
    # Pre-existing file
    old_file = out_dir / "old.txt"
    old_file.write_text("old")

    py_cmd = [
        sys.executable,
        "-c",
        f"import pathlib; (pathlib.Path('{out_dir}') / 'new1.txt').write_text('new1'); (pathlib.Path('{out_dir}') / 'new2.txt').write_text('new2')",
    ]

    res = tracker.exec(
        name="test_outputs_dir",
        argv=py_cmd,
        outputs_dir=out_dir,
        stream_output=False,
    )

    assert res.is_success
    # new1.txt and new2.txt should be captured as outputs, old.txt should NOT be
    output_keys = set(res.outputs.keys())
    assert "new1.txt" in output_keys
    assert "new2.txt" in output_keys
    assert "old.txt" not in output_keys


def test_tracker_exec_baseline_comparison(tracker: Tracker, tmp_path: Path):
    in_file = tmp_path / "data.csv"
    in_file.write_text("version 1")
    out_file = tmp_path / "output.csv"

    script = f"import pathlib; pathlib.Path('{out_file}').write_text('out1')"
    py_cmd = [sys.executable, "-c", script]

    # Run 1: initial baseline
    res1 = tracker.exec(
        name="my_model",
        argv=py_cmd,
        inputs={"data": in_file},
        outputs={"out": out_file},
        stream_output=False,
    )
    assert res1.is_success

    # Modify input file for run 2
    time.sleep(0.01)
    in_file.write_text("version 2")

    res2 = tracker.exec(
        name="my_model",
        argv=py_cmd,
        inputs={"data": in_file},
        outputs={"out": out_file},
        stream_output=False,
        compare_baseline=True,
    )
    assert res2.is_success
    assert res2.run.meta is not None
    cmd_meta = res2.run.meta.get("command", {})
    baseline_comp = cmd_meta.get("baseline_comparison")
    assert baseline_comp is not None
    assert baseline_comp["baseline_run_id"] == res1.run.id
    assert baseline_comp["command_changed"] is False
    # "data" input was modified
    assert "data" in baseline_comp["inputs"]["modified"]


def test_consist_api_exec_delegates(tracker: Tracker, tmp_path: Path):
    out_file = tmp_path / "api_out.txt"
    py_cmd = [
        sys.executable,
        "-c",
        f"import pathlib; pathlib.Path('{out_file}').write_text('api works')",
    ]

    res = consist.exec(
        name="test_api_exec",
        argv=py_cmd,
        tracker=tracker,
        outputs={"out": out_file},
        stream_output=False,
    )

    assert isinstance(res, CommandExecutionResult)
    assert res.is_success
    assert out_file.read_text() == "api_works" or out_file.read_text() == "api works"


def test_tracker_exec_keyboard_interrupt(tracker: Tracker):
    with patch("subprocess.Popen") as mock_popen:
        mock_proc = mock_popen.return_value
        mock_proc.stdout = None
        mock_proc.stderr = None
        mock_proc.wait.side_effect = KeyboardInterrupt

        with pytest.raises(KeyboardInterrupt):
            tracker.exec(
                name="test_interrupt",
                argv=[sys.executable, "-c", "pass"],
                stream_output=False,
            )
