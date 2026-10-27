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
    assert res.error is not None
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
    steady_file = tmp_path / "steady.txt"

    script = (
        f"import pathlib; "
        f"txt = pathlib.Path('{in_file}').read_text(); "
        f"pathlib.Path('{out_file}').write_text('derived:' + txt); "
        f"pathlib.Path('{steady_file}').write_text('constant')"
    )
    py_cmd = [sys.executable, "-c", script]

    # Run 1: initial baseline
    res1 = tracker.exec(
        name="my_model",
        argv=py_cmd,
        inputs={"data": in_file},
        outputs={"out": out_file, "steady": steady_file},
        stream_output=False,
    )
    assert res1.is_success

    # Modify input file for run 2 (which also causes out_file to be overwritten with new bytes)
    time.sleep(0.01)
    in_file.write_text("version 2")

    res2 = tracker.exec(
        name="my_model",
        argv=py_cmd,
        inputs={"data": in_file},
        outputs={"out": out_file, "steady": steady_file},
        stream_output=False,
        compare_baseline=True,
    )
    assert res2.is_success
    assert res2.outputs["out"].hash != res1.outputs["out"].hash
    assert res2.outputs["steady"].hash == res1.outputs["steady"].hash
    assert res2.run.meta is not None
    cmd_meta = res2.run.meta.get("command", {})
    baseline_comp = cmd_meta.get("baseline_comparison")
    assert baseline_comp is not None
    assert baseline_comp["baseline_run_id"] == res1.run.id
    assert baseline_comp["command_changed"] is False
    # "data" input and "out" output were modified; "steady" output was unchanged
    assert "data" in baseline_comp["inputs"]["modified"]
    assert "out" in baseline_comp["outputs"]["modified"]
    assert "steady" in baseline_comp["outputs"]["unchanged"]


def test_tracker_exec_input_modified_out_of_band_after_upstream_output(
    tracker: Tracker, tmp_path: Path
):
    shared_file = tmp_path / "intermediate.csv"
    downstream_out = tmp_path / "final.csv"

    # Step 1 produces intermediate.csv as an output
    step1 = tracker.exec(
        name="producer",
        argv=[
            sys.executable,
            "-c",
            f"import pathlib; pathlib.Path('{shared_file}').write_text('v1')",
        ],
        outputs={"inter": shared_file},
        stream_output=False,
    )
    assert step1.is_success
    v1_hash = step1.outputs["inter"].hash

    # Step 2 consumes intermediate.csv without out-of-band edits -> preserves upstream lineage
    step2_run1 = tracker.exec(
        name="consumer",
        argv=[
            sys.executable,
            "-c",
            f"import pathlib; pathlib.Path('{downstream_out}').write_text(pathlib.Path('{shared_file}').read_text())",
        ],
        inputs={"inter": shared_file},
        outputs={"final": downstream_out},
        stream_output=False,
    )
    assert step2_run1.is_success
    assert step2_run1.inputs["inter"].hash == v1_hash
    assert step2_run1.inputs["inter"].run_id == step1.run.id

    # Now a user edits intermediate.csv out-of-band on disk without re-running Step 1
    shared_file.write_text("v2_edited_by_hand")

    step2_run2 = tracker.exec(
        name="consumer",
        argv=[
            sys.executable,
            "-c",
            f"import pathlib; pathlib.Path('{downstream_out}').write_text(pathlib.Path('{shared_file}').read_text())",
        ],
        inputs={"inter": shared_file},
        outputs={"final": downstream_out},
        stream_output=False,
        compare_baseline=True,
    )
    assert step2_run2.is_success
    assert step2_run2.inputs["inter"].hash != v1_hash
    baseline_comp = step2_run2.run.meta["command"]["baseline_comparison"]
    assert "inter" in baseline_comp["inputs"]["modified"]
    assert "final" in baseline_comp["outputs"]["modified"]


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

    runs = tracker.find_runs(model="test_interrupt")
    assert len(runs) >= 1
    run = runs[0]
    assert run.status == "failed"
    assert run.meta["command"]["interrupted"] is True
    assert run.meta["command"]["exit_code"] == 130
    assert run.meta["command"]["process_exit_code"] == 130


def test_tracker_exec_stream_output_with_stringio(tracker: Tracker):
    import io

    fake_stdout = io.StringIO()
    with patch("sys.stdout", fake_stdout):
        res = tracker.exec(
            name="test_stringio_stream",
            argv=[sys.executable, "-c", "print('hello stringio')"],
            stream_output=True,
        )
    assert res.is_success
    assert "hello stringio" in fake_stdout.getvalue()


def test_tracker_exec_command_not_found_records_failed_127(tracker: Tracker):
    res = tracker.exec(
        name="test_not_found",
        argv=["nonexistent_command_xyz_987"],
        stream_output=False,
        check=False,
    )
    assert not res.is_success
    assert res.exit_code == 127
    assert res.run is not None
    assert res.run.status == "failed"
    assert res.error is not None
    assert "Command not found: nonexistent_command_xyz_987" in res.error

    # Verify DuckDB persisted the failed status
    found = tracker.find_runs(model="test_not_found")
    assert len(found) >= 1
    assert found[0].status == "failed"


def test_tracker_exec_permission_denied_records_failed_126(tracker: Tracker):
    with patch("subprocess.Popen", side_effect=PermissionError("Permission denied")):
        res = tracker.exec(
            name="test_perm_denied",
            argv=["/restricted/binary"],
            stream_output=False,
            check=False,
        )
    assert not res.is_success
    assert res.exit_code == 126
    assert res.run is not None
    assert res.run.status == "failed"
    assert res.error is not None
    assert "Permission denied executing command" in res.error


def test_tracker_exec_signal_conversion(tracker: Tracker):
    import io

    with patch("subprocess.Popen") as mock_popen:
        mock_proc = mock_popen.return_value
        mock_proc.stdout = io.BytesIO(b"")
        mock_proc.stderr = io.BytesIO(b"")
        mock_proc.returncode = -9  # SIGKILL
        mock_proc.wait.return_value = -9

        res = tracker.exec(
            name="test_signal",
            argv=["fake_cmd"],
            stream_output=False,
            check=False,
        )
    assert not res.is_success
    assert res.exit_code == 137  # 128 + 9
    assert res.run is not None
    assert res.run.status == "failed"
    assert res.run.meta["command"]["signal"] == 9


def test_tracker_exec_outputs_dir_relative_to_cwd(tracker: Tracker, tmp_path: Path):
    sub_cwd = tmp_path / "work"
    sub_cwd.mkdir()
    rel_out_dir = Path("results")

    py_cmd = [
        sys.executable,
        "-c",
        "import pathlib; (pathlib.Path('results')).mkdir(exist_ok=True); (pathlib.Path('results/out.txt')).write_text('cwd output')",
    ]

    res = tracker.exec(
        name="test_cwd_rel_outputs_dir",
        argv=py_cmd,
        cwd=sub_cwd,
        outputs_dir=rel_out_dir,
        stream_output=False,
    )

    assert res.is_success
    assert "out.txt" in res.outputs
    assert (sub_cwd / "results" / "out.txt").is_file()


def test_tracker_exec_empty_argv_raises(tracker: Tracker):
    for empty_val in [[], "", "   ", [""], ["", "  "]]:
        with pytest.raises(ValueError, match="argv must not be empty"):
            tracker.exec(name="test_empty", argv=empty_val)


def test_tracker_exec_missing_declared_output_meta(tracker: Tracker, tmp_path: Path):
    missing_file = tmp_path / "never_created.txt"
    res = tracker.exec(
        name="test_missing_out_meta",
        argv=[sys.executable, "-c", "pass"],
        outputs={"missing": missing_file},
        stream_output=False,
        check=False,
    )
    assert not res.is_success
    assert res.exit_code == 1
    assert res.run is not None
    assert res.run.status == "failed"
    assert res.run.meta["command"]["process_exit_code"] == 0
    assert res.run.meta["command"]["exit_code"] == 1


def test_tracker_exec_bounded_memory_streaming(tracker: Tracker, tmp_path: Path):
    py_cmd = [
        sys.executable,
        "-c",
        "print('A' * 1000)",
    ]
    res = tracker.exec(
        name="test_bounded_stream",
        argv=py_cmd,
        max_memory_stream_bytes=64,
        stream_output=False,
    )
    assert res.is_success
    assert len(res.stdout.encode("utf-8")) <= 64
    assert res.stdout_text == res.stdout
    assert res.stderr_text == res.stderr
    assert res.stdout_path is not None
    disk_content = res.stdout_path.read_text().strip()
    assert len(disk_content) == 1000
    assert disk_content == "A" * 1000

    # Test max_memory_stream_bytes=0
    res_zero = tracker.exec(
        name="test_zero_memory_stream",
        argv=py_cmd,
        max_memory_stream_bytes=0,
        stream_output=False,
    )
    assert res_zero.is_success
    assert res_zero.stdout == ""
    assert res_zero.stdout_path is not None
    assert res_zero.stdout_path.read_text().strip() == "A" * 1000

    # Test output exceeding 4MB default limit
    py_cmd_large = [
        sys.executable,
        "-c",
        "import sys; sys.stdout.write('X' * (5 * 1024 * 1024))",
    ]
    res_large = tracker.exec(
        name="test_large_stream",
        argv=py_cmd_large,
        stream_output=False,
    )
    assert res_large.is_success
    assert len(res_large.stdout.encode("utf-8")) <= 4 * 1024 * 1024
    assert res_large.stdout_path is not None
    assert res_large.stdout_path.stat().st_size == 5 * 1024 * 1024


def test_tracker_exec_binary_output(tracker: Tracker):
    py_cmd = [
        sys.executable,
        "-c",
        "import sys; sys.stdout.buffer.write(b'out \\x80\\xff\\xfe ok\\n'); sys.stderr.buffer.write(b'err \\x81\\x82 err\\n')",
    ]
    res = tracker.exec(
        name="test_binary_output",
        argv=py_cmd,
        stream_output=False,
    )
    assert res.is_success
    assert "out " in res.stdout
    assert "\ufffd" in res.stdout
    assert "err " in res.stderr
    assert "\ufffd" in res.stderr
    assert res.stdout_path is not None
    assert res.stderr_path is not None
    assert res.stdout_path.read_bytes() == b"out \x80\xff\xfe ok\n"
    assert res.stderr_path.read_bytes() == b"err \x81\x82 err\n"


def test_tracker_exec_shell_argument_string_and_suggested_inputs(
    tracker: Tracker, tmp_path: Path
):
    out_file = tmp_path / "shell_out.txt"
    cmd_str = f"{sys.executable} -c \"print('shell execution')\" > {out_file}"
    res = tracker.exec(
        name="test_shell_str",
        argv=cmd_str,
        shell=True,
        outputs={"out": out_file},
        stream_output=False,
    )
    assert res.is_success
    assert out_file.read_text().strip() == "shell execution"
    assert res.run is not None
    assert res.run.meta["command"]["argv"] == [cmd_str]
