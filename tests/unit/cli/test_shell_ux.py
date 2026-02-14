from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

from consist.cli import ConsistShell


def test_shell_alias_ls_routes_to_runs_or_artifacts(tmp_path) -> None:
    tracker = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker)

    with (
        patch.object(shell, "do_runs") as do_runs,
        patch.object(shell, "do_artifacts") as do_artifacts,
    ):
        shell.do_ls("")
        do_runs.assert_called_once_with("")
        do_artifacts.assert_not_called()

    with (
        patch.object(shell, "do_runs") as do_runs,
        patch.object(shell, "do_artifacts") as do_artifacts,
    ):
        shell.do_ls("run-123")
        do_artifacts.assert_called_once_with("run-123")
        do_runs.assert_not_called()


def test_shell_alias_cat_and_q_route_to_preview_and_quit(tmp_path) -> None:
    tracker = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker)

    with patch.object(shell, "do_preview") as do_preview:
        shell.do_cat("artifact-key")
        do_preview.assert_called_once_with("artifact-key")

    with patch.object(shell, "do_quit", return_value=True) as do_quit:
        assert shell.do_q("") is True
        do_quit.assert_called_once_with("")


def test_shell_show_picker_selects_run_in_tty_mode(tmp_path) -> None:
    tracker = MagicMock()
    tracker.get_run.return_value = MagicMock()

    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker)

    with (
        patch.object(shell, "_is_tty", return_value=True),
        patch.object(shell, "_recent_run_ids", return_value=["run-a", "run-b"]),
        patch("builtins.input", return_value="2"),
        patch("consist.cli._render_run_details") as render_run,
    ):
        shell.do_show("")

    tracker.get_run.assert_called_once_with("run-b")
    render_run.assert_called_once()


def test_shell_artifacts_no_arg_non_tty_errors_without_prompt(tmp_path, capsys) -> None:
    tracker = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker)

    with (
        patch.object(shell, "_is_tty", return_value=False),
        patch("builtins.input") as fake_input,
    ):
        shell.do_artifacts("")

    out = capsys.readouterr().out
    assert "run_id required" in out
    fake_input.assert_not_called()


def test_shell_preview_picker_selects_artifact_in_tty_mode(tmp_path) -> None:
    tracker = MagicMock()
    tracker.get_artifact.return_value = SimpleNamespace(
        id="artifact-id",
        key="artifact-one",
        driver="csv",
        run_id=None,
        container_uri="inputs://data/file.csv",
        meta={},
    )

    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker)

    with (
        patch.object(shell, "_is_tty", return_value=True),
        patch.object(shell, "_recent_artifact_keys", return_value=["artifact-one"]),
        patch("builtins.input", return_value="1"),
        patch(
            "consist.cli._load_artifact_with_diagnostics",
            return_value=pd.DataFrame({"value": [1, 2]}),
        ),
    ):
        shell.do_preview("")

    tracker.get_artifact.assert_called_once_with("artifact-one")


def test_shell_completions_include_aliases_flags_and_dynamic_values(tmp_path) -> None:
    tracker = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker)

    assert "ls" in shell.completenames("l")
    assert "q" in shell.completenames("q")
    assert shell.complete_runs("--m", "runs --m", 5, 8) == ["--model"]

    with patch.object(shell, "_recent_run_ids", return_value=["run-1", "run-2"]):
        assert shell.complete_show("run-", "show run-", 5, 9) == ["run-1", "run-2"]
        assert shell.complete_artifacts("run-2", "artifacts run-2", 10, 15) == ["run-2"]

    with patch.object(
        shell, "_recent_artifact_keys", return_value=["artifact-1", "table-2"]
    ):
        assert shell.complete_preview("art", "preview art", 8, 11) == ["artifact-1"]
        assert shell.complete_schema_profile(
            "table", "schema_profile table", 15, 20
        ) == ["table-2"]

    assert shell.complete_schema_stub("--c", "schema_stub --c", 12, 15) == [
        "--class-name",
        "--concrete",
    ]


def test_shell_completion_falls_back_to_empty_on_query_failure(tmp_path) -> None:
    tracker = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker)

    with patch("consist.cli._tracker_session", side_effect=RuntimeError("db down")):
        assert shell.complete_show("", "show ", 5, 5) == []
        assert shell.complete_artifacts("", "artifacts ", 10, 10) == []
        assert shell.complete_preview("", "preview ", 8, 8) == []
        assert shell.complete_schema_profile("", "schema_profile ", 15, 15) == []
        assert shell.complete_schema_stub("", "schema_stub ", 12, 12) == []


def test_shell_history_loads_existing_file_on_start(tmp_path) -> None:
    tracker = MagicMock()
    history_path = tmp_path / ".consist" / "shell_history"
    history_path.parent.mkdir(parents=True, exist_ok=True)
    history_path.write_text("runs\n", encoding="utf-8")
    fake_readline = MagicMock()

    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", fake_readline),
    ):
        ConsistShell(tracker)

    fake_readline.read_history_file.assert_called_once_with(str(history_path))


def test_shell_history_saves_on_exit_and_eof(tmp_path) -> None:
    tracker = MagicMock()
    history_path = tmp_path / ".consist" / "shell_history"

    fake_readline_exit = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", fake_readline_exit),
    ):
        shell = ConsistShell(tracker)
        shell.do_exit("")

    fake_readline_exit.write_history_file.assert_called_once_with(str(history_path))

    fake_readline_eof = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", fake_readline_eof),
    ):
        shell = ConsistShell(tracker)
        shell.do_EOF("")

    fake_readline_eof.write_history_file.assert_called_once_with(str(history_path))
