from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

from consist.cli import ConsistShell


def test_shell_tty_check_uses_shared_helper() -> None:
    with patch("consist.cli._is_interactive_tty", return_value=True) as helper:
        assert ConsistShell._is_tty() is True
    helper.assert_called_once()


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
    normalized = " ".join(out.split())
    assert "run_id required" in out
    assert "run `runs` first to populate shortcuts" in normalized
    fake_input.assert_not_called()


def test_shell_preview_no_arg_non_tty_reports_next_steps(tmp_path, capsys) -> None:
    tracker = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker)

    shell.do_preview("")

    out = capsys.readouterr().out
    normalized = " ".join(out.split())
    assert "artifact_key required" in out
    assert "Run `artifacts <run_id>` first to populate @refs." in normalized
    assert "Use `context` to inspect shell defaults." in normalized


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

    shell._last_run_ids = ["cached-run-1", "cached-run-2"]
    shell._last_artifact_ids = ["artifact-id-1", "artifact-id-2"]

    with patch.object(shell, "_recent_run_ids", return_value=["run-1", "run-2"]):
        assert shell.complete_show("run-", "show run-", 5, 9) == ["run-1", "run-2"]
        assert shell.complete_show("#", "show #", 5, 6) == ["#1", "#2"]
        assert shell.complete_artifacts("run-2", "artifacts run-2", 10, 15) == ["run-2"]
        assert shell.complete_artifacts("#", "artifacts #", 10, 11) == ["#1", "#2"]
        assert shell.complete_ls("#", "ls #", 3, 4) == ["#1", "#2"]

    with patch.object(
        shell, "_recent_artifact_keys", return_value=["artifact-1", "table-2"]
    ):
        assert shell.complete_preview("art", "preview art", 8, 11) == ["artifact-1"]
        assert shell.complete_preview("@", "preview @", 8, 9) == ["@1", "@2"]
        assert shell.complete_schema_profile(
            "table", "schema_profile table", 15, 20
        ) == ["table-2"]
        assert shell.complete_schema_profile("@", "schema_profile @", 15, 16) == [
            "@1",
            "@2",
        ]
        assert shell.complete_schema_stub("@", "schema_stub @", 12, 13) == [
            "@1",
            "@2",
        ]

    assert shell.complete_schema_stub("--c", "schema_stub --c", 12, 15) == [
        "--class-name",
        "--concrete",
    ]
    assert shell.complete_db("i", "db i", 3, 4) == ["inspect"]
    assert shell.complete_schema("c", "schema c", 7, 8) == ["capture-file"]
    assert shell.complete_views("c", "views c", 6, 7) == ["create"]
    assert shell.complete_cli("d", "cli d", 4, 5) == ["db"]
    assert shell.complete_consist("s", "consist s", 8, 9) == [
        "search",
        "scenarios",
        "scenario",
        "summary",
        "show",
        "schema",
    ]


def test_shell_hash_flag_completions_include_hash_options(tmp_path) -> None:
    tracker = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker)

    assert shell.complete_preview("--h", "preview --h", 8, 11) == ["--hash"]
    assert shell.complete_schema_profile("--h", "schema_profile --h", 15, 18) == [
        "--hash"
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


def test_shell_hash_lookup_rejects_empty_prefix(tmp_path, capsys) -> None:
    tracker = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker)

    resolved = shell._lookup_artifact_by_hash_prefix("   ", command_name="preview")

    out = capsys.readouterr().out
    assert resolved is None
    assert "--hash requires a non-empty prefix." in out


def test_shell_artifact_ref_missing_suggests_how_to_populate_refs(
    tmp_path, capsys
) -> None:
    tracker = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker)
    shell._last_run_ids = ["run-1"]

    resolved = shell._resolve_artifact_ref("@1")

    out = capsys.readouterr().out
    assert resolved is None
    assert "no cached artifact refs" in out
    assert "artifacts <run_id>" in out
    assert "artifacts #<n>" in out


def test_shell_hash_lookup_reports_run_scoped_miss(tmp_path, capsys) -> None:
    tracker = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker)

    with patch("consist.cli._tracker_session") as tracker_session:
        session = tracker_session.return_value.__enter__.return_value
        session.exec.return_value.all.return_value = []
        resolved = shell._lookup_artifact_by_hash_prefix(
            "deadbeef", command_name="preview", run_id="run-123"
        )

    out = capsys.readouterr().out
    normalized = " ".join(out.split())
    assert resolved is None
    assert "No artifact found for hash prefix 'deadbeef' in run 'run-123'" in out
    assert "while running preview." in normalized


def test_shell_hash_lookup_does_not_inherit_last_artifact_run_scope(
    tmp_path, capsys
) -> None:
    tracker = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker)
    shell._last_artifact_run_id = "run-123"

    with patch("consist.cli._tracker_session") as tracker_session:
        session = tracker_session.return_value.__enter__.return_value
        session.exec.return_value.all.return_value = []
        resolved = shell._lookup_artifact_by_hash_prefix(
            "deadbeef", command_name="preview"
        )

    out = capsys.readouterr().out
    normalized = " ".join(out.split())
    assert resolved is None
    assert "No artifact found for hash prefix 'deadbeef'" in out
    assert "run 'run-123'" not in out
    assert "while running preview." in normalized


def test_shell_hash_lookup_reports_ambiguity_with_candidates(tmp_path, capsys) -> None:
    tracker = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker)

    matches = [
        SimpleNamespace(
            hash="deadbeef001",
            key="trips_2018_it0",
            id="artifact-1",
            run_id="run-a",
        ),
        SimpleNamespace(
            hash="deadbeef002",
            key="trips_2018_it1",
            id="artifact-2",
            run_id="run-a",
        ),
    ]

    with patch("consist.cli._tracker_session") as tracker_session:
        session = tracker_session.return_value.__enter__.return_value
        session.exec.return_value.all.return_value = matches
        resolved = shell._lookup_artifact_by_hash_prefix(
            "deadbeef", command_name="schema_profile", run_id="run-a"
        )

    out = capsys.readouterr().out
    normalized = " ".join(out.split())
    assert resolved is None
    assert "Hash prefix 'deadbeef' is ambiguous in run 'run-a'" in out
    assert "Use a longer prefix for schema_profile." in normalized
    assert "deadbeef001 (key=trips_2018_it0, id=artifact-1, run=run-a)" in out


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


def test_shell_routed_db_command_applies_db_path_default(tmp_path) -> None:
    tracker = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker, db_path="shell.db")

    with patch("consist.cli.app") as app_mock:
        shell.do_db("inspect --json")

    app_mock.assert_called_once()
    assert app_mock.call_args.kwargs["args"] == [
        "db",
        "inspect",
        "--json",
        "--db-path",
        "shell.db",
    ]


def test_shell_group_root_commands_do_not_inject_db_path(tmp_path) -> None:
    tracker = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker, db_path="shell.db")

    with patch("consist.cli.app") as app_mock:
        shell.do_views("")
        shell.do_schema("")
        shell.do_db("")

    assert app_mock.call_count == 3
    assert app_mock.call_args_list[0].kwargs["args"] == ["views"]
    assert app_mock.call_args_list[1].kwargs["args"] == ["schema"]
    assert app_mock.call_args_list[2].kwargs["args"] == ["db"]


def test_shell_artifact_picker_empty_state_suggests_run_listing(
    tmp_path, capsys
) -> None:
    tracker = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker)

    with (
        patch.object(shell, "_is_tty", return_value=True),
        patch.object(shell, "_recent_artifact_keys", return_value=[]),
    ):
        shell.do_preview("")

    out = capsys.readouterr().out
    assert "No artifacts available." in out
    assert "Run `artifacts <run_id>` first to populate @refs." in out


def test_shell_root_option_commands_do_not_inject_db_path(tmp_path) -> None:
    tracker = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker, db_path="shell.db")

    with patch("consist.cli.app") as app_mock:
        shell.do_cli("--help")
        shell.do_cli("--version")

    assert app_mock.call_count == 2
    assert app_mock.call_args_list[0].kwargs["args"] == ["--help"]
    assert app_mock.call_args_list[1].kwargs["args"] == ["--version"]


def test_shell_routed_schema_capture_file_applies_shell_defaults(tmp_path) -> None:
    tracker = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(
            tracker,
            db_path="shell.db",
            trust_db=True,
            mount_overrides={"workspace": "/archive/workspace"},
        )

    with patch("consist.cli.app") as app_mock:
        shell.do_schema("capture-file --artifact-key trips")

    app_mock.assert_called_once()
    routed_args = app_mock.call_args.kwargs["args"]
    assert routed_args[:4] == ["schema", "capture-file", "--artifact-key", "trips"]
    assert "--db-path" in routed_args
    assert "shell.db" in routed_args
    assert "--trust-db" in routed_args
    assert "--mount" in routed_args
    assert "workspace=/archive/workspace" in routed_args


def test_shell_routed_schema_capture_file_accepts_cached_artifact_ref(tmp_path) -> None:
    tracker = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker)
    shell._last_artifact_ids = ["00000000-0000-0000-0000-000000000123"]

    with patch("consist.cli.app") as app_mock:
        shell.do_schema("capture-file @1")

    app_mock.assert_called_once()
    routed_args = app_mock.call_args.kwargs["args"]
    assert routed_args[:4] == [
        "schema",
        "capture-file",
        "--artifact-id",
        "00000000-0000-0000-0000-000000000123",
    ]


def test_shell_routed_schema_capture_file_accepts_artifact_ref_option(
    tmp_path,
) -> None:
    tracker = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker)
    shell._last_artifact_ids = ["00000000-0000-0000-0000-000000000456"]

    with patch("consist.cli.app") as app_mock:
        shell.do_schema("capture-file --artifact-ref @1")

    app_mock.assert_called_once()
    routed_args = app_mock.call_args.kwargs["args"]
    assert routed_args[:4] == [
        "schema",
        "capture-file",
        "--artifact-id",
        "00000000-0000-0000-0000-000000000456",
    ]


def test_shell_routed_schema_capture_file_rejects_invalid_artifact_ref_option(
    tmp_path, capsys
) -> None:
    tracker = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker)

    with patch("consist.cli.app") as app_mock:
        shell.do_schema("capture-file --artifact-ref artifact_key")

    out = capsys.readouterr().out
    assert "--artifact-ref must look like @1" in out
    app_mock.assert_not_called()


def test_shell_artifacts_query_mode_routes_to_cli_parser(tmp_path) -> None:
    tracker = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker)

    with patch.object(shell, "_invoke_cli_command") as routed:
        shell.do_artifacts("--param beam.year=2018 --limit 5")

    routed.assert_called_once_with("artifacts", "--param beam.year=2018 --limit 5")


def test_shell_runs_json_mode_uses_output_json(tmp_path) -> None:
    tracker = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker)

    fake_run = SimpleNamespace(
        id="run-1",
        model_name="model",
        status="completed",
        parent_run_id=None,
        year=2025,
        created_at=datetime(2025, 1, 1, 12, 0, 0),
        duration_seconds=1.5,
        tags=["test"],
        meta={"k": "v"},
    )

    with (
        patch("consist.cli._tracker_session") as tracker_session,
        patch("consist.cli.queries.get_runs", return_value=[fake_run]),
        patch("consist.cli.output_json") as output_json_mock,
    ):
        tracker_session.return_value.__enter__.return_value = MagicMock()
        shell.do_runs("--json")

    output_json_mock.assert_called_once()
    payload = output_json_mock.call_args.args[0]
    assert payload[0]["id"] == "run-1"
    assert payload[0]["created_at"] == "2025-01-01T12:00:00"


def test_shell_artifacts_accepts_cached_run_ref(tmp_path) -> None:
    tracker = MagicMock()
    tracker.get_run.return_value = MagicMock()
    with (
        patch("consist.cli.Path.home", return_value=tmp_path),
        patch("consist.cli._READLINE", None),
    ):
        shell = ConsistShell(tracker)
    shell._last_run_ids = ["run-abc-123"]

    with patch("consist.cli._render_artifacts_table", return_value=[]):
        shell.do_artifacts("#1")

    tracker.get_run.assert_called_once_with("run-abc-123")


def test_shell_preview_accepts_cached_artifact_ref(tmp_path) -> None:
    tracker = MagicMock()
    tracker.get_artifact.return_value = SimpleNamespace(
        id="artifact-id",
        key="artifact-key",
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
    shell._last_artifact_ids = ["artifact-id"]

    with patch(
        "consist.cli._load_artifact_with_diagnostics",
        return_value=pd.DataFrame({"value": [1]}),
    ):
        shell.do_preview("@1")

    tracker.get_artifact.assert_called_once_with("artifact-id")
