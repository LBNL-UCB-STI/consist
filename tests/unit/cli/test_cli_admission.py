from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

import consist.cli as cli
from consist.core.admission import AdmissionReport


runner = CliRunner()


def _report(outcome: str = "verified") -> AdmissionReport:
    return AdmissionReport(
        outcome=outcome,  # type: ignore[arg-type]
        input_role="gtfs_feed",
        artifact_key="gtfs_feed",
        execution_path="configured/feed.zip",
        physical_target_path="/resolved/feed.zip",
        expected_run_id="baseline-run",
        expected_artifact_id="sha256:file:expected",
        observed_artifact_id="sha256:file:observed",
        observations=("matched",) if outcome == "verified" else ("mismatched",),
    )


def test_admission_doctor_passes_options_to_public_engine(monkeypatch) -> None:
    tracker = object()
    calls: dict[str, object] = {}

    monkeypatch.setattr(cli, "get_tracker", lambda db_path: tracker)

    def check_artifact_identity(received_tracker, **kwargs):
        calls["tracker"] = received_tracker
        calls.update(kwargs)
        return _report()

    monkeypatch.setattr(cli, "check_artifact_identity", check_artifact_identity)

    result = runner.invoke(
        cli.app,
        [
            "admission",
            "doctor",
            "--db-path",
            "provenance.duckdb",
            "--expected-run",
            "baseline-run",
            "--artifact-key",
            "gtfs_feed",
            "--file",
            "configured/feed.zip",
            "--expected-file",
            "archive/feed.zip",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls == {
        "tracker": tracker,
        "execution_path": "configured/feed.zip",
        "expected_run_id": "baseline-run",
        "artifact_key": "gtfs_feed",
        "expected_bytes_path": "archive/feed.zip",
    }


def test_admission_doctor_writes_canonical_json_and_prints_verdict_first(
    monkeypatch, tmp_path: Path
) -> None:
    report = _report()
    output_path = tmp_path / "admission.json"
    monkeypatch.setattr(cli, "get_tracker", lambda db_path: object())
    monkeypatch.setattr(cli, "check_artifact_identity", lambda *args, **kwargs: report)

    result = runner.invoke(
        cli.app,
        [
            "admission",
            "doctor",
            "--db-path",
            "provenance.duckdb",
            "--expected-run",
            "baseline-run",
            "--artifact-key",
            "gtfs_feed",
            "--file",
            "configured/feed.zip",
            "--output",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.output.splitlines()[0] == "Outcome: verified"
    assert "Expected digest: sha256:file:expected" in result.output
    assert "Observed digest: sha256:file:observed" in result.output
    assert output_path.read_text() == report.canonical_json() + "\n"


def test_admission_doctor_defaults_to_success_for_non_verified_outcome(
    monkeypatch,
) -> None:
    monkeypatch.setattr(cli, "get_tracker", lambda db_path: object())
    monkeypatch.setattr(
        cli, "check_artifact_identity", lambda *args, **kwargs: _report("unverified")
    )

    result = runner.invoke(
        cli.app,
        [
            "admission",
            "doctor",
            "--db-path",
            "provenance.duckdb",
            "--expected-run",
            "baseline-run",
            "--artifact-key",
            "gtfs_feed",
            "--file",
            "configured/feed.zip",
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.output.splitlines()[0] == "Outcome: unverified"


def test_admission_doctor_require_verified_exits_one(monkeypatch) -> None:
    monkeypatch.setattr(cli, "get_tracker", lambda db_path: object())
    monkeypatch.setattr(
        cli, "check_artifact_identity", lambda *args, **kwargs: _report("mismatched")
    )

    result = runner.invoke(
        cli.app,
        [
            "admission",
            "doctor",
            "--db-path",
            "provenance.duckdb",
            "--expected-run",
            "baseline-run",
            "--artifact-key",
            "gtfs_feed",
            "--file",
            "configured/feed.zip",
            "--require-verified",
        ],
    )

    assert result.exit_code == 1, result.output
    assert result.output.splitlines()[0] == "Outcome: mismatched"
