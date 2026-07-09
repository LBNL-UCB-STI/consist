"""Red contract tests for prior-run file admission."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from consist.core.admission import AdmissionReport, check_artifact_identity
from consist.models.artifact import Artifact
from consist.models.run import RunArtifactLink


FULL_FILE_SHA256 = {
    "version": 1,
    "algorithm": "sha256",
    "kind": "file",
    "digest_contract": "raw_file_bytes",
    "source": "computed_full",
}


def _sha256(contents: bytes) -> str:
    return hashlib.sha256(contents).hexdigest()


def _complete_run(tracker, run_id: str, *, status: str = "completed") -> None:
    with tracker.start_run(run_id=run_id, model="admission_contract"):
        pass
    if status != "completed":
        run = tracker.get_run(run_id)
        assert run is not None
        run.status = status
        with tracker.db.session_scope() as session:
            session.add(run)


def _record_input(
    tracker,
    *,
    run_id: str,
    key: str,
    source_path: Path,
    digest: str | None,
    semantics: dict[str, object] | None,
) -> None:
    artifact = Artifact(
        key=key,
        container_uri=f"inputs://{source_path.name}",
        driver="other",
        hash=digest,
        meta={} if semantics is None else {"hash_semantics": semantics},
    )
    with tracker.db.session_scope() as session:
        session.add(artifact)
        session.add(
            RunArtifactLink(
                run_id=run_id, artifact_id=artifact.id, direction="input"
            )
        )


def _assert_observation(report: AdmissionReport, text: str) -> None:
    assert any(text in item.lower() for item in report.observations), report.observations


def test_future_full_file_metadata_admits_matching_candidate_with_fast_tracker(
    tracker, tmp_path: Path
):
    tracker.identity.hashing_strategy = "fast"
    source = tmp_path / "archive" / "feed.zip"
    source.parent.mkdir()
    source.write_bytes(b"GTFS bytes")
    candidate = tmp_path / "runtime" / "feed.zip"
    candidate.parent.mkdir()
    candidate.write_bytes(source.read_bytes())
    _complete_run(tracker, "completed-run")
    _record_input(
        tracker,
        run_id="completed-run",
        key="raw_gtfs",
        source_path=source,
        digest=_sha256(source.read_bytes()),
        semantics=FULL_FILE_SHA256,
    )

    report = check_artifact_identity(
        tracker,
        execution_path=candidate,
        expected_run_id="completed-run",
        artifact_key="raw_gtfs",
    )

    assert isinstance(report, AdmissionReport)
    assert report.outcome == "verified"


def test_distinct_expected_bytes_reverify_a_historical_bare_hash(
    tracker, tmp_path: Path
):
    source = tmp_path / "archive-feed.zip"
    source.write_bytes(b"immutable bytes")
    candidate = tmp_path / "runtime-feed.zip"
    candidate.write_bytes(source.read_bytes())
    _complete_run(tracker, "legacy-run")
    _record_input(
        tracker,
        run_id="legacy-run",
        key="raw_gtfs",
        source_path=source,
        digest=_sha256(source.read_bytes()),
        semantics=None,
    )

    report = check_artifact_identity(
        tracker,
        execution_path=candidate,
        expected_run_id="legacy-run",
        artifact_key="raw_gtfs",
        expected_bytes_path=source,
    )

    assert report.outcome == "verified"


def test_changed_candidate_mismatches_a_forward_full_file_identity(
    tracker, tmp_path: Path
):
    source = tmp_path / "archive-feed.zip"
    source.write_bytes(b"original bytes")
    candidate = tmp_path / "runtime-feed.zip"
    candidate.write_bytes(b"changed bytes")
    _complete_run(tracker, "completed-run")
    _record_input(
        tracker,
        run_id="completed-run",
        key="raw_gtfs",
        source_path=source,
        digest=_sha256(source.read_bytes()),
        semantics=FULL_FILE_SHA256,
    )

    report = check_artifact_identity(
        tracker,
        execution_path=candidate,
        expected_run_id="completed-run",
        artifact_key="raw_gtfs",
    )

    assert report.outcome == "mismatched"


@pytest.mark.parametrize(
    ("case", "expected_observation"),
    [
        ("missing_run", "run"),
        ("non_completed", "completed"),
        ("missing_input", "input"),
        ("ambiguous_input", "ambiguous"),
    ],
)
def test_unresolvable_expected_input_is_unverified(
    tracker, tmp_path: Path, case: str, expected_observation: str
):
    source = tmp_path / "archive-feed.zip"
    source.write_bytes(b"expected bytes")
    candidate = tmp_path / "runtime-feed.zip"
    candidate.write_bytes(source.read_bytes())
    run_id = "expected-run"

    if case != "missing_run":
        _complete_run(tracker, run_id, status="failed" if case == "non_completed" else "completed")
    if case in {"non_completed", "ambiguous_input"}:
        _record_input(
            tracker,
            run_id=run_id,
            key="raw_gtfs",
            source_path=source,
            digest=_sha256(source.read_bytes()),
            semantics=FULL_FILE_SHA256,
        )
    if case == "ambiguous_input":
        another_source = tmp_path / "another-feed.zip"
        another_source.write_bytes(b"another expected bytes")
        _record_input(
            tracker,
            run_id=run_id,
            key="raw_gtfs",
            source_path=another_source,
            digest=_sha256(another_source.read_bytes()),
            semantics=FULL_FILE_SHA256,
        )

    report = check_artifact_identity(
        tracker,
        execution_path=candidate,
        expected_run_id="missing-run" if case == "missing_run" else run_id,
        artifact_key="raw_gtfs",
    )

    assert report.outcome == "unverified"
    _assert_observation(report, expected_observation)
