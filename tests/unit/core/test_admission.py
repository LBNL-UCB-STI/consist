"""Red contract tests for prior-run file admission."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from consist.core.admission import (
    AdmissionReference,
    AdmissionReport,
    check_admission_reference,
    check_artifact_identity,
    hash_semantics_for_new_artifact,
)
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
            session.commit()


def _record_input(
    tracker,
    *,
    run_id: str,
    key: str,
    source_path: Path,
    digest: str | None,
    semantics: dict[str, object] | None,
    direction: str = "input",
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
            RunArtifactLink(run_id=run_id, artifact_id=artifact.id, direction=direction)
        )
        session.commit()


def _assert_observation(report: AdmissionReport, text: str) -> None:
    assert any(text in item.lower() for item in report.observations), (
        report.observations
    )


def test_public_package_exports_admission_api() -> None:
    import consist

    assert consist.AdmissionReference is AdmissionReference
    assert consist.AdmissionReport is AdmissionReport
    assert consist.check_admission_reference is check_admission_reference
    assert consist.check_artifact_identity is check_artifact_identity


def test_caller_hash_override_does_not_retain_cloned_full_hash_semantics(
    tracker, tmp_path: Path
) -> None:
    source = tmp_path / "feed.zip"
    source.write_bytes(b"trusted bytes")
    candidate = tmp_path / "caller-candidate.zip"
    candidate.write_bytes(b"caller-controlled bytes")
    with tracker.start_run("full-hash", "admission_contract"):
        original = tracker.log_artifact(source, key="raw_gtfs", direction="input")
    assert original.meta["hash_semantics"]["source"] == "computed_full"

    caller_hash = _sha256(candidate.read_bytes())
    with tracker.start_run("caller-override", "admission_contract"):
        overridden = tracker.log_artifact(
            original,
            key="raw_gtfs",
            direction="input",
            content_hash=caller_hash,
            force_hash_override=True,
        )

    assert overridden.hash == caller_hash
    assert overridden.meta["hash_semantics"]["source"] == "caller_supplied"
    report = check_artifact_identity(
        tracker,
        execution_path=candidate,
        expected_run_id="caller-override",
        artifact_key="raw_gtfs",
    )
    assert report.outcome == "unverified"


def test_directory_hash_semantics_distinguish_full_from_fast(tmp_path: Path) -> None:
    directory = tmp_path / "inputs"
    directory.mkdir()

    full = hash_semantics_for_new_artifact(
        path=directory, hashing_strategy="full", source="computed"
    )
    fast = hash_semantics_for_new_artifact(
        path=directory, hashing_strategy="fast", source="computed"
    )

    assert full["source"] == "computed_full_directory"
    assert fast["source"] == "computed_fast_directory"
    assert full["digest_contract"] != fast["digest_contract"]


def test_caller_supplied_hash_semantics_do_not_claim_sha256() -> None:
    semantics = hash_semantics_for_new_artifact(
        path=None,
        hashing_strategy="full",
        source="caller_supplied",
    )

    assert semantics["algorithm"] == "unknown"


def test_logged_artifact_semantics_admit_full_but_not_fast_inputs(
    tracker, tmp_path: Path
) -> None:
    full_source = tmp_path / "full-source.zip"
    full_source.write_bytes(b"full bytes")
    full_candidate = tmp_path / "full-candidate.zip"
    full_candidate.write_bytes(full_source.read_bytes())
    with tracker.start_run("full-run", "admission_contract"):
        full_artifact = tracker.log_artifact(
            full_source, key="full_feed", direction="input"
        )
    assert full_artifact.meta["hash_semantics"]["source"] == "computed_full"

    full_report = check_artifact_identity(
        tracker,
        execution_path=full_candidate,
        expected_run_id="full-run",
        artifact_key="full_feed",
    )
    assert full_report.outcome == "verified"

    tracker.identity.hashing_strategy = "fast"
    fast_source = tmp_path / "fast-source.zip"
    fast_source.write_bytes(b"fast bytes")
    fast_candidate = tmp_path / "fast-candidate.zip"
    fast_candidate.write_bytes(fast_source.read_bytes())
    with tracker.start_run("fast-run", "admission_contract"):
        fast_artifact = tracker.log_artifact(
            fast_source, key="fast_feed", direction="input"
        )
    assert fast_artifact.meta["hash_semantics"]["source"] == "computed_fast"

    fast_report = check_artifact_identity(
        tracker,
        execution_path=fast_candidate,
        expected_run_id="fast-run",
        artifact_key="fast_feed",
    )
    assert fast_report.outcome == "unverified"


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
    canonical_json = report.canonical_json()
    payload = json.loads(canonical_json)
    assert canonical_json == json.dumps(payload, sort_keys=True, separators=(",", ":"))
    assert payload["input_role"] == "raw_gtfs"
    assert payload["execution_path"] == str(candidate)
    assert payload["physical_target_path"] == str(candidate.resolve())
    for field in (
        "config_key",
        "config_reference_key",
        "feed_key",
        "raw_config_value",
        "canonical_value",
        "configured_path",
    ):
        assert payload[field] is None


def test_runtime_reference_preserves_launch_path_evidence(
    tracker, tmp_path: Path
) -> None:
    source = tmp_path / "archive" / "linkstats.csv.gz"
    source.parent.mkdir()
    source.write_bytes(b"linkstats")
    staged = tmp_path / "beam" / "input" / "seattle" / "_pilates" / "linkstats.csv.gz"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(source.read_bytes())
    _complete_run(tracker, "baseline-run")
    _record_input(
        tracker,
        run_id="baseline-run",
        key="linkstats_warmstart",
        source_path=source,
        digest=_sha256(source.read_bytes()),
        semantics=FULL_FILE_SHA256,
    )

    report = check_admission_reference(
        tracker,
        expected_run_id="baseline-run",
        reference=AdmissionReference(
            artifact_key="linkstats_warmstart",
            input_role="beam_linkstats_warmstart",
            config_key="beam.warmStart.initialLinkstatsFilePath",
            config_reference_key="beam.warmStart.initialLinkstatsFilePath",
            feed_key="seattle",
            raw_config_value='${beam.inputDirectory}"/_pilates/linkstats.csv.gz"',
            canonical_value=str(staged),
            configured_path=staged,
            execution_path=staged,
            consumer_path="/app/input/seattle/_pilates/linkstats.csv.gz",
        ),
    )

    assert report.outcome == "verified"
    assert report.input_role == "beam_linkstats_warmstart"
    assert report.config_key == "beam.warmStart.initialLinkstatsFilePath"
    assert report.consumer_path == "/app/input/seattle/_pilates/linkstats.csv.gz"
    assert report.execution_path == str(staged)
    assert report.physical_target_path == str(staged.resolve())
    payload = json.loads(report.canonical_json())
    assert payload["report_schema_version"] == 2
    assert payload["config_key"] == "beam.warmStart.initialLinkstatsFilePath"
    assert payload["config_reference_key"] == "beam.warmStart.initialLinkstatsFilePath"
    assert payload["feed_key"] == "seattle"
    assert (
        payload["raw_config_value"]
        == '${beam.inputDirectory}"/_pilates/linkstats.csv.gz"'
    )
    assert payload["canonical_value"] == str(staged)
    assert payload["consumer_path"] == "/app/input/seattle/_pilates/linkstats.csv.gz"
    assert payload["configured_path"] == str(staged)


def test_runtime_reference_rejects_caller_control_of_physical_target_path(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValidationError, match="physical_target_path"):
        AdmissionReference.model_validate(
            {
                "artifact_key": "linkstats_warmstart",
                "execution_path": tmp_path / "staged.csv.gz",
                "physical_target_path": tmp_path / "unrelated.csv.gz",
            }
        )


def test_runtime_reference_validates_context_fields(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="consumer_path"):
        AdmissionReference(
            artifact_key="linkstats_warmstart",
            execution_path=tmp_path / "staged.csv.gz",
            consumer_path=123,
        )


def test_runtime_reference_is_immutable(tmp_path: Path) -> None:
    reference = AdmissionReference(
        artifact_key="linkstats_warmstart",
        execution_path=tmp_path / "staged.csv.gz",
    )

    with pytest.raises(ValidationError, match="frozen"):
        reference.consumer_path = "/app/input/seattle/_pilates/linkstats.csv.gz"


def test_runtime_reference_retains_context_for_unverified_input(
    tracker, tmp_path: Path
) -> None:
    source = tmp_path / "archive" / "linkstats.csv.gz"
    source.parent.mkdir()
    source.write_bytes(b"linkstats")
    staged = tmp_path / "beam" / "input" / "linkstats.csv.gz"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(source.read_bytes())
    _complete_run(tracker, "baseline-run")
    _record_input(
        tracker,
        run_id="baseline-run",
        key="linkstats_warmstart",
        source_path=source,
        digest=_sha256(source.read_bytes()),
        semantics=None,
    )

    report = check_admission_reference(
        tracker,
        expected_run_id="baseline-run",
        reference=AdmissionReference(
            artifact_key="linkstats_warmstart",
            execution_path=staged,
            config_key="beam.warmStart.initialLinkstatsFilePath",
            consumer_path="/app/input/linkstats.csv.gz",
        ),
    )

    assert report.outcome == "unverified"
    assert report.config_key == "beam.warmStart.initialLinkstatsFilePath"
    assert report.consumer_path == "/app/input/linkstats.csv.gz"


def test_historical_bare_hash_is_unverified_without_expected_bytes(
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
    )

    assert report.outcome == "unverified"
    _assert_observation(report, "unverifiable")


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
    assert report.expected_bytes_source == "explicit_immutable_path"
    assert report.expected_bytes_path == str(source.resolve())


def test_expected_bytes_must_correlate_with_historical_bare_hash(
    tracker, tmp_path: Path
):
    historical_source = tmp_path / "historical-feed.zip"
    historical_source.write_bytes(b"historical bytes")
    candidate = tmp_path / "runtime-feed.zip"
    candidate.write_bytes(b"unrelated bytes")
    operator_expected = tmp_path / "operator-expected.zip"
    operator_expected.write_bytes(candidate.read_bytes())
    _complete_run(tracker, "legacy-run")
    _record_input(
        tracker,
        run_id="legacy-run",
        key="raw_gtfs",
        source_path=historical_source,
        digest=_sha256(historical_source.read_bytes()),
        semantics=None,
    )

    report = check_artifact_identity(
        tracker,
        execution_path=candidate,
        expected_run_id="legacy-run",
        artifact_key="raw_gtfs",
        expected_bytes_path=operator_expected,
    )

    assert report.outcome == "unverified"
    _assert_observation(report, "expected_bytes_not_correlated")
    assert report.expected_bytes_source == "explicit_immutable_path"
    assert report.expected_bytes_path == str(operator_expected.resolve())


@pytest.mark.parametrize("alias_kind", ["same", "symlink", "hardlink"])
def test_legacy_expected_bytes_must_be_distinct_from_candidate(
    tracker, tmp_path: Path, alias_kind: str
):
    candidate = tmp_path / "runtime-feed.zip"
    candidate.write_bytes(b"candidate bytes")
    expected_path = candidate
    if alias_kind == "symlink":
        expected_path = tmp_path / "candidate-symlink.zip"
        expected_path.symlink_to(candidate)
    elif alias_kind == "hardlink":
        expected_path = tmp_path / "candidate-hardlink.zip"
        expected_path.hardlink_to(candidate)
    _complete_run(tracker, "legacy-run")
    _record_input(
        tracker,
        run_id="legacy-run",
        key="raw_gtfs",
        source_path=candidate,
        digest=_sha256(candidate.read_bytes()),
        semantics=None,
    )

    report = check_artifact_identity(
        tracker,
        execution_path=candidate,
        expected_run_id="legacy-run",
        artifact_key="raw_gtfs",
        expected_bytes_path=expected_path,
    )

    assert report.outcome == "unverified"
    _assert_observation(report, "expected_bytes_not_distinct")
    assert report.expected_bytes_source == "explicit_immutable_path"
    assert report.expected_bytes_path == str(expected_path.resolve())


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


def test_same_key_output_link_does_not_make_a_single_input_ambiguous(
    tracker, tmp_path: Path
):
    source = tmp_path / "archive-feed.zip"
    source.write_bytes(b"expected bytes")
    output = tmp_path / "output-feed.zip"
    output.write_bytes(b"different output bytes")
    candidate = tmp_path / "runtime-feed.zip"
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
    _record_input(
        tracker,
        run_id="completed-run",
        key="raw_gtfs",
        source_path=output,
        digest=_sha256(output.read_bytes()),
        semantics=FULL_FILE_SHA256,
        direction="output",
    )

    report = check_artifact_identity(
        tracker,
        execution_path=candidate,
        expected_run_id="completed-run",
        artifact_key="raw_gtfs",
        input_role="gtfs_feed",
    )

    assert report.outcome == "verified"
    assert json.loads(report.canonical_json())["input_role"] == "gtfs_feed"


def test_git_lfs_candidate_is_unverified_with_targeted_observation(
    tracker, tmp_path: Path
):
    source = tmp_path / "archive-feed.zip"
    source.write_bytes(b"real feed bytes")
    candidate = tmp_path / "runtime-feed.zip"
    candidate.write_text(
        "version https://git-lfs.github.com/spec/v1\n"
        "oid sha256:0123456789abcdef\nsize 123\n",
        encoding="utf-8",
    )
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

    assert report.outcome == "unverified"
    _assert_observation(report, "lfs")


def test_git_lfs_expected_fallback_is_unverified_with_targeted_observation(
    tracker, tmp_path: Path
):
    source = tmp_path / "legacy-feed.zip"
    source.write_bytes(b"legacy bytes")
    candidate = tmp_path / "runtime-feed.zip"
    candidate.write_bytes(b"legacy bytes")
    expected_pointer = tmp_path / "archive-pointer.zip"
    expected_pointer.write_text(
        "version https://git-lfs.github.com/spec/v1\n"
        "oid sha256:0123456789abcdef\nsize 123\n",
        encoding="utf-8",
    )
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
        expected_bytes_path=expected_pointer,
    )

    assert report.outcome == "unverified"
    _assert_observation(report, "lfs")
    assert report.expected_bytes_source == "explicit_immutable_path"
    assert report.expected_bytes_path == str(expected_pointer.resolve())


def test_non_regular_expected_fallback_retains_source_audit_fields(
    tracker, tmp_path: Path
):
    source = tmp_path / "legacy-feed.zip"
    source.write_bytes(b"legacy bytes")
    candidate = tmp_path / "runtime-feed.zip"
    candidate.write_bytes(source.read_bytes())
    expected_directory = tmp_path / "archive-directory"
    expected_directory.mkdir()
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
        expected_bytes_path=expected_directory,
    )

    assert report.outcome == "unverified"
    _assert_observation(report, "unverifiable")
    assert report.expected_bytes_source == "explicit_immutable_path"
    assert report.expected_bytes_path == str(expected_directory.resolve())


@pytest.mark.parametrize("path_kind", ["directory", "missing"])
def test_non_regular_or_missing_candidate_is_file_unreadable(
    tracker, tmp_path: Path, path_kind: str
):
    source = tmp_path / "archive-feed.zip"
    source.write_bytes(b"expected bytes")
    candidate = tmp_path / "candidate"
    if path_kind == "directory":
        candidate.mkdir()
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

    assert report.outcome == "unreadable"
    _assert_observation(report, "file_unreadable")


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
        _complete_run(
            tracker, run_id, status="failed" if case == "non_completed" else "completed"
        )
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
