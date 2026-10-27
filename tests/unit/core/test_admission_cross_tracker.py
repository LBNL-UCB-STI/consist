"""Unit tests for output-direction and cross-tracker admission."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest
from pydantic import ValidationError

import consist
import consist.api
from consist.core.admission import (
    AdmissionReference,
    AdmissionReport,
    PriorRunExpectation,
    check_admission_reference,
    check_admission_reference_prior_run_expectation,
    check_artifact_identity,
    check_prior_run_expectation,
)
from consist.core.tracker import Tracker
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


def _complete_run(tracker: Tracker, run_id: str, *, status: str = "completed") -> None:
    with tracker.start_run(run_id=run_id, model="admission_test"):
        pass
    if status != "completed":
        run = tracker.get_run(run_id)
        assert run is not None
        run.status = status
        with tracker.db.session_scope() as session:
            session.add(run)
            session.commit()


def _record_artifact(
    tracker: Tracker,
    *,
    run_id: str,
    key: str,
    source_path: Path,
    digest: str | None,
    semantics: dict[str, object] | None,
    direction: str = "input",
) -> Artifact:
    artifact = Artifact(
        key=key,
        container_uri=f"test://{source_path.name}",
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
    return artifact


# =============================================================================
# 1. API Exports
# =============================================================================


def test_public_api_exports() -> None:
    """Verify that PriorRunExpectation and new check functions are exported."""
    for module in (consist, consist.api):
        assert hasattr(module, "PriorRunExpectation")
        assert getattr(module, "PriorRunExpectation") is PriorRunExpectation
        assert hasattr(module, "check_prior_run_expectation")
        assert (
            getattr(module, "check_prior_run_expectation")
            is check_prior_run_expectation
        )
        assert hasattr(module, "check_admission_reference_prior_run_expectation")
        assert (
            getattr(module, "check_admission_reference_prior_run_expectation")
            is check_admission_reference_prior_run_expectation
        )


# =============================================================================
# 2. PriorRunExpectation Model Tests
# =============================================================================


def test_prior_run_expectation_properties() -> None:
    """Verify property accessors ('run_id', 'direction') reflect canonical fields."""
    exp = PriorRunExpectation(
        expected_run_id="run-123",
        artifact_key="model_weights",
        expected_direction="output",
    )
    assert exp.expected_run_id == "run-123"
    assert exp.run_id == "run-123"
    assert exp.expected_direction == "output"
    assert exp.direction == "output"
    assert exp.artifact_key == "model_weights"
    assert exp.source_tracker_db is None
    assert exp.expected_bytes_path is None
    assert exp.input_role is None


def test_prior_run_expectation_forbids_alias_constructor_kwargs() -> None:
    """Verify alias kwargs ('run_id', 'direction') are forbidden in constructor."""
    with pytest.raises(ValidationError):
        PriorRunExpectation(**{"run_id": "run-123", "artifact_key": "model_weights"})
    with pytest.raises(ValidationError):
        PriorRunExpectation(
            expected_run_id="run-123",
            artifact_key="model_weights",
            **{"direction": "output"},
        )


def test_prior_run_expectation_immutability() -> None:
    """Verify PriorRunExpectation is frozen."""
    exp = PriorRunExpectation(
        expected_run_id="run-1",
        artifact_key="data",
    )
    with pytest.raises(ValidationError):
        exp.expected_run_id = "run-2"  # type: ignore[misc]


def test_prior_run_expectation_forbids_extra_fields() -> None:
    """Verify extra fields are forbidden."""
    with pytest.raises(ValidationError, match="extra"):
        PriorRunExpectation(
            expected_run_id="run-1",
            artifact_key="data",
            unknown_field="invalid",  # type: ignore[call-arg]  # ty: ignore[unknown-argument]
        )


def test_prior_run_expectation_validates_non_empty_fields() -> None:
    """Verify non-empty validation on expected_run_id, artifact_key, and input_role."""
    with pytest.raises(ValidationError, match="must not be empty"):
        PriorRunExpectation(expected_run_id="", artifact_key="valid_key")

    with pytest.raises(ValidationError, match="must not be empty"):
        PriorRunExpectation(expected_run_id="   ", artifact_key="valid_key")

    with pytest.raises(ValidationError, match="must not be empty"):
        PriorRunExpectation(expected_run_id="valid_run", artifact_key="")

    with pytest.raises(ValidationError, match="must not be empty"):
        PriorRunExpectation(expected_run_id="valid_run", artifact_key="   ")

    with pytest.raises(ValidationError, match="must not be empty"):
        PriorRunExpectation(
            expected_run_id="valid_run", artifact_key="valid_key", input_role=""
        )

    with pytest.raises(ValidationError, match="must not be empty"):
        PriorRunExpectation(
            expected_run_id="valid_run", artifact_key="valid_key", input_role="   "
        )


def test_prior_run_expectation_validates_direction_literals() -> None:
    """Verify invalid directions are rejected by Pydantic."""
    with pytest.raises(ValidationError):
        PriorRunExpectation(
            expected_run_id="run-1",
            artifact_key="key-1",
            expected_direction="invalid_direction",  # type: ignore[arg-type]
        )


# =============================================================================
# 3. Output Direction & Any Direction Tests
# =============================================================================


def test_admit_output_artifact_verified(tracker: Tracker, tmp_path: Path) -> None:
    """Verify admitting an output artifact with matching candidate bytes."""
    output_file = tmp_path / "produced_model.bin"
    output_file.write_bytes(b"trained model weights binary data")
    candidate = tmp_path / "candidate_model.bin"
    candidate.write_bytes(output_file.read_bytes())

    _complete_run(tracker, "train-run-1")
    _record_artifact(
        tracker,
        run_id="train-run-1",
        key="model_output",
        source_path=output_file,
        digest=_sha256(output_file.read_bytes()),
        semantics=FULL_FILE_SHA256,
        direction="output",
    )

    report = check_artifact_identity(
        tracker,
        execution_path=candidate,
        expected_run_id="train-run-1",
        artifact_key="model_output",
        expected_direction="output",
    )

    assert report.outcome == "verified"
    assert report.expected_direction == "output"
    assert (
        report.expected_artifact_id == f"sha256:file:{_sha256(candidate.read_bytes())}"
    )
    assert report.observed_artifact_id == report.expected_artifact_id
    assert report.observations == ("matched",)


def test_admit_output_artifact_mismatched(tracker: Tracker, tmp_path: Path) -> None:
    """Verify admitting an output artifact with changed candidate bytes."""
    output_file = tmp_path / "produced_model.bin"
    output_file.write_bytes(b"original model weights")
    candidate = tmp_path / "corrupted_model.bin"
    candidate.write_bytes(b"tampered model weights")

    _complete_run(tracker, "train-run-1")
    _record_artifact(
        tracker,
        run_id="train-run-1",
        key="model_output",
        source_path=output_file,
        digest=_sha256(output_file.read_bytes()),
        semantics=FULL_FILE_SHA256,
        direction="output",
    )

    report = check_artifact_identity(
        tracker,
        execution_path=candidate,
        expected_run_id="train-run-1",
        artifact_key="model_output",
        expected_direction="output",
    )

    assert report.outcome == "mismatched"
    assert report.expected_direction == "output"
    assert report.observations == ("mismatched",)
    assert (
        report.reason == "Resolved file does not match the expected prior-run artifact."
    )


def test_admit_with_direction_any_resolves_output(
    tracker: Tracker, tmp_path: Path
) -> None:
    """Verify expected_direction='any' resolves an output artifact."""
    output_file = tmp_path / "output.csv"
    output_file.write_bytes(b"col1,col2\n1,2\n")

    _complete_run(tracker, "run-any-1")
    _record_artifact(
        tracker,
        run_id="run-any-1",
        key="results",
        source_path=output_file,
        digest=_sha256(output_file.read_bytes()),
        semantics=FULL_FILE_SHA256,
        direction="output",
    )

    report = check_artifact_identity(
        tracker,
        execution_path=output_file,
        expected_run_id="run-any-1",
        artifact_key="results",
        expected_direction="any",
    )

    assert report.outcome == "verified"
    assert report.expected_direction == "any"
    assert report.observations == ("matched",)


def test_admit_with_direction_any_resolves_input(
    tracker: Tracker, tmp_path: Path
) -> None:
    """Verify expected_direction='any' resolves an input artifact."""
    input_file = tmp_path / "input.csv"
    input_file.write_bytes(b"feature1,feature2\n0.1,0.2\n")

    _complete_run(tracker, "run-any-2")
    _record_artifact(
        tracker,
        run_id="run-any-2",
        key="features",
        source_path=input_file,
        digest=_sha256(input_file.read_bytes()),
        semantics=FULL_FILE_SHA256,
        direction="input",
    )

    report = check_artifact_identity(
        tracker,
        execution_path=input_file,
        expected_run_id="run-any-2",
        artifact_key="features",
        expected_direction="any",
    )

    assert report.outcome == "verified"
    assert report.expected_direction == "any"
    assert report.observations == ("matched",)


# =============================================================================
# 4. Observation Codes: Absent & Ambiguous Roles
# =============================================================================


def test_absent_role_observation_codes(tracker: Tracker, tmp_path: Path) -> None:
    """Verify absent observation codes across output, input, and any directions."""
    candidate = tmp_path / "file.txt"
    candidate.write_bytes(b"candidate bytes")
    _complete_run(tracker, "run-absent-test")

    # Output absent
    rep_out = check_artifact_identity(
        tracker,
        execution_path=candidate,
        expected_run_id="run-absent-test",
        artifact_key="nonexistent_key",
        expected_direction="output",
    )
    assert rep_out.outcome == "unverified"
    assert rep_out.observations == ("expected_output_role_absent",)
    assert rep_out.expected_direction == "output"

    # Input absent
    rep_in = check_artifact_identity(
        tracker,
        execution_path=candidate,
        expected_run_id="run-absent-test",
        artifact_key="nonexistent_key",
        expected_direction="input",
    )
    assert rep_in.outcome == "unverified"
    assert rep_in.observations == ("expected_input_role_absent",)
    assert rep_in.expected_direction == "input"

    # Any absent
    rep_any = check_artifact_identity(
        tracker,
        execution_path=candidate,
        expected_run_id="run-absent-test",
        artifact_key="nonexistent_key",
        expected_direction="any",
    )
    assert rep_any.outcome == "unverified"
    assert rep_any.observations == ("expected_role_absent",)
    assert rep_any.expected_direction == "any"


def test_direction_mismatch_produces_absent_role(
    tracker: Tracker, tmp_path: Path
) -> None:
    """Verify that an artifact in the opposite direction is considered absent."""
    file = tmp_path / "data.bin"
    file.write_bytes(b"data bytes")
    _complete_run(tracker, "run-mismatch-test")
    _record_artifact(
        tracker,
        run_id="run-mismatch-test",
        key="shared_key",
        source_path=file,
        digest=_sha256(file.read_bytes()),
        semantics=FULL_FILE_SHA256,
        direction="output",
    )

    # Searching for 'input' when only 'output' exists -> expected_input_role_absent
    rep = check_artifact_identity(
        tracker,
        execution_path=file,
        expected_run_id="run-mismatch-test",
        artifact_key="shared_key",
        expected_direction="input",
    )
    assert rep.outcome == "unverified"
    assert rep.observations == ("expected_input_role_absent",)


def test_ambiguous_role_observation_codes(tracker: Tracker, tmp_path: Path) -> None:
    """Verify ambiguous observation codes when duplicate artifact links exist."""
    file1 = tmp_path / "f1.txt"
    file1.write_bytes(b"content 1")
    file2 = tmp_path / "f2.txt"
    file2.write_bytes(b"content 2")
    _complete_run(tracker, "run-ambiguous-test")

    # Two outputs with same key
    _record_artifact(
        tracker,
        run_id="run-ambiguous-test",
        key="dup_output",
        source_path=file1,
        digest=_sha256(file1.read_bytes()),
        semantics=FULL_FILE_SHA256,
        direction="output",
    )
    _record_artifact(
        tracker,
        run_id="run-ambiguous-test",
        key="dup_output",
        source_path=file2,
        digest=_sha256(file2.read_bytes()),
        semantics=FULL_FILE_SHA256,
        direction="output",
    )

    rep_out = check_artifact_identity(
        tracker,
        execution_path=file1,
        expected_run_id="run-ambiguous-test",
        artifact_key="dup_output",
        expected_direction="output",
    )
    assert rep_out.outcome == "unverified"
    assert rep_out.observations == ("expected_output_role_ambiguous",)

    # Two inputs with same key
    _record_artifact(
        tracker,
        run_id="run-ambiguous-test",
        key="dup_input",
        source_path=file1,
        digest=_sha256(file1.read_bytes()),
        semantics=FULL_FILE_SHA256,
        direction="input",
    )
    _record_artifact(
        tracker,
        run_id="run-ambiguous-test",
        key="dup_input",
        source_path=file2,
        digest=_sha256(file2.read_bytes()),
        semantics=FULL_FILE_SHA256,
        direction="input",
    )

    rep_in = check_artifact_identity(
        tracker,
        execution_path=file1,
        expected_run_id="run-ambiguous-test",
        artifact_key="dup_input",
        expected_direction="input",
    )
    assert rep_in.outcome == "unverified"
    assert rep_in.observations == ("expected_input_role_ambiguous",)


def test_key_as_both_input_and_output_disambiguation(
    tracker: Tracker, tmp_path: Path
) -> None:
    """Verify that an artifact existing as input and output is ambiguous under 'any'

    but disambiguated cleanly when querying 'input' or 'output'.
    """
    input_file = tmp_path / "stage_in.txt"
    input_file.write_bytes(b"initial stage bytes")
    output_file = tmp_path / "stage_out.txt"
    output_file.write_bytes(b"modified output stage bytes")

    _complete_run(tracker, "pipeline-step")
    _record_artifact(
        tracker,
        run_id="pipeline-step",
        key="stage_data",
        source_path=input_file,
        digest=_sha256(input_file.read_bytes()),
        semantics=FULL_FILE_SHA256,
        direction="input",
    )
    _record_artifact(
        tracker,
        run_id="pipeline-step",
        key="stage_data",
        source_path=output_file,
        digest=_sha256(output_file.read_bytes()),
        semantics=FULL_FILE_SHA256,
        direction="output",
    )

    # Under 'any', there are 2 matches -> expected_role_ambiguous
    rep_any = check_artifact_identity(
        tracker,
        execution_path=input_file,
        expected_run_id="pipeline-step",
        artifact_key="stage_data",
        expected_direction="any",
    )
    assert rep_any.outcome == "unverified"
    assert rep_any.observations == ("expected_role_ambiguous",)

    # Disambiguated as 'input'
    rep_in = check_artifact_identity(
        tracker,
        execution_path=input_file,
        expected_run_id="pipeline-step",
        artifact_key="stage_data",
        expected_direction="input",
    )
    assert rep_in.outcome == "verified"
    assert (
        rep_in.expected_artifact_id == f"sha256:file:{_sha256(input_file.read_bytes())}"
    )

    # Disambiguated as 'output'
    rep_out = check_artifact_identity(
        tracker,
        execution_path=output_file,
        expected_run_id="pipeline-step",
        artifact_key="stage_data",
        expected_direction="output",
    )
    assert rep_out.outcome == "verified"
    assert (
        rep_out.expected_artifact_id
        == f"sha256:file:{_sha256(output_file.read_bytes())}"
    )


# =============================================================================
# 5. Cross-Tracker Admission & Precedence Tests
# =============================================================================


def test_cross_tracker_admission_from_external_db(tmp_path: Path) -> None:
    """Verify consumer tracker admitting an output from a producer tracker database."""
    producer_runs = tmp_path / "producer_runs"
    producer_db = tmp_path / "producer_prov.duckdb"
    producer = Tracker(run_dir=producer_runs, db_path=str(producer_db))

    consumer_runs = tmp_path / "consumer_runs"
    consumer_db = tmp_path / "consumer_prov.duckdb"
    consumer = Tracker(run_dir=consumer_runs, db_path=str(consumer_db))

    try:
        model_file = tmp_path / "model.pt"
        model_file.write_bytes(b"trained neural network weights")

        _complete_run(producer, "producer-training-job")
        _record_artifact(
            producer,
            run_id="producer-training-job",
            key="trained_model",
            source_path=model_file,
            digest=_sha256(model_file.read_bytes()),
            semantics=FULL_FILE_SHA256,
            direction="output",
        )

        candidate = tmp_path / "candidate_model.pt"
        candidate.write_bytes(model_file.read_bytes())

        # Consumer checks producer's output via source_tracker_db
        report = check_artifact_identity(
            consumer,
            execution_path=candidate,
            expected_run_id="producer-training-job",
            artifact_key="trained_model",
            expected_direction="output",
            source_tracker_db=producer_db,
        )

        assert report.outcome == "verified"
        assert report.source_tracker_db == str(producer_db.resolve())
        assert report.expected_direction == "output"
        assert report.observations == ("matched",)
    finally:
        if producer.engine:
            producer.engine.dispose()
        if consumer.engine:
            consumer.engine.dispose()


def test_cross_tracker_admission_without_tracker_instance(tmp_path: Path) -> None:
    """Verify standalone check_artifact_identity with tracker=None and source_tracker_db."""
    producer_db = tmp_path / "producer.duckdb"
    producer = Tracker(run_dir=tmp_path / "prod_runs", db_path=str(producer_db))
    try:
        data_file = tmp_path / "export.parquet"
        data_file.write_bytes(b"parquet serialized data")
        _complete_run(producer, "export-run-42")
        _record_artifact(
            producer,
            run_id="export-run-42",
            key="export_data",
            source_path=data_file,
            digest=_sha256(data_file.read_bytes()),
            semantics=FULL_FILE_SHA256,
            direction="output",
        )

        report = check_artifact_identity(
            tracker=None,
            execution_path=data_file,
            expected_run_id="export-run-42",
            artifact_key="export_data",
            expected_direction="output",
            source_tracker_db=producer_db,
        )

        assert report.outcome == "verified"
        assert report.source_tracker_db == str(producer_db.resolve())
    finally:
        if producer.engine:
            producer.engine.dispose()


def test_source_tracker_db_precedence_over_missing_candidate(tmp_path: Path) -> None:
    """If source_tracker_db is unreadable, report source_tracker_db_unreadable

    even if execution_path is also missing or invalid.
    """
    missing_db = tmp_path / "nonexistent_db.duckdb"
    missing_candidate = tmp_path / "nonexistent_candidate.csv"

    report = check_artifact_identity(
        tracker=None,
        execution_path=missing_candidate,
        expected_run_id="run-1",
        artifact_key="key-1",
        source_tracker_db=missing_db,
    )

    assert report.outcome == "unverified"
    assert report.observations == ("source_tracker_db_unreadable",)
    assert "Source tracker database does not exist" in (report.reason or "")
    assert report.source_tracker_db == str(missing_db)


def test_source_tracker_db_run_absent(tmp_path: Path) -> None:
    """Verify run absent observation when querying external database."""
    producer_db = tmp_path / "empty_prov.duckdb"
    producer = Tracker(run_dir=tmp_path / "empty_runs", db_path=str(producer_db))
    try:
        candidate = tmp_path / "sample.txt"
        candidate.write_bytes(b"sample bytes")

        report = check_artifact_identity(
            tracker=None,
            execution_path=candidate,
            expected_run_id="missing-run-id",
            artifact_key="some_key",
            source_tracker_db=producer_db,
        )

        assert report.outcome == "unverified"
        assert report.observations == ("expected_prior_run_absent",)
    finally:
        if producer.engine:
            producer.engine.dispose()


def test_source_tracker_db_run_not_completed(tmp_path: Path) -> None:
    """Verify run not completed observation when querying external database."""
    producer_db = tmp_path / "prov.duckdb"
    producer = Tracker(run_dir=tmp_path / "runs", db_path=str(producer_db))
    try:
        candidate = tmp_path / "sample.txt"
        candidate.write_bytes(b"sample bytes")
        _complete_run(producer, "failed-run", status="failed")

        report = check_artifact_identity(
            tracker=None,
            execution_path=candidate,
            expected_run_id="failed-run",
            artifact_key="some_key",
            source_tracker_db=producer_db,
        )

        assert report.outcome == "unverified"
        assert report.observations == ("expected_prior_run_not_completed",)
    finally:
        if producer.engine:
            producer.engine.dispose()


def test_no_database_available_raises_runtime_error(tmp_path: Path) -> None:
    """Verify RuntimeError is raised when neither tracker nor source_tracker_db is given."""
    candidate = tmp_path / "file.txt"
    candidate.write_bytes(b"some content")

    with pytest.raises(RuntimeError, match="provenance database"):
        check_artifact_identity(
            tracker=None,
            execution_path=candidate,
            expected_run_id="run-1",
            artifact_key="key-1",
            source_tracker_db=None,
        )


def test_check_artifact_identity_fail_fast_without_db(tmp_path: Path) -> None:
    """Verify fail-fast check raises RuntimeError immediately without checking execution_path."""
    non_existent = tmp_path / "does_not_exist.bin"
    with pytest.raises(RuntimeError, match="provenance database"):
        check_artifact_identity(
            tracker=None,
            execution_path=non_existent,
            expected_run_id="run-1",
            artifact_key="key-1",
            source_tracker_db=None,
        )


def test_check_artifact_identity_default_direction_is_any(
    tracker: Tracker, tmp_path: Path
) -> None:
    """Verify check_artifact_identity defaults to expected_direction='any'."""
    file = tmp_path / "output.csv"
    file.write_bytes(b"x,y\n1,2\n")
    _complete_run(tracker, "run-any-default")
    _record_artifact(
        tracker,
        run_id="run-any-default",
        key="any_output",
        source_path=file,
        digest=_sha256(file.read_bytes()),
        semantics=FULL_FILE_SHA256,
        direction="output",
    )

    # Without specifying expected_direction, it defaults to "any" and admits output
    report_default = check_artifact_identity(
        tracker,
        execution_path=file,
        expected_run_id="run-any-default",
        artifact_key="any_output",
    )
    assert report_default.outcome == "verified"
    assert report_default.expected_direction == "any"

    # Specifying expected_direction="output" or "any" verifies successfully
    report_output = check_artifact_identity(
        tracker,
        execution_path=file,
        expected_run_id="run-any-default",
        artifact_key="any_output",
        expected_direction="output",
    )
    assert report_output.outcome == "verified"
    assert report_output.expected_direction == "output"

    report_any = check_artifact_identity(
        tracker,
        execution_path=file,
        expected_run_id="run-any-default",
        artifact_key="any_output",
        expected_direction="any",
    )
    assert report_any.outcome == "verified"
    assert report_any.expected_direction == "any"

    # Explicitly specifying expected_direction="input" fails on output
    report_input = check_artifact_identity(
        tracker,
        execution_path=file,
        expected_run_id="run-any-default",
        artifact_key="any_output",
        expected_direction="input",
    )
    assert report_input.outcome == "unverified"
    assert report_input.expected_direction == "input"
    assert "expected_input_role_absent" in report_input.observations


def test_check_admission_reference_default_direction_is_any(
    tracker: Tracker, tmp_path: Path
) -> None:
    """Verify check_admission_reference defaults to expected_direction='any'."""
    file = tmp_path / "output.csv"
    file.write_bytes(b"x,y\n1,2\n")
    _complete_run(tracker, "run-ref-default")
    _record_artifact(
        tracker,
        run_id="run-ref-default",
        key="ref_output",
        source_path=file,
        digest=_sha256(file.read_bytes()),
        semantics=FULL_FILE_SHA256,
        direction="output",
    )

    reference = AdmissionReference(
        artifact_key="ref_output",
        execution_path=file,
    )
    report = check_admission_reference(
        tracker,
        reference=reference,
        expected_run_id="run-ref-default",
    )
    assert report.outcome == "verified"
    assert report.expected_direction == "any"


# =============================================================================
# 6. PriorRunExpectation Symmetrical Helpers
# =============================================================================


def test_check_prior_run_expectation_helper(tracker: Tracker, tmp_path: Path) -> None:
    """Verify check_prior_run_expectation entry point."""
    file = tmp_path / "output.bin"
    file.write_bytes(b"binary payload")
    _complete_run(tracker, "exp-run-1")
    _record_artifact(
        tracker,
        run_id="exp-run-1",
        key="payload_key",
        source_path=file,
        digest=_sha256(file.read_bytes()),
        semantics=FULL_FILE_SHA256,
        direction="output",
    )

    expectation = PriorRunExpectation(
        expected_run_id="exp-run-1",
        artifact_key="payload_key",
        expected_direction="output",
        input_role="custom_role",
    )

    report = check_prior_run_expectation(
        tracker,
        execution_path=file,
        expectation=expectation,
    )

    assert report.outcome == "verified"
    assert report.input_role == "custom_role"
    assert report.expected_direction == "output"


def test_check_admission_reference_prior_run_expectation_success(
    tracker: Tracker, tmp_path: Path
) -> None:
    """Verify check_admission_reference_prior_run_expectation preserves reference context."""
    file = tmp_path / "dataset.csv"
    file.write_bytes(b"id,name\n1,alice\n")
    _complete_run(tracker, "step-10")
    _record_artifact(
        tracker,
        run_id="step-10",
        key="user_table",
        source_path=file,
        digest=_sha256(file.read_bytes()),
        semantics=FULL_FILE_SHA256,
        direction="output",
    )

    ref = AdmissionReference(
        artifact_key="user_table",
        execution_path=file,
        config_key="pipeline.users",
        feed_key="users_v1",
        consumer_path="/data/users.csv",
    )
    exp = PriorRunExpectation(
        expected_run_id="step-10",
        artifact_key="user_table",
        expected_direction="output",
    )

    report = check_admission_reference_prior_run_expectation(
        tracker,
        reference=ref,
        expectation=exp,
    )

    assert report.outcome == "verified"
    assert report.config_key == "pipeline.users"
    assert report.feed_key == "users_v1"
    assert report.consumer_path == "/data/users.csv"
    assert report.expected_direction == "output"


def test_check_admission_reference_prior_run_expectation_backfills_artifact_key(
    tracker: Tracker, tmp_path: Path
) -> None:
    """Verify reference.artifact_key is backfilled from expectation when None."""
    file = tmp_path / "data.csv"
    file.write_bytes(b"data")
    _complete_run(tracker, "step-11")
    _record_artifact(
        tracker,
        run_id="step-11",
        key="backfilled_key",
        source_path=file,
        digest=_sha256(file.read_bytes()),
        semantics=FULL_FILE_SHA256,
        direction="output",
    )

    ref = AdmissionReference(
        artifact_key=None,
        execution_path=file,
        input_role="my_role",
    )
    exp = PriorRunExpectation(
        expected_run_id="step-11",
        artifact_key="backfilled_key",
        expected_direction="output",
    )

    report = check_admission_reference_prior_run_expectation(
        tracker,
        reference=ref,
        expectation=exp,
    )

    assert report.outcome == "verified"
    assert report.artifact_key == "backfilled_key"
    assert report.input_role == "my_role"


def test_check_admission_reference_prior_run_expectation_mismatch_raises_value_error(
    tracker: Tracker, tmp_path: Path
) -> None:
    """Verify ValueError is raised if reference and expectation artifact keys conflict."""
    file = tmp_path / "file.txt"
    file.write_bytes(b"hello")

    ref = AdmissionReference(
        artifact_key="key_alpha",
        execution_path=file,
    )
    exp = PriorRunExpectation(
        expected_run_id="any-run",
        artifact_key="key_beta",
        expected_direction="output",
    )

    with pytest.raises(ValueError, match="does not match expectation artifact_key"):
        check_admission_reference_prior_run_expectation(
            tracker,
            reference=ref,
            expectation=exp,
        )


def test_canonical_json_with_cross_tracker_and_direction(tmp_path: Path) -> None:
    """Verify AdmissionReport.canonical_json() serialization with new fields on verified report."""
    file = tmp_path / "output.csv"
    file.write_bytes(b"a,b\n1,2\n")

    db_path = tmp_path / "test.duckdb"
    tracker = Tracker(run_dir=tmp_path / "json_runs", db_path=str(db_path))
    _complete_run(tracker, "run-json-1")
    _record_artifact(
        tracker,
        run_id="run-json-1",
        key="csv_out",
        source_path=file,
        digest=_sha256(file.read_bytes()),
        semantics=FULL_FILE_SHA256,
        direction="output",
    )
    assert tracker.db is not None
    tracker.db.engine.dispose()

    report = check_artifact_identity(
        execution_path=file,
        expected_run_id="run-json-1",
        artifact_key="csv_out",
        expected_direction="output",
        source_tracker_db=db_path,
    )

    assert report.outcome == "verified"
    data = json.loads(report.canonical_json())
    assert data["outcome"] == "verified"
    assert data["expected_direction"] == "output"
    assert data["source_tracker_db"] == str(db_path.resolve())
    # Deterministic round trip
    assert report.canonical_json() == json.dumps(
        data, sort_keys=True, separators=(",", ":")
    )


def test_prior_run_expectation_default_direction_is_any(
    tracker: Tracker, tmp_path: Path
) -> None:
    """Verify PriorRunExpectation defaults to expected_direction='any' and admits output."""
    file = tmp_path / "model_out.bin"
    file.write_bytes(b"weights")
    _complete_run(tracker, "run-any-1")
    _record_artifact(
        tracker,
        run_id="run-any-1",
        key="model_out",
        source_path=file,
        digest=_sha256(file.read_bytes()),
        semantics=FULL_FILE_SHA256,
        direction="output",
    )

    expectation = PriorRunExpectation(
        expected_run_id="run-any-1",
        artifact_key="model_out",
    )
    assert expectation.expected_direction == "any"
    assert expectation.direction == "any"

    report = check_prior_run_expectation(
        tracker,
        execution_path=file,
        expectation=expectation,
    )
    assert report.outcome == "verified"
    assert report.expected_direction == "any"


def test_check_prior_run_expectation_input_role_override(
    tracker: Tracker, tmp_path: Path
) -> None:
    """Verify check_prior_run_expectation input_role argument overrides expectation.input_role."""
    file = tmp_path / "feed.zip"
    file.write_bytes(b"gtfs content")
    _complete_run(tracker, "run-role-1")
    _record_artifact(
        tracker,
        run_id="run-role-1",
        key="gtfs_feed",
        source_path=file,
        digest=_sha256(file.read_bytes()),
        semantics=FULL_FILE_SHA256,
        direction="input",
    )

    expectation = PriorRunExpectation(
        expected_run_id="run-role-1",
        artifact_key="gtfs_feed",
        input_role="default_role",
    )

    # Without override
    report1 = check_prior_run_expectation(
        tracker,
        execution_path=file,
        expectation=expectation,
    )
    assert report1.outcome == "verified"
    assert report1.input_role == "default_role"

    # With override
    report2 = check_prior_run_expectation(
        tracker,
        execution_path=file,
        expectation=expectation,
        input_role="overridden_role",
    )
    assert report2.outcome == "verified"
    assert report2.input_role == "overridden_role"


def test_source_tracker_db_read_only_filesystem_permissions(tmp_path: Path) -> None:
    """Verify external provenance database can be read when file permissions are 0o400."""
    file = tmp_path / "result.dat"
    file.write_bytes(b"simulation results")

    db_path = tmp_path / "readonly_tracker.duckdb"
    tracker = Tracker(run_dir=tmp_path / "ro_runs", db_path=str(db_path))
    _complete_run(tracker, "ro-run-1")
    _record_artifact(
        tracker,
        run_id="ro-run-1",
        key="sim_result",
        source_path=file,
        digest=_sha256(file.read_bytes()),
        semantics=FULL_FILE_SHA256,
        direction="output",
    )
    assert tracker.db is not None
    tracker.db.engine.dispose()

    os.chmod(db_path, 0o400)
    try:
        report = check_artifact_identity(
            execution_path=file,
            expected_run_id="ro-run-1",
            artifact_key="sim_result",
            expected_direction="output",
            source_tracker_db=db_path,
        )
        assert report.outcome == "verified"
        assert report.expected_direction == "output"
        assert report.source_tracker_db == str(db_path.resolve())
    finally:
        os.chmod(db_path, 0o600)


def test_source_tracker_db_corrupt_or_invalid_file(tmp_path: Path) -> None:
    """Verify corrupted or invalid database file yields source_tracker_db_unreadable observation."""
    file = tmp_path / "data.bin"
    file.write_bytes(b"valid data")

    corrupt_db = tmp_path / "corrupt.duckdb"
    corrupt_db.write_bytes(b"corrupted or truncated duckdb file contents")

    report = check_artifact_identity(
        execution_path=file,
        expected_run_id="any-run",
        artifact_key="any-key",
        source_tracker_db=corrupt_db,
    )
    assert report.outcome == "unverified"
    assert "source_tracker_db_unreadable" in report.observations
    assert report.reason is not None
    assert (
        "Failed to open source tracker database" in report.reason
        or "Failed to read source tracker database" in report.reason
    )


def test_declared_digest_report_rejects_direction_and_source_db() -> None:
    """Verify declared_digest admission reports forbid expected_direction and source_tracker_db."""
    with pytest.raises(
        ValidationError,
        match="declared-digest admission reports must not contain expected_direction or source_tracker_db",
    ):
        AdmissionReport(
            outcome="verified",
            input_role="role",
            artifact_key=None,
            execution_path="f.txt",
            expected_run_id=None,
            expected_source="declared_digest",
            expected_direction="input",
        )

    with pytest.raises(
        ValidationError,
        match="declared-digest admission reports must not contain expected_direction or source_tracker_db",
    ):
        AdmissionReport(
            outcome="verified",
            input_role="role",
            artifact_key=None,
            execution_path="f.txt",
            expected_run_id=None,
            expected_source="declared_digest",
            source_tracker_db="/path/to/db.duckdb",
        )
