"""Contracts for immutable, execution-exact Scenario bindings."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest


def test_public_package_exports_resolved_binding_contract() -> None:
    import consist
    from consist.core.resolved_binding import ResolvedBindingBuilder

    assert consist.ResolvedBindingBuilder is ResolvedBindingBuilder


def test_artifact_identity_parses_self_describing_file_identity() -> None:
    from consist.core.resolved_binding import ArtifactIdentity

    identity = ArtifactIdentity.parse(f"sha256:file:{'a' * 64}")

    assert identity.kind == "file"
    assert identity.algorithm == "sha256"
    assert str(identity) == f"sha256:file:{'a' * 64}"


def test_artifact_identity_rejects_unknown_or_malformed_contracts() -> None:
    from consist.core.resolved_binding import ArtifactIdentity

    with pytest.raises(ValueError, match="unsupported"):
        ArtifactIdentity.parse(f"sha256:unknown:{'a' * 64}")
    with pytest.raises(ValueError, match="invalid"):
        ArtifactIdentity.parse("sha256:file:not-a-digest")


def test_artifact_identity_from_artifact_requires_explicit_semantics() -> None:
    from consist.core.resolved_binding import ArtifactIdentity
    from consist.models.artifact import Artifact

    artifact = Artifact(
        key="raw",
        container_uri="data://raw.csv",
        driver="csv",
        hash="a" * 64,
        meta={
            "hash_semantics": {
                "version": 1,
                "algorithm": "sha256",
                "kind": "file",
                "digest_contract": "raw_file_bytes",
                "source": "computed_full",
            }
        },
    )

    assert str(ArtifactIdentity.from_artifact(artifact)) == f"sha256:file:{'a' * 64}"

    artifact.meta = {}
    with pytest.raises(ValueError, match="immutable identity"):
        ArtifactIdentity.from_artifact(artifact)


def test_resolved_binding_input_hash_composes_strict_and_ordinary_identity() -> None:
    from consist.core.identity import IdentityManager
    from consist.models.artifact import Artifact

    identity = IdentityManager()
    ordinary = Artifact(
        key="ordinary", container_uri="data://ordinary", driver="txt", run_id="a"
    )
    same = identity.compute_resolved_binding_input_hash(
        ordinary_inputs=[ordinary],
        strict_binding_identity="binding-a",
        signature_lookup=lambda run_id: {"a": "signature-a", "b": "signature-b"}[
            run_id
        ],
    )
    assert same == identity.compute_resolved_binding_input_hash(
        ordinary_inputs=[ordinary],
        strict_binding_identity="binding-a",
        signature_lookup=lambda run_id: {"a": "signature-a", "b": "signature-b"}[
            run_id
        ],
    )
    assert same != identity.compute_resolved_binding_input_hash(
        ordinary_inputs=[ordinary],
        strict_binding_identity="binding-b",
        signature_lookup=lambda run_id: {"a": "signature-a", "b": "signature-b"}[
            run_id
        ],
    )
    assert same != identity.compute_resolved_binding_input_hash(
        ordinary_inputs=[
            Artifact(
                key="ordinary",
                container_uri="data://ordinary",
                driver="txt",
                run_id="b",
            )
        ],
        strict_binding_identity="binding-a",
        signature_lookup=lambda run_id: {"a": "signature-a", "b": "signature-b"}[
            run_id
        ],
    )
    with pytest.raises(ValueError, match="must not be empty"):
        identity.compute_resolved_binding_input_hash(
            ordinary_inputs=[],
            strict_binding_identity=" ",
        )


def test_resolved_binding_separates_cache_identity_from_durable_evidence() -> None:
    from consist.core.resolved_binding import (
        ArtifactIdentity,
        BoundArtifact,
        ResolvedBindingBuilder,
        TrackedArtifactLocator,
    )

    metadata = {"selection": {"role": "warmstart"}}
    diagnostics = {"selection": {"reason": "preferred artifact absent"}}
    artifact_id = uuid.uuid4()
    binding = (
        ResolvedBindingBuilder(
            step_name="beam",
            step_contract_identity="sha256:step-v1:" + "b" * 64,
        )
        .bind_artifact(
            parameter="linkstats",
            artifact=BoundArtifact(
                artifact_id=artifact_id,
                identity=ArtifactIdentity.parse(f"sha256:file:{'a' * 64}"),
                locator=TrackedArtifactLocator(artifact_id=artifact_id),
            ),
            destination=Path("inputs/linkstats.csv.gz"),
            source="pinned",
        )
        .with_metadata(metadata)
        .with_diagnostics(diagnostics)
        .freeze()
    )

    metadata["selection"]["role"] = "mutated"
    diagnostics["selection"]["reason"] = "mutated"

    assert binding.metadata["selection"]["role"] == "warmstart"
    assert binding.diagnostics["selection"]["reason"] == "preferred artifact absent"
    identity_payload = json.loads(binding.identity_json())
    evidence_payload = json.loads(binding.evidence_json())
    assert identity_payload["step_name"] == "beam"
    assert "metadata" not in identity_payload
    assert "diagnostics" not in identity_payload
    assert evidence_payload["metadata"]["selection"]["role"] == "warmstart"
    assert evidence_payload["diagnostics"]["selection"]["reason"] == (
        "preferred artifact absent"
    )
    with pytest.raises(TypeError):
        binding.metadata["selection"]["role"] = "changed"  # type: ignore[index]
    with pytest.raises(TypeError):
        binding.diagnostics["reason"] = "changed"  # type: ignore[index]


def test_resolved_binding_diagnostics_do_not_change_cache_identity() -> None:
    from consist.core.resolved_binding import ResolvedBinding

    common = {
        "schema_version": 1,
        "step_name": "beam",
        "step_contract_identity": "sha256:step-v1:" + "b" * 64,
        "inputs": {},
        "metadata": {},
        "admission_evidence": {},
    }
    preferred = ResolvedBinding(
        **common,
        diagnostics={"selection": {"reason": "preferred"}},
    )
    fallback = ResolvedBinding(
        **common,
        diagnostics={"selection": {"reason": "fallback"}},
    )

    assert preferred.identity_json() == fallback.identity_json()
    assert preferred.identity_digest() == fallback.identity_digest()
    assert preferred.evidence_json() != fallback.evidence_json()


def test_resolved_binding_admission_evidence_does_not_change_cache_identity() -> None:
    from consist.core.resolved_binding import (
        AdmissionEvidence,
        ArtifactIdentity,
        BoundArtifact,
        ResolvedBinding,
        ResolvedInput,
        TrackedArtifactLocator,
    )

    artifact_id = uuid.uuid4()
    identity = ArtifactIdentity.parse(f"sha256:file:{'a' * 64}")
    common = {
        "schema_version": 1,
        "step_name": "beam",
        "step_contract_identity": "sha256:step-v1:" + "b" * 64,
        "inputs": {
            "linkstats": ResolvedInput(
                parameter="linkstats",
                artifact=BoundArtifact(
                    artifact_id=artifact_id,
                    identity=identity,
                    locator=TrackedArtifactLocator(artifact_id=artifact_id),
                ),
                destination=Path("inputs/linkstats.parquet"),
                source="external_admitted",
            )
        },
        "metadata": {},
        "diagnostics": {},
    }
    first = ResolvedBinding(
        **common,
        admission_evidence={
            "linkstats": AdmissionEvidence(
                observed_identity=identity,
                expected_identity=None,
                expected_source=None,
                alias="warmstart-a",
            )
        },
    )
    second = ResolvedBinding(
        **common,
        admission_evidence={
            "linkstats": AdmissionEvidence(
                observed_identity=identity,
                expected_identity=None,
                expected_source=None,
                alias="warmstart-b",
            )
        },
    )

    assert first.identity_digest() == second.identity_digest()
    assert first.evidence_json() != second.evidence_json()


def test_strict_binding_invocation_context_preserves_duplicate_artifact_ids() -> None:
    from consist.core.resolved_binding import (
        ArtifactIdentity,
        BoundArtifact,
        ResolvedBindingBuilder,
        TrackedArtifactLocator,
        _StrictBindingInvocationContext,
        _create_strict_binding_invocation_context,
        _validate_strict_binding_invocation_context,
    )

    artifact_id = uuid.uuid4()
    identity = ArtifactIdentity.parse(f"sha256:file:{'a' * 64}")
    binding = (
        ResolvedBindingBuilder(
            step_name="consume",
            step_contract_identity="sha256:step-v1:" + "b" * 64,
        )
        .bind_artifact(
            parameter="first",
            artifact=BoundArtifact(
                artifact_id=artifact_id,
                identity=identity,
                locator=TrackedArtifactLocator(artifact_id=artifact_id),
            ),
            destination=Path("inputs/first.txt"),
            source="pinned",
        )
        .bind_artifact(
            parameter="second",
            artifact=BoundArtifact(
                artifact_id=artifact_id,
                identity=identity,
                locator=TrackedArtifactLocator(artifact_id=artifact_id),
            ),
            destination=Path("inputs/second.txt"),
            source="pinned",
        )
        .freeze()
    )

    context = _create_strict_binding_invocation_context(
        strict_binding=binding,
        identity_digest=binding.identity_digest(),
        evidence_json=binding.evidence_json(),
        input_artifact_ids=(str(artifact_id), str(artifact_id)),
    )

    assert context.input_artifact_ids == (str(artifact_id), str(artifact_id))
    assert _validate_strict_binding_invocation_context(context) is context

    with pytest.raises(ValueError, match="identity digest"):
        _create_strict_binding_invocation_context(
            strict_binding=binding,
            identity_digest=" ",
            evidence_json=binding.evidence_json(),
            input_artifact_ids=(str(artifact_id), str(artifact_id)),
        )
    with pytest.raises(ValueError, match="artifact IDs"):
        _create_strict_binding_invocation_context(
            strict_binding=binding,
            identity_digest=binding.identity_digest(),
            evidence_json=binding.evidence_json(),
            input_artifact_ids=(),
        )
    with pytest.raises(ValueError, match="does not match"):
        _create_strict_binding_invocation_context(
            strict_binding=binding,
            identity_digest=binding.identity_digest(),
            evidence_json=binding.evidence_json().replace("consume", "tampered", 1),
            input_artifact_ids=(str(artifact_id), str(artifact_id)),
        )
    with pytest.raises(ValueError, match="factory"):
        _validate_strict_binding_invocation_context(
            _StrictBindingInvocationContext(
                identity_digest=binding.identity_digest(),
                evidence_json=binding.evidence_json(),
                input_artifact_ids=(str(artifact_id), str(artifact_id)),
                _factory_token=object(),
            )
        )


def test_strict_binding_invocation_context_rejects_reversed_distinct_artifact_ids() -> (
    None
):
    from consist.core.resolved_binding import (
        ArtifactIdentity,
        BoundArtifact,
        ResolvedBindingBuilder,
        TrackedArtifactLocator,
        _create_strict_binding_invocation_context,
    )

    first_id = uuid.uuid4()
    second_id = uuid.uuid4()
    binding = (
        ResolvedBindingBuilder(
            step_name="consume",
            step_contract_identity="sha256:step-v1:" + "b" * 64,
        )
        .bind_artifact(
            parameter="first",
            artifact=BoundArtifact(
                artifact_id=first_id,
                identity=ArtifactIdentity.parse(f"sha256:file:{'a' * 64}"),
                locator=TrackedArtifactLocator(artifact_id=first_id),
            ),
            destination=Path("inputs/first.txt"),
            source="pinned",
        )
        .bind_artifact(
            parameter="second",
            artifact=BoundArtifact(
                artifact_id=second_id,
                identity=ArtifactIdentity.parse(f"sha256:file:{'c' * 64}"),
                locator=TrackedArtifactLocator(artifact_id=second_id),
            ),
            destination=Path("inputs/second.txt"),
            source="pinned",
        )
        .freeze()
    )

    with pytest.raises(ValueError, match="mapping order"):
        _create_strict_binding_invocation_context(
            strict_binding=binding,
            identity_digest=binding.identity_digest(),
            evidence_json=binding.evidence_json(),
            input_artifact_ids=(str(second_id), str(first_id)),
        )


def test_bind_tracked_artifact_derives_the_strict_artifact_contract() -> None:
    from consist.core.resolved_binding import ResolvedBindingBuilder
    from consist.models.artifact import Artifact

    artifact = Artifact(
        key="warmstart",
        container_uri="data://linkstats.parquet",
        driver="parquet",
        hash="a" * 64,
        meta={
            "hash_semantics": {
                "version": 1,
                "algorithm": "sha256",
                "kind": "file",
                "digest_contract": "raw_file_bytes",
                "source": "computed_full",
            }
        },
    )

    binding = (
        ResolvedBindingBuilder(
            step_name="beam",
            step_contract_identity="sha256:step-v1:" + "b" * 64,
        )
        .bind_tracked_artifact(
            parameter="linkstats_warmstart",
            artifact=artifact,
            destination=Path("inputs/linkstats.parquet"),
            source="external_admitted",
        )
        .freeze()
    )

    resolved = binding.inputs["linkstats_warmstart"]
    assert resolved.artifact.artifact_id == artifact.id
    assert resolved.artifact.locator.artifact_id == artifact.id
    assert str(resolved.artifact.identity) == f"sha256:file:{'a' * 64}"


def test_strict_binding_accepts_a_scenario_preflight_identity(
    tracker, tmp_path: Path
) -> None:
    from consist import ExecutionOptions
    from consist.core.resolved_binding import ResolvedBindingBuilder

    source = tmp_path / "raw.txt"
    source.write_text("accepted\n", encoding="utf-8")
    with tracker.start_run("seed", "test"):
        artifact = tracker.log_artifact(source, key="raw", direction="input")

    @tracker.define_step(name_template="{func_name}__y{year}__{phase}")
    def consume(raw: Path) -> None:
        assert raw.read_text(encoding="utf-8") == "accepted\n"

    with tracker.scenario("strict_preflight") as scenario:
        identity = scenario.resolve_step_identity(
            consume,
            year=2040,
            phase="warmstart",
            execution_options=ExecutionOptions(input_binding="paths"),
        )
        binding = (
            ResolvedBindingBuilder(
                step_name=identity.name,
                step_contract_identity=identity.step_contract_identity,
            )
            .bind_tracked_artifact(
                parameter="raw",
                artifact=artifact,
                destination=Path("inputs/raw.txt"),
                source="external_admitted",
            )
            .freeze()
        )

        result = scenario.run(
            consume,
            binding=binding,
            step_identity=identity,
            execution_options=ExecutionOptions(input_binding="paths"),
        )

    assert result.cache_hit is False


def test_strict_binding_rejects_a_preflight_identity_name_mismatch(
    tracker, tmp_path: Path
) -> None:
    from consist import ExecutionOptions
    from consist.core.resolved_binding import ResolvedBindingBuilder

    source = tmp_path / "raw.txt"
    source.write_text("accepted\n", encoding="utf-8")
    with tracker.start_run("seed", "test"):
        artifact = tracker.log_artifact(source, key="raw", direction="input")

    def consume(raw: Path) -> None:
        return None

    with tracker.scenario("strict_preflight_name_mismatch") as scenario:
        identity = scenario.resolve_step_identity(
            consume,
            execution_options=ExecutionOptions(input_binding="paths"),
        )
        binding = (
            ResolvedBindingBuilder(
                step_name=f"{identity.name}-wrong",
                step_contract_identity=identity.step_contract_identity,
            )
            .bind_tracked_artifact(
                parameter="raw",
                artifact=artifact,
                destination=Path("inputs/raw.txt"),
                source="external_admitted",
            )
            .freeze()
        )

        with pytest.raises(ValueError, match="step name"):
            scenario.run(
                consume,
                binding=binding,
                step_identity=identity,
                execution_options=ExecutionOptions(input_binding="paths"),
            )


def test_resolved_binding_rejects_locator_for_a_different_artifact() -> None:
    from consist.core.resolved_binding import (
        ArtifactIdentity,
        BoundArtifact,
        ResolvedBindingBuilder,
        TrackedArtifactLocator,
    )

    builder = ResolvedBindingBuilder(
        step_name="beam",
        step_contract_identity="sha256:step-v1:" + "b" * 64,
    )
    with pytest.raises(ValueError, match="locator"):
        builder.bind_artifact(
            parameter="linkstats",
            artifact=BoundArtifact(
                artifact_id=uuid.uuid4(),
                identity=ArtifactIdentity.parse(f"sha256:file:{'a' * 64}"),
                locator=TrackedArtifactLocator(artifact_id=uuid.uuid4()),
            ),
            destination=Path("inputs/linkstats.csv.gz"),
            source="pinned",
        )


def test_resolved_binding_rejects_duplicate_execution_destinations() -> None:
    from consist.core.resolved_binding import (
        ArtifactIdentity,
        BoundArtifact,
        ResolvedBindingBuilder,
        TrackedArtifactLocator,
    )

    artifact_id = uuid.uuid4()
    artifact = BoundArtifact(
        artifact_id=artifact_id,
        identity=ArtifactIdentity.parse(f"sha256:file:{'a' * 64}"),
        locator=TrackedArtifactLocator(artifact_id=artifact_id),
    )
    builder = ResolvedBindingBuilder(
        step_name="beam",
        step_contract_identity="sha256:step-v1:" + "b" * 64,
    )
    builder.bind_artifact(
        parameter="first",
        artifact=artifact,
        destination=Path("inputs/shared.csv"),
        source="pinned",
    ).bind_artifact(
        parameter="second",
        artifact=artifact,
        destination=Path("inputs/shared.csv"),
        source="pinned",
    )

    with pytest.raises(ValueError, match="duplicate execution destination"):
        builder.freeze()


def test_direct_resolved_binding_rejects_input_key_mismatch() -> None:
    from consist.core.resolved_binding import (
        ArtifactIdentity,
        BoundArtifact,
        ResolvedBinding,
        ResolvedInput,
        TrackedArtifactLocator,
    )

    artifact_id = uuid.uuid4()
    with pytest.raises(ValueError, match="input key"):
        ResolvedBinding(
            schema_version=1,
            step_name="beam",
            step_contract_identity="sha256:step-v1:" + "b" * 64,
            inputs={
                "wrong": ResolvedInput(
                    parameter="actual",
                    artifact=BoundArtifact(
                        artifact_id=artifact_id,
                        identity=ArtifactIdentity.parse(f"sha256:file:{'a' * 64}"),
                        locator=TrackedArtifactLocator(artifact_id=artifact_id),
                    ),
                    destination=Path("inputs/raw.csv"),
                    source="pinned",
                )
            },
            metadata={},
            admission_evidence={},
        )


def test_direct_resolved_binding_rejects_empty_parameter() -> None:
    from consist.core.resolved_binding import (
        ArtifactIdentity,
        BoundArtifact,
        ResolvedBinding,
        ResolvedInput,
        TrackedArtifactLocator,
    )

    artifact_id = uuid.uuid4()
    with pytest.raises(ValueError, match="parameter"):
        ResolvedBinding(
            schema_version=1,
            step_name="beam",
            step_contract_identity="sha256:step-v1:" + "b" * 64,
            inputs={
                "": ResolvedInput(
                    parameter="",
                    artifact=BoundArtifact(
                        artifact_id=artifact_id,
                        identity=ArtifactIdentity.parse(f"sha256:file:{'a' * 64}"),
                        locator=TrackedArtifactLocator(artifact_id=artifact_id),
                    ),
                    destination=Path("inputs/raw.csv"),
                    source="pinned",
                )
            },
            metadata={},
            admission_evidence={},
        )


def test_direct_resolved_binding_freezes_directory_manifest(tmp_path: Path) -> None:
    from consist.core.directory_artifacts import build_directory_manifest
    from consist.core.resolved_binding import (
        ArtifactIdentity,
        BoundArtifact,
        ResolvedBinding,
        ResolvedInput,
        TrackedArtifactLocator,
    )

    source = tmp_path / "source.zarr"
    source.mkdir()
    (source / "chunk").write_bytes(b"immutable chunk")
    manifest = build_directory_manifest(source)
    artifact_id = uuid.uuid4()
    binding = ResolvedBinding(
        schema_version=1,
        step_name="beam",
        step_contract_identity="sha256:step-v1:" + "b" * 64,
        inputs={
            "directory": ResolvedInput(
                parameter="directory",
                artifact=BoundArtifact(
                    artifact_id=artifact_id,
                    identity=ArtifactIdentity.parse(
                        f"sha256:manifest-v1:{manifest['tree_hash']}"
                    ),
                    locator=TrackedArtifactLocator(artifact_id=artifact_id),
                    artifact_kind="directory_manifest",
                    directory_manifest=manifest,
                ),
                destination=Path("inputs/source.zarr"),
                source="pinned",
            )
        },
        metadata={},
        admission_evidence={},
    )

    manifest["tree_hash"] = "a" * 64

    assert (
        binding.inputs["directory"].artifact.directory_manifest["tree_hash"] != "a" * 64
    )


def test_resolved_binding_rejects_mismatched_admission_identity() -> None:
    from consist.core.resolved_binding import (
        AdmissionEvidence,
        ArtifactIdentity,
        BoundArtifact,
        ResolvedBindingBuilder,
        TrackedArtifactLocator,
    )

    observed = ArtifactIdentity.parse(f"sha256:file:{'a' * 64}")
    different = ArtifactIdentity.parse(f"sha256:file:{'b' * 64}")
    artifact_id = uuid.uuid4()
    builder = ResolvedBindingBuilder(
        step_name="beam",
        step_contract_identity="sha256:step-v1:" + "c" * 64,
    ).bind_artifact(
        parameter="linkstats",
        artifact=BoundArtifact(
            artifact_id=artifact_id,
            identity=observed,
            locator=TrackedArtifactLocator(artifact_id=artifact_id),
        ),
        destination=Path("inputs/linkstats.csv.gz"),
        source="external_admitted",
    )

    with pytest.raises(ValueError, match="observed identity"):
        builder.with_admission(
            parameter="linkstats",
            evidence=AdmissionEvidence(
                observed_identity=different,
                expected_identity=observed,
                expected_source="declared_digest",
            ),
        ).freeze()


def test_execution_snapshot_preserves_verified_file_after_source_mutates(
    tmp_path: Path,
) -> None:
    from consist.core.resolved_binding import (
        ArtifactIdentity,
        create_execution_snapshot,
    )

    source = tmp_path / "external.csv"
    source.write_bytes(b"accepted bytes")
    identity = ArtifactIdentity.parse(
        "sha256:file:af77f61c6d49263ab0a0e93e4f4245d7cc682159eb52579e04771271b322c7c7"
    )
    destination = tmp_path / "run-owned" / "external.csv"

    create_execution_snapshot(source=source, destination=destination, identity=identity)
    source.write_bytes(b"mutated bytes")

    assert destination.read_bytes() == b"accepted bytes"


def test_execution_snapshot_rejects_source_equals_destination(tmp_path: Path) -> None:
    from consist.core.resolved_binding import (
        ArtifactIdentity,
        create_execution_snapshot,
    )

    source = tmp_path / "external.csv"
    source.write_bytes(b"accepted bytes")
    identity = ArtifactIdentity.parse(
        "sha256:file:af77f61c6d49263ab0a0e93e4f4245d7cc682159eb52579e04771271b322c7c7"
    )

    with pytest.raises(ValueError, match="fresh run-owned"):
        create_execution_snapshot(source=source, destination=source, identity=identity)


def test_execution_snapshot_validates_manifest_backed_directory(tmp_path: Path) -> None:
    from consist.core.directory_artifacts import build_directory_manifest
    from consist.core.resolved_binding import (
        ArtifactIdentity,
        create_execution_snapshot,
    )

    source = tmp_path / "source.zarr"
    source.mkdir()
    (source / "chunk").write_bytes(b"immutable chunk")
    manifest = build_directory_manifest(source)
    identity = ArtifactIdentity.parse(f"sha256:manifest-v1:{manifest['tree_hash']}")
    destination = tmp_path / "run-owned" / "source.zarr"

    create_execution_snapshot(
        source=source,
        destination=destination,
        identity=identity,
        directory_manifest=manifest,
    )
    (source / "chunk").write_bytes(b"mutated chunk")

    assert (destination / "chunk").read_bytes() == b"immutable chunk"


def test_scenario_runs_resolved_binding_from_verified_snapshot(
    tracker, tmp_path: Path
) -> None:
    from consist import ExecutionOptions
    from consist.core.resolved_binding import (
        ArtifactIdentity,
        BoundArtifact,
        ResolvedBindingBuilder,
        TrackedArtifactLocator,
        step_contract_identity,
    )

    source = tmp_path / "raw.txt"
    source.write_text("accepted\n", encoding="utf-8")
    with tracker.start_run("seed", "test"):
        artifact = tracker.log_artifact(source, key="raw", direction="input")

    def consume(payload: Path) -> None:
        assert payload.read_text(encoding="utf-8") == "accepted\n"
        source.write_text("mutated\n", encoding="utf-8")
        assert payload.read_text(encoding="utf-8") == "accepted\n"
        assert ".resolved-bindings" in str(payload)

    binding = (
        ResolvedBindingBuilder(
            step_name="consume",
            step_contract_identity=step_contract_identity(consume, "consume"),
        )
        .bind_artifact(
            parameter="payload",
            artifact=BoundArtifact(
                artifact_id=artifact.id,
                identity=ArtifactIdentity.from_artifact(artifact),
                locator=TrackedArtifactLocator(artifact_id=artifact.id),
            ),
            destination=Path("inputs/raw.txt"),
            source="pinned",
        )
        .freeze()
    )

    with tracker.scenario("strict_binding") as scenario:
        result = scenario.run(
            fn=consume,
            name="consume",
            binding=binding,
            execution_options=ExecutionOptions(input_binding="paths"),
        )

    assert result.cache_hit is False


def test_strict_scenario_uses_execution_snapshot_helper(
    tracker, tmp_path: Path, monkeypatch
) -> None:
    from consist import ExecutionOptions
    from consist.core.resolved_binding import (
        ArtifactIdentity,
        BoundArtifact,
        ResolvedBindingBuilder,
        TrackedArtifactLocator,
        step_contract_identity,
    )

    source = tmp_path / "raw.txt"
    source.write_text("accepted\n", encoding="utf-8")
    with tracker.start_run("seed", "test"):
        artifact = tracker.log_artifact(source, key="raw", direction="input")

    def consume(raw: Path) -> None:
        return None

    binding = (
        ResolvedBindingBuilder(
            step_name="consume",
            step_contract_identity=step_contract_identity(consume, "consume"),
        )
        .bind_artifact(
            parameter="raw",
            artifact=BoundArtifact(
                artifact_id=artifact.id,
                identity=ArtifactIdentity.from_artifact(artifact),
                locator=TrackedArtifactLocator(artifact_id=artifact.id),
            ),
            destination=Path("inputs/raw.txt"),
            source="pinned",
        )
        .freeze()
    )

    def fail_snapshot(**kwargs) -> None:
        raise RuntimeError("strict snapshot helper called")

    monkeypatch.setattr(
        "consist.core.cache.create_execution_snapshot",
        fail_snapshot,
        raising=False,
    )
    with tracker.scenario("strict_snapshot_helper") as scenario:
        with pytest.raises(RuntimeError, match="strict snapshot helper called"):
            scenario.run(
                fn=consume,
                name="consume",
                binding=binding,
                execution_options=ExecutionOptions(input_binding="paths"),
            )


def test_strict_binding_materializes_one_artifact_for_multiple_parameters(
    tracker, tmp_path: Path
) -> None:
    from consist import ExecutionOptions
    from consist.core.resolved_binding import (
        ResolvedBindingBuilder,
        step_contract_identity,
    )

    source = tmp_path / "raw.txt"
    source.write_text("accepted\n", encoding="utf-8")
    with tracker.start_run("seed", "test"):
        artifact = tracker.log_artifact(source, key="raw", direction="input")

    received: list[Path] = []

    def consume(first: Path, second: Path, third: Path) -> None:
        received.extend([first, second, third])

    binding = (
        ResolvedBindingBuilder(
            step_name="consume",
            step_contract_identity=step_contract_identity(consume, "consume"),
        )
        .bind_tracked_artifact(
            parameter="first",
            artifact=artifact,
            destination=Path("inputs/first.txt"),
            source="pinned",
        )
        .bind_tracked_artifact(
            parameter="second",
            artifact=artifact,
            destination=Path("inputs/second.txt"),
            source="pinned",
        )
        .bind_tracked_artifact(
            parameter="third",
            artifact=artifact,
            destination=Path("inputs/third.txt"),
            source="pinned",
        )
        .freeze()
    )

    with tracker.scenario("strict_binding_multiple_parameters") as scenario:
        result = scenario.run(
            fn=consume,
            name="consume",
            binding=binding,
            execution_options=ExecutionOptions(input_binding="paths"),
        )

    snapshot_root = (
        tracker.run_dir / ".resolved-bindings" / result.run.id / "inputs"
    ).resolve()
    assert result.cache_hit is False
    assert received == [
        snapshot_root / "first.txt",
        snapshot_root / "second.txt",
        snapshot_root / "third.txt",
    ]
    assert all(path.read_text(encoding="utf-8") == "accepted\n" for path in received)
    invocation = tracker.db.get_binding_invocations()[0]
    invocation_inputs = json.loads(invocation.binding_json)["inputs"]
    assert set(invocation_inputs) == {
        "first",
        "second",
        "third",
    }
    assert {item["artifact"]["artifact_id"] for item in invocation_inputs.values()} == {
        str(artifact.id)
    }


def test_scenario_forwards_ordered_duplicate_strict_context_to_lifecycle(
    tracker, tmp_path: Path, monkeypatch
) -> None:
    from consist import ExecutionOptions
    from consist.core.resolved_binding import (
        ResolvedBindingBuilder,
        step_contract_identity,
    )

    source = tmp_path / "raw.txt"
    dependency = tmp_path / "dependency.txt"
    source.write_text("accepted\n", encoding="utf-8")
    dependency.write_text("dependency\n", encoding="utf-8")
    with tracker.start_run("seed", "test"):
        artifact = tracker.log_artifact(source, key="raw", direction="input")

    def consume(first: Path, second: Path) -> None:
        assert first.read_text(encoding="utf-8") == "accepted\n"
        assert second.read_text(encoding="utf-8") == "accepted\n"

    binding = (
        ResolvedBindingBuilder(
            step_name="consume",
            step_contract_identity=step_contract_identity(consume, "consume"),
        )
        .bind_tracked_artifact(
            parameter="first",
            artifact=artifact,
            destination=Path("inputs/first.txt"),
            source="pinned",
        )
        .bind_tracked_artifact(
            parameter="second",
            artifact=artifact,
            destination=Path("inputs/second.txt"),
            source="pinned",
        )
        .freeze()
    )
    contexts = []
    original_begin_run = tracker._run_lifecycle.begin_run

    def capture_context(*args, **kwargs):
        context = tracker._strict_binding_context.get()
        if context is not None:
            contexts.append(context)
        return original_begin_run(*args, **kwargs)

    monkeypatch.setattr(tracker._run_lifecycle, "begin_run", capture_context)

    with tracker.scenario("strict_context_order") as scenario:
        scenario.run(
            fn=consume,
            name="consume",
            binding=binding,
            depends_on=[dependency],
            execution_options=ExecutionOptions(input_binding="paths"),
        )

    assert [context.input_artifact_ids for context in contexts] == [
        (str(artifact.id), str(artifact.id))
    ]


def test_scenario_rejects_resolved_binding_for_wrong_callable(
    tracker, tmp_path: Path
) -> None:
    from consist import ExecutionOptions
    from consist.core.resolved_binding import (
        ArtifactIdentity,
        BoundArtifact,
        ResolvedBindingBuilder,
        TrackedArtifactLocator,
        step_contract_identity,
    )

    source = tmp_path / "raw.txt"
    source.write_text("accepted\n", encoding="utf-8")
    with tracker.start_run("seed", "test"):
        artifact = tracker.log_artifact(source, key="raw", direction="input")

    def expected(raw: Path) -> None:
        return None

    def other(raw: Path) -> None:
        return None

    binding = (
        ResolvedBindingBuilder(
            step_name="expected",
            step_contract_identity=step_contract_identity(expected, "expected"),
        )
        .bind_artifact(
            parameter="raw",
            artifact=BoundArtifact(
                artifact_id=artifact.id,
                identity=ArtifactIdentity.from_artifact(artifact),
                locator=TrackedArtifactLocator(artifact_id=artifact.id),
            ),
            destination=Path("inputs/raw.txt"),
            source="pinned",
        )
        .freeze()
    )

    with tracker.scenario("strict_binding_wrong_callable") as scenario:
        with pytest.raises(ValueError, match="step contract"):
            scenario.run(
                fn=other,
                name="expected",
                binding=binding,
                execution_options=ExecutionOptions(input_binding="paths"),
            )


def test_scenario_records_each_strict_binding_invocation(
    tracker, tmp_path: Path
) -> None:
    from consist import ExecutionOptions
    from consist.core.resolved_binding import (
        ArtifactIdentity,
        BoundArtifact,
        ResolvedBindingBuilder,
        TrackedArtifactLocator,
        step_contract_identity,
    )

    source = tmp_path / "raw.txt"
    source.write_text("accepted\n", encoding="utf-8")
    with tracker.start_run("seed", "test"):
        artifact = tracker.log_artifact(source, key="raw", direction="input")

    def consume(raw: Path) -> None:
        assert raw.read_text(encoding="utf-8") == "accepted\n"

    binding = (
        ResolvedBindingBuilder(
            step_name="consume",
            step_contract_identity=step_contract_identity(consume, "consume"),
        )
        .bind_artifact(
            parameter="raw",
            artifact=BoundArtifact(
                artifact_id=artifact.id,
                identity=ArtifactIdentity.from_artifact(artifact),
                locator=TrackedArtifactLocator(artifact_id=artifact.id),
            ),
            destination=Path("inputs/raw.txt"),
            source="pinned",
        )
        .with_diagnostics({"selection": {"reason": "the pinned artifact was admitted"}})
        .freeze()
    )

    with tracker.scenario("first") as scenario:
        first = scenario.run(
            fn=consume,
            name="consume",
            binding=binding,
            execution_options=ExecutionOptions(input_binding="paths"),
        )
    with tracker.scenario("second") as scenario:
        second = scenario.run(
            fn=consume,
            name="consume",
            binding=binding,
            execution_options=ExecutionOptions(input_binding="paths"),
        )

    invocations = tracker.db.get_binding_invocations()

    assert first.cache_hit is False
    assert second.cache_hit is True
    assert [item.cache_outcome for item in invocations] == ["miss", "hit"]
    assert all(item.binding_json == binding.evidence_json() for item in invocations)
    assert all(
        json.loads(item.binding_json)["diagnostics"]["selection"]["reason"]
        == "the pinned artifact was admitted"
        for item in invocations
    )
    assert invocations[1].execution_run_id == first.run.id
    assert invocations[1].cache_source_run_id == first.run.id


def test_strict_binding_reuses_across_equivalent_selected_producers(
    tracker, tmp_path: Path
) -> None:
    from consist import ExecutionOptions
    from consist.core.resolved_binding import (
        ResolvedBindingBuilder,
        step_contract_identity,
    )

    def seed(label: str):
        source = tmp_path / f"{label}.txt"
        source.write_text("accepted\n", encoding="utf-8")
        with tracker.start_run(f"seed_{label}", "seed", config={"producer": label}):
            return tracker.log_artifact(source, key="raw", direction="output")

    first_artifact = seed("first")
    second_artifact = seed("second")
    assert first_artifact.id != second_artifact.id
    assert first_artifact.run_id != second_artifact.run_id

    def consume(raw: Path) -> None:
        assert raw.read_text(encoding="utf-8") == "accepted\n"

    def binding_for(artifact, diagnostic: str):
        return (
            ResolvedBindingBuilder(
                step_name="consume",
                step_contract_identity=step_contract_identity(consume, "consume"),
            )
            .bind_tracked_artifact(
                parameter="raw",
                artifact=artifact,
                destination=Path("inputs/raw.txt"),
                source="pinned",
            )
            .with_diagnostics({"selected": diagnostic})
            .freeze()
        )

    with tracker.scenario("strict_first") as scenario:
        first = scenario.run(
            fn=consume,
            name="consume",
            binding=binding_for(first_artifact, "first"),
            execution_options=ExecutionOptions(input_binding="paths"),
        )
    second_binding = binding_for(second_artifact, "second")
    with tracker.scenario("strict_second") as scenario:
        second = scenario.run(
            fn=consume,
            name="consume",
            binding=second_binding,
            execution_options=ExecutionOptions(input_binding="paths"),
        )
    identity_input = tmp_path / "identity.yaml"
    identity_input.write_text("revision: 2\n", encoding="utf-8")
    with tracker.scenario("strict_identity_input_changed") as scenario:
        third = scenario.run(
            fn=consume,
            name="consume",
            binding=second_binding,
            identity_inputs=[identity_input],
            execution_options=ExecutionOptions(input_binding="paths"),
        )

    invocations = tracker.db.get_binding_invocations()
    assert first.cache_hit is False
    assert second.cache_hit is True
    assert third.cache_hit is False
    assert json.loads(invocations[-1].binding_json)["inputs"]["raw"]["artifact"][
        "artifact_id"
    ] == str(second_artifact.id)
    for result in (first, second, third):
        identity = result.run.identity_summary["input_identity"]
        assert identity["mode"] == "action-v2"
        assert identity["version"] == 2
        assert identity["code"] == {
            "version": 1,
            "mode": "repo_git",
            "digest": "static_test_hash",
        }
        assert identity["strict_input_count"] == 1
        assert identity["ordinary_input_count"] == 0
        assert identity["strict_binding_identity"] == second_binding.identity_digest()
        assert identity["bindings"] == []


def test_non_git_callable_identity_evidence_records_resolved_mode_and_digest(tracker):
    from consist import ExecutionOptions
    from consist.core.identity import CodeIdentityUnavailableError

    def consume() -> None:
        return None

    with patch.object(
        tracker.identity,
        "get_code_version",
        side_effect=CodeIdentityUnavailableError(
            mode="repo_git", reason="repository identity unavailable"
        ),
    ):
        result = tracker.run(
            fn=consume,
            name="non_git_consume",
            execution_options=ExecutionOptions(input_binding="none"),
        )

    identity = result.run.identity_summary["code_identity"]
    assert identity["mode"] == "callable_module"
    assert result.run.git_hash == tracker.identity.compute_callable_hash(
        consume, strategy="module"
    )


def test_strict_binding_reuses_attested_ordinary_dependency_content(
    tracker, tmp_path: Path
) -> None:
    from consist import ExecutionOptions
    from consist.core.resolved_binding import (
        ResolvedBindingBuilder,
        step_contract_identity,
    )

    strict_source = tmp_path / "strict.txt"
    strict_source.write_text("strict\n", encoding="utf-8")
    with tracker.start_run("strict_seed", "seed"):
        strict_artifact = tracker.log_artifact(
            strict_source, key="strict", direction="output"
        )

    def seed_dependency(label: str):
        dependency = tmp_path / f"dependency_{label}.txt"
        dependency.write_text("ordinary\n", encoding="utf-8")
        with tracker.start_run(
            f"dependency_{label}", "seed", config={"producer": label}
        ):
            return tracker.log_artifact(
                dependency, key="dependency", direction="output"
            )

    first_dependency = seed_dependency("first")
    second_dependency = seed_dependency("second")
    assert first_dependency.run_id != second_dependency.run_id

    def consume(raw: Path) -> None:
        assert raw.read_text(encoding="utf-8") == "strict\n"

    binding = (
        ResolvedBindingBuilder(
            step_name="consume",
            step_contract_identity=step_contract_identity(consume, "consume"),
        )
        .bind_tracked_artifact(
            parameter="raw",
            artifact=strict_artifact,
            destination=Path("inputs/raw.txt"),
            source="pinned",
        )
        .freeze()
    )

    with tracker.scenario("ordinary_dependency_first") as scenario:
        first = scenario.run(
            fn=consume,
            name="consume",
            binding=binding,
            depends_on=[first_dependency],
            execution_options=ExecutionOptions(input_binding="paths"),
        )
    with tracker.scenario("ordinary_dependency_second") as scenario:
        second = scenario.run(
            fn=consume,
            name="consume",
            binding=binding,
            depends_on=[second_dependency],
            execution_options=ExecutionOptions(input_binding="paths"),
        )

    assert first.cache_hit is False
    assert second.cache_hit is True
    assert first.run.identity_summary["input_identity"]["ordinary_input_count"] == 1
    assert second.run.identity_summary["input_identity"]["ordinary_input_count"] == 1
    assert first.run.identity_summary["input_identity"]["bindings"] == [
        {
            "kind": "dependency",
            "role": 0,
            "mode": "content-v1",
            "value": f"sha256:file:{first_dependency.hash}",
            "artifact_id": str(first_dependency.id),
            "selector": {
                "driver": "txt",
                "table_path": None,
                "array_path": None,
            },
            "evidence": {"identity_strength": "content-v1"},
        }
    ]


def test_strict_binding_protocol_mismatch_raises_before_callable_execution(
    tracker, tmp_path: Path
) -> None:
    from consist import ExecutionOptions
    from consist.core.resolved_binding import (
        ResolvedBindingBuilder,
        _create_strict_binding_invocation_context,
        step_contract_identity,
    )

    strict_path = tmp_path / "strict.txt"
    wrong_path = tmp_path / "wrong.txt"
    strict_path.write_text("strict\n", encoding="utf-8")
    wrong_path.write_text("wrong\n", encoding="utf-8")
    with tracker.start_run("strict_seed", "seed"):
        strict_artifact = tracker.log_artifact(
            strict_path, key="strict", direction="output"
        )
    with tracker.start_run("wrong_seed", "seed"):
        wrong_artifact = tracker.log_artifact(
            wrong_path, key="wrong", direction="output"
        )

    def declared(raw: Path) -> None:
        return None

    binding = (
        ResolvedBindingBuilder(
            step_name="declared",
            step_contract_identity=step_contract_identity(declared, "declared"),
        )
        .bind_tracked_artifact(
            parameter="raw",
            artifact=strict_artifact,
            destination=Path("inputs/raw.txt"),
            source="pinned",
        )
        .freeze()
    )
    context = _create_strict_binding_invocation_context(
        strict_binding=binding,
        identity_digest=binding.identity_digest(),
        evidence_json=binding.evidence_json(),
        input_artifact_ids=(str(strict_artifact.id),),
    )
    calls: list[str] = []

    def consume() -> None:
        calls.append("executed")

    with pytest.raises(ValueError, match="strict binding input protocol mismatch"):
        tracker.run(
            fn=consume,
            name="declared",
            inputs=[wrong_artifact],
            execution_options=ExecutionOptions(input_binding="none"),
            _strict_binding_context=context,
        )

    assert calls == []


def test_strict_binding_contract_partitions_cache_reuse(
    tracker, tmp_path: Path
) -> None:
    from consist import ExecutionOptions
    from consist.core.resolved_binding import (
        ArtifactIdentity,
        BoundArtifact,
        ResolvedBindingBuilder,
        TrackedArtifactLocator,
        step_contract_identity,
    )

    source = tmp_path / "raw.txt"
    source.write_text("accepted\n", encoding="utf-8")
    with tracker.start_run("seed", "test"):
        artifact = tracker.log_artifact(source, key="raw", direction="input")

    def consume(raw: Path) -> None:
        assert raw.read_text(encoding="utf-8") == "accepted\n"

    def binding_for(destination: Path):
        return (
            ResolvedBindingBuilder(
                step_name="consume",
                step_contract_identity=step_contract_identity(consume, "consume"),
            )
            .bind_artifact(
                parameter="raw",
                artifact=BoundArtifact(
                    artifact_id=artifact.id,
                    identity=ArtifactIdentity.from_artifact(artifact),
                    locator=TrackedArtifactLocator(artifact_id=artifact.id),
                ),
                destination=destination,
                source="pinned",
            )
            .freeze()
        )

    with tracker.scenario("first") as scenario:
        first = scenario.run(
            fn=consume,
            name="consume",
            binding=binding_for(Path("inputs/first.txt")),
            execution_options=ExecutionOptions(input_binding="paths"),
        )
    with tracker.scenario("second") as scenario:
        second = scenario.run(
            fn=consume,
            name="consume",
            binding=binding_for(Path("inputs/second.txt")),
            execution_options=ExecutionOptions(input_binding="paths"),
        )

    assert first.cache_hit is False
    assert second.cache_hit is False


def test_failed_strict_binding_invocation_persists_evidence(
    tracker, tmp_path: Path
) -> None:
    from consist import ExecutionOptions
    from consist.core.resolved_binding import (
        ArtifactIdentity,
        BoundArtifact,
        ResolvedBindingBuilder,
        TrackedArtifactLocator,
        step_contract_identity,
    )

    source = tmp_path / "raw.txt"
    source.write_text("accepted\n", encoding="utf-8")
    with tracker.start_run("seed", "test"):
        artifact = tracker.log_artifact(source, key="raw", direction="input")

    def fail(raw: Path) -> None:
        raise RuntimeError("expected failure")

    binding = (
        ResolvedBindingBuilder(
            step_name="fail",
            step_contract_identity=step_contract_identity(fail, "fail"),
        )
        .bind_artifact(
            parameter="raw",
            artifact=BoundArtifact(
                artifact_id=artifact.id,
                identity=ArtifactIdentity.from_artifact(artifact),
                locator=TrackedArtifactLocator(artifact_id=artifact.id),
            ),
            destination=Path("inputs/raw.txt"),
            source="pinned",
        )
        .freeze()
    )

    with pytest.raises(RuntimeError, match="expected failure"):
        with tracker.scenario("failed") as scenario:
            scenario.run(
                fn=fail,
                name="fail",
                binding=binding,
                execution_options=ExecutionOptions(input_binding="paths"),
            )

    invocations = tracker.db.get_binding_invocations()

    assert len(invocations) == 1
    assert invocations[0].cache_outcome == "miss"
    assert invocations[0].binding_json == binding.canonical_json()


def test_strict_binding_refuses_execution_when_evidence_persistence_fails(
    tracker, tmp_path: Path, monkeypatch
) -> None:
    from consist import ExecutionOptions
    from consist.core.resolved_binding import (
        ArtifactIdentity,
        BoundArtifact,
        ResolvedBindingBuilder,
        TrackedArtifactLocator,
        step_contract_identity,
    )

    source = tmp_path / "raw.txt"
    source.write_text("accepted\n", encoding="utf-8")
    with tracker.start_run("seed", "test"):
        artifact = tracker.log_artifact(source, key="raw", direction="input")

    executed = False

    def consume(raw: Path) -> None:
        nonlocal executed
        executed = True

    binding = (
        ResolvedBindingBuilder(
            step_name="consume",
            step_contract_identity=step_contract_identity(consume, "consume"),
        )
        .bind_artifact(
            parameter="raw",
            artifact=BoundArtifact(
                artifact_id=artifact.id,
                identity=ArtifactIdentity.from_artifact(artifact),
                locator=TrackedArtifactLocator(artifact_id=artifact.id),
            ),
            destination=Path("inputs/raw.txt"),
            source="pinned",
        )
        .freeze()
    )

    def fail_sync_run(*args, **kwargs) -> None:
        raise RuntimeError("database unavailable")

    with tracker.scenario("strict_persistence_failure") as scenario:
        monkeypatch.setattr(tracker.db, "sync_run", fail_sync_run)
        with pytest.raises(RuntimeError, match="database unavailable"):
            scenario.run(
                fn=consume,
                name="consume",
                binding=binding,
                execution_options=ExecutionOptions(input_binding="paths"),
            )

    assert executed is False
