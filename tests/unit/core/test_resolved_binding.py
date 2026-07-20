"""Contracts for immutable, execution-exact Scenario bindings."""

from __future__ import annotations

import json
import uuid
from pathlib import Path

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
