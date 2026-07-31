"""Acceptance tests for Consist's standalone LinkML provenance module."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

import jsonschema
import yaml
from linkml_runtime.linkml_model import SlotDefinition
from linkml_runtime.utils.schemaview import SchemaView


PROJECT_ROOT = Path(__file__).parents[2]
PROJECT_VERSION = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())[
    "project"
]["version"]
SCHEMA_VERSION = "0.1.0"
SCHEMA_PATH = PROJECT_ROOT / "src" / "consist" / "schemas" / "provenance.yaml"
BINDING_SCHEMA_PATH = PROJECT_ROOT / "src" / "consist" / "schemas" / "binding.yaml"
RELEASE_BUILDER = PROJECT_ROOT / "scripts" / "build_provenance_schema_release.py"
DOWNSTREAM_FIXTURE = (
    PROJECT_ROOT
    / "tests"
    / "fixtures"
    / "linkml"
    / "downstream_operational_profile.yaml"
)
PROVENANCE_GRAPH_FIXTURE = (
    PROJECT_ROOT / "tests" / "fixtures" / "linkml" / "provenance_graph.yaml"
)
BINDING_INVOCATIONS_FIXTURE = (
    PROJECT_ROOT / "tests" / "fixtures" / "linkml" / "binding_invocations.yaml"
)
LINKML_LINT = Path(sys.executable).with_name("linkml-lint")
JSON_SCHEMA_GENERATOR = Path(sys.executable).with_name("gen-json-schema")


def test_schema_defines_portable_graph_and_binding_module() -> None:
    """The public modules model durable graph facts, not Consist persistence rows."""
    provenance_schema = SchemaView(SCHEMA_PATH).schema
    binding_schema = SchemaView(BINDING_SCHEMA_PATH).schema

    assert provenance_schema.id == "https://w3id.org/consist/provenance"
    assert provenance_schema.version == SCHEMA_VERSION
    assert provenance_schema.version != PROJECT_VERSION
    assert provenance_schema.status == "experimental"
    assert set(provenance_schema.classes) == {
        "ConsistProvenanceContext",
        "ConsistProvenanceDocument",
        "ConsistRunReference",
        "ConsistRunIdentityEvidence",
        "ConsistArtifactIdentity",
        "ConsistArtifactReference",
        "ConsistRunArtifactAssociation",
    }
    assert binding_schema.id == "https://w3id.org/consist/binding"
    assert binding_schema.version == SCHEMA_VERSION
    assert set(binding_schema.classes) == {"ConsistBindingInvocationReference"}

    context_attributes = provenance_schema.classes[
        "ConsistProvenanceContext"
    ].attributes
    assert set(context_attributes) == {"namespace", "locator"}
    assert context_attributes["namespace"].required is True
    assert context_attributes["locator"].required is not True

    run_attributes = provenance_schema.classes["ConsistRunReference"].attributes
    assert set(run_attributes) == {
        "run_id",
        "provenance_context",
        "signature",
        "model_name",
        "parent_run",
        "identity_evidence",
    }
    assert run_attributes["run_id"].required is True
    assert run_attributes["provenance_context"].required is not True
    assert run_attributes["signature"].required is not True
    assert run_attributes["parent_run"].range == "ConsistRunReference"

    artifact_attributes = provenance_schema.classes[
        "ConsistArtifactReference"
    ].attributes
    assert set(artifact_attributes) == {
        "artifact_id",
        "provenance_context",
        "artifact_key",
        "fingerprint",
        "fingerprint_strategy",
        "immutable_identity",
        "parent_artifact",
        "producing_run",
    }
    assert artifact_attributes["artifact_id"].required is True
    assert artifact_attributes["artifact_id"].range == "UUID"
    assert artifact_attributes["immutable_identity"].range == "ConsistArtifactIdentity"
    assert artifact_attributes["parent_artifact"].range == "ConsistArtifactReference"
    assert artifact_attributes["producing_run"].range == "ConsistRunReference"
    assert artifact_attributes["producing_run"].required is not True
    assert provenance_schema.types["UUID"].base == "str"
    assert provenance_schema.types["UUID"].pattern == (
        "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-"
        "[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
    )
    assert provenance_schema.types["SHA256Digest"].pattern == "^[0-9a-f]{64}$"
    assert set(
        provenance_schema.enums["ConsistArtifactIdentityAlgorithm"].permissible_values
    ) == {"sha256"}
    assert provenance_schema.types["ConsistArtifactIdentityKind"].pattern == (
        "^(file|manifest-v1)$"
    )

    association_attributes = provenance_schema.classes[
        "ConsistRunArtifactAssociation"
    ].attributes
    assert association_attributes["run"].required is True
    assert association_attributes["artifact"].required is True
    assert association_attributes["direction"].required is True
    assert association_attributes["role"].required is not True
    assert association_attributes["binding_parameter"].required is not True
    assert association_attributes["is_implicit"].required is not True

    invocation_attributes = binding_schema.classes[
        "ConsistBindingInvocationReference"
    ].attributes
    assert invocation_attributes["requested_run"].required is True
    assert invocation_attributes["effective_execution_run"].required is True
    assert invocation_attributes["cache_source_run"].required is not True
    assert invocation_attributes["cache_outcome"].required is True
    assert invocation_attributes["binding_identity_digest"].required is not True
    assert set(
        binding_schema.enums["ConsistBindingCacheOutcome"].permissible_values
    ) == {"hit", "miss"}


def test_fixtures_cover_production_consumption_identity_and_cache_semantics() -> None:
    """Examples preserve the graph and cache boundaries consumers must understand."""
    provenance = yaml.safe_load(PROVENANCE_GRAPH_FIXTURE.read_text())
    invocations = yaml.safe_load(BINDING_INVOCATIONS_FIXTURE.read_text())

    assert provenance["provenance_context"]["namespace"] == "urn:consist:alpha"
    assert provenance["associations"][0]["direction"] == "input"
    assert provenance["associations"][1]["direction"] == "output"
    assert (
        provenance["associations"][1]["run"]
        == provenance["associations"][1]["artifact"]["producing_run"]
    )
    assert "role" not in provenance["associations"][1]
    assert provenance["runs"][1]["parent_run"]["run_id"] == "pipeline-parent"
    assert provenance["artifacts"][2]["parent_artifact"]["artifact_id"] == (
        "22222222-2222-4222-8222-222222222222"
    )
    assert {
        artifact["immutable_identity"]["kind"]
        for artifact in provenance["artifacts"]
        if "immutable_identity" in artifact
    } == {
        "file",
        "manifest-v1",
    }
    assert provenance["runs"][2]["run_id"] == provenance["runs"][3]["run_id"]
    assert (
        provenance["runs"][2]["provenance_context"]["namespace"]
        != provenance["runs"][3]["provenance_context"]["namespace"]
    )
    assert (
        provenance["artifacts"][3]["artifact_id"]
        == provenance["artifacts"][4]["artifact_id"]
    )
    assert (
        provenance["artifacts"][3]["provenance_context"]["namespace"]
        != provenance["artifacts"][4]["provenance_context"]["namespace"]
    )

    miss, hit = invocations["invocations"]
    assert miss["cache_outcome"] == "miss"
    assert miss["requested_run"] == miss["effective_execution_run"]
    assert "cache_source_run" not in miss
    assert hit["cache_outcome"] == "hit"
    assert hit["effective_execution_run"] == hit["cache_source_run"]
    assert hit["requested_run"] != hit["effective_execution_run"]


def test_fixtures_validate_as_linkml_instances() -> None:
    """Examples validate as the public LinkML instance classes they document."""
    provenance = yaml.safe_load(PROVENANCE_GRAPH_FIXTURE.read_text())
    invocations = yaml.safe_load(BINDING_INVOCATIONS_FIXTURE.read_text())

    provenance_schema = json.loads(
        subprocess.run(
            [
                str(JSON_SCHEMA_GENERATOR),
                "--preserve-names",
                "--top-class",
                "ConsistProvenanceDocument",
                str(SCHEMA_PATH),
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    binding_schema = json.loads(
        subprocess.run(
            [
                str(JSON_SCHEMA_GENERATOR),
                "--preserve-names",
                "--top-class",
                "ConsistBindingInvocationReference",
                str(BINDING_SCHEMA_PATH),
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )

    jsonschema.validate(provenance, provenance_schema)
    for invocation in invocations["invocations"]:
        jsonschema.validate(invocation, binding_schema)


def test_schemas_pass_linkml_lint() -> None:
    """Both published source modules stay valid under the supported LinkML linter."""
    assert LINKML_LINT.is_file()

    for schema_path in (SCHEMA_PATH, BINDING_SCHEMA_PATH):
        result = subprocess.run(
            [str(LINKML_LINT), str(schema_path)],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stdout + result.stderr


def test_schema_preserves_public_fingerprint_and_external_artifact_boundaries() -> None:
    """Only Artifact.hash is portable, and an external artifact needs no producer."""
    schema = SchemaView(SCHEMA_PATH).schema
    artifact = schema.classes["ConsistArtifactReference"]
    attributes = cast(dict[str, SlotDefinition], artifact.attributes)
    fingerprint = attributes["fingerprint"]
    producing_run = attributes["producing_run"]
    fingerprint_description = cast(str, fingerprint.description)
    producing_run_description = cast(str, producing_run.description)

    assert "Artifact.hash" in fingerprint_description
    assert "content- or metadata-based" in fingerprint_description
    assert "content_id" not in attributes
    assert "database-local" in fingerprint_description
    assert producing_run.required is not True
    assert "external" in producing_run_description.lower()


def test_provenance_schema_documentation_states_cross_module_semantics() -> None:
    """Consumers receive the cache and graph consistency rules with the schema."""
    documentation = (PROJECT_ROOT / "docs" / "provenance-schema.md").read_text()

    assert "requested run" in documentation
    assert "effective execution run" in documentation
    assert "cache-source run" in documentation
    assert "never identity-bearing" in documentation
    assert "inherits the enclosing" in documentation
    assert "output association" in documentation
    assert "binding.merged.yaml" in documentation


def test_downstream_fixture_generates_json_schema_from_local_import() -> None:
    """A downstream operational profile can resolve the local modular source."""
    assert JSON_SCHEMA_GENERATOR.is_file()

    result = subprocess.run(
        [str(JSON_SCHEMA_GENERATOR), "--preserve-names", str(DOWNSTREAM_FIXTURE)],
        check=True,
        capture_output=True,
        text=True,
    )

    generated = json.loads(result.stdout)
    definitions = generated["$defs"]
    assert "PilatesAtlasVehicleArtifact" in definitions
    assert "ConsistArtifactReference" in definitions


def test_release_builder_writes_modular_merged_and_documented_assets(
    tmp_path: Path,
) -> None:
    """Release assets are self-contained and protected by reproducible checksums."""
    output_dir = tmp_path / f"provenance-schema-{SCHEMA_VERSION}"

    subprocess.run(
        [sys.executable, str(RELEASE_BUILDER), "--output", str(output_dir)],
        check=True,
        capture_output=True,
        text=True,
    )

    provenance_source = output_dir / "provenance.yaml"
    provenance_merged = output_dir / "provenance.merged.yaml"
    binding_source = output_dir / "binding.yaml"
    binding_merged = output_dir / "binding.merged.yaml"
    checksums = output_dir / "SHA256SUMS"
    reference_docs = list((output_dir / "reference").rglob("*.md"))

    assert provenance_source.is_file()
    assert provenance_merged.is_file()
    assert binding_source.is_file()
    assert binding_merged.is_file()
    assert checksums.is_file()
    assert reference_docs
    assert "ConsistArtifactReference" in provenance_merged.read_text()
    assert "ConsistRunArtifactAssociation" in provenance_merged.read_text()
    assert "ConsistBindingInvocationReference" in binding_merged.read_text()
    assert any("provenance" in path.as_posix() for path in reference_docs)
    assert any("binding" in path.as_posix() for path in reference_docs)
    assert any("Artifact.hash" in path.read_text() for path in reference_docs)

    for line in checksums.read_text().splitlines():
        digest, relative_path = line.split("  ", maxsplit=1)
        asset = output_dir / relative_path
        assert asset.is_file()
        assert hashlib.sha256(asset.read_bytes()).hexdigest() == digest

    downstream_release_fixture = output_dir / "downstream_release_profile.yaml"
    downstream_release_fixture.write_text(
        """\
id: https://example.org/pilates/released-operational-profile
name: released_operational_profile
prefixes:
  linkml: https://w3id.org/linkml/
  example: https://example.org/pilates/
default_prefix: example
default_range: string
imports:
  - linkml:types
  - provenance.merged
  - binding.merged
classes:
  ReleasedArtifact:
    attributes:
      physical_artifact:
        range: ConsistArtifactReference
        required: true
      cache_aware_invocation:
        range: ConsistBindingInvocationReference
""",
        encoding="utf-8",
    )
    generated = subprocess.run(
        [
            str(JSON_SCHEMA_GENERATOR),
            "--preserve-names",
            str(downstream_release_fixture),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert "ReleasedArtifact" in json.loads(generated.stdout)["$defs"]
    assert "ConsistBindingInvocationReference" in json.loads(generated.stdout)["$defs"]

    downstream_binding_release_fixture = output_dir / "downstream_binding_release.yaml"
    downstream_binding_release_fixture.write_text(
        """\
id: https://example.org/pilates/released-binding-consumer
name: released_binding_consumer
prefixes:
  linkml: https://w3id.org/linkml/
  example: https://example.org/pilates/
default_prefix: example
default_range: string
imports:
  - linkml:types
  - binding.merged
classes:
  ReleasedBindingConsumer:
    attributes:
      invocation:
        range: ConsistBindingInvocationReference
        required: true
""",
        encoding="utf-8",
    )
    binding_generated = subprocess.run(
        [
            str(JSON_SCHEMA_GENERATOR),
            "--preserve-names",
            str(downstream_binding_release_fixture),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    binding_definitions = json.loads(binding_generated.stdout)["$defs"]
    assert "ReleasedBindingConsumer" in binding_definitions
    assert "ConsistBindingInvocationReference" in binding_definitions

    rerun = subprocess.run(
        [sys.executable, str(RELEASE_BUILDER), "--output", str(output_dir)],
        capture_output=True,
        text=True,
    )
    assert rerun.returncode != 0


def test_release_builder_writes_lintable_timezone_aware_merged_schemas(
    tmp_path: Path,
) -> None:
    """Merged release inputs have UTC metadata accepted by LinkML lint."""
    output_dir = tmp_path / f"provenance-schema-{SCHEMA_VERSION}"

    subprocess.run(
        [sys.executable, str(RELEASE_BUILDER), "--output", str(output_dir)],
        check=True,
        capture_output=True,
        text=True,
    )

    for schema_name in ("provenance", "binding"):
        merged_path = output_dir / f"{schema_name}.merged.yaml"
        metadata = yaml.safe_load(merged_path.read_text())
        for field_name in ("source_file_date", "generation_date"):
            value = metadata[field_name]
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            assert parsed.tzinfo == timezone.utc

        result = subprocess.run(
            [str(LINKML_LINT), "--ignore-warnings", str(merged_path)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stdout + result.stderr


def test_importing_consist_does_not_import_linkml() -> None:
    """Normal Consist runtime use remains independent of LinkML tooling."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import consist; "
            "print('linkml' in sys.modules); "
            "print('linkml_runtime' in sys.modules)",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout.splitlines() == ["False", "False"]
