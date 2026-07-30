"""Acceptance tests for Consist's standalone LinkML provenance module."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tomllib
from pathlib import Path

from linkml_runtime.utils.schemaview import SchemaView


PROJECT_ROOT = Path(__file__).parents[2]
PROJECT_VERSION = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())[
    "project"
]["version"]
SCHEMA_VERSION = "0.1.0"
SCHEMA_PATH = PROJECT_ROOT / "src" / "consist" / "schemas" / "provenance.yaml"
RELEASE_BUILDER = PROJECT_ROOT / "scripts" / "build_provenance_schema_release.py"
DOWNSTREAM_FIXTURE = (
    PROJECT_ROOT
    / "tests"
    / "fixtures"
    / "linkml"
    / "downstream_operational_profile.yaml"
)
LINKML_LINT = Path(sys.executable).with_name("linkml-lint")
JSON_SCHEMA_GENERATOR = Path(sys.executable).with_name("gen-json-schema")


def test_schema_declares_only_portable_run_and_artifact_references() -> None:
    """The public schema models stable references, not Consist persistence rows."""
    schema = SchemaView(SCHEMA_PATH).schema

    assert schema.id == "https://w3id.org/consist/provenance"
    assert schema.version == SCHEMA_VERSION
    assert schema.version != PROJECT_VERSION
    assert schema.status == "experimental"
    assert set(schema.classes) == {
        "ConsistRunReference",
        "ConsistArtifactReference",
    }

    run_attributes = schema.classes["ConsistRunReference"].attributes
    assert set(run_attributes) == {"run_id", "provenance_namespace", "signature"}
    assert run_attributes["run_id"].required is True
    assert run_attributes["provenance_namespace"].required is not True
    assert run_attributes["signature"].required is not True

    artifact_attributes = schema.classes["ConsistArtifactReference"].attributes
    assert set(artifact_attributes) == {
        "artifact_id",
        "provenance_namespace",
        "artifact_key",
        "fingerprint",
        "fingerprint_strategy",
        "producing_run",
    }
    assert artifact_attributes["artifact_id"].required is True
    assert artifact_attributes["artifact_id"].range == "UUID"
    assert artifact_attributes["producing_run"].range == "ConsistRunReference"
    assert artifact_attributes["producing_run"].required is not True
    assert schema.types["UUID"].base == "str"
    assert schema.types["UUID"].pattern == (
        "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-"
        "[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
    )


def test_schema_passes_linkml_lint() -> None:
    """The published source schema stays valid under the supported LinkML linter."""
    assert LINKML_LINT.is_file()

    result = subprocess.run(
        [str(LINKML_LINT), str(SCHEMA_PATH)],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_schema_preserves_public_fingerprint_and_external_artifact_boundaries() -> None:
    """Only Artifact.hash is portable, and an external artifact needs no producer."""
    schema = SchemaView(SCHEMA_PATH).schema
    artifact = schema.classes["ConsistArtifactReference"]
    fingerprint = artifact.attributes["fingerprint"]
    producing_run = artifact.attributes["producing_run"]

    assert "Artifact.hash" in fingerprint.description
    assert "content- or metadata-based" in fingerprint.description
    assert "content_id" not in artifact.attributes
    assert "database-local" in fingerprint.description
    assert producing_run.required is not True
    assert "external" in producing_run.description.lower()


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

    source = output_dir / "provenance.yaml"
    merged = output_dir / "provenance.merged.yaml"
    checksums = output_dir / "SHA256SUMS"
    reference_docs = list((output_dir / "reference").rglob("*.md"))

    assert source.is_file()
    assert merged.is_file()
    assert checksums.is_file()
    assert reference_docs
    assert "ConsistArtifactReference" in merged.read_text()
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
classes:
  ReleasedArtifact:
    attributes:
      physical_artifact:
        range: ConsistArtifactReference
        required: true
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

    rerun = subprocess.run(
        [sys.executable, str(RELEASE_BUILDER), "--output", str(output_dir)],
        capture_output=True,
        text=True,
    )
    assert rerun.returncode != 0


def test_importing_consist_does_not_import_linkml() -> None:
    """Normal Consist runtime use remains independent of LinkML tooling."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import consist; print('linkml' in sys.modules)",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout.strip() == "False"
