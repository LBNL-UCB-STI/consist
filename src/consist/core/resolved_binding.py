"""Immutable input contracts for execution-exact Scenario runs."""

from __future__ import annotations

import hashlib
import inspect
import json
import re
import shutil
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal, Mapping, TYPE_CHECKING, TypeAlias, cast

from consist.core.directory_artifacts import (
    materialize_directory_tree,
    validate_directory_manifest,
    validate_directory_tree,
)

if TYPE_CHECKING:
    from consist.models.artifact import Artifact


JSONValue: TypeAlias = (
    None
    | bool
    | int
    | float
    | str
    | tuple["JSONValue", ...]
    | Mapping[str, "JSONValue"]
)

_IDENTITY_PATTERN = re.compile(
    r"^(?P<algorithm>sha256):(?P<kind>file|manifest-v1):(?P<digest>[0-9a-f]{64})$"
)
_STEP_IDENTITY_PATTERN = re.compile(r"^sha256:step-v1:[0-9a-f]{64}$")


def _freeze_json(value: Any) -> JSONValue:
    """Return an immutable, JSON-safe copy of a JSON-compatible value."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Mapping):
        return MappingProxyType(
            {str(key): _freeze_json(item) for key, item in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item) for item in value)
    raise TypeError(f"Resolved binding values must be JSON-safe, got {type(value)!r}.")


def _json_value(value: JSONValue) -> Any:
    if isinstance(value, Mapping):
        return {key: _json_value(cast(JSONValue, item)) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    return value


def step_contract_identity(callable_: Any, step_name: str) -> str:
    """Return a stable contract digest for one Python callable invocation."""
    try:
        source = inspect.getsource(callable_)
    except (OSError, TypeError):
        source = None
    payload = {
        "version": 1,
        "step_name": step_name,
        "module": getattr(callable_, "__module__", None),
        "qualname": getattr(callable_, "__qualname__", None),
        "signature": str(inspect.signature(callable_)),
        "source": source,
    }
    digest = hashlib.sha256(
        json.dumps(
            payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode()
    ).hexdigest()
    return f"sha256:step-v1:{digest}"


@dataclass(frozen=True, slots=True)
class ArtifactIdentity:
    """Self-describing immutable identity for a strict bound artifact."""

    algorithm: Literal["sha256"]
    kind: Literal["file", "manifest-v1"]
    digest: str

    @classmethod
    def parse(cls, value: str) -> "ArtifactIdentity":
        """Parse one supported self-describing artifact identity."""
        match = _IDENTITY_PATTERN.fullmatch(value)
        if match is None:
            parts = value.split(":", 2)
            if len(parts) == 3 and parts[1] in {"file", "manifest-v1"}:
                raise ValueError(f"invalid artifact identity: {value!r}")
            if value.startswith("sha256:"):
                raise ValueError(f"unsupported artifact identity contract: {value!r}")
            raise ValueError(f"invalid artifact identity: {value!r}")
        return cls(
            algorithm="sha256",
            kind=cast(Literal["file", "manifest-v1"], match.group("kind")),
            digest=match.group("digest"),
        )

    @classmethod
    def from_artifact(cls, artifact: Any) -> "ArtifactIdentity":
        """Return a strict identity only when persisted semantics prove it."""
        meta = artifact.meta if isinstance(artifact.meta, Mapping) else {}
        semantics = meta.get("hash_semantics")
        artifact_hash = artifact.hash
        if semantics == {
            "version": 1,
            "algorithm": "sha256",
            "kind": "file",
            "digest_contract": "raw_file_bytes",
            "source": "computed_full",
        } and isinstance(artifact_hash, str):
            return cls.parse(f"sha256:file:{artifact_hash}")
        manifest = meta.get("directory_manifest")
        if meta.get("directory_artifact") is True and isinstance(manifest, Mapping):
            tree_hash = manifest.get("tree_hash")
            if isinstance(tree_hash, str) and artifact_hash == tree_hash:
                return cls.parse(f"sha256:manifest-v1:{tree_hash}")
        raise ValueError("artifact does not declare a trusted immutable identity")

    def __str__(self) -> str:
        return f"{self.algorithm}:{self.kind}:{self.digest}"


def create_execution_snapshot(
    *,
    source: Path,
    destination: Path,
    identity: ArtifactIdentity,
    directory_manifest: Mapping[str, Any] | None = None,
) -> Path:
    """Copy into a fresh run-owned destination and verify the typed identity."""
    source_path = Path(source)
    destination_path = Path(destination)
    if source_path.resolve() == destination_path.resolve():
        raise ValueError("execution snapshots require a fresh run-owned destination")
    if destination_path.exists():
        raise ValueError("execution snapshot destination must be fresh")
    if identity.kind == "manifest-v1":
        if directory_manifest is None:
            raise ValueError(
                "execution snapshot requires a manifest for directory identity"
            )
        normalized_manifest = validate_directory_manifest(directory_manifest)
        if normalized_manifest["tree_hash"] != identity.digest:
            raise ValueError("directory manifest does not match the bound identity")
        materialize_directory_tree(
            source_path,
            destination_path,
            normalized_manifest,
            preserve_existing=False,
        )
        validate_directory_tree(destination_path, normalized_manifest)
        return destination_path
    if not source_path.is_file():
        raise ValueError(f"file identity requires a regular file source: {source_path}")

    destination_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, destination_path)
    digest = hashlib.sha256(destination_path.read_bytes()).hexdigest()
    if digest != identity.digest:
        destination_path.unlink()
        raise ValueError("execution snapshot does not match the bound identity")
    return destination_path


@dataclass(frozen=True, slots=True)
class TrackedArtifactLocator:
    """Stable local lookup of a tracked artifact."""

    artifact_id: uuid.UUID


ArtifactLocator: TypeAlias = TrackedArtifactLocator


@dataclass(frozen=True, slots=True)
class BoundArtifact:
    """Immutable artifact snapshot retained by a resolved binding."""

    artifact_id: uuid.UUID | None
    identity: ArtifactIdentity
    locator: ArtifactLocator
    artifact_kind: Literal["file", "directory_manifest"] = "file"
    directory_manifest: Mapping[str, JSONValue] | None = None


@dataclass(frozen=True, slots=True)
class ResolvedInput:
    """One named frozen input and its optional executable destination."""

    parameter: str
    artifact: BoundArtifact
    destination: Path | None
    source: Literal["explicit", "coupler", "fallback", "pinned", "external_admitted"]
    selected_role: str | None = None


@dataclass(frozen=True, slots=True)
class AdmissionEvidence:
    """Immutable evidence connecting an admitted observation to a bound input."""

    observed_identity: ArtifactIdentity
    expected_identity: ArtifactIdentity | None
    expected_source: (
        Literal["prior_run", "declared_digest", "registry_alias", "handoff_bundle"]
        | None
    )
    alias: str | None = None
    alias_revision: int | None = None
    local_report_id: str | None = None
    registry_record_id: str | None = None


def _validate_bound_artifact(artifact: BoundArtifact) -> None:
    if artifact.artifact_id != artifact.locator.artifact_id:
        raise ValueError("resolved binding artifact id must match its locator")
    if artifact.artifact_kind == "file":
        if artifact.identity.kind != "file" or artifact.directory_manifest is not None:
            raise ValueError("file bindings require a file identity and no manifest")
        return
    if artifact.identity.kind != "manifest-v1" or artifact.directory_manifest is None:
        raise ValueError(
            "directory bindings require a manifest identity and directory manifest"
        )
    manifest = validate_directory_manifest(artifact.directory_manifest)
    if manifest["tree_hash"] != artifact.identity.digest:
        raise ValueError("directory manifest does not match bound identity")


def _freeze_bound_artifact(artifact: BoundArtifact) -> BoundArtifact:
    _validate_bound_artifact(artifact)
    manifest_mapping: Mapping[str, JSONValue] | None = None
    if artifact.artifact_kind == "directory_manifest":
        manifest = artifact.directory_manifest
        if manifest is None:
            raise ValueError("directory bindings require a directory manifest")
        frozen_manifest = _freeze_json(validate_directory_manifest(manifest))
        if not isinstance(frozen_manifest, Mapping):
            raise TypeError("resolved binding directory manifest must be a mapping")
        manifest_mapping = cast(Mapping[str, JSONValue], frozen_manifest)
    return BoundArtifact(
        artifact_id=artifact.artifact_id,
        identity=artifact.identity,
        locator=artifact.locator,
        artifact_kind=artifact.artifact_kind,
        directory_manifest=manifest_mapping,
    )


def _validate_resolved_binding(
    *,
    schema_version: int,
    step_name: str,
    step_contract: str,
    inputs: Mapping[str, ResolvedInput],
    admission_evidence: Mapping[str, AdmissionEvidence],
) -> None:
    if schema_version != 1:
        raise ValueError("unsupported resolved binding schema version")
    if not step_name:
        raise ValueError("resolved binding step name must not be empty")
    if not _STEP_IDENTITY_PATTERN.fullmatch(step_contract):
        raise ValueError("invalid step contract identity")
    destinations: set[Path] = set()
    for key, resolved_input in inputs.items():
        if not key or not resolved_input.parameter:
            raise ValueError("resolved binding input parameter must not be empty")
        if key != resolved_input.parameter:
            raise ValueError("resolved binding input key must match its parameter")
        _validate_bound_artifact(resolved_input.artifact)
        destination = resolved_input.destination
        if destination is None:
            continue
        if destination.is_absolute() or ".." in destination.parts:
            raise ValueError("resolved binding destinations must be relative paths")
        if destination in destinations:
            raise ValueError("duplicate execution destination in resolved binding")
        destinations.add(destination)
    for parameter, evidence in admission_evidence.items():
        try:
            bound = inputs[parameter]
        except KeyError as exc:
            raise ValueError(
                f"admission evidence has no bound input: {parameter!r}"
            ) from exc
        if evidence.observed_identity != bound.artifact.identity:
            raise ValueError(
                "admission evidence observed identity does not match bound artifact"
            )
        if (evidence.expected_identity is None) != (evidence.expected_source is None):
            raise ValueError(
                "admission expected identity and expected source must be provided together"
            )


@dataclass(frozen=True, slots=True)
class ResolvedBinding:
    """Frozen, inspectable input contract for one exact Scenario invocation."""

    schema_version: Literal[1]
    step_name: str
    step_contract_identity: str
    inputs: Mapping[str, ResolvedInput]
    metadata: Mapping[str, JSONValue]
    admission_evidence: Mapping[str, AdmissionEvidence]
    diagnostics: Mapping[str, JSONValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Defensively freeze every mapping accepted by the public value type."""
        frozen_inputs = {
            key: ResolvedInput(
                parameter=resolved_input.parameter,
                artifact=_freeze_bound_artifact(resolved_input.artifact),
                destination=(
                    Path(resolved_input.destination)
                    if resolved_input.destination is not None
                    else None
                ),
                source=resolved_input.source,
                selected_role=resolved_input.selected_role,
            )
            for key, resolved_input in self.inputs.items()
        }
        _validate_resolved_binding(
            schema_version=self.schema_version,
            step_name=self.step_name,
            step_contract=self.step_contract_identity,
            inputs=frozen_inputs,
            admission_evidence=self.admission_evidence,
        )
        metadata = _freeze_json(self.metadata)
        diagnostics = _freeze_json(self.diagnostics)
        if not isinstance(metadata, Mapping) or not isinstance(diagnostics, Mapping):
            raise TypeError(
                "resolved binding metadata and diagnostics must be mappings"
            )
        object.__setattr__(self, "inputs", MappingProxyType(frozen_inputs))
        object.__setattr__(self, "metadata", MappingProxyType(dict(metadata)))
        object.__setattr__(
            self,
            "admission_evidence",
            MappingProxyType(dict(self.admission_evidence)),
        )
        object.__setattr__(self, "diagnostics", MappingProxyType(dict(diagnostics)))

    def identity_json(self) -> str:
        """Serialize the cache-partitioning execution contract deterministically."""
        payload = {
            "schema_version": self.schema_version,
            "step_name": self.step_name,
            "step_contract_identity": self.step_contract_identity,
            "inputs": {
                key: {
                    "parameter": item.parameter,
                    "identity": str(item.artifact.identity),
                    "destination": str(item.destination)
                    if item.destination is not None
                    else None,
                }
                for key, item in self.inputs.items()
            },
        }
        return json.dumps(
            payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        )

    def evidence_json(self) -> str:
        """Serialize durable invocation evidence without changing cache identity."""
        payload = json.loads(self.identity_json())
        payload["inputs"] = {
            key: {
                "parameter": item.parameter,
                "artifact": {
                    "artifact_id": str(item.artifact.artifact_id)
                    if item.artifact.artifact_id is not None
                    else None,
                    "identity": str(item.artifact.identity),
                    "artifact_kind": item.artifact.artifact_kind,
                    "locator": {
                        "kind": "tracked_artifact",
                        "artifact_id": str(item.artifact.locator.artifact_id),
                    },
                    "directory_manifest": _json_value(item.artifact.directory_manifest)
                    if item.artifact.directory_manifest is not None
                    else None,
                },
                "destination": str(item.destination)
                if item.destination is not None
                else None,
                "source": item.source,
                "selected_role": item.selected_role,
            }
            for key, item in self.inputs.items()
        }
        payload.update(
            {
                "metadata": _json_value(self.metadata),
                "admission_evidence": {
                    key: {
                        "observed_identity": str(item.observed_identity),
                        "expected_identity": str(item.expected_identity)
                        if item.expected_identity is not None
                        else None,
                        "expected_source": item.expected_source,
                        "alias": item.alias,
                        "alias_revision": item.alias_revision,
                        "local_report_id": item.local_report_id,
                        "registry_record_id": item.registry_record_id,
                    }
                    for key, item in self.admission_evidence.items()
                },
                "diagnostics": _json_value(self.diagnostics),
            }
        )
        return json.dumps(
            payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        )

    def canonical_json(self) -> str:
        """Serialize durable evidence using the legacy method name."""
        return self.evidence_json()

    def identity_digest(self) -> str:
        """Return the strict contract digest used to partition cache reuse."""
        return hashlib.sha256(self.identity_json().encode("utf-8")).hexdigest()


class ResolvedBindingBuilder:
    """Build one immutable resolved binding without retaining runtime objects."""

    def __init__(self, *, step_name: str, step_contract_identity: str) -> None:
        if not _STEP_IDENTITY_PATTERN.fullmatch(step_contract_identity):
            raise ValueError("invalid step contract identity")
        self._step_name = step_name
        self._step_contract_identity = step_contract_identity
        self._inputs: dict[str, ResolvedInput] = {}
        self._metadata: Mapping[str, JSONValue] = MappingProxyType({})
        self._admission_evidence: dict[str, AdmissionEvidence] = {}
        self._diagnostics: Mapping[str, JSONValue] = MappingProxyType({})

    def bind_artifact(
        self,
        *,
        parameter: str,
        artifact: BoundArtifact,
        destination: Path | None,
        source: Literal[
            "explicit", "coupler", "fallback", "pinned", "external_admitted"
        ],
        selected_role: str | None = None,
    ) -> "ResolvedBindingBuilder":
        if not parameter:
            raise ValueError("resolved binding parameter must not be empty")
        if parameter in self._inputs:
            raise ValueError(f"duplicate resolved binding parameter: {parameter!r}")
        frozen_artifact = _freeze_bound_artifact(artifact)
        self._inputs[parameter] = ResolvedInput(
            parameter=parameter,
            artifact=frozen_artifact,
            destination=Path(destination) if destination is not None else None,
            source=source,
            selected_role=selected_role,
        )
        return self

    def bind_tracked_artifact(
        self,
        *,
        parameter: str,
        artifact: "Artifact",
        destination: Path | None,
        source: Literal[
            "explicit", "coupler", "fallback", "pinned", "external_admitted"
        ],
        selected_role: str | None = None,
    ) -> "ResolvedBindingBuilder":
        """Bind a locally tracked artifact after deriving its trusted identity."""
        identity = ArtifactIdentity.from_artifact(artifact)
        manifest = artifact.meta.get("directory_manifest")
        return self.bind_artifact(
            parameter=parameter,
            artifact=BoundArtifact(
                artifact_id=artifact.id,
                identity=identity,
                locator=TrackedArtifactLocator(artifact_id=artifact.id),
                artifact_kind=(
                    "directory_manifest" if identity.kind == "manifest-v1" else "file"
                ),
                directory_manifest=(
                    cast(Mapping[str, JSONValue], manifest)
                    if identity.kind == "manifest-v1" and isinstance(manifest, Mapping)
                    else None
                ),
            ),
            destination=destination,
            source=source,
            selected_role=selected_role,
        )

    def with_metadata(self, metadata: Mapping[str, Any]) -> "ResolvedBindingBuilder":
        frozen_metadata = _freeze_json(metadata)
        if not isinstance(frozen_metadata, Mapping):
            raise TypeError("resolved binding metadata must be a mapping")
        self._metadata = cast(Mapping[str, JSONValue], frozen_metadata)
        return self

    def with_diagnostics(
        self, diagnostics: Mapping[str, Any]
    ) -> "ResolvedBindingBuilder":
        """Attach immutable selection diagnostics to durable invocation evidence."""
        frozen_diagnostics = _freeze_json(diagnostics)
        if not isinstance(frozen_diagnostics, Mapping):
            raise TypeError("resolved binding diagnostics must be a mapping")
        self._diagnostics = cast(Mapping[str, JSONValue], frozen_diagnostics)
        return self

    def with_admission(
        self, *, parameter: str, evidence: AdmissionEvidence
    ) -> "ResolvedBindingBuilder":
        self._admission_evidence[parameter] = evidence
        return self

    def freeze(self) -> ResolvedBinding:
        """Validate cross-field invariants and return the immutable contract."""
        for parameter, evidence in self._admission_evidence.items():
            try:
                bound = self._inputs[parameter]
            except KeyError as exc:
                raise ValueError(
                    f"admission evidence has no bound input: {parameter!r}"
                ) from exc
            if evidence.observed_identity != bound.artifact.identity:
                raise ValueError(
                    "admission evidence observed identity does not match bound artifact"
                )
            if (evidence.expected_identity is None) != (
                evidence.expected_source is None
            ):
                raise ValueError(
                    "admission expected identity and expected source must be provided together"
                )
        return ResolvedBinding(
            schema_version=1,
            step_name=self._step_name,
            step_contract_identity=self._step_contract_identity,
            inputs=MappingProxyType(dict(self._inputs)),
            metadata=MappingProxyType(dict(self._metadata)),
            admission_evidence=MappingProxyType(dict(self._admission_evidence)),
            diagnostics=MappingProxyType(dict(self._diagnostics)),
        )
