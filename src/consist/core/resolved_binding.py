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
from consist.digest import Digest, verify_digest

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

_STEP_IDENTITY_PATTERN = re.compile(r"^sha256:step-v1:[0-9a-f]{64}$")
_STRICT_BINDING_CONTEXT_FACTORY_TOKEN = object()


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
    try:
        module_name = callable_.__module__
    except AttributeError:
        module_name = None
    try:
        qual_name = callable_.__qualname__
    except AttributeError:
        qual_name = None
    payload = {
        "version": 1,
        "step_name": step_name,
        "module": module_name,
        "qualname": qual_name,
        "signature": str(inspect.signature(callable_)),
        "source": source,
    }
    digest = hashlib.sha256(
        json.dumps(
            payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode()
    ).hexdigest()
    return f"sha256:step-v1:{digest}"


def parse_strict_artifact_digest(value: str | Digest) -> Digest:
    """Parse a strict bound artifact ``Digest`` (``sha256:file`` or ``dir_manifest_v1:sha256``)."""
    parsed = Digest.parse(value)
    if parsed.scheme not in {"sha256:file", "dir_manifest_v1:sha256"}:
        raise ValueError(f"unsupported artifact identity contract: {value!r}")
    return parsed


def trusted_digest_from_artifact(artifact: "Artifact") -> Digest:
    """Return a strict ``Digest`` only when persisted artifact semantics prove it."""
    meta = artifact.meta if isinstance(artifact.meta, Mapping) else {}
    semantics = meta.get("hash_semantics")
    if isinstance(semantics, Mapping) and semantics.get("source") == "caller_supplied":
        raise ValueError("artifact does not declare a trusted immutable identity")

    digest = artifact.digest
    if not isinstance(digest, Digest):
        raise ValueError("artifact does not declare a trusted immutable identity")

    if digest.scheme == "sha256:file":
        if isinstance(semantics, Mapping):
            source = semantics.get("source")
            if source is not None and source != "computed_full":
                raise ValueError(
                    "artifact does not declare a trusted immutable identity"
                )
        return digest

    if digest.scheme == "dir_manifest_v1:sha256":
        manifest = meta.get("directory_manifest")
        if meta.get("directory_artifact") is True and isinstance(manifest, Mapping):
            tree_hash = manifest.get("tree_hash")
            if isinstance(tree_hash, str) and digest.hex == tree_hash:
                return digest
        raise ValueError("artifact does not declare a trusted immutable identity")

    raise ValueError("artifact does not declare a trusted immutable identity")


def create_execution_snapshot(
    *,
    source: Path,
    destination: Path,
    identity: Digest | str,
    directory_manifest: Mapping[str, Any] | None = None,
) -> Path:
    """Copy into a fresh run-owned destination and verify the typed ``Digest``."""
    parsed_identity = parse_strict_artifact_digest(identity)
    source_path = Path(source)
    destination_path = Path(destination)
    if source_path.resolve() == destination_path.resolve():
        raise ValueError("execution snapshots require a fresh run-owned destination")
    if destination_path.exists():
        raise ValueError("execution snapshot destination must be fresh")
    if parsed_identity.scheme == "dir_manifest_v1:sha256":
        if directory_manifest is None:
            raise ValueError(
                "execution snapshot requires a manifest for directory identity"
            )
        normalized_manifest = validate_directory_manifest(directory_manifest)
        if normalized_manifest["tree_hash"] != parsed_identity.hex:
            raise ValueError("directory manifest does not match the bound identity")
        materialize_directory_tree(
            source_path,
            destination_path,
            normalized_manifest,
            preserve_existing=False,
        )
        validate_directory_tree(destination_path, normalized_manifest)
        if not verify_digest(destination_path, parsed_identity):
            shutil.rmtree(destination_path, ignore_errors=True)
            raise ValueError("execution snapshot does not match the bound identity")
        return destination_path
    if not source_path.is_file():
        raise ValueError(f"file identity requires a regular file source: {source_path}")

    destination_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, destination_path)
    if not verify_digest(destination_path, parsed_identity):
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
    identity: Digest
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

    observed_identity: Digest
    expected_identity: Digest | None
    expected_source: (
        Literal["prior_run", "declared_digest", "registry_alias", "handoff_bundle"]
        | None
    )
    alias: str | None = None
    alias_revision: int | None = None
    local_report_id: str | None = None
    registry_record_id: str | None = None


def _validate_bound_artifact(artifact: BoundArtifact) -> None:
    parsed_identity = parse_strict_artifact_digest(artifact.identity)
    if artifact.artifact_id != artifact.locator.artifact_id:
        raise ValueError("resolved binding artifact id must match its locator")
    if artifact.artifact_kind == "file":
        if (
            parsed_identity.scheme != "sha256:file"
            or artifact.directory_manifest is not None
        ):
            raise ValueError("file bindings require a file identity and no manifest")
        return
    if (
        parsed_identity.scheme != "dir_manifest_v1:sha256"
        or artifact.directory_manifest is None
    ):
        raise ValueError(
            "directory bindings require a manifest identity and directory manifest"
        )
    manifest = validate_directory_manifest(artifact.directory_manifest)
    if manifest["tree_hash"] != parsed_identity.hex:
        raise ValueError("directory manifest does not match bound identity")


def _freeze_bound_artifact(artifact: BoundArtifact) -> BoundArtifact:
    _validate_bound_artifact(artifact)
    parsed_identity = parse_strict_artifact_digest(artifact.identity)
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
        identity=parsed_identity,
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
    destinations: dict[Path, BoundArtifact] = {}
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
        existing = destinations.get(destination)
        if existing is not None:
            if (
                existing.identity,
                existing.locator,
                existing.artifact_kind,
                existing.directory_manifest,
            ) != (
                resolved_input.artifact.identity,
                resolved_input.artifact.locator,
                resolved_input.artifact.artifact_kind,
                resolved_input.artifact.directory_manifest,
            ):
                raise ValueError("duplicate execution destination in resolved binding")
            continue
        destinations[destination] = resolved_input.artifact
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


class _ResolvedBindingMeta(type):
    """Normalize and freeze ``ResolvedBinding`` mappings before frozen dataclass init."""

    def __call__(
        cls,
        schema_version: Literal[1],
        step_name: str,
        step_contract_identity: str,
        inputs: Mapping[str, ResolvedInput],
        metadata: Mapping[str, JSONValue],
        admission_evidence: Mapping[str, AdmissionEvidence],
        diagnostics: Mapping[str, JSONValue] | None = None,
    ) -> Any:
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
            for key, resolved_input in inputs.items()
        }
        normalized_evidence = {
            key: AdmissionEvidence(
                observed_identity=parse_strict_artifact_digest(ev.observed_identity),
                expected_identity=(
                    parse_strict_artifact_digest(ev.expected_identity)
                    if ev.expected_identity is not None
                    else None
                ),
                expected_source=ev.expected_source,
                alias=ev.alias,
                alias_revision=ev.alias_revision,
                local_report_id=ev.local_report_id,
                registry_record_id=ev.registry_record_id,
            )
            for key, ev in admission_evidence.items()
        }
        _validate_resolved_binding(
            schema_version=schema_version,
            step_name=step_name,
            step_contract=step_contract_identity,
            inputs=frozen_inputs,
            admission_evidence=normalized_evidence,
        )
        frozen_meta = _freeze_json(metadata)
        frozen_diag = _freeze_json({} if diagnostics is None else diagnostics)
        if not isinstance(frozen_meta, Mapping) or not isinstance(frozen_diag, Mapping):
            raise TypeError(
                "resolved binding metadata and diagnostics must be mappings"
            )
        return super().__call__(
            schema_version=schema_version,
            step_name=step_name,
            step_contract_identity=step_contract_identity,
            inputs=MappingProxyType(frozen_inputs),
            metadata=MappingProxyType(dict(frozen_meta)),
            admission_evidence=MappingProxyType(normalized_evidence),
            diagnostics=MappingProxyType(dict(frozen_diag)),
        )


@dataclass(frozen=True, slots=True)
class ResolvedBinding(metaclass=_ResolvedBindingMeta):
    """Frozen, inspectable input contract for one exact Scenario invocation."""

    schema_version: Literal[1]
    step_name: str
    step_contract_identity: str
    inputs: Mapping[str, ResolvedInput]
    metadata: Mapping[str, JSONValue]
    admission_evidence: Mapping[str, AdmissionEvidence]
    diagnostics: Mapping[str, JSONValue] = field(
        default_factory=lambda: MappingProxyType({})
    )

    def input_for(self, parameter: str) -> ResolvedInput:
        """Return the resolved input contract for ``parameter``."""
        return self.inputs[parameter]

    def path_for(self, parameter: str) -> Path:
        """Return the execution destination path for ``parameter``."""
        destination = self.input_for(parameter).destination
        if destination is None:
            raise ValueError(
                f"resolved binding input {parameter!r} has no execution destination"
            )
        return destination

    def artifact_for(self, parameter: str) -> BoundArtifact:
        """Return the bound artifact contract for ``parameter``."""
        return self.input_for(parameter).artifact

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


@dataclass(frozen=True, slots=True)
class _StrictBindingInvocationContext:
    """Scenario-created strict-binding facts retained across the run handoff."""

    identity_digest: str
    evidence_json: str
    input_artifact_ids: tuple[str, ...]
    _factory_token: object = field(repr=False, compare=False)


def _identity_payload_from_binding_evidence(evidence_json: str) -> dict[str, Any]:
    """Recover the identity-bearing subset from canonical binding evidence."""
    try:
        evidence = json.loads(evidence_json)
        inputs = evidence["inputs"]
        identity_inputs = {
            key: {
                "parameter": item["parameter"],
                "identity": item["artifact"]["identity"],
                "destination": item["destination"],
            }
            for key, item in inputs.items()
        }
        return {
            "schema_version": evidence["schema_version"],
            "step_name": evidence["step_name"],
            "step_contract_identity": evidence["step_contract_identity"],
            "inputs": identity_inputs,
        }
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("strict binding evidence JSON is invalid") from exc


def _create_strict_binding_invocation_context(
    *,
    strict_binding: ResolvedBinding,
    identity_digest: str,
    evidence_json: str,
    input_artifact_ids: tuple[str, ...],
) -> _StrictBindingInvocationContext:
    """Create a verified internal strict-binding handoff value."""
    if not isinstance(identity_digest, str) or not identity_digest.strip():
        raise ValueError("strict binding identity digest must not be empty")
    if not isinstance(evidence_json, str):
        raise ValueError("strict binding evidence JSON must be a string")
    binding_artifact_ids: list[str] = []
    for resolved_input in strict_binding.inputs.values():
        artifact_id = resolved_input.artifact.artifact_id
        if artifact_id is None:
            raise ValueError("strict binding input artifact IDs must not be missing")
        binding_artifact_ids.append(str(artifact_id))
    if input_artifact_ids != tuple(binding_artifact_ids):
        raise ValueError(
            "strict binding input artifact IDs must match binding mapping order"
        )
    if (
        not isinstance(input_artifact_ids, tuple)
        or not input_artifact_ids
        or any(
            not isinstance(artifact_id, str) or not artifact_id.strip()
            for artifact_id in input_artifact_ids
        )
    ):
        raise ValueError("strict binding input artifact IDs must not be missing")

    identity_payload = _identity_payload_from_binding_evidence(evidence_json)
    evidence_digest = hashlib.sha256(
        json.dumps(
            identity_payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
    ).hexdigest()
    if evidence_digest != identity_digest:
        raise ValueError("strict binding evidence does not match the identity digest")

    evidence = json.loads(evidence_json)
    evidence_artifact_ids: list[str] = []
    for item in evidence["inputs"].values():
        artifact_id = item["artifact"]["artifact_id"]
        if artifact_id is None:
            raise ValueError("strict binding input artifact IDs must not be missing")
        evidence_artifact_ids.append(str(artifact_id))
    if sorted(evidence_artifact_ids) != sorted(input_artifact_ids):
        raise ValueError("strict binding input artifact IDs do not match evidence")

    return _StrictBindingInvocationContext(
        identity_digest=identity_digest,
        evidence_json=evidence_json,
        input_artifact_ids=input_artifact_ids,
        _factory_token=_STRICT_BINDING_CONTEXT_FACTORY_TOKEN,
    )


def _validate_strict_binding_invocation_context(
    context: object,
) -> _StrictBindingInvocationContext:
    """Reject forged values before they can alter strict run behavior."""
    if not isinstance(context, _StrictBindingInvocationContext):
        raise ValueError("strict binding context must be Scenario-created")
    if context._factory_token is not _STRICT_BINDING_CONTEXT_FACTORY_TOKEN:
        raise ValueError("strict binding context has an invalid factory token")
    return context


_COMPOUND_ARCHIVE_EXTENSIONS = frozenset(
    {
        ".gz",
        ".bz2",
        ".xz",
        ".zst",
        ".lz4",
        ".zip",
    }
)


def _extract_artifact_suffix(artifact: "Artifact") -> str:
    """Return the file extension(s) from an artifact's URI or path."""
    for candidate in (artifact.uri, artifact.abs_path):
        if not candidate:
            continue
        raw_path = candidate.split("://", 1)[1] if "://" in candidate else candidate
        name = Path(raw_path).name
        if not name:
            continue
        suffixes = Path(name).suffixes
        if not suffixes:
            continue
        if (
            len(suffixes) >= 2
            and suffixes[-1].lower() in _COMPOUND_ARCHIVE_EXTENSIONS
            and any(ch.isalpha() for ch in suffixes[-2])
        ):
            return f"{suffixes[-2]}{suffixes[-1]}"
        if any(ch.isalpha() for ch in suffixes[-1]):
            return suffixes[-1]
    return ""


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
        destination: Path | str | None = None,
        source: Literal[
            "explicit", "coupler", "fallback", "pinned", "external_admitted"
        ] = "explicit",
        selected_role: str | None = None,
        preserve_suffix: bool = True,
    ) -> "ResolvedBindingBuilder":
        """Bind a locally tracked artifact after deriving its trusted ``Digest``."""
        identity = trusted_digest_from_artifact(artifact)
        manifest = artifact.meta.get("directory_manifest")
        dest_path = (
            Path("inputs") / parameter if destination is None else Path(destination)
        )
        suffix = _extract_artifact_suffix(artifact) if preserve_suffix else ""
        has_alpha_suffix = any(ch.isalpha() for ch in dest_path.suffix)
        resolved_dest = (
            dest_path.with_name(f"{dest_path.name}{suffix}")
            if suffix and not has_alpha_suffix
            else dest_path
        )
        is_manifest = identity.scheme == "dir_manifest_v1:sha256"
        return self.bind_artifact(
            parameter=parameter,
            artifact=BoundArtifact(
                artifact_id=artifact.id,
                identity=identity,
                locator=TrackedArtifactLocator(artifact_id=artifact.id),
                artifact_kind="directory_manifest" if is_manifest else "file",
                directory_manifest=(
                    cast(Mapping[str, JSONValue], manifest)
                    if is_manifest and isinstance(manifest, Mapping)
                    else None
                ),
            ),
            destination=resolved_dest,
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
        return ResolvedBinding(
            schema_version=1,
            step_name=self._step_name,
            step_contract_identity=self._step_contract_identity,
            inputs=MappingProxyType(dict(self._inputs)),
            metadata=MappingProxyType(dict(self._metadata)),
            admission_evidence=MappingProxyType(dict(self._admission_evidence)),
            diagnostics=MappingProxyType(dict(self._diagnostics)),
        )
