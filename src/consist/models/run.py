"""
Value models for runs and run-level provenance snapshots in Consist.

This module belongs to Layer 2 (``consist.models``):
- Uses pure Pydantic ``BaseModel`` and stdlib types.
- Zero imports from ``consist.core.*``, ``consist.api``, ``sqlalchemy``, or ``sqlmodel``.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional
import uuid

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator

from consist.models.artifact import Artifact
from consist.protocols import ArtifactResolver
from consist.types import (
    VALID_MATERIALIZE_DB_FALLBACK as _VALID_MATERIALIZE_DB_FALLBACK,
    VALID_MATERIALIZE_ON_MISSING as _VALID_MATERIALIZE_ON_MISSING,
    normalize_materialize_output_keys,
    resolve_output_from_mapping,
    validate_materialize_option,
)

UTC = timezone.utc


def _normalize_utc_datetime(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


CanonicalRunMetaField = Literal["stage", "phase"]
CouplerSnapshotPhase = Literal["init", "pre", "post"]


def resolve_canonical_run_meta_field(
    run: "Run", field_name: CanonicalRunMetaField
) -> Optional[str]:
    """
    Resolve a canonical run field with fallback to legacy mirrored metadata.

    Canonical ``Run`` fields take precedence; ``run.meta`` is consulted only for
    older snapshot payloads.
    """
    canonical_value = run.phase if field_name == "phase" else run.stage
    if isinstance(canonical_value, str):
        return canonical_value

    meta = run.meta if isinstance(run.meta, Mapping) else {}
    meta_value = meta.get(field_name)
    return meta_value if isinstance(meta_value, str) else None


class RunArtifactLink(BaseModel):
    """Link record connecting a ``Run`` to an ``Artifact`` with direction and role."""

    run_id: str
    artifact_id: uuid.UUID
    direction: str
    role: str = ""
    is_implicit: bool = False


class RunBindingInvocation(BaseModel):
    """Immutable evidence for one resolved-binding execution attempt."""

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    requested_run_id: str
    execution_run_id: str
    cache_source_run_id: Optional[str] = None
    cache_outcome: str
    binding_json: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("created_at", mode="after")
    @classmethod
    def _ensure_utc_created_at(cls, value: datetime) -> datetime:
        normalized = _normalize_utc_datetime(value)
        assert normalized is not None
        return normalized


class RunCouplerSnapshot(BaseModel):
    """Relational snapshot of ``ScenarioContext.coupler`` bindings at a run boundary."""

    run_id: str
    phase: str
    role_key: str
    scenario_run_id: str
    step_ordinal: int = 0
    artifact_id: uuid.UUID
    producer_run_id: Optional[str] = None
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("updated_at", mode="after")
    @classmethod
    def _ensure_utc_updated_at(cls, value: datetime) -> datetime:
        normalized = _normalize_utc_datetime(value)
        assert normalized is not None
        return normalized


class Run(BaseModel):
    """
    Value record representing a single tracked execution in Consist.

    Captures execution metadata, status, timing, tags, and cryptographic identity
    hashes (``config_hash``, ``git_hash``, ``input_hash``, ``signature``).
    """

    id: str
    parent_run_id: Optional[str] = None
    status: str = "running"

    model_name: str
    description: Optional[str] = None
    year: Optional[int] = None
    iteration: Optional[int] = None
    stage: Optional[str] = None
    phase: Optional[str] = None

    tags: List[str] = Field(default_factory=list)

    config_hash: Optional[str] = None
    git_hash: Optional[str] = None
    input_hash: Optional[str] = None
    signature: Optional[str] = None
    cacheable: bool = True

    meta: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("tags", mode="before")
    @classmethod
    def _parse_tags_json(cls, value: Any) -> Any:
        if value is None:
            return []
        if isinstance(value, str):
            if not value:
                return []
            return json.loads(value)
        return value

    @field_validator("meta", mode="before")
    @classmethod
    def _parse_meta_json(cls, value: Any) -> Any:
        if value is None:
            return {}
        if isinstance(value, str):
            if not value:
                return {}
            return json.loads(value)
        return value

    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    ended_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("started_at", "ended_at", "created_at", "updated_at", mode="after")
    @classmethod
    def _ensure_utc_timestamps(cls, value: Optional[datetime]) -> Optional[datetime]:
        return _normalize_utc_datetime(value)

    @property
    def is_cache_hit(self) -> bool:
        """Whether this run was satisfied from a prior cached run."""
        return bool(isinstance(self.meta, Mapping) and self.meta.get("cache_hit"))

    @property
    def cache_source_run_id(self) -> Optional[str]:
        """The run ID of the upstream cache source, if recorded."""
        if not isinstance(self.meta, Mapping):
            return None
        val = self.meta.get("cache_source")
        return val if isinstance(val, str) and val else None

    @property
    def duration_seconds(self) -> Optional[float]:
        """Return elapsed duration in seconds when both ``started_at`` and ``ended_at`` exist."""
        if self.ended_at and self.started_at:
            return (self.ended_at - self.started_at).total_seconds()
        return None

    @property
    def identity_summary(self) -> Dict[str, Any]:
        """Return a structured cache-identity breakdown for this run."""
        meta = self.meta if isinstance(self.meta, Mapping) else {}
        identity_input_digests = meta.get("consist_hash_inputs")
        if not isinstance(identity_input_digests, Mapping):
            identity_input_digests = {}
        else:
            identity_input_digests = dict(identity_input_digests)

        run_fields = {
            "model": self.model_name,
            "year": self.year,
            "iteration": self.iteration,
            "phase": resolve_canonical_run_meta_field(self, "phase"),
            "stage": resolve_canonical_run_meta_field(self, "stage"),
            "cache_epoch": meta.get("cache_epoch"),
            "cache_version": meta.get("cache_version"),
        }

        adapter = {
            "name": meta.get("config_adapter"),
            "hash": meta.get("config_bundle_hash"),
            "version": meta.get("config_adapter_version"),
            "identity_manifest": meta.get("config_identity_manifest"),
        }

        config_payload = meta.get("config")
        config_dict = (
            dict(config_payload) if isinstance(config_payload, Mapping) else {}
        )

        inputs = meta.get("inputs")
        inputs_list = list(inputs) if isinstance(inputs, list) else []

        summary: Dict[str, Any] = {
            "code_version": self.git_hash,
            "config": config_dict,
            "adapter": adapter,
            "identity_inputs": identity_input_digests,
            "run_fields": run_fields,
            "inputs": inputs_list,
            "config_hash": self.config_hash,
            "input_hash": self.input_hash,
            "code_hash": self.git_hash,
            "signature": self.signature,
            "signature_ready": bool(
                self.config_hash and self.input_hash and self.git_hash
            ),
            "components": {
                "config_hash": self.config_hash,
                "input_hash": self.input_hash,
                "code_hash": self.git_hash,
            },
            "identity_inputs_count": len(identity_input_digests),
            "config_adapter": {
                "name": meta.get("config_adapter"),
                "version": meta.get("config_adapter_version"),
                "identity_hash": meta.get("config_bundle_hash"),
                "identity_manifest": meta.get("config_identity_manifest"),
            },
            "code_identity": {
                "mode": meta.get("code_identity", "repo_git"),
                "extra_deps": meta.get("code_identity_extra_deps"),
            },
            "cache": {
                "mode": meta.get("cache_mode"),
                "hit": meta.get("cache_hit"),
                "source_run_id": meta.get("cache_source"),
                "epoch": meta.get("cache_epoch"),
                "version": meta.get("cache_version"),
            },
        }
        input_identity = meta.get("input_identity")
        if isinstance(input_identity, Mapping):
            summary["input_identity"] = dict(input_identity)
        return summary

    def __repr__(self) -> str:
        status_icon = (
            "🟢"
            if self.status == "completed"
            else "🔴"
            if self.status == "failed"
            else "🟡"
        )
        return f"<{status_icon} Run id='{self.id}' model='{self.model_name}' status='{self.status}'>"

    def materialize_outputs(
        self,
        tracker: Any,
        *,
        target_root: str | Path,
        source_root: str | Path | None = None,
        keys: Sequence[str] | None = None,
        preserve_existing: bool = True,
        on_missing: Literal["warn", "raise"] = "warn",
        db_fallback: Literal["never", "if_ingested"] = "if_ingested",
    ) -> Any:
        """Materialize this run's linked outputs through ``tracker``."""
        normalized_keys = normalize_materialize_output_keys(
            keys,
            caller="materialize_outputs",
        )
        validate_materialize_option(
            name="on_missing",
            value=on_missing,
            allowed=_VALID_MATERIALIZE_ON_MISSING,
        )
        validate_materialize_option(
            name="db_fallback",
            value=db_fallback,
            allowed=_VALID_MATERIALIZE_DB_FALLBACK,
        )

        return tracker.materialize_run_outputs(
            self.id,
            target_root=target_root,
            source_root=source_root,
            keys=normalized_keys,
            preserve_existing=preserve_existing,
            on_missing=on_missing,
            db_fallback=db_fallback,
        )

    def hydrate_outputs(
        self,
        tracker: Any,
        *,
        target_root: str | Path,
        source_root: str | Path | None = None,
        keys: Sequence[str] | None = None,
        preserve_existing: bool = True,
        on_missing: Literal["warn", "raise"] = "warn",
        db_fallback: Literal["never", "if_ingested"] = "if_ingested",
    ) -> Any:
        """Hydrate this run's linked outputs through ``tracker``."""
        normalized_keys = normalize_materialize_output_keys(
            keys,
            caller="hydrate_outputs",
        )
        validate_materialize_option(
            name="on_missing",
            value=on_missing,
            allowed=_VALID_MATERIALIZE_ON_MISSING,
        )
        validate_materialize_option(
            name="db_fallback",
            value=db_fallback,
            allowed=_VALID_MATERIALIZE_DB_FALLBACK,
        )

        return tracker.hydrate_run_outputs(
            self.id,
            target_root=target_root,
            source_root=source_root,
            keys=normalized_keys,
            preserve_existing=preserve_existing,
            on_missing=on_missing,
            db_fallback=db_fallback,
        )


class RunArtifacts(BaseModel):
    """Structured in-memory container for ``inputs`` and ``outputs`` of a run."""

    inputs: Dict[str, Artifact] = Field(default_factory=dict)
    outputs: Dict[str, Artifact] = Field(default_factory=dict)

    model_config = ConfigDict(arbitrary_types_allowed=True)


class RunResult(BaseModel):
    """Structured return value for ``Tracker.run`` and ``ScenarioContext.run``."""

    run: Run
    outputs: Dict[str, Artifact] = Field(default_factory=dict)
    cache_hit: bool = False

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @property
    def is_cache_hit(self) -> bool:
        """Whether this run result was satisfied from a prior cached run."""
        return bool(self.cache_hit or self.run.is_cache_hit)

    @property
    def cache_source_run_id(self) -> Optional[str]:
        """The run ID of the upstream cache source, if recorded."""
        return self.run.cache_source_run_id

    @property
    def output(self) -> Optional[Artifact]:
        if not self.outputs:
            return None
        return next(iter(self.outputs.values()))

    def as_dict(self) -> dict[str, Artifact]:
        return dict(self.outputs)

    def output_path(self, key: str, tracker: Optional[ArtifactResolver] = None) -> Path:
        """Resolve a run output key to a filesystem path."""
        artifact = resolve_run_result_output(self, key=key)
        return artifact.as_path(tracker=tracker)


def resolve_run_result_output(
    run_result: RunResult, key: Optional[str] = None
) -> Artifact:
    """Resolve one output artifact from a ``RunResult``."""
    return resolve_output_from_mapping(run_result.outputs, key=key)


@dataclass(slots=True)
class _PendingOutputMetaEntry:
    """In-flight output metadata queued by ``set_output_meta(...)``."""

    meta: Dict[str, Any]
    merge: bool = True
    applied_object_ids: set[int] = field(default_factory=set)


class ConsistRecord(BaseModel):
    """
    Self-contained JSON snapshot of a run, its input/output artifacts, and config.
    """

    run: Run
    inputs: List[Artifact] = Field(default_factory=list)
    outputs: List[Artifact] = Field(default_factory=list)

    cached_run: Optional[Run] = None
    config: Dict[str, Any] = Field(default_factory=dict)
    facet: Dict[str, Any] = Field(default_factory=dict)

    _pending_output_meta: Dict[str, _PendingOutputMetaEntry] = PrivateAttr(
        default_factory=dict
    )
    _pending_archive_policy: Any = PrivateAttr(default=None)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def sync_output_artifact(
        self,
        *,
        key: Optional[str] = None,
        artifact_id: str | uuid.UUID | None = None,
        meta: Optional[Mapping[str, Any]] = None,
        abs_path: Optional[str | Path] = None,
        facet_id: Optional[str] = None,
        preserve_existing_abs_path: bool = False,
    ) -> List[Artifact]:
        """Immutably update matching recorded outputs and return the updated handles."""
        matched: List[Artifact] = []
        target_id = str(artifact_id) if artifact_id is not None else None
        for idx, recorded in enumerate(self.outputs):
            matches_key = key is not None and recorded.key == key
            matches_id = (
                target_id is not None
                and recorded.id is not None
                and str(recorded.id) == target_id
            )
            if not (matches_key or matches_id):
                continue
            updated = recorded
            if meta is not None:
                updated = updated.with_meta(meta, merge=False)
            if facet_id is not None:
                updated = updated.with_facet_id(facet_id)
            if abs_path is not None and (
                not preserve_existing_abs_path or updated.abs_path is None
            ):
                updated = updated.with_abs_path(abs_path)
            self.outputs[idx] = updated
            matched.append(updated)
        return matched

    def replace_artifact(
        self, updated: Artifact, *, direction: Optional[str] = None
    ) -> Artifact:
        """Replace any matching ``inputs`` or ``outputs`` entry by ``Artifact.id``."""
        if direction in (None, "output"):
            for idx, item in enumerate(self.outputs):
                if item.id == updated.id:
                    self.outputs[idx] = updated
        if direction in (None, "input"):
            for idx, item in enumerate(self.inputs):
                if item.id == updated.id:
                    self.inputs[idx] = updated
        return updated
