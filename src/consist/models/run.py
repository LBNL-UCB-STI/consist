"""
Domain models for runs and run-level snapshots in Consist.

These Pydantic models represent runs, operational receipts, Coupler snapshots,
and human-readable per-run JSON snapshots (`consist.json`).
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Sequence

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator

from consist.artifact import ArchivedOutputs, Artifact
from consist.core.materialize_options import (
    VALID_MATERIALIZE_DB_FALLBACK as _VALID_MATERIALIZE_DB_FALLBACK,
    VALID_MATERIALIZE_ON_MISSING as _VALID_MATERIALIZE_ON_MISSING,
    normalize_materialize_output_keys,
    validate_materialize_option,
)
from consist.location import Location, Observation

UTC = timezone.utc

if TYPE_CHECKING:
    from consist.core.materialize import (
        HydratedRunOutputsResult,
        MaterializationResult,
    )
    from consist.core.tracker import Tracker

CanonicalRunMetaField = Literal["stage", "phase"]


def resolve_canonical_run_meta_field(
    run: "Run", field: CanonicalRunMetaField
) -> Optional[str]:
    """
    Resolve a canonical run field with fallback to legacy mirrored metadata.

    Canonical ``Run`` columns take precedence. ``run.meta`` is only consulted for
    compatibility with older rows or snapshot payloads that predate the schema
    change.
    """
    if field == "phase":
        canonical_value = run.phase
    else:
        canonical_value = run.stage
    if isinstance(canonical_value, str):
        return canonical_value

    meta = run.meta if isinstance(run.meta, dict) else {}
    meta_value = meta.get(field)
    return meta_value if isinstance(meta_value, str) else None


class RunArtifactLink(BaseModel):
    """
    Data transfer object connecting a ``Run`` to an ``Artifact`` with directionality.

    Attributes:
        run_id (str): The ID of the associated run.
        artifact_id (uuid.UUID): The ID of the associated artifact.
        direction (str): Specifies whether the artifact was an "input" or "output" for the run.
        is_implicit (bool): True if the artifact was automatically detected through lineage,
                            rather than explicitly logged. Defaults to False.
    """

    model_config = ConfigDict(validate_assignment=True)

    run_id: str
    artifact_id: uuid.UUID
    direction: str  # "input" or "output"
    is_implicit: bool = Field(default=False)


class RunBindingInvocation(BaseModel):
    """Immutable evidence for one resolved-binding execution attempt.

    A cache hit reuses a producer ``Run`` and therefore cannot safely store
    per-invocation input intent on that historical row. This ledger keeps the
    requested run identifier, the run that actually supplied the result, and
    the canonical resolved binding as distinct durable facts.
    """

    model_config = ConfigDict(validate_assignment=True)

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    requested_run_id: str
    execution_run_id: str
    cache_source_run_id: Optional[str] = Field(default=None)
    cache_outcome: str
    binding_json: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


CouplerSnapshotPhase = Literal["init", "pre", "post"]


class RunCouplerSnapshot(BaseModel):
    """
    Normalized relational snapshot of ScenarioContext.coupler bindings at a
    specific run boundary.
    """

    model_config = ConfigDict(validate_assignment=True)

    run_id: str
    phase: str
    role_key: str
    scenario_run_id: str
    step_ordinal: int = Field(default=0)
    artifact_id: uuid.UUID
    producer_run_id: Optional[str] = Field(default=None)
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class Run(BaseModel):
    """
    Primary run domain model in Consist.

    Each run captures execution metadata (status, timing, identity hashes, tags)
    and links to artifacts through `StoreLedger`. Full configuration details
    live in the JSON run snapshot on disk, while the database stores hashes and
    a queryable subset of metadata for caching and discovery.

    Attributes:
        id (str): A unique identifier for the run, often combining model name, year, and a UUID.
        parent_run_id (Optional[str]): The ID of the parent run, if this is a nested execution.
        status (str): The current state of the run (e.g., "running", "completed", "failed").
        model_name (str): The name of the model or workflow being executed.
        description (Optional[str]): Human-readable description of the run's purpose or outcome.
        year (Optional[int]): The simulation or data year, if applicable.
        iteration (Optional[int]): The iteration number, if applicable.
        stage (Optional[str]): The workflow stage, if applicable.
        phase (Optional[str]): The lifecycle phase, if applicable.
        tags (List[str]): A list of string labels for categorization and filtering (e.g., ["production", "urbansim"]).
        config_hash (Optional[str]): A hash of the run's configuration, used for caching.
        git_hash (Optional[str]): The Git commit hash of the code version used for the run.
        meta (dict[str, object]): A flexible dictionary for storing arbitrary metadata (e.g., hostname).
        started_at (datetime): The timestamp when the run execution began.
        ended_at (Optional[datetime]): The timestamp when the run execution completed or failed.
        created_at (datetime): The timestamp when the run record was created.
        updated_at (datetime): The timestamp when the run record was last updated.
    """

    model_config = ConfigDict(validate_assignment=True, arbitrary_types_allowed=True)

    id: str
    parent_run_id: Optional[str] = Field(default=None)

    # State
    status: str = Field(default="running")

    # Context
    model_name: str
    description: Optional[str] = Field(
        default=None,
        description="Human-readable description of the run's purpose or outcome",
    )
    year: Optional[int] = Field(default=None)
    iteration: Optional[int] = Field(default=None)
    stage: Optional[str] = Field(default=None)
    phase: Optional[str] = Field(default=None)

    # Tags (for filtering and categorization)
    tags: List[str] = Field(
        default_factory=list,
        description="List of string labels for categorization and filtering",
    )

    # Identity / Caching
    # These fields are crucial for Consist's Merkle DAG caching strategy, forming the "signature"
    # that determines run uniqueness and cache hits.
    config_hash: Optional[str] = Field(
        default=None,
        description="SHA256 hash of the canonicalized run configuration.",
    )
    git_hash: Optional[str] = Field(
        default=None,
        description="Git commit hash of the code version at the time of the run.",
    )
    input_hash: Optional[str] = Field(
        default=None,
        description="SHA256 hash derived from the provenance IDs of input artifacts.",
    )
    signature: Optional[str] = Field(
        default=None,
        description="Composite hash (SHA256) of code, config, and input hashes, defining the unique identity for caching.",
    )

    # Metadata
    meta: Dict[str, Any] = Field(default_factory=dict)

    # Timing
    started_at: Optional[datetime] = Field(
        default_factory=lambda: datetime.now(UTC),
    )
    ended_at: Optional[datetime] = Field(default=None)

    # Audit (record timestamps, distinct from execution timing)
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
    )
    updated_at: Optional[datetime] = Field(
        default_factory=lambda: datetime.now(UTC),
    )

    @field_validator("tags", mode="before")
    @classmethod
    def _coerce_tags(cls, value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(item) for item in parsed]
            return []
        if isinstance(value, (list, tuple)):
            return [str(item) for item in value]
        return list(value)

    @field_validator("meta", mode="before")
    @classmethod
    def _coerce_meta(cls, value: Any) -> Dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, str):
            parsed = json.loads(value)
            return dict(parsed) if isinstance(parsed, dict) else {}
        if isinstance(value, dict):
            return dict(value)
        return dict(value)

    @property
    def is_cache_hit(self) -> bool:
        """Whether this run was satisfied from a prior cached run."""
        return bool(isinstance(self.meta, dict) and self.meta.get("cache_hit"))

    @property
    def cache_source_run_id(self) -> Optional[str]:
        """The run ID of the upstream cache source, if recorded."""
        if not isinstance(self.meta, dict):
            return None
        val = self.meta.get("cache_source")
        return val if isinstance(val, str) and val else None

    @property
    def duration_seconds(self) -> Optional[float]:
        """
        Calculate the duration of the run in seconds.

        Returns the elapsed time between `started_at` and `ended_at` if both are set,
        otherwise returns None (e.g., if the run is still in progress).

        Returns
        -------
        Optional[float]
            The duration in seconds, or None if the run hasn't ended.
        """
        if self.ended_at and self.started_at:
            return (self.ended_at - self.started_at).total_seconds()
        return None

    @property
    def identity_summary(self) -> Dict[str, Any]:
        """
        Return a structured cache-identity breakdown for this run.

        The summary is intended for debugging cache behavior and explains the
        component hashes that form the run signature, plus identity-relevant
        metadata persisted on the run.

        Returns
        -------
        dict[str, Any]
            Cache-identity components and persisted identity metadata. Strict
            resolved-binding runs additionally include ``input_identity`` with
            the versioned hash mode, strict and ordinary input counts, and the
            frozen strict-binding digest.
        """
        meta = self.meta if isinstance(self.meta, dict) else {}
        identity_input_digests = meta.get("consist_hash_inputs")
        if not isinstance(identity_input_digests, dict):
            identity_input_digests = {}

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
        if not isinstance(config_payload, dict):
            config_payload = {}

        inputs = meta.get("inputs")
        if not isinstance(inputs, list):
            inputs = []

        summary = {
            "code_version": self.git_hash,
            "config": config_payload,
            "adapter": adapter,
            "identity_inputs": identity_input_digests,
            "run_fields": run_fields,
            "inputs": inputs,
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
        if isinstance(input_identity, dict):
            summary["input_identity"] = input_identity
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
        tracker: "Tracker",
        *,
        target_root: str | Path,
        source_root: str | Path | None = None,
        keys: Sequence[str] | None = None,
        preserve_existing: bool = True,
        on_missing: Literal["warn", "raise"] = "warn",
        db_fallback: Literal["never", "if_ingested"] = "if_ingested",
    ) -> "MaterializationResult":
        """
        Materialize this run's linked outputs through the supplied tracker.

        This is the convenience ``Run`` wrapper for
        ``Tracker.materialize_run_outputs(...)``. It restores historical bytes
        into ``target_root`` and returns the legacy aggregate
        ``MaterializationResult`` summary.

        Prefer ``hydrate_outputs(...)`` when you want per-key statuses,
        detached artifacts, or directly usable hydrated paths.

        Parameters
        ----------
        tracker : Tracker
            Tracker instance that owns the provenance database and path policy.
        target_root : str | Path
            Destination root for rematerialized outputs.
        source_root : str | Path | None, optional
            Optional alternate source root for filesystem-backed recovery.
        keys : Sequence[str] | None, optional
            Optional subset of output keys to restore.
        preserve_existing : bool, default True
            If True, keep existing destination paths in place.
        on_missing : {"warn", "raise"}, default "warn"
            Missing-source handling policy forwarded to the tracker.
        db_fallback : {"never", "if_ingested"}, default "if_ingested"
            DB fallback policy forwarded to the tracker.

        Returns
        -------
        MaterializationResult
            Aggregate summary of which outputs were restored, skipped, or failed.
        """
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
        tracker: "Tracker",
        *,
        target_root: str | Path,
        source_root: str | Path | None = None,
        keys: Sequence[str] | None = None,
        preserve_existing: bool = True,
        on_missing: Literal["warn", "raise"] = "warn",
        db_fallback: Literal["never", "if_ingested"] = "if_ingested",
    ) -> "HydratedRunOutputsResult":
        """
        Hydrate this run's linked outputs through the supplied tracker.

        This is the convenience ``Run`` wrapper for
        ``Tracker.hydrate_run_outputs(...)``. It restores historical bytes into
        ``target_root`` and returns a key-indexed result whose detached
        artifacts and paths can be used immediately in the new workspace.

        Parameters
        ----------
        tracker : Tracker
            Tracker instance that owns the provenance database and path policy.
        target_root : str | Path
            Destination root for hydrated outputs.
        source_root : str | Path | None, optional
            Optional alternate source root for filesystem-backed recovery.
        keys : Sequence[str] | None, optional
            Optional subset of output keys to restore.
        preserve_existing : bool, default True
            If True, keep existing destination paths in place.
        on_missing : {"warn", "raise"}, default "warn"
            Missing-source handling policy forwarded to the tracker.
        db_fallback : {"never", "if_ingested"}, default "if_ingested"
            DB fallback policy forwarded to the tracker.

        Returns
        -------
        HydratedRunOutputsResult
            Per-key hydration outcomes keyed by output name.
        """
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
    """
    Structured container for artifacts associated with a run.
    Allows dictionary-style access by artifact key.

    This model is in-memory only and is not persisted to the database.

    Usage:
        artifacts.outputs['persons'] -> Artifact(...)
        artifacts.inputs['config'] -> Artifact(...)
    """

    inputs: dict[str, Artifact] = Field(default_factory=dict)
    outputs: dict[str, Artifact] = Field(default_factory=dict)

    model_config = ConfigDict(arbitrary_types_allowed=True)


class RunResult(BaseModel):
    """
    Structured return value for Tracker.run/ScenarioContext.run.

    This model is in-memory only and is not persisted to the database.
    """

    run: Run
    outputs: dict[str, Artifact] = Field(default_factory=dict)
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

    def output_path(self, key: str, tracker: Optional["Tracker"] = None) -> Path:
        """
        Resolve a run output key to a filesystem path.

        Parameters
        ----------
        key : str
            Output artifact key to resolve.
        tracker : Optional[Tracker], optional
            Explicit tracker to use for URI resolution when the artifact is
            detached from tracker context.

        Returns
        -------
        Path
            Resolved filesystem path for the selected output artifact.
        """
        artifact = resolve_run_result_output(self, key=key)
        return artifact.as_path(tracker=tracker)


def resolve_run_result_output(
    run_result: RunResult, key: Optional[str] = None
) -> Artifact:
    """
    Resolve one output artifact from a RunResult.

    Parameters
    ----------
    run_result : RunResult
        The run result to select from.
    key : Optional[str], optional
        Output key to select. If omitted, exactly one output must exist.

    Returns
    -------
    Artifact
        Selected output artifact.

    Raises
    ------
    KeyError
        If ``key`` is provided but missing.
    ValueError
        If ``key`` is omitted and output selection is ambiguous.
    """
    output_keys = list(run_result.outputs.keys())
    available_keys = ", ".join(repr(item) for item in output_keys) or "<none>"

    if key is not None:
        if key in run_result.outputs:
            return run_result.outputs[key]
        raise KeyError(
            f"RunResult output key {key!r} was not found. Available keys: {available_keys}."
        )

    if len(output_keys) == 1:
        return run_result.outputs[output_keys[0]]

    if not output_keys:
        raise ValueError(
            "RunResult has no outputs. Select an explicit output with "
            "consist.ref(..., key='...')."
        )

    raise ValueError(
        "RunResult has multiple outputs. Select an explicit output with "
        "consist.ref(..., key='...'). "
        f"Available keys: {available_keys}."
    )


@dataclass(slots=True)
class _PendingOutputMetaEntry:
    """In-flight output metadata queued by ``set_output_meta(...)``."""

    meta: Dict[str, Any]
    merge: bool = True
    applied_artifact_ids: set[uuid.UUID] = field(default_factory=set)


class ConsistRecord(BaseModel):
    """
    A comprehensive record of a run, acting as the primary log entry.

    This class aggregates all information related to a single execution, including the
    run itself, its input and output artifacts, and a snapshot of the configuration used.
    It represents the "log as truth" by providing a complete, self-contained
    description of what happened during the run. Consist serializes this model to
    JSON in ``consist.json``; the database stores a summarized view
    (run row + artifact bindings + optional facets) for querying.

    Attributes:
        run (Run): The core `Run` object containing the execution's metadata and state.
        inputs (List[Artifact]): A list of all artifacts that served as inputs to the run.
        outputs (List[Artifact]): A list of all artifacts that were generated as outputs by the run.
        config (dict[str, object]): A snapshot of the configuration dictionary used for this run.
    """

    model_config = ConfigDict(validate_assignment=True, arbitrary_types_allowed=True)

    run: Run
    inputs: List[Artifact] = Field(default_factory=list)
    outputs: List[Artifact] = Field(default_factory=list)

    # Cache Detection State
    cached_run: Optional[Run] = None

    config: Dict[str, Any] = Field(default_factory=dict)

    # Optional, minimal, queryable config facet included in per-run `consist.json`.
    facet: Dict[str, Any] = Field(default_factory=dict)

    _pending_output_meta: Dict[str, _PendingOutputMetaEntry] = PrivateAttr(
        default_factory=dict
    )
    _pending_archive_policy: Any = PrivateAttr(default=None)

    def sync_output_artifact(
        self,
        artifact: Optional[Artifact] = None,
        *,
        key: Optional[str] = None,
        artifact_id: str | uuid.UUID | None = None,
        meta: Optional[Dict[str, Any]] = None,
        hints: Optional[Sequence[Observation]] = None,
        abs_path: Optional[str] = None,
        preserve_existing_abs_path: bool = False,
    ) -> List[Artifact]:
        """Replace matching recorded output ``Artifact`` instances with updated immutable copies."""
        matched: List[Artifact] = []
        updated_outputs: List[Artifact] = []
        effective_id = (
            artifact_id
            if artifact_id is not None
            else (artifact.id if (artifact is not None and key is None) else None)
        )
        effective_key = (
            key
            if key is not None
            else (
                artifact.key
                if (artifact is not None and effective_id is None)
                else None
            )
        )
        target_id = str(effective_id) if effective_id is not None else None

        for recorded in self.outputs:
            matches_key = effective_key is not None and recorded.key == effective_key
            matches_id = target_id is not None and str(recorded.id) == target_id
            if not (matches_key or matches_id):
                updated_outputs.append(recorded)
                continue

            bound_tracker = recorded.resolve_bound_tracker()
            candidate = artifact if artifact is not None else recorded
            if meta is not None:
                candidate = candidate.with_meta(meta, merge=False)
            if hints is not None:
                candidate = candidate.with_hints(hints)
            if abs_path is not None and (
                not preserve_existing_abs_path
                or candidate.locate(verify_on_stale=False) is None
            ):
                resolved_abs = Path(abs_path).expanduser().resolve()
                obs = Observation(
                    location=Location(
                        uri=resolved_abs.as_uri(),
                        member=candidate.member,
                    ),
                    identity=candidate.identity,
                    access_driver=candidate.driver,
                    basis="derived",
                    site="local",
                    observed_at_ns=time.time_ns(),
                )
                candidate = candidate.with_hints((*candidate.hints, obs))

            if bound_tracker is not None:
                candidate.bind_tracker(bound_tracker)
            matched.append(candidate)
            updated_outputs.append(candidate)

        if matched:
            self.outputs = updated_outputs
        return matched


__all__ = [
    "ArchivedOutputs",
    "CanonicalRunMetaField",
    "ConsistRecord",
    "CouplerSnapshotPhase",
    "Run",
    "RunArtifactLink",
    "RunArtifacts",
    "RunBindingInvocation",
    "RunCouplerSnapshot",
    "RunResult",
    "resolve_canonical_run_meta_field",
    "resolve_run_result_output",
]
