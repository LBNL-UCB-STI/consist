"""
Database models for runs and run-level snapshots in the Consist main database.

These tables form the core provenance ledger: runs, run↔artifact links, and
in-memory snapshot helpers used to serialize full run records.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Sequence

from pydantic import BaseModel, ConfigDict, Field as PydanticField

from sqlalchemy import Column, JSON, String
from sqlmodel import Field, SQLModel
from consist.models.artifact import Artifact, UUIDType

UTC = timezone.utc

if TYPE_CHECKING:
    from consist.core.materialize import MaterializationResult
    from consist.core.tracker import Tracker


_VALID_MATERIALIZE_ON_MISSING = {"warn", "raise"}
_VALID_MATERIALIZE_DB_FALLBACK = {"never", "if_ingested"}
CanonicalRunMetaField = Literal["stage", "phase"]


def _normalize_materialize_output_keys(
    keys: Sequence[str] | None,
) -> tuple[str, ...] | None:
    if keys is None:
        return None
    if isinstance(keys, (str, bytes)):
        raise TypeError(
            "keys must be a sequence of output-key strings, not a single str/bytes value."
        )

    normalized: list[str] = []
    for key in keys:
        if not isinstance(key, str):
            raise TypeError(
                "keys must contain only strings "
                f"(got {type(key).__name__!s} in materialize_outputs)."
            )
        normalized.append(key)
    return tuple(normalized)


def _validate_materialize_option(
    *,
    name: str,
    value: str,
    allowed: set[str],
) -> None:
    if value not in allowed:
        allowed_display = ", ".join(repr(item) for item in sorted(allowed))
        raise ValueError(f"{name} must be one of: {allowed_display}")


def resolve_canonical_run_meta_field(
    run: "Run", field: CanonicalRunMetaField
) -> Optional[str]:
    """
    Resolve a canonical run field with fallback to legacy mirrored metadata.

    Canonical ``Run`` columns take precedence. ``run.meta`` is only consulted for
    compatibility with older rows or snapshot payloads that predate the schema
    change.
    """
    canonical_value = getattr(run, field, None)
    if isinstance(canonical_value, str):
        return canonical_value

    meta = run.meta if isinstance(run.meta, dict) else {}
    meta_value = meta.get(field)
    return meta_value if isinstance(meta_value, str) else None


class RunArtifactLink(SQLModel, table=True):
    """
    Link table that records run inputs/outputs in the main Consist database.

    Each row connects a ``Run`` to an ``Artifact`` and records whether the artifact
    was an input or output. This enables lineage queries and run reconstruction in
    DuckDB without re-reading JSON snapshots.

    Attributes:
        run_id (str): The ID of the associated run.
        artifact_id (uuid.UUID): The ID of the associated artifact.
        direction (str): Specifies whether the artifact was an "input" or "output" for the run.
        is_implicit (bool): True if the artifact was automatically detected through lineage,
                            rather than explicitly logged. Defaults to False.
    """

    __tablename__ = "run_artifact_link"

    run_id: str = Field(primary_key=True, sa_column_kwargs={"autoincrement": False})
    artifact_id: uuid.UUID = Field(
        primary_key=True, sa_type=UUIDType, sa_column_kwargs={"autoincrement": False}
    )

    direction: str  # "input" or "output"
    is_implicit: bool = Field(default=False)


class Run(SQLModel, table=True):
    """
    Primary run table in the Consist database.

    Each run captures execution metadata (status, timing, identity hashes, tags)
    and links to artifacts through ``run_artifact_link``. Full configuration details
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
        meta (Dict[str, Any]): A flexible dictionary for storing arbitrary metadata (e.g., hostname).
        started_at (datetime): The timestamp when the run execution began.
        ended_at (Optional[datetime]): The timestamp when the run execution completed or failed.
        created_at (datetime): The timestamp when the run record was created.
        updated_at (datetime): The timestamp when the run record was last updated.
    """

    __tablename__ = "run"

    id: str = Field(primary_key=True, sa_column_kwargs={"autoincrement": False})
    parent_run_id: Optional[str] = Field(
        default=None,
        sa_column=Column(
            String,
            nullable=True,
            index=True,
        ),
    )

    # State
    status: str = Field(default="running", index=True)

    # Context
    model_name: str = Field(index=True)
    description: Optional[str] = Field(
        default=None,
        description="Human-readable description of the run's purpose or outcome",
    )
    year: Optional[int] = Field(default=None, index=True)
    iteration: Optional[int] = Field(default=None, index=True)
    stage: Optional[str] = Field(default=None, index=True)
    phase: Optional[str] = Field(default=None, index=True)

    # Tags (for filtering and categorization)
    # Note: DuckDB supports arrays natively. For SQLite compatibility, this would need JSON storage.
    tags: List[str] = Field(
        default_factory=list,
        sa_column=Column(JSON),  # Using JSON for broader DB compatibility
        description="List of string labels for categorization and filtering",
    )

    # Identity / Caching
    # These fields are crucial for Consist's Merkle DAG caching strategy, forming the "signature"
    # that determines run uniqueness and cache hits.
    config_hash: Optional[str] = Field(
        index=True, description="SHA256 hash of the canonicalized run configuration."
    )
    git_hash: Optional[str] = Field(
        description="Git commit hash of the code version at the time of the run."
    )
    input_hash: Optional[str] = Field(
        default=None,
        index=True,
        description="SHA256 hash derived from the provenance IDs of input artifacts.",
    )
    signature: Optional[str] = Field(
        default=None,
        index=True,
        description="Composite hash (SHA256) of code, config, and input hashes, defining the unique identity for caching.",
    )

    # Metadata
    # Uses SQLAlchemy's JSON type for efficient persistence of arbitrary JSON structures.
    meta: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))

    # Timing
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    ended_at: Optional[datetime] = Field(default=None)

    # Audit (record timestamps, distinct from execution timing)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

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
        }

        config_payload = meta.get("config")
        if not isinstance(config_payload, dict):
            config_payload = {}

        inputs = meta.get("inputs")
        if not isinstance(inputs, list):
            inputs = []

        return {
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
            },
            "code_identity": {
                "mode": meta.get("code_identity", "repo_git"),
                "extra_deps": meta.get("code_identity_extra_deps"),
            },
            "cache": {
                "mode": meta.get("cache_mode"),
                "hit": meta.get("cache_hit"),
                "epoch": meta.get("cache_epoch"),
                "version": meta.get("cache_version"),
            },
        }

    def __repr__(self):
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
        """
        normalized_keys = _normalize_materialize_output_keys(keys)
        _validate_materialize_option(
            name="on_missing",
            value=on_missing,
            allowed=_VALID_MATERIALIZE_ON_MISSING,
        )
        _validate_materialize_option(
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


class RunArtifacts(BaseModel):
    """
    Structured container for artifacts associated with a run.
    Allows dictionary-style access by artifact key.

    This model is in-memory only and is not persisted to the database.

    Usage:
        artifacts.outputs['persons'] -> Artifact(...)
        artifacts.inputs['config'] -> Artifact(...)
    """

    inputs: Dict[str, Artifact] = PydanticField(default_factory=dict)
    outputs: Dict[str, Artifact] = PydanticField(default_factory=dict)

    model_config = ConfigDict(arbitrary_types_allowed=True)


class RunResult(BaseModel):
    """
    Structured return value for Tracker.run/ScenarioContext.run.

    This model is in-memory only and is not persisted to the database.
    """

    run: Run
    outputs: Dict[str, Artifact] = PydanticField(default_factory=dict)
    cache_hit: bool = False

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @property
    def output(self) -> Optional[Artifact]:
        if not self.outputs:
            return None
        return next(iter(self.outputs.values()))

    def as_dict(self) -> Dict[str, Artifact]:
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


class ConsistRecord(SQLModel):
    """
    A comprehensive record of a run, acting as the primary log entry.

    This class aggregates all information related to a single execution, including the
    run itself, its input and output artifacts, and a snapshot of the configuration used.
    It represents the "log as truth" by providing a complete, self-contained
    description of what happened during the run. Consist serializes this model to
    JSON in ``consist_runs/<run_id>.json``; the database stores a summarized view
    (run row + artifact links + optional facets) for querying.

    Attributes:
        run (Run): The core `Run` object containing the execution's metadata and state.
        inputs (List[Artifact]): A list of all artifacts that served as inputs to the run.
        outputs (List[Artifact]): A list of all artifacts that were generated as outputs by the run.
        config (Dict[str, Any]): A snapshot of the configuration dictionary used for this run.
    """

    run: Run
    inputs: List[Artifact] = PydanticField(default_factory=list)
    outputs: List[Artifact] = PydanticField(default_factory=list)

    # Cache Detection State
    cached_run: Optional[Run] = None  # NEW: Stores the potential cache hit

    config: Dict[str, Any] = PydanticField(default_factory=dict)

    # Optional, minimal, queryable config facet (not persisted to Run table).
    # This is included in the per-run `consist.json` snapshot for inspection/debugging.
    facet: Dict[str, Any] = PydanticField(default_factory=dict)
