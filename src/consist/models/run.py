from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field as PydanticField

from sqlalchemy import Column, JSON, String
from sqlmodel import Field, SQLModel
from consist.models.artifact import Artifact, UUIDType

UTC = timezone.utc


class RunArtifactLink(SQLModel, table=True):
    """
    A link table that represents the many-to-many relationship between Runs and Artifacts.

    This table tracks which artifacts (files or data) were used as inputs
    and which were generated as outputs for a specific run.

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
    Represents a single, trackable execution unit, such as a model step or a data processing workflow.

    Each run captures the context, configuration, and state of a specific execution,
    allowing for reproducibility and observability.

    Attributes:
        id (str): A unique identifier for the run, often combining model name, year, and a UUID.
        parent_run_id (Optional[str]): The ID of the parent run, if this is a nested execution.
        status (str): The current state of the run (e.g., "running", "completed", "failed").
        model_name (str): The name of the model or workflow being executed.
        description (Optional[str]): Human-readable description of the run's purpose or outcome.
        year (Optional[int]): The simulation or data year, if applicable.
        iteration (Optional[int]): The iteration number, if applicable.
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

    def __repr__(self):
        status_icon = (
            "🟢"
            if self.status == "completed"
            else "🔴"
            if self.status == "failed"
            else "🟡"
        )
        return f"<{status_icon} Run id='{self.id}' model='{self.model_name}' status='{self.status}'>"


class RunArtifacts(BaseModel):
    """
    Structured container for artifacts associated with a run.
    Allows dictionary-style access by artifact key.

    Usage:
        artifacts.outputs['persons'] -> Artifact(...)
        artifacts.inputs['config'] -> Artifact(...)
    """

    inputs: Dict[str, Artifact] = PydanticField(default_factory=dict)
    outputs: Dict[str, Artifact] = PydanticField(default_factory=dict)

    class Config:
        arbitrary_types_allowed = True


class RunResult(BaseModel):
    """
    Structured return value for Tracker.run/ScenarioContext.run.
    """

    run: Run
    outputs: Dict[str, Artifact] = PydanticField(default_factory=dict)
    cache_hit: bool = False

    class Config:
        arbitrary_types_allowed = True

    @property
    def output(self) -> Optional[Artifact]:
        if not self.outputs:
            return None
        return next(iter(self.outputs.values()))

    def as_dict(self) -> Dict[str, Artifact]:
        return dict(self.outputs)


class ConsistRecord(SQLModel):
    """
    A comprehensive record of a run, acting as the primary log entry.

    This class aggregates all information related to a single execution, including the
    run itself, its input and output artifacts, and a snapshot of the configuration used.
    It represents the "log as truth" by providing a complete, self-contained
    description of what happened during the run.

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
