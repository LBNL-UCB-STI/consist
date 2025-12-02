import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime
from datetime import UTC
from sqlalchemy import Column, JSON
from sqlmodel import Field, SQLModel
from consist.models.artifact import Artifact


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

    run_id: str = Field(primary_key=True)
    artifact_id: uuid.UUID = Field(primary_key=True)

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
        year (Optional[int]): The simulation or data year, if applicable.
        iteration (Optional[int]): The iteration number, if applicable.
        config_hash (Optional[str]): A hash of the run's configuration, used for caching.
        git_hash (Optional[str]): The Git commit hash of the code version used for the run.
        meta (Dict[str, Any]): A flexible dictionary for storing arbitrary metadata (e.g., hostname, tags).
        created_at (datetime): The timestamp when the run was created.
        updated_at (datetime): The timestamp when the run was last updated.
    """

    __tablename__ = "run"

    id: str = Field(primary_key=True)
    parent_run_id: Optional[str] = Field(default=None, foreign_key="run.id", index=True)

    # State
    status: str = Field(default="running", index=True)

    # Context
    model_name: str = Field(index=True)
    year: Optional[int] = Field(default=None, index=True)
    iteration: Optional[int] = Field(default=None, index=True)

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
    )  # NEW
    signature: Optional[str] = Field(
        default=None,
        index=True,
        description="Composite hash (SHA256) of code, config, and input hashes, defining the unique identity for caching.",
    )  # NEW (The composite key)

    # Metadata
    # Uses SQLAlchemy's JSON type for efficient persistence of arbitrary JSON structures.
    meta: Dict[str, Any] = Field(default={}, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    def __repr__(self):
        status_icon = "🟢" if self.status == "completed" else "🔴" if self.status == "failed" else "🟡"
        return f"<{status_icon} Run id='{self.id}' model='{self.model_name}' status='{self.status}'>"


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
    inputs: List[Artifact] = []
    outputs: List[Artifact] = []

    # Cache Detection State
    cached_run: Optional[Run] = None  # NEW: Stores the potential cache hit

    config: Dict[str, Any] = {}
