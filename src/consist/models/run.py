import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime
from sqlalchemy import Column, JSON
from sqlmodel import Field, SQLModel
from consist.models.artifact import Artifact


class RunArtifactLink(SQLModel, table=True):
    """Link table tracking inputs and outputs for a Run."""

    __tablename__ = "run_artifact_link"

    run_id: str = Field(primary_key=True)
    artifact_id: uuid.UUID = Field(primary_key=True)

    direction: str  # "input" or "output"
    is_implicit: bool = Field(
        default=False, description="Was this auto-detected via lineage?"
    )


class Run(SQLModel, table=True):
    """Represents a single execution unit (Model Step or Workflow)."""

    __tablename__ = "run"

    id: str = Field(
        primary_key=True, description="Unique Run ID (e.g. 'asim_2010_iter2_uuid')"
    )
    parent_run_id: Optional[str] = Field(default=None, foreign_key="run.id", index=True)

    # State
    status: str = Field(default="running", index=True)  # running, completed, failed

    # Context
    model_name: str = Field(index=True)
    year: Optional[int] = Field(default=None, index=True)
    iteration: Optional[int] = Field(default=None, index=True)

    # Hashing (For Caching/Optimization)
    config_hash: Optional[str] = Field(index=True)
    git_hash: Optional[str]

    # Metadata (Tags, Hostname, Duration)
    meta: Dict[str, Any] = Field(default={}, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class ConsistRecord(SQLModel):
    """
    The 'Log is Truth' Aggregation.
    """

    run: Run
    inputs: List[Artifact] = []
    outputs: List[Artifact] = []

    # Configuration snapshot
    config: Dict[str, Any] = {}
