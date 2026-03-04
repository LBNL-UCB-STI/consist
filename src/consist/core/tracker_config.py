from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict
from sqlmodel import SQLModel

from consist.models.run import Run

AccessMode = Literal["standard", "analysis", "read_only"]


class TrackerConfig(BaseModel):
    """
    Structured configuration for constructing ``Tracker`` instances.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    run_dir: Path
    db_path: Optional[str | Path] = None
    mounts: Optional[dict[str, str]] = None
    project_root: str = "."
    hashing_strategy: str = "full"
    cache_epoch: int = 1
    schemas: Optional[list[type[SQLModel]]] = None
    access_mode: AccessMode = "standard"
    run_subdir_fn: Optional[Callable[[Run], str]] = None
    allow_external_paths: Optional[bool] = None
    openlineage_enabled: bool = False
    openlineage_namespace: Optional[str] = None

    def to_init_kwargs(self) -> dict[str, Any]:
        """
        Return kwargs compatible with ``Tracker.__init__``.
        """
        return {
            "run_dir": self.run_dir,
            "db_path": self.db_path,
            "mounts": self.mounts,
            "project_root": self.project_root,
            "hashing_strategy": self.hashing_strategy,
            "cache_epoch": self.cache_epoch,
            "schemas": self.schemas,
            "access_mode": self.access_mode,
            "run_subdir_fn": self.run_subdir_fn,
            "allow_external_paths": self.allow_external_paths,
            "openlineage_enabled": self.openlineage_enabled,
            "openlineage_namespace": self.openlineage_namespace,
        }
