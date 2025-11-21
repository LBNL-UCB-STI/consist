import os
import json
from pathlib import Path
from typing import Dict, Optional, List, Any
from datetime import datetime
from uuid import uuid4
from contextlib import contextmanager

from sqlmodel import create_engine, Session, select, SQLModel

# Models
from consist.models.artifact import Artifact
from consist.models.run import Run, RunArtifactLink, ConsistRecord


class Tracker:
    def __init__(
            self,
            run_dir: Path,
            db_path: Optional[str] = None,
            mounts: Dict[str, str] = None
    ):
        """
        Args:
            run_dir: Where the `consist.json` log will be written.
            db_path: Path to DuckDB file (e.g. 'provenance.duckdb').
            mounts: Dictionary of {name: path} for path virtualization.
                    e.g. {'inputs': '/mnt/data'}
        """
        self.run_dir = Path(run_dir)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.mounts = mounts or {}

        # Database Setup (Optional, tolerant to missing DB)
        self.engine = None
        if db_path:
            # Using duckdb-engine for SQLAlchemy support
            self.engine = create_engine(f"duckdb:///{db_path}")
            # In production, we'd check/run Alembic migrations here
            SQLModel.metadata.create_all(self.engine)

        # In-Memory State (The Source of Truth)
        self.current_consist: Optional[ConsistRecord] = None

    @contextmanager
    def start_run(self, run_id: str, model: str, config: Dict[str, Any] = None, **kwargs):
        """
        Context manager for an execution block.
        Handles initialization, error catching, and status updates.
        """

        year = kwargs.pop("year", None)
        iteration = kwargs.pop("iteration", None)

        run = Run(
            id=run_id,
            model_name=model,
            year=year,           # Goes to optimized SQL column
            iteration=iteration, # Goes to optimized SQL column
            status="running",
            meta=kwargs,
            created_at=datetime.utcnow()
        )

        self.current_consist = ConsistRecord(
            run=run,
            config=config or {}
        )

        # Initial Flush
        self._flush_json()
        self._sync_run_to_db(run)

        try:
            yield self
            run.status = "completed"
        except Exception as e:
            run.status = "failed"
            run.meta["error"] = str(e)
            raise e
        finally:
            run.updated_at = datetime.utcnow()
            self._flush_json()
            self._sync_run_to_db(run)
            self.current_consist = None

    def log_artifact(self, path: str, key: str, direction: str = "output", **meta):
        """
        Log a file usage.
        resolves absolute paths to portable URIs based on Mounts.
        """
        if not self.current_consist:
            raise RuntimeError("Cannot log artifact outside of a run context.")

        # 1. Path Virtualization
        uri = self._virtualize_path(path)

        # 2. Driver Inference (Simple extension check for now)
        driver = Path(path).suffix.lstrip(".").lower() or "unknown"

        # 3. Create Object
        artifact = Artifact(
            key=key,
            uri=uri,
            driver=driver,
            run_id=self.current_consist.run.id if direction == "output" else None,
            meta=meta
        )

        # 4. Update Memory
        if direction == "input":
            self.current_consist.inputs.append(artifact)
        else:
            self.current_consist.outputs.append(artifact)

        # 5. Write
        self._flush_json()
        self._sync_artifact_to_db(artifact, direction)

        return artifact

    # --- Internals ---

    def _virtualize_path(self, path: str) -> str:
        """
        Converts /mnt/data/file.csv -> inputs://file.csv
        """
        abs_path = str(Path(path).resolve())

        # Check mounts longest-match first
        for name, root in sorted(self.mounts.items(), key=lambda x: len(x[1]), reverse=True):
            root_abs = str(Path(root).resolve())
            if abs_path.startswith(root_abs):
                rel = os.path.relpath(abs_path, root_abs)
                return f"{name}://{rel}"

        # Fallback: Relative to run_dir if possible, else strict absolute
        try:
            rel = os.path.relpath(abs_path, self.run_dir)
            if not rel.startswith(".."):
                return f"./{rel}"
        except ValueError:
            pass

        return abs_path

    def _flush_json(self):
        """Atomic write of the human-readable log."""
        if not self.current_consist:
            return

        # Note: Pydantic V2 uses model_dump_json()
        # We rely on a standard encoder for things like numpy (not implemented here yet)
        json_str = self.current_consist.model_dump_json(indent=2)

        target = self.run_dir / "consist.json"
        # Write temp then rename to ensure no corrupted files
        tmp = target.with_suffix(".tmp")
        with open(tmp, "w") as f:
            f.write(json_str)
        tmp.rename(target)

    def _sync_run_to_db(self, run: Run):
        """Sync Run status to DB. Tolerates DB failures."""
        if not self.engine: return
        try:
            with Session(self.engine) as session:
                session.merge(run)
                session.commit()
        except Exception as e:
            # We log but DO NOT CRASH. JSON is the truth; DB is just for optimization.
            print(f"[Consist Warning] Database sync failed: {e}")

    def _sync_artifact_to_db(self, artifact: Artifact, direction: str):
        """Sync Artifact and Link to DB."""
        if not self.engine or not self.current_consist: return
        try:
            with Session(self.engine) as session:
                # Merge artifact (create or update)
                db_artifact = session.merge(artifact)

                # Create Link
                link = RunArtifactLink(
                    run_id=self.current_consist.run.id,
                    artifact_id=db_artifact.id,  # Use DB ID
                    direction=direction
                )
                session.merge(link)
                session.commit()
        except Exception as e:
            print(f"[Consist Warning] Artifact sync failed: {e}")