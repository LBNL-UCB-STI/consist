from __future__ import annotations

import itertools
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Literal, Optional

import pandas as pd

from consist.core._tracker_service_base import _TrackerServiceBase
from consist.models.artifact import Artifact, set_tracker_ref
from consist.models.run import ConsistRecord, Run, RunArtifacts, RunResult


class TrackerHistoryService(_TrackerServiceBase):
    def resolve_historical_path(self, artifact: Artifact, run: Run) -> Path:
        """
        Resolve the original filesystem path for an artifact from a prior run.

        Parameters
        ----------
        artifact : Artifact
            Artifact whose historical location should be resolved.
        run : Run
            Run that originally produced or consumed the artifact.

        Returns
        -------
        Path
            Resolved historical filesystem path.
        """
        if not run:
            return Path(self.resolve_uri(artifact.container_uri))

        old_dir = run.meta.get("_physical_run_dir")
        path_str = self.fs.resolve_historical_path(artifact.container_uri, old_dir)
        return Path(path_str)

    def get_run(self, run_id: str) -> Optional[Run]:
        """
        Retrieve a run by id.

        Parameters
        ----------
        run_id : str
            Run identifier.

        Returns
        -------
        Optional[Run]
            Matching run, or ``None`` if it does not exist.
        """
        if self.db:
            return self.db.get_run(run_id)
        return None

    def snapshot_db(
        self, dest_path: str | os.PathLike[str], checkpoint: bool = True
    ) -> Path:
        """
        Snapshot the provenance database to a destination path.

        Parameters
        ----------
        dest_path : str | os.PathLike[str]
            Destination path for the snapshot database file.
        checkpoint : bool, default True
            Whether to checkpoint the source database before copying.

        Returns
        -------
        Path
            Snapshot database path.
        """
        if self.db is None:
            raise RuntimeError("Database snapshot requires a configured database.")

        active_run_id = self.current_consist.run.id if self.current_consist else None
        last_completed_run_id: Optional[str] = None
        if (
            self._last_consist is not None
            and self._last_consist.run.status == "completed"
        ):
            last_completed_run_id = self._last_consist.run.id
        else:
            completed_runs = self.db.find_runs(status="completed", limit=1)
            if completed_runs:
                last_completed_run_id = completed_runs[0].id

        return self.db.snapshot_to(
            dest_path=dest_path,
            checkpoint=checkpoint,
            metadata={
                "run_id": active_run_id,
                "last_completed_run_id": last_completed_run_id,
                "cache_epoch": self._cache_epoch,
            },
        )

    def get_run_record(
        self, run_id: str, *, allow_missing: bool = False
    ) -> Optional[ConsistRecord]:
        """
        Load the JSON snapshot record for a historical run.

        Parameters
        ----------
        run_id : str
            Run identifier.
        allow_missing : bool, default False
            Return ``None`` instead of raising when the snapshot is absent.

        Returns
        -------
        Optional[ConsistRecord]
            Parsed run snapshot record.
        """
        run = self.get_run(run_id) if self.db else None
        snapshot_path = self._resolve_run_snapshot_path(run_id, run)
        if not snapshot_path.exists():
            if allow_missing:
                return None
            raise FileNotFoundError(
                f"Run snapshot not found at {snapshot_path!s} for run_id={run_id}."
            )
        try:
            return ConsistRecord.model_validate_json(
                snapshot_path.read_text(encoding="utf-8")
            )
        except Exception as exc:
            if allow_missing:
                return None
            raise ValueError(
                f"Failed to parse run snapshot at {snapshot_path!s} for run_id={run_id}."
            ) from exc

    def get_run_config(
        self, run_id: str, *, allow_missing: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Load the full config snapshot for a historical run.

        Parameters
        ----------
        run_id : str
            Run identifier.
        allow_missing : bool, default False
            Return ``None`` instead of raising when the snapshot is absent.

        Returns
        -------
        Optional[Dict[str, Any]]
            Stored config payload for the run.
        """
        record = self.get_run_record(run_id, allow_missing=allow_missing)
        if record is None:
            return None
        return record.config

    def get_config_bundle(
        self,
        run_id: str,
        *,
        adapter: str | None = None,
        role: str = "bundle",
        allow_missing: bool = False,
    ) -> Path | None:
        """
        Resolve a config artifact path for a run by role and optional adapter.

        Parameters
        ----------
        run_id : str
            Run identifier.
        adapter : Optional[str], optional
            Optional adapter name filter.
        role : str, default "bundle"
            Config artifact role to select.
        allow_missing : bool, default False
            Return ``None`` when the matching file is absent.

        Returns
        -------
        Path | None
            Resolved config artifact path.
        """
        artifacts = self.get_artifacts_for_run(run_id)
        input_artifacts = list(artifacts.inputs.values())

        matching: list[Artifact] = []
        for artifact in input_artifacts:
            meta = artifact.meta if isinstance(artifact.meta, dict) else {}
            if meta.get("config_role") == role:
                matching.append(artifact)

        run = self.get_run(run_id)
        run_adapter: str | None = None
        if run is not None and isinstance(run.meta, dict):
            candidate = run.meta.get("config_adapter")
            if isinstance(candidate, str) and candidate:
                run_adapter = candidate

        if adapter is not None:
            if run_adapter is not None and run_adapter != adapter:
                matching = []
            else:
                filtered: list[Artifact] = []
                for artifact in matching:
                    meta = artifact.meta if isinstance(artifact.meta, dict) else {}
                    artifact_adapters: list[str] = []
                    for key in ("config_adapter", "adapter"):
                        value = meta.get(key)
                        if isinstance(value, str) and value:
                            artifact_adapters.append(value)
                    if artifact_adapters:
                        if adapter in artifact_adapters:
                            filtered.append(artifact)
                    elif run_adapter == adapter:
                        filtered.append(artifact)
                matching = filtered

        if matching:
            selected = sorted(
                matching,
                key=lambda artifact: (
                    artifact.key,
                    artifact.created_at.isoformat() if artifact.created_at else "",
                    str(artifact.id),
                ),
            )[0]
            resolved = Path(self.resolve_uri(selected.container_uri))
            if resolved.exists():
                return resolved

            if allow_missing:
                return None
            raise FileNotFoundError(
                "Config artifact was found but the resolved file is missing for "
                f"run_id={run_id!r}, role={role!r}, key={selected.key!r}: {resolved!s}. "
                "Check path mounts or regenerate config artifacts for this run."
            )

        if allow_missing:
            return None
        adapter_hint = f", adapter={adapter!r}" if adapter is not None else ""
        raise FileNotFoundError(
            "No config artifact found for "
            f"run_id={run_id!r}, role={role!r}{adapter_hint}. "
            "Ensure config artifacts were logged with meta['config_role'] and, when "
            "adapter filtering is requested, run.meta['config_adapter'] and/or "
            "artifact.meta['config_adapter'|'adapter'] match."
        )

    def get_artifacts_for_run(self, run_id: str) -> RunArtifacts:
        """
        Retrieve input and output artifacts for a run.

        Parameters
        ----------
        run_id : str
            Run identifier.

        Returns
        -------
        RunArtifacts
            Inputs and outputs grouped by artifact key.
        """
        if not self.db:
            return RunArtifacts()

        current_run_id = self.current_consist.run.id if self.current_consist else None
        if run_id != current_run_id:
            cached = self._run_artifacts_cache.get(run_id)
            if cached is not None:
                return cached

        raw_list = self.db.get_artifacts_for_run(run_id)

        inputs = {}
        outputs = {}

        for artifact, direction in raw_list:
            if direction == "input":
                inputs[artifact.key] = artifact
            elif direction == "output":
                outputs[artifact.key] = artifact

        artifacts = RunArtifacts(inputs=inputs, outputs=outputs)
        for artifact in itertools.chain(inputs.values(), outputs.values()):
            set_tracker_ref(artifact, self._tracker)
        if run_id != current_run_id:
            self._run_artifacts_cache[run_id] = artifacts
            if len(self._run_artifacts_cache) > self._run_artifacts_cache_max_entries:
                self._run_artifacts_cache.pop(next(iter(self._run_artifacts_cache)))
        return artifacts

    def get_run_outputs(self, run_id: str) -> Dict[str, Artifact]:
        """
        Return output artifacts for a run, keyed by artifact key.
        """
        return self.get_artifacts_for_run(run_id).outputs

    def get_run_result(
        self,
        run_id: str,
        *,
        keys: Optional[Iterable[str]] = None,
        validate: Literal["lazy", "strict", "none"] = "lazy",
    ) -> RunResult:
        """
        Build a ``RunResult`` view for a historical run.

        Parameters
        ----------
        run_id : str
            Run identifier.
        keys : Optional[Iterable[str]], optional
            Optional subset of output keys to include.
        validate : {"lazy", "strict", "none"}, default "lazy"
            Output validation policy.

        Returns
        -------
        RunResult
            Historical run metadata plus selected outputs.
        """
        run = self.get_run(run_id)
        if run is None:
            raise KeyError(f"Run {run_id!r} was not found.")

        outputs = self.get_run_outputs(run_id)

        selected_outputs: Dict[str, Artifact]
        if keys is None:
            selected_outputs = dict(outputs)
        else:
            key_list = list(keys)
            if any(not isinstance(key, str) for key in key_list):
                raise ValueError("keys must contain only strings.")
            missing = sorted(key for key in key_list if key not in outputs)
            if missing:
                missing_str = ", ".join(repr(key) for key in missing)
                available = ", ".join(repr(key) for key in sorted(outputs)) or "<none>"
                raise KeyError(
                    f"Run {run_id!r} missing requested output keys: {missing_str}. "
                    f"Available keys: {available}."
                )
            selected_outputs = {key: outputs[key] for key in key_list}

        validation_policy = str(validate).lower()
        if validation_policy not in {"lazy", "strict", "none"}:
            raise ValueError("validate must be one of: 'lazy', 'strict', 'none'.")

        if validation_policy == "strict":
            missing_paths: list[str] = []
            for key, artifact in selected_outputs.items():
                if artifact.meta.get("is_ingested", False):
                    continue
                resolved = Path(self.resolve_uri(artifact.container_uri))
                if not resolved.exists():
                    missing_paths.append(f"{key!r} -> {resolved!s}")
            if missing_paths:
                details = "; ".join(missing_paths)
                raise FileNotFoundError(
                    f"Run {run_id!r} has missing output files: {details}"
                )

        cache_hit = (
            bool(run.meta.get("cache_hit")) if isinstance(run.meta, dict) else False
        )
        return RunResult(run=run, outputs=selected_outputs, cache_hit=cache_hit)

    def get_run_inputs(self, run_id: str) -> Dict[str, Artifact]:
        """
        Return input artifacts for a run, keyed by artifact key.
        """
        return self.get_artifacts_for_run(run_id).inputs

    def get_run_artifact(
        self,
        run_id: str,
        key: Optional[str] = None,
        key_contains: Optional[str] = None,
        direction: str = "output",
    ) -> Optional[Artifact]:
        """
        Retrieve a single artifact from a run by exact key or substring match.
        """
        record = self.get_artifacts_for_run(run_id)
        collection = record.outputs if direction == "output" else record.inputs
        if key and key in collection:
            return collection[key]
        if key_contains:
            for k, art in collection.items():
                if key_contains in k:
                    return art
        return next(iter(collection.values()), None)

    def load_run_output(self, run_id: str, key: str, **kwargs: Any) -> Any:
        """
        Load one output artifact from a historical run.
        """
        artifact = self.get_run_artifact(run_id, key=key, direction="output")
        if artifact is None:
            raise ValueError(
                f"No output artifact found for run_id={run_id!r} key={key!r}."
            )
        return self.load(artifact, **kwargs)

    def find_matching_run(
        self,
        config_hash: str,
        input_hash: str,
        git_hash: str,
        *,
        signature: Optional[str] = None,
    ) -> Optional[Run]:
        """
        Find a previously completed run matching the supplied identity hashes.
        """
        if self.db:
            if signature:
                matched = self.db.find_run_by_signature(signature)
                if matched is not None:
                    return matched
            return self.db.find_matching_run(config_hash, input_hash, git_hash)
        return None

    def find_recent_completed_runs_for_model(
        self, model_name: str, *, limit: int = 20
    ) -> list[Run]:
        """
        Return recent completed runs for a model, newest first.
        """
        if self.db:
            return self.db.find_recent_completed_runs_for_model(model_name, limit=limit)
        return []

    def history(
        self, limit: int = 10, tags: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Return recent runs as a Pandas DataFrame.
        """
        if self.db:
            return self.db.get_history(limit, tags)
        return pd.DataFrame()

    def load_input_bundle(self, run_id: str) -> dict[str, Artifact]:
        """
        Load output artifacts from a prior bundle run as reusable inputs.
        """
        run = self.get_run(run_id)
        if not run:
            raise ValueError(f"Input bundle run_id={run_id!r} not found.")

        outputs = self.get_artifacts_for_run(run_id).outputs
        if not outputs:
            raise ValueError(f"Input bundle run_id={run_id!r} has no output artifacts.")
        return outputs
