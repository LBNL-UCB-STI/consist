from __future__ import annotations

from pathlib import Path
from typing import Literal, Mapping, Optional, Sequence, TYPE_CHECKING, Union

from consist.core._tracker_service_base import _TrackerServiceBase
from consist.core.materialize import (
    build_allowed_materialization_roots,
    fold_hydrated_run_outputs_result,
    materialize_artifacts,
    materialize_artifact as materialize_artifact_core,
    stage_artifact as stage_artifact_core,
    stage_inputs as stage_inputs_core,
    validate_allowed_materialization_destination,
)
from consist.core.materialize_options import (
    VALID_MATERIALIZE_DB_FALLBACK as _VALID_MATERIALIZE_DB_FALLBACK,
    VALID_MATERIALIZE_ON_MISSING as _VALID_MATERIALIZE_ON_MISSING,
    normalize_materialize_output_keys,
    validate_materialize_option,
)
from consist.models.artifact import Artifact

if TYPE_CHECKING:
    from consist.core.materialize import (
        HydratedRunOutputsResult,
        MaterializedArtifact,
        MaterializationResult,
        StagedInput,
        StagedInputsResult,
    )


class TrackerRecoveryService(_TrackerServiceBase):
    """Artifact staging and historical output recovery helpers for ``Tracker``.

    Notes
    -----
    ``Tracker`` owns the public API. This service centralizes the recovery
    implementation so staging and historical-output methods share validation,
    source resolution, and destination safety rules.
    """

    def materialize(
        self,
        artifact: Artifact,
        destination_path: Union[str, Path],
        *,
        on_missing: Literal["warn", "raise"] = "warn",
    ) -> Optional[str]:
        """Materialize one artifact to an exact destination path.

        Parameters
        ----------
        artifact : Artifact
            Artifact whose filesystem bytes should be restored.
        destination_path : str | Path
            Exact path that receives the materialized artifact.
        on_missing : {"warn", "raise"}, default "warn"
            Policy applied when no recoverable filesystem source is available.

        Returns
        -------
        str | None
            Destination path when materialization succeeds; otherwise ``None``.
        """
        result = materialize_artifacts(
            tracker=self._tracker,
            items=[(artifact, Path(destination_path))],
            on_missing=on_missing,
        )
        return result.get(artifact.key)

    def stage_artifact(
        self,
        artifact: Artifact,
        *,
        destination: str | Path,
        mode: Literal["copy", "hardlink", "symlink"] = "copy",
        overwrite: bool = False,
        validate_content_hash: Literal["never", "if-present", "always"] = "if-present",
        allow_external_paths: Optional[bool] = None,
    ) -> "StagedInput":
        """Stage one artifact for use outside a run lifecycle.

        Parameters
        ----------
        artifact : Artifact
            Resolved artifact to stage.
        destination : str | Path
            Requested local destination path.
        mode : {"copy", "hardlink", "symlink"}, default "copy"
            Filesystem operation used to create the staged input.
        overwrite : bool, default False
            Whether an existing destination may be replaced.
        validate_content_hash : {"never", "if-present", "always"}, default "if-present"
            Content-hash validation policy for staged bytes.
        allow_external_paths : bool | None, optional
            Override for the tracker's external-destination policy.

        Returns
        -------
        StagedInput
            Per-artifact staging outcome and resolved destination.
        """
        return stage_artifact_core(
            self._tracker,
            artifact,
            Path(destination),
            mode=mode,
            overwrite=overwrite,
            validate_content_hash=validate_content_hash,
            allow_external_paths=allow_external_paths,
        )

    def materialize_artifact(
        self,
        artifact: Artifact,
        *,
        target_root: str | Path,
        source_root: str | Path | None = None,
        preserve_existing: bool = True,
        on_missing: Literal["warn", "raise"] = "warn",
        validate_content_hash: Literal["never", "if-present", "always"] = "if-present",
        allow_external_paths: Optional[bool] = None,
    ) -> "MaterializedArtifact":
        """Materialize one artifact below a caller-selected target root.

        Parameters
        ----------
        artifact : Artifact
            Historical artifact to recover.
        target_root : str | Path
            Root beneath which Consist derives the artifact's canonical path.
        source_root : str | Path | None, optional
            Preferred source root before historical and recovery-root probes.
        preserve_existing : bool, default True
            Whether an existing target may be retained.
        on_missing : {"warn", "raise"}, default "warn"
            Policy for unavailable source bytes.
        validate_content_hash : {"never", "if-present", "always"}, default "if-present"
            Content-hash validation policy for materialized bytes.
        allow_external_paths : bool | None, optional
            Override for the tracker's external-destination policy.

        Returns
        -------
        MaterializedArtifact
            Keyed outcome containing the destination, status, and diagnostics.

        Raises
        ------
        TypeError
            If ``artifact`` is not an :class:`Artifact`.
        ValueError
            If an option is invalid or the destination is disallowed.
        """
        if not isinstance(artifact, Artifact):
            raise TypeError("artifact must be an Artifact instance.")

        validate_materialize_option(
            name="on_missing",
            value=on_missing,
            allowed=_VALID_MATERIALIZE_ON_MISSING,
        )

        allow_external = (
            self.allow_external_paths
            if allow_external_paths is None
            else allow_external_paths
        )
        allowed_roots = build_allowed_materialization_roots(
            run_dir=self.run_dir,
            mounts=self.mounts,
            allow_external_paths=allow_external,
        )
        destination_root = Path(target_root).resolve()
        validate_allowed_materialization_destination(destination_root, allowed_roots)
        source_root_override = (
            Path(source_root).resolve() if source_root is not None else None
        )
        return materialize_artifact_core(
            self._tracker,
            artifact,
            target_root=destination_root,
            source_root=source_root_override,
            allowed_base=allowed_roots,
            preserve_existing=preserve_existing,
            on_missing=on_missing,
            validate_content_hash=validate_content_hash,
        )

    def stage_inputs(
        self,
        inputs_by_key: Mapping[str, Artifact],
        *,
        destinations_by_key: Mapping[str, str | Path],
        mode: Literal["copy", "hardlink", "symlink"] = "copy",
        overwrite: bool = False,
        validate_content_hash: Literal["never", "if-present", "always"] = "if-present",
        allow_external_paths: Optional[bool] = None,
    ) -> "StagedInputsResult":
        """Stage multiple keyed artifacts to exact destination paths.

        Parameters
        ----------
        inputs_by_key : Mapping[str, Artifact]
            Artifacts keyed by the destination mapping keys.
        destinations_by_key : Mapping[str, str | Path]
            Exact local destination for each input key.
        mode : {"copy", "hardlink", "symlink"}, default "copy"
            Filesystem operation used for staging.
        overwrite : bool, default False
            Whether existing destinations may be replaced.
        validate_content_hash : {"never", "if-present", "always"}, default "if-present"
            Content-hash validation policy for staged bytes.
        allow_external_paths : bool | None, optional
            Override for the tracker's external-destination policy.

        Returns
        -------
        StagedInputsResult
            Per-key staging outcomes in destination mapping order.
        """
        normalized_destinations = {
            str(key): Path(destination)
            for key, destination in destinations_by_key.items()
        }
        return stage_inputs_core(
            self._tracker,
            inputs_by_key,
            normalized_destinations,
            mode=mode,
            overwrite=overwrite,
            validate_content_hash=validate_content_hash,
            allow_external_paths=allow_external_paths,
        )

    def materialize_run_outputs(
        self,
        run_id: str,
        *,
        target_root: str | Path,
        source_root: str | Path | None = None,
        keys: Sequence[str] | None = None,
        preserve_existing: bool = True,
        on_missing: Literal["warn", "raise"] = "warn",
        db_fallback: Literal["never", "if_ingested"] = "if_ingested",
    ) -> "MaterializationResult":
        """Materialize selected outputs from a completed historical run.

        Parameters
        ----------
        run_id : str
            Historical run whose outputs should be restored.
        target_root : str | Path
            Root beneath which canonical output paths are recreated.
        source_root : str | Path | None, optional
            Preferred source root before historical and recovery-root probes.
        keys : Sequence[str] | None, optional
            Selected output keys; ``None`` selects all outputs.
        preserve_existing : bool, default True
            Whether existing target files may be retained.
        on_missing : {"warn", "raise"}, default "warn"
            Policy for outputs whose bytes are unavailable.
        db_fallback : {"never", "if_ingested"}, default "if_ingested"
            Whether eligible ingested tabular outputs may be rebuilt from the DB.

        Returns
        -------
        MaterializationResult
            Compatibility aggregate grouped by materialization outcome.
        """
        return fold_hydrated_run_outputs_result(
            self._tracker.hydrate_run_outputs(
                run_id,
                target_root=target_root,
                source_root=source_root,
                keys=keys,
                preserve_existing=preserve_existing,
                on_missing=on_missing,
                db_fallback=db_fallback,
            )
        )

    def hydrate_run_outputs(
        self,
        run_id: str,
        *,
        target_root: str | Path,
        source_root: str | Path | None = None,
        keys: Sequence[str] | None = None,
        preserve_existing: bool = True,
        on_missing: Literal["warn", "raise"] = "warn",
        db_fallback: Literal["never", "if_ingested"] = "if_ingested",
    ) -> "HydratedRunOutputsResult":
        """Hydrate selected historical outputs with per-key recovery outcomes.

        Parameters
        ----------
        run_id : str
            Historical run whose outputs should be hydrated.
        target_root : str | Path
            Root beneath which canonical output paths are recreated.
        source_root : str | Path | None, optional
            Preferred source root before historical and recovery-root probes.
        keys : Sequence[str] | None, optional
            Selected output keys; ``None`` selects all outputs.
        preserve_existing : bool, default True
            Whether existing targets may be retained.
        on_missing : {"warn", "raise"}, default "warn"
            Policy for unavailable source bytes.
        db_fallback : {"never", "if_ingested"}, default "if_ingested"
            Whether eligible ingested tabular outputs may be rebuilt from the DB.

        Returns
        -------
        HydratedRunOutputsResult
            Mapping of output keys to detached artifacts, paths, and statuses.

        Raises
        ------
        KeyError
            If ``run_id`` is not found.
        RuntimeError
            If the tracker has no metadata database.
        """
        normalized_keys = normalize_materialize_output_keys(
            keys,
            caller="hydrate_run_outputs",
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

        if self.db is None:
            raise RuntimeError(
                "Cannot materialize run outputs: tracker has no database configured."
            )

        run = self.get_run(run_id)
        if run is None:
            raise KeyError(f"Run {run_id!r} was not found.")

        from consist.core import materialize as materialize_core

        destination_root = Path(target_root).resolve()
        allowed_roots = materialize_core.build_allowed_materialization_roots(
            run_dir=self.run_dir,
            mounts=self.mounts,
            allow_external_paths=self.allow_external_paths,
        )
        materialize_core.validate_allowed_materialization_destination(
            destination_root, allowed_roots
        )

        source_root_override = (
            Path(source_root).resolve() if source_root is not None else None
        )

        from consist.core import tracker as tracker_module

        return tracker_module.hydrate_run_outputs_core(
            tracker=self._tracker,
            run=run,
            target_root=destination_root,
            source_root=source_root_override,
            keys=normalized_keys,
            allowed_base=allowed_roots,
            preserve_existing=preserve_existing,
            on_missing=on_missing,
            db_fallback=db_fallback,
        )

    def hydrate_run_outputs_to_destinations(
        self,
        run_id: str,
        *,
        destinations_by_key: Mapping[str, str | Path],
        source_root: str | Path | None = None,
        preserve_existing: bool = True,
        on_missing: Literal["warn", "raise"] = "warn",
        db_fallback: Literal["never", "if_ingested"] = "if_ingested",
    ) -> "HydratedRunOutputsResult":
        """Hydrate historical outputs to caller-selected exact destinations.

        Parameters
        ----------
        run_id : str
            Historical run whose outputs should be hydrated.
        destinations_by_key : Mapping[str, str | Path]
            Exact target path for every requested output key.
        source_root : str | Path | None, optional
            Preferred source root before historical and recovery-root probes.
        preserve_existing : bool, default True
            Whether existing destinations may be retained.
        on_missing : {"warn", "raise"}, default "warn"
            Policy for unavailable source bytes.
        db_fallback : {"never", "if_ingested"}, default "if_ingested"
            Whether eligible ingested tabular outputs may be rebuilt from the DB.

        Returns
        -------
        HydratedRunOutputsResult
            Mapping of requested keys to hydration outcomes.

        Raises
        ------
        TypeError
            If ``destinations_by_key`` is not a string-keyed path mapping.
        KeyError
            If ``run_id`` is not found.
        RuntimeError
            If the tracker has no metadata database.
        """
        if not isinstance(destinations_by_key, Mapping):
            raise TypeError(
                "destinations_by_key must be a mapping of output keys to paths."
            )

        requested_destinations: dict[str, Path] = {}
        for key, destination in destinations_by_key.items():
            if not isinstance(key, str):
                raise TypeError(
                    "destinations_by_key must contain only string output keys."
                )
            if not isinstance(destination, (str, Path)):
                raise TypeError(
                    "destinations_by_key values must be str or Path instances."
                )
            requested_destinations[key] = Path(destination).expanduser().absolute()

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

        if self.db is None:
            raise RuntimeError(
                "Cannot materialize run outputs: tracker has no database configured."
            )

        run = self.get_run(run_id)
        if run is None:
            raise KeyError(f"Run {run_id!r} was not found.")

        requested_outputs = self.get_run_outputs(run_id)
        strict_tree_keys = {
            key
            for key, artifact in requested_outputs.items()
            if isinstance(artifact.meta, dict)
            and (
                artifact.meta.get("directory_artifact") is True
                or artifact.meta.get("file_bundle_artifact") is True
            )
        }
        normalized_destinations = {
            key: (destination if key in strict_tree_keys else destination.resolve())
            for key, destination in requested_destinations.items()
        }

        from consist.core import materialize as materialize_core

        allowed_roots = materialize_core.build_allowed_materialization_roots(
            run_dir=self.run_dir,
            mounts=self.mounts,
            allow_external_paths=self.allow_external_paths,
        )
        for destination in normalized_destinations.values():
            materialize_core.validate_allowed_materialization_destination(
                destination, allowed_roots
            )

        source_root_override = None
        if source_root is not None:
            source_root_path = Path(source_root).expanduser().absolute()
            source_root_override = (
                source_root_path
                if strict_tree_keys and db_fallback == "never"
                else source_root_path.resolve()
            )

        from consist.core import tracker as tracker_module

        return tracker_module.hydrate_run_outputs_to_destinations_core(
            tracker=self._tracker,
            run=run,
            destinations_by_key=normalized_destinations,
            source_root=source_root_override,
            allowed_base=allowed_roots,
            preserve_existing=preserve_existing,
            on_missing=on_missing,
            db_fallback=db_fallback,
        )
