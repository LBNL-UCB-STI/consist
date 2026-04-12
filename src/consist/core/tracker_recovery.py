from __future__ import annotations

from pathlib import Path
from typing import Literal, Mapping, Optional, Sequence, TYPE_CHECKING, Union

from consist.core._tracker_service_base import _TrackerServiceBase
from consist.core.materialize import (
    fold_hydrated_run_outputs_result,
    materialize_artifacts,
    stage_artifact as stage_artifact_core,
    stage_inputs as stage_inputs_core,
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
        MaterializationResult,
        StagedInput,
        StagedInputsResult,
    )


class TrackerRecoveryService(_TrackerServiceBase):
    def materialize(
        self,
        artifact: Artifact,
        destination_path: Union[str, Path],
        *,
        on_missing: Literal["warn", "raise"] = "warn",
    ) -> Optional[str]:
        result = materialize_artifacts(
            tracker=self,
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
        return stage_artifact_core(
            self,
            artifact,
            Path(destination),
            mode=mode,
            overwrite=overwrite,
            validate_content_hash=validate_content_hash,
            allow_external_paths=allow_external_paths,
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
        normalized_destinations = {
            str(key): Path(destination)
            for key, destination in destinations_by_key.items()
        }
        return stage_inputs_core(
            self,
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
