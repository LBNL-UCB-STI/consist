"""Internal artifact logging coordination for ``Tracker``.

This module encapsulates ordering-sensitive artifact logging behavior:
artifact creation/reuse, cache-hit demotion ergonomics, run-state mutation,
persistence calls, and optional facet/schema profiling side effects.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Literal, Optional, TYPE_CHECKING, Type, Union, cast

from sqlmodel import SQLModel

from consist.core.cache_output_logging import (
    maybe_return_cached_output_or_demote_cache_hit,
)
from consist.models.artifact import Artifact, set_tracker_ref
from consist.types import ArtifactRef, FacetLike, HasFacetSchemaVersion

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


class ArtifactLoggingCoordinator:
    """Coordinate artifact logging side effects for active tracker runs."""

    def __init__(self, tracker: "Tracker") -> None:
        self._tracker = tracker

    def _sync_profile_label(
        self,
        *,
        artifact: Artifact,
        direction: str,
    ) -> str:
        if artifact.driver == "h5_table":
            return f"log_artifact:h5_table:{direction}"
        return f"log_artifact:{direction}"

    def log_artifact(
        self,
        path: ArtifactRef,
        key: Optional[str] = None,
        direction: str = "output",
        schema: Optional[Type[SQLModel]] = None,
        driver: Optional[str] = None,
        table_path: Optional[str] = None,
        array_path: Optional[str] = None,
        content_hash: Optional[str] = None,
        force_hash_override: bool = False,
        validate_content_hash: bool = False,
        reuse_if_unchanged: bool = False,
        reuse_scope: Literal["same_uri", "any_uri"] = "same_uri",
        profile_file_schema: bool | Literal["if_changed"] | None = None,
        file_schema_sample_rows: Optional[int] = None,
        facet: Optional[FacetLike] = None,
        facet_schema_version: Optional[Union[str, int]] = None,
        facet_index: bool = False,
        **meta: Any,
    ) -> Artifact:
        """Log one artifact while preserving tracker run-state invariants.

        Parameters
        ----------
        path : ArtifactRef
            Artifact reference to log.
        key : str | None, optional
            Artifact key override.
        direction : str, default "output"
            Link direction relative to active run ("input" or "output").
        schema : type[SQLModel] | None, optional
            Optional user-provided schema model.
        driver : str | None, optional
            Explicit driver override.
        table_path : str | None, optional
            Optional table path for container-backed artifacts.
        array_path : str | None, optional
            Optional array path for array/container-backed artifacts.
        content_hash : str | None, optional
            Optional precomputed content hash override.
        force_hash_override : bool, default False
            Whether mismatched content hash overrides are forced.
        validate_content_hash : bool, default False
            Whether provided content hash is validated against on-disk content.
        reuse_if_unchanged : bool, default False
            Deprecated for outputs. A fresh output artifact row is always created; identical
            bytes share `content_id`. Setting this on outputs emits a warning.
        reuse_scope : {"same_uri", "any_uri"}, default "same_uri"
            Deprecated for outputs. `any_uri` is ignored; deduplication is governed by `content_id`.
        profile_file_schema : bool | Literal["if_changed"] | None, optional
            Controls automatic file schema profiling behavior.
        file_schema_sample_rows : int | None, optional
            Sample size used for schema profiling.
        facet : FacetLike | None, optional
            Optional artifact facet payload.
        facet_schema_version : str | int | None, optional
            Optional artifact facet schema version.
        facet_index : bool, default False
            Whether artifact facet key/value rows are indexed.
        **meta : Any
            Additional artifact metadata.

        Returns
        -------
        Artifact
            Logged artifact record.

        Notes
        -----
        Persistence order is deliberate: mutate in-memory run state first, flush
        JSON snapshot second, sync artifact/link rows third, then persist optional
        facet/schema metadata.
        """
        tracker = self._tracker
        tracker._ensure_write_provenance()

        if not tracker.current_consist:
            raise RuntimeError("Cannot log artifact: no active run.")

        if direction == "output" and tracker.is_cached:
            cached = maybe_return_cached_output_or_demote_cache_hit(
                tracker=tracker,
                path=path,
                key=key,
            )
            if cached is not None:
                if tracker._active_coupler is not None and cached.key:
                    tracker._active_coupler.set(cached.key, cached)
                return cached

        run_id = tracker.current_consist.run.id if direction == "output" else None

        artifact_obj = tracker.artifacts.create_artifact(
            path,
            run_id,
            key,
            direction,
            schema,
            driver,
            table_path=table_path,
            array_path=array_path,
            content_hash=content_hash,
            force_hash_override=force_hash_override,
            validate_content_hash=validate_content_hash,
            reuse_if_unchanged=reuse_if_unchanged,
            reuse_scope=reuse_scope,
            **meta,
        )

        resolved_artifact_facet: Optional[Dict[str, Any]] = None
        prepared_artifact_facet_bundle = None
        artifact_facet_payload: Optional[FacetLike] = facet
        if artifact_facet_payload is None and artifact_obj.key:
            artifact_facet_payload = (
                tracker._parse_artifact_facet_from_registered_parsers(artifact_obj.key)
            )
        if artifact_facet_payload is not None:
            resolved_artifact_facet = tracker.artifact_facets.resolve_facet_dict(
                artifact_facet_payload
            )
        if resolved_artifact_facet is not None:
            schema_version = facet_schema_version
            if schema_version is None and isinstance(
                artifact_facet_payload, HasFacetSchemaVersion
            ):
                schema_version = artifact_facet_payload.facet_schema_version
            prepared_artifact_facet_bundle = (
                tracker.artifact_facets.prepare_facet_bundle(
                    artifact=artifact_obj,
                    namespace=tracker.current_consist.run.model_name,
                    facet_dict=resolved_artifact_facet,
                    schema_name=tracker.artifact_facets.infer_schema_name(
                        artifact_facet_payload
                    ),
                    schema_version=schema_version,
                    index_kv=facet_index,
                )
            )

        if isinstance(path, Artifact) and direction == "output":
            producing_run_id = artifact_obj.run_id
            if producing_run_id is None:
                artifact_obj.run_id = run_id
            elif producing_run_id != run_id:
                logging.warning(
                    "[Consist] log_artifact received an Artifact with run_id=%s but is logging it as output of run_id=%s (artifact key=%s id=%s).",
                    producing_run_id,
                    run_id,
                    getattr(artifact_obj, "key", None),
                    getattr(artifact_obj, "id", None),
                )

        run_ctx = tracker.current_consist.run
        inherited_fields = {
            "year": run_ctx.year,
            "iteration": run_ctx.iteration,
            "tags": run_ctx.tags or [],
        }
        if artifact_obj.meta is None:
            artifact_obj.meta = {}
        for inherited_key, inherited_value in inherited_fields.items():
            if inherited_value is not None and inherited_key not in artifact_obj.meta:
                artifact_obj.meta[inherited_key] = inherited_value

        set_tracker_ref(artifact_obj, tracker)
        if direction == "input":
            tracker.current_consist.inputs.append(artifact_obj)
        else:
            tracker.current_consist.outputs.append(artifact_obj)
            if tracker._active_coupler is not None and artifact_obj.key:
                tracker._active_coupler.set(artifact_obj.key, artifact_obj)

        # Preserve dual-write ordering so snapshot state reflects artifact links
        # before DB synchronization side effects occur.
        tracker._flush_json()
        if prepared_artifact_facet_bundle is not None:
            tracker.persistence.sync_artifact_with_facet_bundle(
                artifact_obj,
                direction,
                facet=prepared_artifact_facet_bundle.facet,
                meta_updates=prepared_artifact_facet_bundle.meta_updates,
                kv_rows=prepared_artifact_facet_bundle.kv_rows,
            )
        else:
            tracker._sync_artifact_to_db(
                artifact_obj,
                direction,
                profile_label=self._sync_profile_label(
                    artifact=artifact_obj,
                    direction=direction,
                ),
            )

        profile_mode = (
            tracker.settings.schema_profile_enabled
            if profile_file_schema is None
            else profile_file_schema
        )
        sample_rows = (
            tracker.settings.schema_sample_rows
            if file_schema_sample_rows is None
            else file_schema_sample_rows
        )
        if (
            profile_mode
            and artifact_obj.is_tabular
            and tracker.current_consist is not None
        ):
            try:
                if isinstance(artifact_obj.meta, dict) and artifact_obj.meta.get(
                    "schema_id"
                ):
                    return artifact_obj
                resolved_path = artifact_obj.abs_path or tracker.resolve_uri(
                    artifact_obj.container_uri
                )
                if resolved_path:
                    resolved_driver = artifact_obj.driver
                    if resolved_driver not in ("csv", "parquet", "h5_table"):
                        return artifact_obj
                    tracker.artifact_schemas.profile_file_artifact(
                        artifact=artifact_obj,
                        run=tracker.current_consist.run,
                        resolved_path=str(resolved_path),
                        driver=cast(
                            Literal["csv", "parquet", "h5_table"], resolved_driver
                        ),
                        sample_rows=sample_rows,
                        source="file",
                        reuse_if_unchanged=profile_mode == "if_changed",
                    )
            except FileNotFoundError:
                logging.warning(
                    "[Consist] File schema capture skipped; file not found: %s",
                    artifact_obj.container_uri,
                )

        if (
            schema is not None
            and artifact_obj.is_tabular
            and tracker.current_consist is not None
        ):
            try:
                tracker.artifact_schemas.profile_user_provided_schema(
                    artifact=artifact_obj,
                    run=tracker.current_consist.run,
                    schema_model=schema,
                    source="user_provided",
                )
            except Exception as exc:
                logging.warning(
                    "[Consist] Failed to store user-provided schema for artifact=%s: %s",
                    getattr(artifact_obj, "key", None),
                    exc,
                )

        return artifact_obj
