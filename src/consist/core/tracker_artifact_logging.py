"""Internal artifact logging coordination for ``Tracker``.

This module encapsulates ordering-sensitive artifact logging behavior:
artifact creation/reuse, cache-hit demotion ergonomics, run-state mutation,
persistence calls, and optional facet/schema profiling side effects.
"""

from __future__ import annotations

from collections.abc import Mapping
import logging
import uuid
from pathlib import Path
from typing import Any, Dict, Literal, Optional, TYPE_CHECKING, Union

from pydantic import BaseModel

from consist.core.cache_output_logging import (
    maybe_return_cached_output_or_demote_cache_hit,
)
from consist.core._performance_attribution import _track_begin_run_phase
from consist.core.directory_artifacts import (
    build_directory_manifest,
    build_shapefile_bundle_manifest,
)
from consist.digest import Digest
from consist.models.artifact import Artifact
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
        if artifact.driver in {"h5_table", "gtfs"}:
            return f"log_artifact:{artifact.driver}:{direction}"
        return f"log_artifact:{direction}"

    def prepare_artifact_facet_bundle(
        self,
        *,
        artifact: Artifact,
        facet: Optional[FacetLike],
        facet_schema_version: Optional[Union[str, int]],
        facet_index: bool,
    ) -> Any:
        """Prepare a persistable artifact facet bundle without mutating run state."""
        tracker = self._tracker
        resolved_artifact_facet: Optional[Dict[str, Any]] = None
        artifact_facet_payload: Optional[FacetLike] = facet
        if artifact_facet_payload is None and artifact.key:
            artifact_facet_payload = (
                tracker._parse_artifact_facet_from_registered_parsers(artifact.key)
            )
        if artifact_facet_payload is not None:
            resolved_artifact_facet = tracker.artifact_facets.resolve_facet_dict(
                artifact_facet_payload
            )
        if resolved_artifact_facet is None:
            return None

        schema_version = facet_schema_version
        if schema_version is None and isinstance(
            artifact_facet_payload, HasFacetSchemaVersion
        ):
            schema_version = artifact_facet_payload.facet_schema_version
        return tracker.artifact_facets.prepare_facet_bundle(
            artifact=artifact,
            namespace=tracker.current_consist.run.model_name
            if tracker.current_consist
            else None,
            facet_dict=resolved_artifact_facet,
            schema_name=tracker.artifact_facets.infer_schema_name(
                artifact_facet_payload
            ),
            schema_version=schema_version,
            index_kv=facet_index,
        )

    def apply_inherited_run_metadata(self, artifact: Artifact) -> Artifact:
        """Mirror standard run metadata onto a newly logged artifact."""
        tracker = self._tracker
        if tracker.current_consist is None:
            return artifact
        run_ctx = tracker.current_consist.run
        inherited_fields = {
            "year": run_ctx.year,
            "iteration": run_ctx.iteration,
            "tags": run_ctx.tags or [],
        }
        updates: dict[str, Any] = {}
        for inherited_key, inherited_value in inherited_fields.items():
            if inherited_value is not None and inherited_key not in artifact.meta:
                updates[inherited_key] = inherited_value
        if not updates:
            return artifact
        return artifact.with_meta(updates, merge=True)

    def attach_logged_artifact(
        self,
        *,
        artifact: Artifact,
        direction: str,
        register_with_coupler: bool,
    ) -> Artifact:
        """Attach a logged artifact to the active run state and optional coupler."""
        tracker = self._tracker
        bound = artifact.with_resolver(tracker)
        if tracker.current_consist is None:
            return bound
        if direction == "input":
            tracker.current_consist.inputs.append(bound)
            return bound
        tracker.current_consist.outputs.append(bound)
        if register_with_coupler and tracker._active_coupler is not None and bound.key:
            tracker._active_coupler.set(bound.key, bound)
        return bound

    def _replace_attached_artifact(
        self, artifact: Artifact, *, direction: str
    ) -> Artifact:
        tracker = self._tracker
        bound = artifact.with_resolver(tracker)
        if tracker.current_consist is not None:
            tracker.current_consist.replace_artifact(bound)
        if (
            direction == "output"
            and tracker._active_coupler is not None
            and bound.key
            and bound.key in tracker._active_coupler._artifacts
        ):
            tracker._active_coupler._artifacts[bound.key] = bound
        return bound

    def log_artifact(
        self,
        path: ArtifactRef,
        key: Optional[str] = None,
        direction: str = "output",
        schema: Optional[type[BaseModel]] = None,
        strict_schema: bool = False,
        driver: Optional[str] = None,
        member: Optional[str] = None,
        content_hash: Optional[str | Digest] = None,
        digest: Optional[Digest] = None,
        force_hash_override: bool = False,
        validate_content_hash: bool = False,
        reuse_if_unchanged: bool = False,
        reuse_scope: Literal["same_uri", "any_uri"] = "same_uri",
        parent_artifact_id: Optional[uuid.UUID] = None,
        profile_file_schema: bool | Literal["if_changed"] | None = None,
        file_schema_sample_rows: Optional[int] = None,
        facet: Optional[FacetLike] = None,
        facet_schema_version: Optional[Union[str, int]] = None,
        facet_index: bool = False,
        known_latest_artifact_at_uri: Optional[Artifact] = None,
        latest_artifact_lookup_done: bool = False,
        link_role: Optional[str] = None,
        **meta: Any,
    ) -> Artifact:
        """Log one artifact while preserving tracker run-state invariants."""
        tracker = self._tracker
        tracker._ensure_write_provenance()

        if not tracker.current_consist:
            raise RuntimeError("Cannot log artifact: no active run.")

        raw_recovery_roots = meta.pop("recovery_roots", None)
        normalized_recovery_roots = (
            tracker.fs.normalize_recovery_roots(raw_recovery_roots)
            if raw_recovery_roots is not None
            else None
        )

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

        if driver == "artifact_directory":
            raise ValueError(
                "artifact_directory is not a content driver; use artifact_kind='directory'."
            )

        is_path_target = isinstance(path, (str, Path))
        is_path_output = direction == "output" and is_path_target
        inferred_zarr = is_path_output and (
            driver == "zarr" or Path(path).suffix.lower() == ".zarr"
        )
        inferred_shapefile = is_path_target and (
            driver == "shapefile" or Path(path).suffix.lower() == ".shp"
        )
        requested_directory = meta.get("directory_artifact") is True
        if inferred_zarr or requested_directory:
            if not is_path_output:
                raise TypeError(
                    "immutable directory outputs must be logged from a path."
                )
            manifest = build_directory_manifest(Path(path))
            expected_digest = Digest("dir_manifest_v1:sha256", manifest["tree_hash"])
            if content_hash is not None and str(content_hash) not in {
                manifest["tree_hash"],
                str(expected_digest),
            }:
                raise ValueError(
                    "directory output content_hash must match its manifest tree hash."
                )
            if inferred_zarr:
                driver = "zarr"
            digest = expected_digest
            content_hash = None
            force_hash_override = False
            validate_content_hash = False
            meta = {
                **meta,
                "directory_artifact": True,
                "directory_manifest": manifest,
                "directory_manifest_version": manifest["version"],
            }
        elif driver == "shapefile" and not is_path_target and direction == "output":
            raise TypeError("immutable shapefile outputs must be logged from a path.")
        elif inferred_shapefile and (
            direction == "output"
            or (
                Path(path).is_file()
                and not (content_hash is not None and not validate_content_hash)
            )
        ):
            shp_manifest: dict[str, Any] | None = None
            if direction == "output":
                shp_manifest = build_shapefile_bundle_manifest(Path(path))
            else:
                try:
                    shp_manifest = build_shapefile_bundle_manifest(Path(path))
                except ValueError:
                    shp_manifest = None
            if shp_manifest is not None:
                expected_digest = Digest(
                    "dir_manifest_v1:sha256", shp_manifest["tree_hash"]
                )
                if content_hash is not None and str(content_hash) not in {
                    shp_manifest["tree_hash"],
                    str(expected_digest),
                }:
                    raise ValueError(
                        "shapefile content_hash must match its bundle tree hash."
                    )
                driver = "shapefile"
                digest = expected_digest
                content_hash = None
                force_hash_override = False
                validate_content_hash = False
                meta = {
                    **meta,
                    "file_bundle_artifact": True,
                    "file_bundle_entry": Path(path).name,
                    "file_bundle_manifest": shp_manifest,
                    "file_bundle_manifest_version": shp_manifest["version"],
                }

        run_id = tracker.current_consist.run.id if direction == "output" else None

        with _track_begin_run_phase("input_binding.create_artifact"):
            artifact_obj = tracker.artifacts.create_artifact(
                path,
                run_id,
                key,
                direction,
                schema,
                driver,
                member=member,
                content_hash=content_hash,
                digest=digest,
                force_hash_override=force_hash_override,
                validate_content_hash=validate_content_hash,
                reuse_if_unchanged=reuse_if_unchanged,
                reuse_scope=reuse_scope,
                parent_artifact_id=parent_artifact_id,
                known_latest_artifact_at_uri=known_latest_artifact_at_uri,
                latest_artifact_lookup_done=latest_artifact_lookup_done,
                strict_schema=strict_schema,
                **meta,
            )
        with _track_begin_run_phase("input_binding.prepare_facet"):
            prepared_artifact_facet_bundle = self.prepare_artifact_facet_bundle(
                artifact=artifact_obj,
                facet=facet,
                facet_schema_version=facet_schema_version,
                facet_index=facet_index,
            )

        if isinstance(path, Artifact) and direction == "output":
            producing_run_id = artifact_obj.run_id
            if producing_run_id is None:
                artifact_obj = artifact_obj.with_run_id(run_id)
            elif producing_run_id != run_id:
                logging.warning(
                    "[Consist] log_artifact received an Artifact with run_id=%s but is logging it as output of run_id=%s (artifact key=%s id=%s).",
                    producing_run_id,
                    run_id,
                    artifact_obj.key,
                    artifact_obj.id,
                )
        with _track_begin_run_phase("input_binding.apply_inherited_metadata"):
            artifact_obj = self.apply_inherited_run_metadata(artifact_obj)
        with _track_begin_run_phase("input_binding.attach_logged_artifact"):
            artifact_obj = self.attach_logged_artifact(
                artifact=artifact_obj,
                direction=direction,
                register_with_coupler=True,
            )

        # Preserve dual-write ordering so snapshot state reflects artifact links
        # before DB synchronization side effects occur.
        tracker._flush_json()
        if prepared_artifact_facet_bundle is not None:
            artifact_obj = tracker.persistence.sync_artifact_with_facet_bundle(
                artifact_obj,
                direction,
                facet=prepared_artifact_facet_bundle.facet,
                meta_updates=prepared_artifact_facet_bundle.meta_updates,
                kv_rows=prepared_artifact_facet_bundle.kv_rows,
                role=link_role,
                recovery_roots=normalized_recovery_roots,
            )
            artifact_obj = self._replace_attached_artifact(
                artifact_obj, direction=direction
            )
            tracker._flush_json()
        else:
            tracker._sync_artifact_to_db(
                artifact_obj,
                direction,
                profile_label=self._sync_profile_label(
                    artifact=artifact_obj,
                    direction=direction,
                ),
                role=link_role,
                recovery_roots=normalized_recovery_roots,
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
                if isinstance(artifact_obj.meta, Mapping) and artifact_obj.meta.get(
                    "schema_id"
                ):
                    return artifact_obj
                resolved_path = artifact_obj.abs_path or tracker.resolve_uri(
                    artifact_obj.uri
                )
                if resolved_path:
                    resolved_driver = artifact_obj.driver
                    if resolved_driver not in ("csv", "parquet", "gtfs", "h5_table"):
                        return artifact_obj
                    artifact_obj = tracker.artifact_schemas.profile_file_artifact(
                        artifact=artifact_obj,
                        run=tracker.current_consist.run,
                        resolved_path=str(resolved_path),
                        driver=resolved_driver,
                        sample_rows=sample_rows,
                        source="file",
                        reuse_if_unchanged=profile_mode == "if_changed",
                    )
                    artifact_obj = self._replace_attached_artifact(
                        artifact_obj, direction=direction
                    )
                    tracker._flush_json()
            except FileNotFoundError:
                logging.warning(
                    "[Consist] File schema capture skipped; file not found: %s",
                    artifact_obj.uri,
                )

        if (
            schema is not None
            and artifact_obj.is_tabular
            and tracker.current_consist is not None
        ):
            try:
                artifact_obj = tracker.artifact_schemas.profile_user_provided_schema(
                    artifact=artifact_obj,
                    run=tracker.current_consist.run,
                    schema_model=schema,
                    source="user_provided",
                )
                artifact_obj = self._replace_attached_artifact(
                    artifact_obj, direction=direction
                )
                tracker._flush_json()
            except Exception as exc:
                logging.warning(
                    "[Consist] Failed to store user-provided schema for artifact=%s: %s",
                    artifact_obj.key,
                    exc,
                )

        return artifact_obj
