import hashlib
import logging
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    List,
    Literal,
    Optional,
    TypeAlias,
    Union,
)

from pydantic import BaseModel

from consist.core.admission import hash_semantics_for_new_artifact
from consist.core.cache_output_logging import _demote_cache_hit
from consist.core.container_policy import (
    ChildRecoveryPolicy,
    ContainerRecoveryUnit,
    apply_container_policy_meta,
)
from consist.core.drivers import ARRAY_DRIVERS, TABLE_DRIVERS
from consist.core.logical_array_identity import open_h5py_read_only
from consist.core.validation import validate_artifact_key
from consist.digest import (
    Digest,
    build_manifest_v1,
    build_shapefile_manifest_v1,
    compute_digest,
)
from consist.models.artifact import Artifact
from consist.types import (
    ArtifactRef,
    FacetLike,
    H5ChildSelectionMode,
    H5ChildSpec,
)

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


ContainerPolicyMetaValue: TypeAlias = (
    ContainerRecoveryUnit | ChildRecoveryPolicy | Mapping[str, object]
)

_HEX64_CHARS = frozenset("0123456789abcdef")
_WEAK_OBSERVATION_SCHEMES = frozenset(
    {
        "stat_file_v1:sha256",
        "stat_dir_v1:sha256",
        "caller_supplied_v1:sha256",
    }
)


@dataclass(slots=True)
class _DigestResolutionState:
    """Mutable per-call state for lazy digest computation and caller overrides."""

    effective_digest: Optional[Digest]
    caller_raw_token: Optional[str] = None
    computed_digest: Optional[Digest] = None
    computed_dir_manifest: Optional[dict[str, Any]] = None
    computed_shp_manifest: Optional[dict[str, Any]] = None
    attempted_compute: bool = False
    caller_supplied: bool = False
    override_applied: bool = False
    caller_supplied_override_applied: bool = False


def _coerce_caller_content_hash(
    raw: Optional[str | Digest],
    *,
    directory_artifact: bool = False,
) -> tuple[Optional[Digest], Optional[str]]:
    """Normalize caller-supplied ``content_hash`` into ``(Digest, raw_opaque_token)``."""
    if raw is None or raw == "":
        return None, None
    if isinstance(raw, Digest):
        return raw, None
    if ":" in raw:
        try:
            return Digest.parse(raw), None
        except ValueError:
            pass
    if len(raw) == 64 and all(ch in _HEX64_CHARS for ch in raw):
        scheme = "dir_manifest_v1:sha256" if directory_artifact else "sha256:file"
        return Digest(scheme, raw), None
    opaque_hex = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return Digest("caller_supplied_v1:sha256", opaque_hex), raw


def _infer_driver_from_path(path: Path) -> str:
    driver = ARRAY_DRIVERS.resolve_driver(path)
    if driver:
        return driver
    driver = TABLE_DRIVERS.resolve_driver(path)
    if driver:
        return driver
    suffixes = [suffix.lower() for suffix in path.suffixes]
    suffix = suffixes[-1].lstrip(".") if suffixes else ""
    if suffix == "shp":
        return "shapefile"
    if suffix == "gpkg":
        return "geopackage"
    if suffix == "geojson":
        return "geojson"
    if suffix in {"h5", "hdf5"}:
        return "h5"
    return suffix or "unknown"


def _normalize_h5_child_path(path: str) -> str:
    stripped = path.strip()
    if not stripped:
        return "/"
    return f"/{stripped.lstrip('/')}"


def _resolve_h5_child_spec(
    child_specs: Optional[Mapping[str, H5ChildSpec]],
    table_path: str,
) -> Optional[H5ChildSpec]:
    if not child_specs:
        return None
    normalized_path = _normalize_h5_child_path(table_path)
    return (
        child_specs.get(normalized_path)
        or child_specs.get(normalized_path.lstrip("/"))
        or child_specs.get(table_path)
    )


def _container_policy_updates(
    meta: Mapping[str, object],
) -> dict[str, ContainerPolicyMetaValue]:
    updates: dict[str, ContainerPolicyMetaValue] = {}
    recovery_unit = meta.get("container_recovery_unit")
    if recovery_unit == "parent_file":
        updates["container_recovery_unit"] = "parent_file"
    elif recovery_unit == "derived_children":
        updates["container_recovery_unit"] = "derived_children"
    child_policy = meta.get("child_recovery_policy")
    if child_policy == "descriptive_only":
        updates["child_recovery_policy"] = "descriptive_only"
    elif child_policy == "independent":
        updates["child_recovery_policy"] = "independent"
    representation_policy = meta.get("representation_policy")
    if isinstance(representation_policy, Mapping):
        updates["representation_policy"] = {
            str(key): value for key, value in representation_policy.items()
        }
    return updates


def _list_h5_dataset_names(
    path_obj: Path,
    *,
    filter_fn: Callable[[str], bool],
) -> list[str]:
    try:
        import h5py
    except ImportError:
        logging.warning("[Consist] h5py not installed. Cannot inspect HDF5 tables.")
        return []

    dataset_names: list[str] = []
    try:
        with open_h5py_read_only(str(path_obj), h5py_module=h5py) as handle:

            def visit_datasets(name: str, obj: Any) -> None:
                if isinstance(obj, h5py.Dataset) and filter_fn(name):
                    dataset_names.append(_normalize_h5_child_path(name))

            handle.visititems(visit_datasets)
    except Exception as exc:
        logging.warning("[Consist] Failed to inspect HDF5 tables: %s", exc)
        return []

    return dataset_names


class ArtifactManager:
    """Manage virtualization and unified scheme digests for Consist Artifacts."""

    def __init__(self, tracker: "Tracker") -> None:
        self.tracker = tracker

    def create_artifact(
        self,
        path: ArtifactRef,
        run_id: Optional[str] = None,
        key: Optional[str] = None,
        direction: str = "output",
        schema: Optional[type[BaseModel]] = None,
        driver: Optional[str] = None,
        member: Optional[str] = None,
        content_hash: Optional[str | Digest] = None,
        digest: Optional[Digest] = None,
        skip_digest: bool = False,
        force_hash_override: bool = False,
        validate_content_hash: bool = False,
        reuse_if_unchanged: bool = False,
        reuse_scope: Literal["same_uri", "any_uri"] = "same_uri",
        parent_artifact_id: Optional[uuid.UUID] = None,
        known_latest_artifact_at_uri: Optional[Artifact] = None,
        latest_artifact_lookup_done: bool = False,
        strict_schema: bool = False,
        **meta: Any,
    ) -> Artifact:
        """Instantiate or resolve a tracked immutable ``Artifact``."""
        legacy_table_path = meta.pop("table_path", None)
        legacy_array_path = meta.pop("array_path", None)
        explicit_member_kw = meta.pop("member", None)
        if member is None:
            if isinstance(explicit_member_kw, str) and explicit_member_kw:
                member = explicit_member_kw
            elif isinstance(legacy_table_path, str) and legacy_table_path:
                member = legacy_table_path
            elif isinstance(legacy_array_path, str) and legacy_array_path:
                member = legacy_array_path
        if "recovery_roots" in meta:
            meta["recovery_roots"] = self.tracker.fs.normalize_recovery_roots(
                meta["recovery_roots"]
            )
        meta.pop("hash_semantics", None)
        meta.pop("content_identity", None)

        directory_flag = bool(meta.get("directory_artifact")) or bool(
            meta.get("file_bundle_artifact")
        )
        caller_digest, caller_raw_token = _coerce_caller_content_hash(
            content_hash,
            directory_artifact=directory_flag,
        )
        initial_digest = digest if digest is not None else caller_digest
        digest_state = _DigestResolutionState(
            effective_digest=initial_digest,
            caller_raw_token=caller_raw_token,
            caller_supplied=(digest is None and caller_digest is not None),
        )

        artifact_obj: Optional[Artifact] = None
        is_new_artifact = False
        resolved_abs_path: Optional[str] = None
        mount_scheme: Optional[str] = None
        mount_root: Optional[str] = None

        if direction == "output" and reuse_if_unchanged:
            logging.warning(
                "[Consist] reuse_if_unchanged on outputs is deprecated; "
                "a new artifact row is always created now and identical bytes share "
                "content digest identity."
            )
            if reuse_scope == "any_uri":
                logging.warning(
                    "[Consist] reuse_scope='any_uri' is ignored for outputs; "
                    "digest equality governs deduplication instead."
                )

        def _clone_artifact(
            artifact: Artifact,
            *,
            driver_override: Optional[str],
            member_override: Optional[str],
            parent_artifact_id_override: Optional[uuid.UUID],
        ) -> Artifact:
            updates: dict[str, Any] = {}
            if driver_override:
                updates["driver"] = driver_override
            if member_override is not None:
                updates["member"] = member_override
            if parent_artifact_id_override is not None:
                updates["parent_artifact_id"] = parent_artifact_id_override
            if not updates:
                return artifact
            return replace(artifact, **updates)

        def _compute_on_disk_digest(*, raise_on_error: bool) -> Optional[Digest]:
            if (
                digest_state.computed_digest is not None
                or digest_state.attempted_compute
            ):
                return digest_state.computed_digest
            if not resolved_abs_path:
                if raise_on_error:
                    raise ValueError(
                        "validate_content_hash=True requires a resolvable path."
                    )
                return None
            digest_state.attempted_compute = True
            target = Path(resolved_abs_path)
            strategy = self.tracker.identity.hashing_strategy
            try:
                if target.is_dir():
                    if strategy != "fast":
                        if self.tracker.identity._is_zarr_store(target):
                            logging.warning(
                                "[Consist Warning] Performing full content hashing on Zarr store '%s'. "
                                "This can be slow. Consider using 'fast' hashing_strategy for metadata-based hashing.",
                                target.name,
                            )
                        else:
                            logging.warning(
                                "[Consist Warning] Performing full content hashing on directory '%s'. "
                                "This can be slow. Consider using 'fast' hashing_strategy or pre-computed hashes for directories.",
                                target.name,
                            )
                        dir_manifest = build_manifest_v1(target)
                        digest_state.computed_dir_manifest = dir_manifest
                        digest_state.computed_digest = Digest(
                            "dir_manifest_v1:sha256",
                            dir_manifest["tree_hash"],
                        )
                    else:
                        digest_state.computed_digest = compute_digest(
                            target,
                            driver="directory",
                            options={"hashing_strategy": "fast"},
                        )
                else:
                    effective_driver = driver
                    if effective_driver == "shapefile" and strategy != "fast":
                        shp_manifest: Optional[dict[str, Any]] = None
                        if meta.get("file_bundle_artifact"):
                            shp_manifest = build_shapefile_manifest_v1(target)
                        elif target.is_file():
                            try:
                                shp_manifest = build_shapefile_manifest_v1(target)
                            except ValueError:
                                shp_manifest = None
                        if shp_manifest is not None:
                            digest_state.computed_shp_manifest = shp_manifest
                            digest_state.computed_digest = Digest(
                                "dir_manifest_v1:sha256",
                                shp_manifest["tree_hash"],
                            )
                        else:
                            digest_state.computed_digest = compute_digest(
                                target,
                                driver="file",
                                member=member,
                                options={"hashing_strategy": strategy},
                            )
                    else:
                        if effective_driver in {"h5", "hdf5"} and member is None:
                            effective_driver = "file"
                        elif effective_driver == "shapefile":
                            effective_driver = "file"
                        digest_state.computed_digest = compute_digest(
                            target,
                            driver=effective_driver,
                            member=member,
                            options={"hashing_strategy": strategy},
                        )
            except Exception as exc:
                if raise_on_error:
                    raise ValueError(
                        f"Failed to validate content_hash for {resolved_abs_path}: {exc}"
                    ) from exc
                logging.warning(
                    "[Consist Warning] Failed to compute hash for %s: %s",
                    resolved_abs_path,
                    exc,
                )
            return digest_state.computed_digest

        def _ensure_digest(*, raise_on_error: bool = False) -> Optional[Digest]:
            if skip_digest:
                return digest_state.effective_digest
            if digest_state.effective_digest is None:
                digest_state.effective_digest = _compute_on_disk_digest(
                    raise_on_error=raise_on_error
                )
            return digest_state.effective_digest

        def _validate_supplied_hash() -> None:
            if not validate_content_hash or digest_state.effective_digest is None:
                return
            computed = _compute_on_disk_digest(raise_on_error=True)
            if computed is None:
                raise ValueError(
                    f"content_hash does not match on-disk data for {resolved_abs_path}."
                )
            matches = computed == digest_state.effective_digest or (
                isinstance(content_hash, str)
                and content_hash in {computed.hex, str(computed)}
            )
            if not matches:
                raise ValueError(
                    f"content_hash does not match on-disk data for {resolved_abs_path}."
                )
            digest_state.effective_digest = computed
            digest_state.caller_raw_token = None

        def _apply_digest_override(artifact: Artifact) -> Artifact:
            if digest_state.effective_digest is None:
                return artifact
            _validate_supplied_hash()
            assert digest_state.effective_digest is not None
            if artifact.digest == digest_state.effective_digest:
                return artifact
            if artifact.digest is not None and not force_hash_override:
                logging.warning(
                    "[Consist Warning] Ignoring content_hash override for artifact key=%s uri=%s "
                    "(existing digest differs). Use force_hash_override=True to override.",
                    artifact.key,
                    artifact.uri,
                )
                return artifact
            if digest_state.caller_supplied:
                digest_state.caller_supplied_override_applied = True
            digest_state.override_applied = True
            return artifact.with_digest(digest_state.effective_digest)

        if isinstance(path, Artifact):
            source_artifact = path
            resolved_abs_path = source_artifact.abs_path or self.tracker.resolve_uri(
                source_artifact.uri
            )
            if key is None:
                key = source_artifact.key
            validate_artifact_key(key)
            artifact_obj = _clone_artifact(
                source_artifact,
                driver_override=driver,
                member_override=member,
                parent_artifact_id_override=parent_artifact_id,
            )
            artifact_obj = _apply_digest_override(artifact_obj)
            if meta:
                artifact_obj = artifact_obj.with_meta(**meta)
        else:
            if key is None:
                raise ValueError("Argument 'key' required when 'path' is a path-like.")
            validate_artifact_key(key)

            resolved_abs_path = str(Path(path).resolve())
            uri = self.tracker.fs.virtualize_path(resolved_abs_path)

            if "://" in uri:
                scheme = uri.split("://", 1)[0]
                if scheme in self.tracker.mounts:
                    mount_scheme = scheme
                    mount_root = str(Path(self.tracker.mounts[scheme]).resolve())

            if driver is None:
                driver = _infer_driver_from_path(Path(path))

            if direction == "input" and self.tracker.db:
                parent = known_latest_artifact_at_uri
                if not latest_artifact_lookup_done:
                    parent = self.tracker.db.find_latest_artifact_at_uri(
                        uri,
                        driver=driver,
                        member=member,
                    )
                if parent is not None:
                    should_reuse = True
                    if driver in {"h5", "hdf5"}:
                        if digest_state.effective_digest is None:
                            _ensure_digest()
                        should_reuse = (
                            digest_state.effective_digest is not None
                            and parent.digest == digest_state.effective_digest
                        )
                    elif driver == "shapefile":
                        if digest_state.effective_digest is None:
                            _ensure_digest()
                        should_reuse = (
                            digest_state.effective_digest is not None
                            and parent.digest == digest_state.effective_digest
                            and (
                                digest_state.computed_shp_manifest is None
                                or "file_bundle_manifest" in parent.meta
                            )
                        )
                    elif driver in {"h5_table", "gtfs"}:
                        should_reuse = (
                            digest_state.effective_digest is not None
                            and parent.digest == digest_state.effective_digest
                        )

                    if should_reuse:
                        artifact_obj = _clone_artifact(
                            parent,
                            driver_override=driver,
                            member_override=member,
                            parent_artifact_id_override=parent_artifact_id,
                        )
                        artifact_obj = _apply_digest_override(artifact_obj)
                        if meta:
                            artifact_obj = artifact_obj.with_meta(**meta)

            if artifact_obj is None:
                if digest_state.effective_digest is not None and validate_content_hash:
                    _validate_supplied_hash()
                elif digest_state.effective_digest is None:
                    _ensure_digest()

                if (
                    digest_state.computed_dir_manifest is not None
                    and "directory_manifest" not in meta
                    and (directory_flag or driver == "zarr")
                ):
                    meta["directory_artifact"] = True
                    meta["directory_manifest"] = digest_state.computed_dir_manifest
                    meta["directory_manifest_version"] = (
                        digest_state.computed_dir_manifest["version"]
                    )
                if (
                    digest_state.computed_shp_manifest is not None
                    and "file_bundle_manifest" not in meta
                ):
                    meta["file_bundle_artifact"] = True
                    meta["file_bundle_entry"] = Path(resolved_abs_path).name
                    meta["file_bundle_manifest"] = digest_state.computed_shp_manifest
                    meta["file_bundle_manifest_version"] = (
                        digest_state.computed_shp_manifest["version"]
                    )
                if digest_state.caller_raw_token is not None:
                    meta["caller_supplied_hash"] = digest_state.caller_raw_token

                artifact_obj = Artifact(
                    key=key,
                    uri=uri,
                    driver=driver,
                    member=member,
                    digest=digest_state.effective_digest,
                    run_id=run_id,
                    parent_artifact_id=parent_artifact_id,
                    meta=meta,
                )
                is_new_artifact = True

        meta_updates: dict[str, Any] = dict(artifact_obj.meta)
        meta_changed = False

        if schema is not None:
            meta_updates["schema_name"] = schema.__name__
            if strict_schema:
                meta_updates["has_strict_schema"] = True
            else:
                meta_updates.pop("has_strict_schema", None)
            meta_changed = True

        if is_new_artifact and artifact_obj.digest is not None and resolved_abs_path:
            meta_updates["hash_semantics"] = hash_semantics_for_new_artifact(
                path=Path(resolved_abs_path),
                hashing_strategy=self.tracker.identity.hashing_strategy,
                source=(
                    "caller_supplied"
                    if (digest_state.caller_supplied and not validate_content_hash)
                    else "computed"
                ),
                digest=artifact_obj.digest,
            )
            meta_changed = True
        elif digest_state.caller_supplied_override_applied:
            meta_updates["hash_semantics"] = hash_semantics_for_new_artifact(
                path=None,
                hashing_strategy=self.tracker.identity.hashing_strategy,
                source="caller_supplied",
                digest=artifact_obj.digest,
            )
            if digest_state.caller_raw_token is not None:
                meta_updates["caller_supplied_hash"] = digest_state.caller_raw_token
            meta_changed = True
            if not is_new_artifact:
                artifact_obj = replace(
                    artifact_obj,
                    id=uuid.uuid4(),
                    run_id=None,
                    created_at=datetime.now(timezone.utc),
                )
        elif digest_state.override_applied and not is_new_artifact:
            if resolved_abs_path:
                meta_updates["hash_semantics"] = hash_semantics_for_new_artifact(
                    path=Path(resolved_abs_path),
                    hashing_strategy=self.tracker.identity.hashing_strategy,
                    source="computed",
                    digest=artifact_obj.digest,
                )
                meta_changed = True
            artifact_obj = replace(
                artifact_obj,
                id=uuid.uuid4(),
                run_id=None,
                created_at=datetime.now(timezone.utc),
            )

        if mount_scheme and "mount_scheme" not in meta_updates:
            meta_updates["mount_scheme"] = mount_scheme
            meta_changed = True
        if mount_root and "mount_root" not in meta_updates:
            meta_updates["mount_root"] = mount_root
            meta_changed = True

        semantics = meta_updates.get("hash_semantics")
        is_caller_source = (
            isinstance(semantics, Mapping)
            and semantics.get("source") == "caller_supplied"
        )
        if artifact_obj.digest is not None and not is_caller_source:
            if artifact_obj.digest.scheme not in _WEAK_OBSERVATION_SCHEMES:
                meta_updates["content_identity"] = str(artifact_obj.digest)
                meta_changed = True
            elif "content_identity" in meta_updates:
                meta_updates.pop("content_identity", None)
                meta_changed = True
        elif is_caller_source and "content_identity" in meta_updates:
            meta_updates.pop("content_identity", None)
            meta_changed = True

        if meta_changed:
            artifact_obj = replace(artifact_obj, meta=meta_updates)

        return artifact_obj.with_resolver(self.tracker).with_abs_path(resolved_abs_path)

    def scan_h5_container(
        self,
        container: Artifact,
        path_obj: Path,
        key: str,
        direction: str,
        filter_fn: Callable[[str], bool],
        *,
        hash_tables: bool,
        table_hash_chunk_rows: Optional[int],
        child_specs: Optional[Mapping[str, H5ChildSpec]] = None,
    ) -> List[tuple[Artifact, Any]]:
        """Discover HDF5 datasets contained within ``container``."""
        try:
            import h5py
        except ImportError:
            logging.warning(
                "[Consist] h5py not installed. Cannot discover HDF5 tables."
            )
            return []

        table_artifacts: List[tuple[Artifact, Any]] = []

        try:
            with open_h5py_read_only(str(path_obj), h5py_module=h5py) as f:

                def visit_datasets(name: str, obj: Any) -> None:
                    if not isinstance(obj, h5py.Dataset) or not filter_fn(name):
                        return
                    child_spec = _resolve_h5_child_spec(child_specs, name)
                    table_key = (
                        child_spec.key
                        if child_spec and child_spec.key
                        else f"{key}_{name.replace('/', '_')}"
                    )
                    table_digest: Optional[Digest] = None
                    table_meta: dict[str, Any] = {
                        "parent_id": str(container.id),
                        "shape": list(obj.shape),
                        "dtype": str(obj.dtype),
                    }
                    if child_spec and child_spec.description:
                        table_meta["description"] = child_spec.description
                    if child_spec and child_spec.metadata:
                        table_meta.update(dict(child_spec.metadata))
                    is_pytables_table = obj.dtype.names is not None or obj.attrs.get(
                        "CLASS"
                    ) in {b"TABLE", "TABLE"}
                    leaf_driver = "h5_table" if is_pytables_table else "h5"
                    if hash_tables:
                        try:
                            if leaf_driver == "h5_table":
                                table_digest = compute_digest(
                                    path_obj,
                                    driver="h5_table",
                                    member=name,
                                    options={"chunk_rows": table_hash_chunk_rows},
                                )
                            else:
                                table_digest = compute_digest(
                                    path_obj,
                                    driver="h5",
                                    member=name,
                                    options={
                                        "dataset": obj,
                                        "chunk_rows": table_hash_chunk_rows,
                                    },
                                )
                        except (
                            OSError,
                            ValueError,
                            RuntimeError,
                            KeyError,
                            TypeError,
                        ) as exc:
                            logging.warning(
                                "[Consist] Failed to hash HDF5 dataset %s: %s",
                                name,
                                exc,
                            )
                        if table_digest is not None:
                            table_meta["table_hash"] = table_digest.hex
                            table_meta["table_hash_algo"] = "sha256"
                            if table_hash_chunk_rows is not None:
                                table_meta["table_hash_chunk_rows"] = (
                                    table_hash_chunk_rows
                                )
                    table_art = self.create_artifact(
                        str(path_obj),
                        run_id=container.run_id,
                        key=table_key,
                        direction=direction,
                        driver=leaf_driver,
                        member=name,
                        digest=table_digest,
                        skip_digest=not hash_tables,
                        parent_artifact_id=container.id,
                        **table_meta,
                    )
                    prepared_bundle = (
                        self.tracker._artifact_logging.prepare_artifact_facet_bundle(
                            artifact=table_art,
                            facet=child_spec.facet if child_spec else None,
                            facet_schema_version=(
                                child_spec.facet_schema_version if child_spec else None
                            ),
                            facet_index=(
                                child_spec.facet_index if child_spec else False
                            ),
                        )
                    )
                    table_artifacts.append((table_art, prepared_bundle))

                f.visititems(visit_datasets)
        except (OSError, ValueError, RuntimeError, KeyError) as exc:
            logging.warning("[Consist] Failed to discover HDF5 tables: %s", exc)

        return table_artifacts

    def log_h5_container(
        self,
        path: Union[str, Path],
        *,
        key: Optional[str] = None,
        direction: str = "output",
        discover_tables: bool = True,
        table_filter: Optional[Union[Callable[[str], bool], List[str]]] = None,
        hash_tables: Literal["always", "if_unchanged", "never"] = "if_unchanged",
        table_hash_chunk_rows: Optional[int] = None,
        child_specs: Optional[Mapping[str, H5ChildSpec]] = None,
        child_selection: H5ChildSelectionMode = "all",
        container_recovery_unit: ContainerRecoveryUnit | None = None,
        child_recovery_policy: ChildRecoveryPolicy | None = None,
        representation_policy: Optional[Mapping[str, Any]] = None,
        **meta: Any,
    ) -> tuple[Artifact, List[Artifact]]:
        """Log an HDF5 file and optionally discover its internal datasets."""
        if not self.tracker.current_consist:
            raise RuntimeError("Cannot log artifact outside of a run context.")
        if child_selection not in {"all", "include_only"}:
            raise ValueError("child_selection must be 'all' or 'include_only'.")

        apply_container_policy_meta(
            meta,
            container_recovery_unit=container_recovery_unit,
            child_recovery_policy=child_recovery_policy,
            representation_policy=representation_policy,
        )

        path_obj = Path(path)
        if key is None:
            key = path_obj.stem
        normalized_child_specs = (
            {
                _normalize_h5_child_path(path_key): spec
                for path_key, spec in child_specs.items()
            }
            if child_specs is not None
            else None
        )
        prior_container = None
        if self.tracker.db:
            try:
                resolved_abs_path = str(path_obj.resolve())
                uri = self.tracker.fs.virtualize_path(resolved_abs_path)
                prior_container = self.tracker.db.find_latest_artifact_at_uri(
                    uri, driver="h5", include_inputs=True
                )
            except (OSError, ValueError, RuntimeError):
                prior_container = None

        with self.tracker.persistence.batch_artifact_writes():
            container = self.tracker.log_artifact(
                str(path_obj),
                key=key,
                direction=direction,
                driver="h5",
                is_container=True,
                **meta,
            )

            table_artifacts: List[Artifact] = []
            reusing_cached_children = False
            should_hash_tables = False
            skip_reason: Optional[str] = None
            if hash_tables not in {"always", "if_unchanged", "never"}:
                raise ValueError(
                    "hash_tables must be one of: 'always', 'if_unchanged', 'never'."
                )
            if hash_tables == "always":
                should_hash_tables = True
            elif hash_tables == "if_unchanged":
                if prior_container is None:
                    should_hash_tables = True
                else:
                    prior_meta = (
                        prior_container.meta
                        if isinstance(prior_container.meta, Mapping)
                        else {}
                    )
                    current_meta = (
                        container.meta if isinstance(container.meta, Mapping) else {}
                    )
                    prior_ci = prior_meta.get("content_identity")
                    current_ci = current_meta.get("content_identity")
                    if (
                        isinstance(prior_ci, str)
                        and prior_ci
                        and isinstance(current_ci, str)
                        and current_ci
                    ):
                        should_hash_tables = prior_ci == current_ci
                        if not should_hash_tables:
                            skip_reason = "content_identity_changed"
                    elif prior_container.digest and container.digest:
                        should_hash_tables = prior_container.digest == container.digest
                        if not should_hash_tables:
                            skip_reason = "file_hash_changed"
                    else:
                        skip_reason = "file_hash_unavailable"

            container_updates: dict[str, Any] = {
                "table_hashes_checked": should_hash_tables
            }
            if not should_hash_tables and hash_tables != "always":
                if hash_tables == "never":
                    container_updates["table_hashes_skip_reason"] = "disabled"
                elif prior_container is not None and skip_reason is not None:
                    container_updates["table_hashes_skip_reason"] = skip_reason
            container = container.with_meta(container_updates, merge=True)
            self.tracker.current_consist.replace_artifact(container)

            if discover_tables:
                include_only_paths = (
                    (
                        set(normalized_child_specs.keys())
                        if normalized_child_specs is not None
                        else set()
                    )
                    if child_selection == "include_only"
                    else None
                )
                if table_filter is None:

                    def base_filter_fn(name: str) -> bool:
                        return True

                elif isinstance(table_filter, list):
                    filter_set = set(table_filter)

                    def base_filter_fn(name: str) -> bool:
                        stripped = name.lstrip("/")
                        return name in filter_set or stripped in filter_set

                else:
                    base_filter_fn = table_filter

                def filter_fn(name: str) -> bool:
                    if not base_filter_fn(name):
                        return False
                    if include_only_paths is None:
                        return True
                    return _normalize_h5_child_path(name) in include_only_paths

                if direction == "output" and self.tracker.is_cached:
                    cached_children = [
                        artifact
                        for artifact in self.tracker.current_consist.outputs
                        if artifact.id != container.id
                        and (
                            artifact.parent_artifact_id == container.id
                            or (
                                isinstance(artifact.meta, Mapping)
                                and artifact.meta.get("parent_id") == str(container.id)
                            )
                        )
                        and artifact.member is not None
                        and filter_fn(artifact.member)
                    ]
                    cached_child_paths = {
                        _normalize_h5_child_path(artifact.member)
                        for artifact in cached_children
                        if artifact.member is not None
                    }
                    requested_child_paths = set(
                        _list_h5_dataset_names(path_obj, filter_fn=filter_fn)
                    )
                    if cached_child_paths == requested_child_paths:
                        table_artifacts = list(cached_children)
                        reusing_cached_children = True
                        policy_updates = _container_policy_updates(meta)
                        if policy_updates:
                            container = container.with_meta(policy_updates, merge=True)
                            self.tracker.current_consist.replace_artifact(container)
                            if self.tracker.db is not None:
                                self.tracker.db.update_artifact_meta(
                                    container,
                                    dict(policy_updates),
                                    raise_on_error=True,
                                )
                    else:
                        _demote_cache_hit(
                            self.tracker.current_consist,
                            reason=(
                                "caller requested HDF5 child artifacts that do not "
                                "match the cached output set"
                            ),
                        )
                        container = self.tracker.log_artifact(
                            str(path_obj),
                            key=key,
                            direction=direction,
                            driver="h5",
                            is_container=True,
                            **meta,
                        )
                        container = container.with_meta(**container_updates)
                        self.tracker.current_consist.replace_artifact(container)

                if not table_artifacts:
                    scanned_children = self.scan_h5_container(
                        container,
                        path_obj,
                        key,
                        direction,
                        filter_fn,
                        hash_tables=should_hash_tables,
                        table_hash_chunk_rows=table_hash_chunk_rows,
                        child_specs=normalized_child_specs,
                    )

                    for table_artifact, prepared_bundle in scanned_children:
                        table_artifact = (
                            self.tracker._artifact_logging.apply_inherited_run_metadata(
                                table_artifact
                            )
                        )
                        table_artifact = (
                            self.tracker._artifact_logging.attach_logged_artifact(
                                artifact=table_artifact,
                                direction=direction,
                                register_with_coupler=False,
                            )
                        )
                        if prepared_bundle is not None:
                            synced = self.tracker.persistence.sync_artifact_with_facet_bundle(
                                table_artifact,
                                direction,
                                facet=prepared_bundle.facet,
                                meta_updates=prepared_bundle.meta_updates,
                                kv_rows=prepared_bundle.kv_rows,
                            )
                            if isinstance(synced, Artifact):
                                table_artifact = synced
                                self.tracker.current_consist.replace_artifact(
                                    table_artifact
                                )
                        else:
                            self.tracker.persistence.sync_artifact(
                                table_artifact,
                                direction,
                            )
                        table_artifacts.append(table_artifact)

                if reusing_cached_children:
                    return container, table_artifacts

            container = container.with_meta(
                table_count=len(table_artifacts),
                table_ids=[str(t.id) for t in table_artifacts],
            )
            self.tracker.current_consist.replace_artifact(container)
            if (
                direction == "output"
                and self.tracker._active_coupler is not None
                and container.key
            ):
                self.tracker._active_coupler.set(container.key, container)

            self.tracker.persistence.flush_json()
            self.tracker.persistence.sync_artifact(container, direction)

            return container, table_artifacts

    def log_h5_table(
        self,
        path: Union[str, Path],
        *,
        table_path: str,
        key: Optional[str] = None,
        direction: str = "output",
        parent: Optional[Artifact] = None,
        hash_table: bool = True,
        table_hash_chunk_rows: Optional[int] = None,
        profile_file_schema: bool | Literal["if_changed"] = False,
        file_schema_sample_rows: Optional[int] = None,
        description: Optional[str] = None,
        facet: Optional[FacetLike] = None,
        facet_schema_version: Optional[Union[str, int]] = None,
        facet_index: bool = False,
        **meta: Any,
    ) -> Artifact:
        """Log a single HDF5 table or dataset as an artifact."""
        if not self.tracker.current_consist:
            raise RuntimeError("Cannot log artifact outside of a run context.")

        path_obj = Path(path)
        if key is None:
            suffix = table_path.lstrip("/").replace("/", "_")
            key = f"{path_obj.stem}_{suffix}" if suffix else path_obj.stem

        table_digest: Optional[Digest] = None
        if hash_table:
            table_digest = compute_digest(
                path_obj,
                driver="h5_table",
                member=table_path,
                options={"chunk_rows": table_hash_chunk_rows},
            )

        table_meta: dict[str, Any] = {}
        if parent is not None:
            table_meta["parent_id"] = str(parent.id)
            parent_artifact_id = parent.id
        else:
            parent_artifact_id = None
        if table_digest is not None:
            table_meta["table_hash"] = table_digest.hex
            table_meta["table_hash_algo"] = "sha256"
            if table_hash_chunk_rows is not None:
                table_meta["table_hash_chunk_rows"] = table_hash_chunk_rows
        if description is not None:
            table_meta["description"] = description

        if meta:
            table_meta.update(meta)

        artifact = self.create_artifact(
            str(path_obj),
            run_id=(
                self.tracker.current_consist.run.id if direction == "output" else None
            ),
            key=key,
            direction=direction,
            driver="h5_table",
            member=table_path,
            digest=table_digest,
            skip_digest=not hash_table,
            parent_artifact_id=parent_artifact_id,
            **table_meta,
        )
        prepared_bundle = self.tracker._artifact_logging.prepare_artifact_facet_bundle(
            artifact=artifact,
            facet=facet,
            facet_schema_version=facet_schema_version,
            facet_index=facet_index,
        )
        artifact = self.tracker._artifact_logging.apply_inherited_run_metadata(artifact)
        artifact = self.tracker._artifact_logging.attach_logged_artifact(
            artifact=artifact,
            direction=direction,
            register_with_coupler=direction == "output",
        )

        self.tracker._flush_json()
        if prepared_bundle is not None:
            synced = self.tracker.persistence.sync_artifact_with_facet_bundle(
                artifact,
                direction,
                facet=prepared_bundle.facet,
                meta_updates=prepared_bundle.meta_updates,
                kv_rows=prepared_bundle.kv_rows,
            )
            if isinstance(synced, Artifact):
                artifact = synced
                self.tracker.current_consist.replace_artifact(artifact)
        else:
            self.tracker.persistence.sync_artifact(artifact, direction)

        if (
            profile_file_schema
            and artifact.is_tabular
            and self.tracker.current_consist is not None
        ):
            try:
                if artifact.meta.get("schema_id"):
                    return artifact
                resolved_path = artifact.abs_path or self.tracker.resolve_uri(
                    artifact.uri
                )
                if resolved_path:
                    updated = self.tracker.artifact_schemas.profile_file_artifact(
                        artifact=artifact,
                        run=self.tracker.current_consist.run,
                        resolved_path=str(resolved_path),
                        driver="h5_table",
                        sample_rows=(
                            self.tracker.settings.schema_sample_rows
                            if file_schema_sample_rows is None
                            else file_schema_sample_rows
                        ),
                        source="file",
                        reuse_if_unchanged=profile_file_schema == "if_changed",
                    )
                    if isinstance(updated, Artifact):
                        artifact = updated
                        self.tracker.current_consist.replace_artifact(artifact)
            except FileNotFoundError:
                logging.warning(
                    "[Consist] File schema capture skipped; file not found: %s",
                    artifact.uri,
                )

        if (
            direction == "output"
            and self.tracker._active_coupler is not None
            and artifact.key
        ):
            self.tracker._active_coupler.set(artifact.key, artifact)
        return artifact
